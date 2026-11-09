"""Live text of a running chat answer, kept in Redis so a user who comes back mid-run sees it (EL-7385).

Answer and thinking text arrive as ``agent_llm_chunk`` deltas and are written to the database only when
an LLM step ends. Frames sent while nobody is in the chat room are dropped. This module keeps the text
streamed so far per LLM step (``tool_run_id``) of a response message, so ``chat_enter_room`` can hand it
to a client that joins mid-run.

Every pylon_main replica receives every event-node frame, so the append is one atomic script keyed by a
frame fingerprint: the first replica appends, the others get the same ``seq`` back without appending
again. ``seq`` counts appends per step. The client skips a live chunk whose ``snapshot_seq`` is not above
the snapshot it already applied, so a chunk that is both in the snapshot and live is shown once.
"""
import hashlib
import json

from .chat_constants import RUN_SNAPSHOT_FRAME_TTL, RUN_SNAPSHOT_MAX_CHARS, RUN_SNAPSHOT_TTL

SNAPSHOT_KEY_PREFIX = 'chat_run_text:'
FRAME_KEY_PREFIX = 'chat_run_chunk_seen:'

# KEYS[1] snapshot hash, KEYS[2] frame marker
# ARGV: tool_run_id, content, thinking, snapshot ttl, frame ttl, max chars, metadata json (kept from the first chunk)
APPEND_SCRIPT = """
local seen = redis.call('GET', KEYS[2])
if seen then
  return tonumber(seen)
end
local raw = redis.call('HGET', KEYS[1], ARGV[1])
local entry
if raw then
  entry = cjson.decode(raw)
else
  entry = {content = '', thinking = '', seq = 0, truncated = false, metadata = cjson.decode(ARGV[7])}
end
local max_chars = tonumber(ARGV[6])
if string.len(entry.content) + string.len(entry.thinking) + string.len(ARGV[2]) + string.len(ARGV[3]) > max_chars then
  entry.truncated = true
else
  entry.content = entry.content .. ARGV[2]
  entry.thinking = entry.thinking .. ARGV[3]
end
entry.seq = entry.seq + 1
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(entry))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
redis.call('SET', KEYS[2], entry.seq, 'EX', tonumber(ARGV[5]))
return entry.seq
"""


def snapshot_key(message_id: str) -> str:
    return f'{SNAPSHOT_KEY_PREFIX}{message_id}'


def frame_fingerprint(tool_run_id: str, created_at, content: str, thinking: str) -> str:
    """Same on every replica for the same frame; differs for two frames with equal text."""
    raw = json.dumps([tool_run_id, str(created_at or ''), content, thinking], ensure_ascii=False)
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()


def frame_key(message_id: str, fingerprint: str) -> str:
    return f'{FRAME_KEY_PREFIX}{message_id}:{fingerprint}'


def _chunk_text(value) -> str:
    if value is None:
        return ''
    if isinstance(value, str):
        return value
    # A provider may hand a content block list; the client renders it as JSON text too
    return json.dumps(value, ensure_ascii=False)


def chunk_metadata(payload: dict) -> dict:
    """Where the step sits (node name, parent agent), so a seeded chip renders where live chunks put it."""
    metadata = (payload.get('response_metadata') or {}).get('metadata')
    return metadata if isinstance(metadata, dict) else {}


def chunk_fields(payload: dict) -> tuple[str, str, str] | None:
    """(tool_run_id, content, thinking) of an agent_llm_chunk frame, or None when there is nothing to keep."""
    response_metadata = payload.get('response_metadata') or {}
    tool_run_id = response_metadata.get('tool_run_id')
    if not tool_run_id:
        return None
    content = _chunk_text(payload.get('content'))
    thinking = _chunk_text(payload.get('thinking'))
    if not content and not thinking:
        return None
    return str(tool_run_id), content, thinking


def append_chunk(client, message_id: str, payload: dict) -> int | None:
    """Add one agent_llm_chunk frame to the snapshot; returns its seq, or None when nothing was kept."""
    fields = chunk_fields(payload)
    if not message_id or fields is None:
        return None
    tool_run_id, content, thinking = fields
    fingerprint = frame_fingerprint(tool_run_id, payload.get('created_at'), content, thinking)
    script = client.register_script(APPEND_SCRIPT)
    seq = script(
        keys=[snapshot_key(message_id), frame_key(message_id, fingerprint)],
        args=[
            tool_run_id, content, thinking, RUN_SNAPSHOT_TTL, RUN_SNAPSHOT_FRAME_TTL, RUN_SNAPSHOT_MAX_CHARS,
            json.dumps(chunk_metadata(payload), ensure_ascii=False, default=str),
        ],
    )
    return int(seq) if seq is not None else None


def drop_runs(client, message_id: str, tool_run_ids) -> None:
    """Forget finished LLM steps: their text is now in the persisted thinking steps."""
    ids = [str(i) for i in tool_run_ids or [] if i]
    if message_id and ids:
        client.hdel(snapshot_key(message_id), *ids)


def drop_snapshot(client, message_id: str) -> None:
    if message_id:
        client.delete(snapshot_key(message_id))


def read_snapshot(client, message_id: str) -> dict:
    """{tool_run_id: {content, thinking, seq, truncated, metadata}} of a message; empty when nothing is kept."""
    if not message_id:
        return {}
    raw = client.hgetall(snapshot_key(message_id)) or {}
    result = {}
    for tool_run_id, value in raw.items():
        if isinstance(tool_run_id, bytes):
            tool_run_id = tool_run_id.decode('utf-8')
        try:
            entry = json.loads(value)
        except (TypeError, ValueError):
            continue
        if not isinstance(entry, dict):
            continue
        metadata = entry.get('metadata')
        result[tool_run_id] = {
            'content': entry.get('content') or '',
            'thinking': entry.get('thinking') or '',
            'seq': int(entry.get('seq') or 0),
            'truncated': bool(entry.get('truncated')),
            # cjson turns an empty object into an empty array
            'metadata': metadata if isinstance(metadata, dict) else {},
        }
    return result


def streaming_groups(session, conversation_id: int) -> list[tuple[str, str | None]]:
    """(response uuid, question uuid) of the conversation's live runs; usually 0 or 1."""
    from sqlalchemy.orm import aliased  # pylint: disable=C0415
    from ..models.message_group import ConversationMessageGroup  # pylint: disable=C0415 -- keeps helpers DB-free
    from .running_conversations import is_live_run  # pylint: disable=C0415

    question = aliased(ConversationMessageGroup)
    rows = session.query(ConversationMessageGroup.uuid, question.uuid).outerjoin(
        question, question.id == ConversationMessageGroup.reply_to_id,
    ).filter(
        ConversationMessageGroup.conversation_id == conversation_id,
        is_live_run(),
    ).all()
    return [(str(uuid), str(question_uuid) if question_uuid else None) for uuid, question_uuid in rows]


def snapshot_frames(client, conversation_uuid, groups) -> list[dict]:
    """``chat_run_snapshot`` frames for the streaming groups that have kept text."""
    frames = []
    for message_id, question_id in groups:
        tool_runs = read_snapshot(client, message_id)
        if not tool_runs:
            continue
        frames.append({
            'type': 'chat_run_snapshot',
            'stream_id': str(conversation_uuid),
            'message_id': message_id,
            'question_id': question_id,
            'response_metadata': {'tool_runs': tool_runs},
        })
    return frames


def finished_run_ids(payload: dict) -> list[str]:
    """tool_run_ids an agent_llm_end frame closes."""
    response_metadata = payload.get('response_metadata') or {}
    ids = []
    if response_metadata.get('tool_run_id'):
        ids.append(str(response_metadata['tool_run_id']))
    for step in response_metadata.get('thinking_steps') or []:
        if isinstance(step, dict) and step.get('tool_run_id'):
            ids.append(str(step['tool_run_id']))
    return list(dict.fromkeys(ids))
