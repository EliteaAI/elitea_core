"""Read side of one schedule run for the Test schedule panel (EL-7011, owner CR 2026-10-21).

The run writers store the run's own events. Tool calls are not copied into them: the chat stream keeps
them as trace step rows of the run conversation, and rewrites those rows on every partial save. So they are
read here, at detail time, from the same rows the conversation view shows; the log and the chat agree.
"""
from datetime import UTC, datetime, timedelta

from pylon.core.tools import log
from sqlalchemy import case, column, func, null, select, table

from ..models.all import ApplicationVersion
from ..models.conversation import Conversation
from ..models.message_group import ConversationMessageGroup
from ..models.message_trace_step import MessageTraceStep
from .schedule_claim import EVENT_LIMIT, TEXT_LIMIT

SUMMARY_LIMIT = 120  # characters of a tool result in its log line
NANO_USD = 10 ** 9
# usage_event rows of a run fall inside the run; ts is the partition key, so the bounds keep the scan short
USAGE_MARGIN = timedelta(minutes=5)
# The usage plugin's table (central schema); read-only, only the columns used here
USAGE_EVENT = table(
    'usage_event', column('project_id'), column('conversation_id'), column('ts'), column('event_type'),
    column('input_tokens'), column('output_tokens'), column('cost_nano_usd'),
)


def version_label(session, version_id: int | None) -> str | None:
    """The name of the version a run started; None when the version is gone."""
    if version_id is None:
        return None
    return session.scalar(select(ApplicationVersion.name).where(ApplicationVersion.id == version_id))


def _utc_iso(moment: datetime | None) -> str | None:
    if moment is None:
        return None
    return (moment if moment.tzinfo else moment.replace(tzinfo=UTC)).astimezone(UTC).isoformat()


def _toolkit(attrs) -> str | None:
    attrs = attrs if isinstance(attrs, dict) else {}
    for source in (attrs.get('metadata'), (attrs.get('tool_meta') or {}).get('metadata')):
        if isinstance(source, dict) and source.get('toolkit_name'):
            return source['toolkit_name']
    return None


def _tool_event(row) -> dict:
    """One tool call line: toolkit, tool, the start of its result on one line, and its duration."""
    summary = ' '.join((row.output or '').split())[:SUMMARY_LIMIT] or None
    duration_ms = (round((row.finished_at - row.started_at).total_seconds() * 1000)
                   if row.started_at and row.finished_at else None)
    toolkit, tool = _toolkit(row.attrs), row.tool_name or '?'
    text = f"Tool call: {f'{toolkit} · ' if toolkit else ''}{tool}"
    if duration_ms is not None:
        text += f' ({duration_ms} ms)'
    if summary:
        text += f': {summary}'
    event = {'at': _utc_iso(row.started_at or row.finished_at), 'kind': 'tool_call', 'text': text[:TEXT_LIMIT],
             'level': 'error' if row.is_error else 'info', 'toolkit': toolkit, 'tool': tool,
             'summary': summary, 'duration_ms': duration_ms}
    return {key: value for key, value in event.items() if value is not None or key == 'at'}


def tool_call_events(session, conversation_id: int | None, with_results: bool) -> list[dict]:
    """The tool calls of the run conversation as log events, oldest first, at most `EVENT_LIMIT`.

    `with_results` False leaves out the result text (no `summary`, no result in `text`): a caller who is not
    the owner sees toolkit, tool and duration only (owner decision 2026-10-21).
    """
    if conversation_id is None:
        return []
    # Only the start of a tool result is read; a large output, or any output for a non-owner, stays in the database
    output = func.substr(MessageTraceStep.tool_output, 1, SUMMARY_LIMIT * 4) if with_results else null()
    rows = session.execute(
        select(
            MessageTraceStep.started_at, MessageTraceStep.finished_at, MessageTraceStep.tool_name,
            MessageTraceStep.is_error, MessageTraceStep.attrs, output.label('output'),
        )
        .join(ConversationMessageGroup, ConversationMessageGroup.id == MessageTraceStep.message_group_id)
        .where(ConversationMessageGroup.conversation_id == conversation_id, MessageTraceStep.kind == 'tool_call')
        .order_by(MessageTraceStep.started_at, MessageTraceStep.id)
        .limit(EVENT_LIMIT)
    ).all()
    return [_tool_event(row) for row in rows]


def usage_summary(session, project_id: int, run) -> tuple[int | None, float | None]:
    """(tokens, cost in USD) of the run conversation from the usage store; (None, None) when it has no row.

    Tokens are the input and output tokens of the model calls; the cost sums every priced row.
    """
    # lean: an unpriced model call counts as 0 USD; mark the cost partial when the panel needs it
    if run.conversation_id is None:
        return None, None
    conversation_uuid = session.scalar(select(Conversation.uuid).where(Conversation.id == run.conversation_id))
    if conversation_uuid is None:
        return None, None
    usage = USAGE_EVENT.c
    where = [usage.project_id == project_id, usage.conversation_id == str(conversation_uuid),
             usage.ts >= run.started_at - USAGE_MARGIN]
    if run.finished_at:
        where.append(usage.ts <= run.finished_at + USAGE_MARGIN)
    statement = select(
        func.count(),
        func.sum(case((usage.event_type == 'llm', usage.input_tokens + usage.output_tokens), else_=0)),
        func.sum(usage.cost_nano_usd),
    ).where(*where)
    try:
        # Late: the usage table is in the central schema, outside the project session
        from tools import db
        with db.engine.connect() as connection:
            count, tokens, cost_nano_usd = connection.execute(statement).one()
    except Exception:  # noqa: BLE001 -- the panel shows the run without the usage figures
        log.exception('Schedule run %s: cannot read the usage of its conversation', run.id)
        return None, None
    if not count:
        return None, None
    return int(tokens or 0), round((cost_nano_usd or 0) / NANO_USD, 6)
