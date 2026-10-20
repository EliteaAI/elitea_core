"""Input guardrail (#6886): platform checkpoints A (chat pre-persist) and B (start_task).

The policy is read from ``this.descriptor.config['input_guardrail']`` on every call, so
an admin change applies on the next turn. Nothing here logs or returns input text: the
verdict carries rule ids and counts, and redacted text only ever holds placeholders.

Checkpoint C (the SDK middleware) skips every segment whose hash B lists in
``payload['input_guardrail_inspected']``, so one value produces one audit row.
"""

import json
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from tools import this  # pylint: disable=E0401

from ..exceptions import InputGuardrailBlockedError
from .input_detector import (
    ACTION_SEVERITY,
    Policy,
    Segment,
    effective_action,
    inspect,
    is_platform_context_block,
    policy_from_dict,
    segment_hash,
    summary,
)

CONFIG_KEY = 'input_guardrail'
INSPECTED_PAYLOAD_KEY = 'input_guardrail_inspected'
VERDICT_PAYLOAD_KEY = 'input_guardrail'
AUDIT_TRACER_NAME = 'audit-trail'
AUDIT_OBSERVATION_TYPE = 'guardrail'
# Shown in place of a value that could not be fully inspected (see redact_value)
UNCHECKED_PLACEHOLDER = '[REDACTED:unchecked]'
# Shorter than any text a rule can match (the shortest is a 19-char phone number)
_MIN_INSPECT_CHARS = 8
_POLICY_CACHE_SIZE = 8
# Keys of the summary that may reach the stored message, the payload and the UI
VERDICT_KEYS = ('action', 'categories', 'rules', 'segments', 'detection_count', 'not_inspected', 'error')


# Compiled policies by config content: the config is re-read on every call so an admin
# change applies at once, but allow_patterns are only recompiled when the content changes
_policy_cache: Dict[str, Policy] = {}


def policy_from_config() -> Policy:
    """The live policy. An unreadable block means disabled: it cannot express fail_closed."""
    try:
        raw = this.descriptor.config.get(CONFIG_KEY) or {}
        if not isinstance(raw, dict):
            log.error('[INPUT_GUARDRAIL] config is not a mapping; guardrail disabled')
            return Policy()
        if not raw:
            return Policy()
        key = json.dumps(raw, sort_keys=True, default=str)
        policy = _policy_cache.get(key)
        if policy is None:
            policy = policy_from_dict(raw)
            if len(_policy_cache) >= _POLICY_CACHE_SIZE:
                _policy_cache.clear()
            _policy_cache[key] = policy
        return policy
    except Exception:  # pylint: disable=W0703
        log.error('[INPUT_GUARDRAIL] config unreadable; guardrail disabled')
        return Policy()


def _applies(policy: Policy, project_id) -> bool:
    if not policy.active:
        return False
    try:
        return int(project_id) not in policy.exempt_project_ids
    except (TypeError, ValueError):
        return True


def verdict_meta(result_summary: Dict[str, Any]) -> Dict[str, Any]:
    return {key: result_summary.get(key) for key in VERDICT_KEYS}


# --- text helpers -----------------------------------------------------------------


class _Slot:
    """One inspectable text inside a payload value, with a setter for the redacted copy."""

    __slots__ = ('kind', 'text', 'write')

    def __init__(self, kind: str, text: str, write):
        self.kind = kind
        self.text = text
        self.write = write


def _content_slots(kind: str, content, not_inspected: List[str], write_back) -> List[_Slot]:
    """Slots for a str or a list of content blocks; non-text blocks are counted, not read.

    The platform's own ``<runtime_context>`` block is not user input, but only that exact
    block, written as a separate text block after the first one, is skipped: a string
    that merely starts with the tag is inspected.
    """
    if isinstance(content, str):
        return [_Slot(kind, content, write_back)]
    slots = []
    if not isinstance(content, list):
        return slots
    for i, block in enumerate(content):
        if isinstance(block, str):
            slots.append(_Slot(kind, block, lambda text, i=i: content.__setitem__(i, text)))
        elif isinstance(block, dict) and block.get('type') == 'text':
            text = block.get('text') or ''
            if i > 0 and is_platform_context_block(text):
                continue
            slots.append(_Slot(kind, text, lambda text, b=block: b.__setitem__('text', text)))
        elif isinstance(block, dict):
            not_inspected.append(str(block.get('type') or 'unknown'))
    return slots


def _payload_slots(payload: Dict[str, Any], not_inspected: List[str], kind: str = 'current_turn') -> List[_Slot]:
    slots = _content_slots(
        kind, payload.get('user_input'), not_inspected,
        lambda text: payload.__setitem__('user_input', text),
    )
    for message in payload.get('chat_history') or []:
        # User turns only: assistant output is outside input-guardrail scope
        if isinstance(message, dict) and message.get('role') in ('user', 'human'):
            slots.extend(_content_slots(
                'history', message.get('content'), not_inspected,
                lambda text, m=message: m.__setitem__('content', text),
            ))
    application = payload.get('application') or {}
    version_details = application.get('version_details') or {}
    for owner in (application, version_details, payload):
        if isinstance(owner, dict) and isinstance(owner.get('instructions'), str):
            slots.append(_Slot(
                'instructions', owner['instructions'], lambda text, o=owner: o.__setitem__('instructions', text),
            ))
    return [s for s in slots if s.text]


def _segments(slots: List[_Slot]) -> List[Segment]:
    counters: Dict[str, int] = {}
    segments = []
    for slot in slots:
        index = counters.get(slot.kind, 0)
        counters[slot.kind] = index + 1
        segments.append(Segment(slot.kind, index, slot.text))
    return segments


def _strict_policy(policy: Policy) -> Policy:
    """The live policy with every category not off set to redact."""
    return Policy(
        enabled=True,
        on_error=policy.on_error,
        timeout_ms=policy.timeout_ms,
        max_inspect_chars=policy.max_inspect_chars,
        categories={c: ('redact' if a != 'off' else 'off') for c, a in policy.categories.items()},
        allow_patterns=policy.allow_patterns,
    )


def redact_value(text: str) -> Optional[str]:
    """The text with every detected value replaced, or None if it could not be fully inspected.

    For copies that are never meant to hold a value, where the caller must not keep the
    input when the check failed (tracing span payloads). None means the scan hit the
    size budget, the deadline or an internal error: mask the whole value. A text that
    needs no inspection (not a string, too short, guardrail off) is returned unchanged.
    Never raises, and nothing it returns or logs carries a detected value.
    """
    if not isinstance(text, str) or len(text) < _MIN_INSPECT_CHARS:
        return text
    try:
        policy = policy_from_config()
        if not policy.active:
            return text
        result = inspect([Segment('current_turn', 0, text)], _strict_policy(policy))
    except Exception:  # pylint: disable=W0703
        return None
    if result.error:
        return None
    return result.redacted.get(('current_turn', 0), text)


def redact_text(text: str) -> str:
    """Every detected value replaced, whatever the configured action.

    Fails to the input unchanged when the text could not be fully inspected; callers
    that cannot accept that use ``redact_value`` and mask the whole value on None.
    """
    redacted = redact_value(text)
    return text if redacted is None else redacted


def _redact_or_mask(text: str) -> str:
    """For stored and previewed copies: the redacted text, or a placeholder for the whole
    value when it could not be inspected (a copy that keeps a value is the worse failure)."""
    redacted = redact_value(text)
    return UNCHECKED_PLACEHOLDER if redacted is None else redacted


def redacted_input(user_input):
    """A copy of a chat input with every detected value replaced: the stored copy of a
    blocked turn (plan D5). Text blocks of a list are rewritten in a new list. A text
    that could not be inspected is replaced whole."""
    if isinstance(user_input, list):
        return [
            {**block, 'text': _redact_or_mask(block.get('text') or '')}
            if isinstance(block, dict) and block.get('type') == 'text'
            else (_redact_or_mask(block) if isinstance(block, str) else block)
            for block in user_input
        ]
    return _redact_or_mask(user_input)


# --- audit ------------------------------------------------------------------------


def emit_audit_spans(
    entries: Iterable[Dict[str, Any]],
    user_id: Optional[int] = None,
    project_id: Optional[int] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    entity_name: Optional[str] = None,
) -> int:
    """One zero-length ``audit-trail`` span per entry; the tracing plugin writes the row.

    ``entity_*`` name the agent the input was sent to (``entity.type`` 'application' or
    'datasource', as tracing's own spans carry); the tracing plugin reads them for the row.

    Twin of indexer_worker's utils/input_guardrail.py emit_audit_spans. Never raises.
    """
    entries = [e for e in entries or [] if isinstance(e, dict) and e.get('action')]
    if not entries:
        return 0
    try:
        from opentelemetry import trace  # pylint: disable=C0415
        tracer = trace.get_tracer(AUDIT_TRACER_NAME)
    except Exception:  # pylint: disable=W0703
        return 0
    base = {'audit.observation.type': AUDIT_OBSERVATION_TYPE}
    for key, value in (('user.id', user_id), ('project.id', project_id)):
        try:
            if value is not None:
                base[key] = int(value)
        except (TypeError, ValueError):
            pass
    if entity_type:
        base['entity.type'] = str(entity_type)[:32]
        try:
            if entity_id is not None:
                base['entity.id'] = int(entity_id)
        except (TypeError, ValueError):
            pass
        if entity_name:
            base['entity.name'] = str(entity_name)[:256]
    emitted = 0
    for entry in entries:
        try:
            attrs = {
                **base,
                'audit.guardrail.action': str(entry['action'])[:512],
                'audit.is_error': bool(entry.get('is_error')),
            }
            if entry.get('segment_kind'):
                attrs['audit.tool.name'] = str(entry['segment_kind'])
            tracer.start_span(name=attrs['audit.guardrail.action'], attributes=attrs).end()
            emitted += 1
        except Exception as e:  # pylint: disable=W0703
            log.debug(f'[INPUT_GUARDRAIL] Failed to emit audit span: {e}')
    return emitted


def _payload_entity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """The agent a start_task payload runs, as ``emit_audit_spans`` entity kwargs."""
    application = payload.get('application')
    if not isinstance(application, dict) or not (application.get('id') or application.get('name')):
        return {}
    return {
        'entity_type': 'application',
        'entity_id': application.get('id'),
        'entity_name': application.get('name'),
    }


def participant_entity(participant) -> Dict[str, Any]:
    """The agent a chat participant row stands for, as ``emit_audit_spans`` entity kwargs;
    empty for a user, a model or anything that is not an application."""
    if participant is None or str(getattr(participant, 'entity_name', '')) != 'application':
        return {}
    meta = getattr(participant, 'entity_meta', None) or {}
    if not isinstance(meta, dict) or meta.get('id') is None:
        return {}
    return {'entity_type': 'application', 'entity_id': meta.get('id'), 'entity_name': meta.get('name')}


def _audit_entries(result_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    # A record/warn hit in history was audited on the turn it was sent and is stored
    # as-is; repeating it on every later turn would multiply the row
    return [
        e for e in result_summary.get('audit') or []
        if not (e.get('segment_kind') == 'history' and str(e.get('action', '')).rsplit(':', 1)[-1] in ('record', 'warn'))
    ]


def accepted_history_hashes(message_groups) -> List[str]:
    """Hashes of stored user turns checkpoint A let through on record or warn.

    Such a turn is stored as sent, and its verdict is on the group's meta. Listing it
    in ``input_guardrail_inspected`` keeps it from blocking every later turn once it
    is history. A turn stored while the guardrail was off has no verdict and is still
    inspected.
    """
    hashes: Set[str] = set()
    for group in message_groups or []:
        verdict = (getattr(group, 'meta', None) or {}).get(VERDICT_PAYLOAD_KEY)
        if not isinstance(verdict, dict) or verdict.get('action') not in ('record', 'warn'):
            continue
        for item in getattr(group, 'message_items', None) or []:
            content = getattr(item, 'content', None)
            if isinstance(content, str) and content:
                hashes.add(segment_hash(content))
    return sorted(hashes)


# --- checkpoints ------------------------------------------------------------------


def _blocked(result_summary: Dict[str, Any]) -> InputGuardrailBlockedError:
    return InputGuardrailBlockedError(
        categories=result_summary.get('categories') or ['error'],
        rules=result_summary.get('rules') or [],
        segments=result_summary.get('segments') or [],
    )


def _run(
    slots: List[_Slot], policy: Policy, not_inspected: List[str], user_id, project_id, audit_on=None,
    entity: Optional[Dict[str, Any]] = None,
):
    """Inspect, audit and write redactions back. Returns (summary, action).

    ``audit_on`` limits auditing to those actions; None audits every hit.
    """
    segments = _segments(slots)
    result = inspect(segments, policy, not_inspected)
    result_summary = summary(result, policy)
    action = effective_action(result, policy)
    if audit_on is None or action in audit_on:
        emit_audit_spans(_audit_entries(result_summary), user_id=user_id, project_id=project_id, **(entity or {}))
    if action != 'block':
        for slot, segment in zip(slots, segments):
            new_text = result.redacted.get((segment.kind, segment.index))
            if new_text is not None:
                slot.write(new_text)
                slot.text = new_text
    return result_summary, action


def check_chat_input(
    user_input, project_id, user_id=None, kind: str = 'current_turn',
    entity: Optional[Dict[str, Any]] = None,
) -> Tuple[Any, Optional[Dict[str, Any]]]:
    """Checkpoint A: inspect a chat message before it is stored or broadcast.

    Returns ``(user_input, verdict)``: the input with redactions applied (a str, or the
    same block list rewritten in place) and the verdict, or None when nothing was found.
    Raises InputGuardrailBlockedError on block.

    The chat payload is rebuilt before dispatch, so checkpoint B inspects this turn
    again. Only block and redact are audited here: a blocked turn never reaches B, and
    a redacted one reaches it as a placeholder that matches nothing. A record or warn
    hit is audited once, by B.

    ``entity`` names the agent the message is for, as ``emit_audit_spans`` entity kwargs
    (``entity_type``, ``entity_id``, ``entity_name``); without it the audit row has no agent.
    """
    policy = policy_from_config()
    if not _applies(policy, project_id):
        return user_input, None
    holder = {'user_input': user_input}
    not_inspected: List[str] = []
    try:
        slots = _payload_slots(holder, not_inspected, kind)
        if not slots:
            return user_input, None
        result_summary, action = _run(
            slots, policy, not_inspected, user_id, project_id, audit_on=('block', 'redact'), entity=entity,
        )
    except Exception:  # pylint: disable=W0703
        log.warning('[INPUT_GUARDRAIL] chat check failed (on_error=%s)', policy.on_error)
        if policy.on_error == 'fail_closed':
            raise InputGuardrailBlockedError(categories=['error'], rules=[], segments=[])
        return user_input, None
    if action == 'block':
        raise _blocked(result_summary)
    if not result_summary['detection_count']:
        return holder['user_input'], None
    return holder['user_input'], verdict_meta(result_summary)


def check_resume_input(
    parsed, project_id, user_id=None, entity: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Checkpoint A for a resume: the replacement input and HITL edit/answer text.

    Rewrites the redacted values on ``parsed`` in place and returns the merged verdict,
    or None. Raises InputGuardrailBlockedError on block.
    """
    verdict = None

    def check(value, kind):
        nonlocal verdict
        if not isinstance(value, str) or not value:
            return value
        value, found = check_chat_input(value, project_id, user_id, kind=kind, entity=entity)
        if found:
            verdict = _merge(verdict, found)
        return value

    def check_value(value):
        if isinstance(value, dict):
            return {k: check(v, 'hitl_edit') for k, v in value.items()}
        return check(value, 'hitl_edit')

    hitl_value = getattr(parsed, 'hitl_value', None)
    decisions = getattr(parsed, 'hitl_decisions', None) or []
    # On a HITL resume the input carries the reviewer's edit, not a new turn
    hitl = hitl_value is not None or bool(decisions)
    user_input = getattr(parsed, 'user_input', None)
    if user_input and not (hitl and user_input == hitl_value):
        parsed.user_input = check(user_input, 'hitl_edit' if hitl else 'current_turn')
    elif user_input:
        parsed.user_input = None  # set below from the checked hitl_value
    if hitl_value is not None:
        parsed.hitl_value = check_value(parsed.hitl_value)
    if user_input and parsed.user_input is None:
        parsed.user_input = parsed.hitl_value
    for decision in decisions:
        if isinstance(decision, dict) and decision.get('value') is not None:
            decision['value'] = check_value(decision['value'])
    return verdict


def apply_input_guardrail(call_kwargs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Checkpoint B: inspect a start_task payload in place.

    Inspects user_input, user turns of chat_history and instructions; redacts them on
    redact, raises InputGuardrailBlockedError on block, always masks the Active Tasks
    preview, and lists the hashes of what it inspected for the SDK to skip. A segment
    whose hash the caller already lists (checkpoint A) is not inspected again.
    """
    payload = call_kwargs.get('kwargs')
    meta = call_kwargs.get('meta')
    if not isinstance(payload, dict):
        return None
    meta = meta if isinstance(meta, dict) else {}
    project_id = meta.get('project_id') or payload.get('project_id')
    user_id = meta.get('user_id') or payload.get('user_id')
    policy = policy_from_config()
    if not _applies(policy, project_id):
        return None
    # The preview is set from the raw input before dispatch; it never keeps a value
    if isinstance(meta.get('user_input_preview'), str):
        meta['user_input_preview'] = _redact_or_mask(meta['user_input_preview'])
    already: Set[str] = {h for h in payload.get(INSPECTED_PAYLOAD_KEY) or [] if isinstance(h, str)}
    not_inspected: List[str] = []
    try:
        slots = _payload_slots(payload, not_inspected)
        todo = [s for s in slots if segment_hash(s.text) not in already]
        result_summary, action = _run(
            todo, policy, not_inspected, user_id, project_id, entity=_payload_entity(payload),
        ) if todo else (None, 'none')
    except Exception:  # pylint: disable=W0703
        log.warning('[INPUT_GUARDRAIL] dispatch check failed (on_error=%s)', policy.on_error)
        if policy.on_error == 'fail_closed':
            raise InputGuardrailBlockedError(categories=['error'], rules=[], segments=[])
        return None
    if action == 'block':
        raise _blocked(result_summary)
    payload[INSPECTED_PAYLOAD_KEY] = sorted(already | {segment_hash(s.text) for s in slots})
    if result_summary and (result_summary['detection_count'] or result_summary['error']):
        verdict = verdict_meta(result_summary)
        payload[VERDICT_PAYLOAD_KEY] = _merge(payload.get(VERDICT_PAYLOAD_KEY), verdict)
        return verdict
    return None


def _merge(current: Optional[Dict[str, Any]], incoming: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(current, dict):
        return incoming
    merged = dict(current)
    if ACTION_SEVERITY.get(incoming.get('action') or 'none', 0) > ACTION_SEVERITY.get(current.get('action') or 'none', 0):
        merged['action'] = incoming['action']
    for key in ('categories', 'rules', 'segments', 'not_inspected'):
        merged[key] = sorted(set(current.get(key) or []) | set(incoming.get(key) or []))
    merged['detection_count'] = int(current.get('detection_count') or 0) + int(incoming.get('detection_count') or 0)
    merged['error'] = current.get('error') or incoming.get('error')
    return merged


__all__ = [
    'CONFIG_KEY', 'INSPECTED_PAYLOAD_KEY', 'VERDICT_PAYLOAD_KEY', 'VERDICT_KEYS',
    'apply_input_guardrail', 'check_chat_input', 'check_resume_input', 'emit_audit_spans', 'policy_from_config',
    'participant_entity', 'redact_text', 'redact_value', 'redacted_input', 'verdict_meta', 'UNCHECKED_PLACEHOLDER',
]
