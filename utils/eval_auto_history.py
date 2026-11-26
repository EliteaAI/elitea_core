"""Which stored chat turns the scheduled evaluation scores in history mode, as pure functions.

History mode scores an agent from what its users already asked it, without running it. The DB
loader (``eval_auto_history_loader``) reads the agent's turns in the window into plain dicts; this
module decides which of them are eligible, whether the agent is Not Evaluated from what the turns
show, and which turns make up the sample.

A turn is one user group and the agent groups that answer it::

    {'id': <user group id>, 'conversation_id', 'user_id', 'is_private', 'version_id',
     'created_at': datetime, 'input': str, 'output': str | None, 'group_ids': [agent group ids],
     'user_meta': dict, 'agent_metas': [dict], 'streaming': bool, 'latency_ms': int | None}

The loader reads in two steps so a busy agent costs only its sample's text: the turns first come
with ``input_kind`` (``TEXT_PRESENT`` / ``TEXT_PLACEHOLDER`` / blank) and ``has_output`` instead of
``input`` and ``output``, enough to classify and sample them, then :func:`attach_texts` fills the
text of the sampled turns only.

Stdlib only, no plugin imports (same convention as ``eval_auto_rules``); the reason codes are
literals that a unit test holds equal to the rules module's.
"""

import hashlib
import math
from typing import Dict, List, Optional, Tuple

HITL_REQUIRED = 'HITL_REQUIRED'
OAUTH_REQUIRED = 'OAUTH_REQUIRED'
AGENT_BROKEN = 'AGENT_BROKEN'
NO_TRAFFIC = 'NO_TRAFFIC'

STRATEGY_STRATIFIED = 'stratified'
STRATEGY_RECENT = 'recent'

#: What the schedule tick stores as the user's message (``schedule_run_starter.ENTRY_POINT_MESSAGE``).
SCHEDULED_PLACEHOLDER = '[Scheduled execution triggered]'
#: Conversations that are not real use of the agent: evaluation runs and toolkit test chats.
EXCLUDED_SOURCES = ('evaluation', 'toolkit')
#: Newest groups read per agent before sampling; bounds the read on a busy agent.
MAX_SCAN_GROUPS = 2000
#: Characters kept of a sampled turn's input and of its output; longer text is cut with ``TRUNCATED_MARK``.
MAX_TURN_TEXT_CHARS = 20000
TRUNCATED_MARK = '\n[…truncated]'

#: What the first read knows of a group's text: some non-blank text, or exactly the scheduled placeholder.
TEXT_PRESENT = 'text'
TEXT_PLACEHOLDER = 'placeholder'

OK = 'ok'
SKIP = 'skip'
ERROR = 'error'
HITL_OPEN = 'hitl_open'
OAUTH_OPEN = 'oauth_open'
GUARDRAIL = 'guardrail'
EXCLUSIONS = (ERROR, HITL_OPEN, OAUTH_OPEN, GUARDRAIL)

# The outcomes that classify the agent, in the order they are checked.
_OBSERVED = (
    (HITL_OPEN, HITL_REQUIRED, 'waited for a person who never answered'),
    (OAUTH_OPEN, OAUTH_REQUIRED, 'waited for a sign-in that never happened'),
    (ERROR, AGENT_BROKEN, 'failed with an error or gave no answer'),
)


def is_evaluation_conversation(conversation: dict) -> bool:
    """Was this conversation made by an evaluation (or a toolkit test), not by a user?"""
    meta = conversation.get('meta') if isinstance(conversation.get('meta'), dict) else {}
    return bool(meta.get('evaluation_run_id')) or conversation.get('source') in EXCLUDED_SOURCES


def _interrupt_identity(item: dict) -> str:
    # parallel_hitl.interrupt_identity
    if item.get('interrupt_id'):
        return item['interrupt_id']
    thread_id = item.get('child_thread_id') or item.get('thread_id')
    tool_call_id = item.get('tool_call_id')
    return f'{thread_id}:{tool_call_id}' if thread_id or tool_call_id else ''


def pending_hitl(meta: Optional[dict]) -> List[dict]:
    """The approvals an agent group still waits on (``parallel_hitl.pending_interrupts``, less
    the ones a reply already resolved)."""
    if not isinstance(meta, dict):
        return []
    plural = meta.get('hitl_interrupts')
    if isinstance(plural, list) and plural:
        items = [item for item in plural if isinstance(item, dict)]
    elif isinstance(meta.get('hitl_interrupt'), dict):
        items = [meta['hitl_interrupt']]
    else:
        items = []
    resolved = set(meta.get('resolved_hitl_interrupt_ids') or [])
    return [item for item in items if _interrupt_identity(item) not in resolved]


def pending_oauth(meta: Optional[dict]) -> List[dict]:
    """The sign-ins an agent group still waits on (``toolkit_authorization``)."""
    if not isinstance(meta, dict):
        return []
    resolved = {str(value) for value in meta.get('resolved_authorization_request_ids') or []}
    pending = []
    for item in meta.get('authorization_requests') or []:
        if not isinstance(item, dict):
            continue
        identity = str(item.get('interrupt_id') or item.get('tool_run_id') or item.get('tool_call_id') or '')
        if identity not in resolved:
            pending.append(item)
    return pending


def _blocked(meta) -> bool:
    guardrail = meta.get('input_guardrail') if isinstance(meta, dict) else None
    return isinstance(guardrail, dict) and guardrail.get('action') == 'block'


def turn_outcome(turn: dict) -> str:
    """What one turn counts as: ``ok`` (scorable), ``skip`` (not a use of the agent), or the
    exclusion it falls under.

    A resolved approval or sign-in is ``ok``: a person completed the turn, which is what history
    mode can score that a live run cannot.
    """
    if 'input_kind' in turn:
        input_kind, has_output = turn['input_kind'], turn.get('has_output')
    else:
        text = (turn.get('input') or '').strip()
        input_kind = TEXT_PLACEHOLDER if text == SCHEDULED_PLACEHOLDER else (TEXT_PRESENT if text else None)
        has_output = bool((turn.get('output') or '').strip())
    if turn.get('streaming') or input_kind != TEXT_PRESENT:
        return SKIP
    agent_metas = [meta for meta in turn.get('agent_metas') or [] if isinstance(meta, dict)]
    # The guardrail stops the input before the agent sees it; the mark is on either side.
    if _blocked(turn.get('user_meta')) or any(_blocked(meta) for meta in agent_metas):
        return GUARDRAIL
    if any(pending_hitl(meta) for meta in agent_metas):
        return HITL_OPEN
    if any(pending_oauth(meta) for meta in agent_metas):
        return OAUTH_OPEN
    if not agent_metas or any(meta.get('is_error') for meta in agent_metas) or not has_output:
        return ERROR
    return OK


def observed_reason(counts: Dict[str, int], threshold: float) -> Optional[Tuple[str, str]]:
    """``(reason_code, detail)`` when enough of the turns show the agent could not do the job.

    The share is over the turns that reached the agent, before sampling, so a broken agent is not
    hidden by scoring only its good turns. A guardrail block never reached it.
    """
    reached = sum(counts.get(key, 0) for key in (OK, ERROR, HITL_OPEN, OAUTH_OPEN))
    if not reached:
        return None
    for outcome, reason, text in _OBSERVED:
        count = counts.get(outcome, 0)
        if count and count / reached >= threshold:
            return reason, f'{count} of {reached} recent turns {text}'
    return None


def _rank(turn_id, salt: str) -> str:
    # The same hash order as eval_auto_rules.stable_case_sample.
    return hashlib.sha256(f'{salt}:{turn_id}'.encode()).hexdigest()


def _newest_first(turns: List[dict]) -> List[dict]:
    return sorted(turns, key=lambda t: (t.get('created_at') is not None, t.get('created_at'), t['id']), reverse=True)


def select_turns(turns: List[dict], settings: dict, salt: str = '') -> List[dict]:
    """The sample, oldest first.

    ``recent`` takes the newest turns. ``stratified`` takes at most a few turns per conversation and
    a share per user, so that one long chat or one heavy user does not decide the score, then a
    stable hash sample: a rerun of the same job picks the same turns, another job other ones. When
    the caps alone would leave fewer than ``history_min_turns``, turns they held back fill the gap,
    so an agent with one user is still scored, from that user.
    """
    cap = settings['history_max_turns']
    if settings.get('history_strategy') == STRATEGY_RECENT:
        chosen = _newest_first(turns)[:cap]
    else:
        per_conversation = settings['history_max_turns_per_conversation']
        per_user = max(1, math.floor(settings['history_max_share_per_user'] * cap))
        conversation_count: Dict = {}
        user_count: Dict = {}
        chosen, held_back = [], []
        for turn in sorted(turns, key=lambda t: _rank(t['id'], salt)):
            conversation, user = turn.get('conversation_id'), turn.get('user_id')
            if len(chosen) >= cap:
                break
            if conversation_count.get(conversation, 0) >= per_conversation or user_count.get(user, 0) >= per_user:
                held_back.append(turn)
                continue
            conversation_count[conversation] = conversation_count.get(conversation, 0) + 1
            user_count[user] = user_count.get(user, 0) + 1
            chosen.append(turn)
        floor = min(settings['history_min_turns'], cap)
        chosen.extend(held_back[:max(0, floor - len(chosen))])
    return sorted(chosen, key=lambda t: (t.get('created_at') is None, t.get('created_at'), t['id']))


def plan_history(turns: List[dict], settings: dict, salt: str = '') -> dict:
    """Everything the tick needs to score an agent from its turns.

    Returns ``{'reason': (code, detail) | None, 'sample': [turn], 'meta': {...}}``. A reason means
    the agent is Not Evaluated (or, in hybrid mode, runs live on ``NO_TRAFFIC``); ``meta`` goes on
    the item and the run as ``meta.history`` in either case.
    """
    by_outcome: Dict[str, List[dict]] = {}
    for turn in turns:
        by_outcome.setdefault(turn_outcome(turn), []).append(turn)
    counts = {key: len(value) for key, value in by_outcome.items()}
    eligible = by_outcome.get(OK, [])
    window = settings['history_window_days']
    meta = {
        'window_days': window,
        'strategy': settings.get('history_strategy') or STRATEGY_STRATIFIED,
        'version': settings.get('history_version'),
        'eligible': len(eligible),
        'excluded': {key: counts.get(key, 0) for key in EXCLUSIONS},
        'sampled': 0,
        'conversations': 0,
        'users': 0,
        'reference_free': True,
        # A conversation pins one version, so a mid-chat switch attributes earlier turns to the later one.
        'version_attribution': 'conversation',
    }
    reason = observed_reason(counts, settings['classify_threshold'])
    minimum = settings['history_min_turns']
    if reason is None and len(eligible) < minimum:
        reason = NO_TRAFFIC, f'{len(eligible)} turns in {window} days, minimum {minimum}'
    if reason is not None:
        return {'reason': reason, 'sample': [], 'meta': meta}
    sample = select_turns(eligible, settings, salt)
    meta.update(sampled=len(sample),
                conversations=len({t.get('conversation_id') for t in sample}),
                users=len({t.get('user_id') for t in sample}))
    return {'reason': None, 'sample': sample, 'meta': meta}


def latency_ms(agent_metas: List[dict]) -> Optional[int]:
    """The turn's run time, from the seconds each agent group recorded; None when none did."""
    seconds = [meta.get('execution_time_seconds') for meta in agent_metas or [] if isinstance(meta, dict)]
    seconds = [float(value) for value in seconds if isinstance(value, (int, float)) and not isinstance(value, bool)]
    return int(round(sum(seconds) * 1000)) if seconds else None


def assemble_turns(user_groups: List[dict], agent_groups: List[dict], text_kinds: Dict[int, str],
                   conversations: Dict[int, dict]) -> List[dict]:
    """Turn dicts, without their text, from the rows the loader read.

    ``user_groups``: ``{id, conversation_id, user_id, created_at, meta, is_streaming}``, one per
    message sent to the agent. ``agent_groups``: ``{id, reply_to_id, meta, is_streaming}``, this
    agent's replies only, so another agent's answer in a shared chat is never its output.
    ``text_kinds``: group id → ``TEXT_PRESENT`` or ``TEXT_PLACEHOLDER``, absent for a blank group.
    ``conversations``: id → ``{is_private, version_id, source, meta}``. A conversation an evaluation
    made is dropped here, before anything is counted.
    """
    replies: Dict[int, List[dict]] = {}
    for group in sorted(agent_groups, key=lambda g: g['id']):
        replies.setdefault(group.get('reply_to_id'), []).append(group)
    turns = []
    for group in user_groups:
        conversation = conversations.get(group.get('conversation_id'))
        if conversation is None or is_evaluation_conversation(conversation):
            continue
        answers = replies.get(group['id'], [])
        agent_metas = [a.get('meta') if isinstance(a.get('meta'), dict) else {} for a in answers]
        turns.append({
            'id': group['id'], 'conversation_id': group.get('conversation_id'), 'user_id': group.get('user_id'),
            'is_private': bool(conversation.get('is_private')), 'version_id': conversation.get('version_id'),
            'created_at': group.get('created_at'), 'input_kind': text_kinds.get(group['id']),
            'has_output': any(a['id'] in text_kinds for a in answers),
            'group_ids': [a['id'] for a in answers],
            'user_meta': group.get('meta') if isinstance(group.get('meta'), dict) else {},
            'agent_metas': agent_metas,
            'streaming': bool(group.get('is_streaming')) or any(a.get('is_streaming') for a in answers),
            'latency_ms': latency_ms(agent_metas),
        })
    return turns


def cap_text(text: str, limit: int = MAX_TURN_TEXT_CHARS) -> str:
    return text if len(text) <= limit else text[:limit] + TRUNCATED_MARK


def attach_texts(turns: List[dict], texts: Dict[int, str]) -> List[dict]:
    """Fill ``input`` and ``output`` of the sampled turns from group id → text, each capped."""
    for turn in turns:
        output = '\n\n'.join(text for text in (texts.get(group_id, '') for group_id in turn['group_ids']) if text)
        turn['input'] = cap_text(texts.get(turn['id'], ''))
        turn['output'] = cap_text(output) if output else None
    return turns
