"""The status of a run row in Run History (EliteaAI/elitea_issues#7682, AC-24, AC-25).

A run with a schedule run record shows that record's status. A plain chat, API or MCP run has no record, so its status
is worked out from data already stored; nothing is written when a run ends (PO decision 2026-11-23):

1. a schedule run record          -> its stored status
2. the chat has a live task        -> running
3. the last answer has a failed tool call (trace step is_error), or its group is flagged is_error -> failed
4. otherwise                       -> success

A stopped chat run reads success (AC-24a). The display (`run_outcomes`) and the filter (`outcome_condition`) apply
the same rule, so a filter never lists a run its row would show differently.
"""
from sqlalchemy import Integer, and_, cast, exists, func, or_, select
from sqlalchemy.orm import aliased

from ..models.conversation import Conversation
from ..models.entity_schedule import EntityScheduleRun
from ..models.message_group import ConversationMessageGroup
from ..models.message_trace_step import MessageTraceStep
from .running_conversations import is_live_run

RUNNING = 'running'
FAILED = 'failed'
SUCCESS = 'success'
BLOCKED = 'blocked'


def _last_group_id_subquery():
    """The id of each conversation's last message group, found once and reused by every condition below."""
    group = aliased(ConversationMessageGroup)
    return (
        select(func.max(group.id))
        .where(group.conversation_id == Conversation.id)
        .correlate(Conversation)
        .scalar_subquery()
    )


def _last_answer_blocked_condition():
    """The last group was refused by an input guardrail: a policy block, reported as Blocked, not Failed (review)."""
    return exists().where(
        ConversationMessageGroup.id == _last_group_id_subquery(),
        ConversationMessageGroup.meta['input_guardrail'].isnot(None),
    ).correlate(Conversation)


def _last_answer_failed_condition():
    """The conversation's last message group failed: a failed tool call, or the group flagged is_error.

    A group the input guardrail refused is not a failure here (see `_last_answer_blocked_condition`). A run that failed
    before any tool call (an MCP that cannot connect) leaves an error on its group, not a step.
    """
    last_group_id = _last_group_id_subquery()
    step = aliased(MessageTraceStep)
    failed_call = exists().where(
        step.message_group_id == last_group_id,
        step.kind == 'tool_call',
        step.is_error.is_(True),
    ).correlate(Conversation)
    flagged_group_error = exists().where(
        ConversationMessageGroup.id == last_group_id,
        ConversationMessageGroup.meta['is_error'].as_boolean().is_(True),
    ).correlate(Conversation)
    return and_(or_(failed_call, flagged_group_error), ~_last_answer_blocked_condition())


def _has_live_run_condition():
    """The conversation has a live run: the same rule as the list spinner (running_conversations.is_live_run)."""
    return exists().where(
        ConversationMessageGroup.conversation_id == Conversation.id,
        is_live_run(),
    ).correlate(Conversation)


def _linked_record_condition(*extra):
    """A conversation whose meta names a schedule run record that points back to it (the display's rule).

    The display (`_schedule_runs`) reads the run only through `meta.schedule_run_id` and only when the run's own
    conversation_id is this conversation. The filter uses the same two links, so a row and its filter agree. The
    subquery drops NULL conversation ids, which would otherwise make a NOT IN test NULL for every row.
    """
    run_id = cast(Conversation.meta['schedule_run_id'].astext, Integer)
    return Conversation.id.in_(
        select(EntityScheduleRun.conversation_id).where(
            EntityScheduleRun.id == run_id,
            EntityScheduleRun.conversation_id.isnot(None),
            *extra,
        )
    )


def _record_status_condition(outcome: str):
    """The conversation has its own schedule run record with this status."""
    return _linked_record_condition(EntityScheduleRun.status == outcome)


def _without_record_condition():
    """The conversation has no linked schedule run record, so it is a plain chat, API or MCP run."""
    return ~_linked_record_condition()


def outcome_condition(outcome: str):
    """A SQL condition on a conversation row: its derived outcome is `outcome` (the rule in the module docstring).

    Mirrors `run_outcomes`. A row with a record is matched by the record's status; a row without one by the chat
    rules. Each branch is parenthesised so AND and OR never mix.
    """
    record_match = _record_status_condition(outcome)
    plain = _without_record_condition()
    not_live = ~_has_live_run_condition()
    if outcome == RUNNING:
        return or_(record_match, and_(plain, _has_live_run_condition()))
    if outcome == FAILED:
        return or_(record_match, and_(plain, not_live, _last_answer_failed_condition()))
    if outcome == BLOCKED:
        return or_(record_match, and_(plain, not_live, _last_answer_blocked_condition()))
    if outcome == SUCCESS:
        return or_(
            record_match,
            and_(plain, not_live, ~_last_answer_failed_condition(), ~_last_answer_blocked_condition()),
        )
    # Any other stored schedule status (skipped, waiting_for_approval) is matched by its record alone
    return record_match


def run_outcomes(session, conversation_ids, statuses_by_conversation) -> dict[int, str]:
    """The outcome of each conversation, for the display; one batched read bounded by the page.

    `statuses_by_conversation` maps a conversation id to its schedule run status, for rows that have a record.
    The rule is the filter's (`outcome_condition`): the last message group decides failed or blocked, and it is read
    once here, with its guardrail marker and its failed tool calls, not one query per condition.
    """
    ids = [i for i in conversation_ids if i is not None]
    if not ids:
        return {}
    running = set(
        row[0] for row in session.query(ConversationMessageGroup.conversation_id).filter(
            ConversationMessageGroup.conversation_id.in_(ids), is_live_run(),
        ).distinct().all()
    )
    last_group = (
        select(func.max(ConversationMessageGroup.id).label('id'))
        .where(ConversationMessageGroup.conversation_id.in_(ids))
        .group_by(ConversationMessageGroup.conversation_id)
        .subquery()
    )
    last = session.query(
        ConversationMessageGroup.conversation_id,
        ConversationMessageGroup.meta,
        exists().where(
            MessageTraceStep.message_group_id == ConversationMessageGroup.id,
            MessageTraceStep.kind == 'tool_call',
            MessageTraceStep.is_error.is_(True),
        ).label('failed_call'),
    ).filter(ConversationMessageGroup.id.in_(select(last_group.c.id))).all()
    last_by_conv = {row[0]: row for row in last}
    outcomes = {}
    for conv_id in ids:
        if conv_id in statuses_by_conversation:
            outcomes[conv_id] = statuses_by_conversation[conv_id]
        elif conv_id in running:
            outcomes[conv_id] = RUNNING
        else:
            outcomes[conv_id] = _outcome_of_last_group(last_by_conv.get(conv_id))
    return outcomes


def _outcome_of_last_group(row) -> str:
    """The chat rule for the last group: a guardrail block is Blocked, a failure is Failed, else Success."""
    if row is None:
        return SUCCESS
    _conv_id, meta, failed_call = row
    meta = meta or {}
    if meta.get('input_guardrail') is not None:
        return BLOCKED
    if failed_call or meta.get('is_error') is True:
        return FAILED
    return SUCCESS


def tool_names_by_conversation(session, conversation_ids) -> dict[int, list[str]]:
    """The tool names called in each conversation, in call order, without repeats (AC-44, AC-47a).

    One query bounded by the page's conversation ids. Only tool-call steps count; a conversation with none gets no
    entry.
    """
    ids = [i for i in conversation_ids if i is not None]
    if not ids:
        return {}
    # One row per (conversation, tool), keeping the first call's time: the repeats are removed in SQL, not in Python,
    # so a long run with thousands of steps sends only its distinct tools
    first_call = func.min(MessageTraceStep.started_at).label('first_call')
    rows = session.query(
        ConversationMessageGroup.conversation_id,
        MessageTraceStep.tool_name,
        first_call,
    ).join(
        MessageTraceStep, MessageTraceStep.message_group_id == ConversationMessageGroup.id,
    ).filter(
        ConversationMessageGroup.conversation_id.in_(ids),
        MessageTraceStep.kind == 'tool_call',
        MessageTraceStep.tool_name.isnot(None),
        MessageTraceStep.tool_name != '',
    ).group_by(
        ConversationMessageGroup.conversation_id,
        MessageTraceStep.tool_name,
    ).order_by(
        ConversationMessageGroup.conversation_id,
        first_call,
        MessageTraceStep.tool_name,
    ).all()
    names: dict[int, list[str]] = {}
    for conv_id, tool_name, _first in rows:
        names.setdefault(conv_id, []).append(tool_name)
    return names


def tool_name_condition(tool_names: list[str]):
    """A conversation whose tool calls include one of these names (OR across the names)."""
    step = aliased(MessageTraceStep)
    group = aliased(ConversationMessageGroup)
    return exists().where(
        group.conversation_id == Conversation.id,
        step.message_group_id == group.id,
        step.kind == 'tool_call',
        step.tool_name.in_(tool_names),
    ).correlate(Conversation)


__all__ = ['outcome_condition', 'run_outcomes', 'tool_names_by_conversation', 'tool_name_condition',
           'RUNNING', 'FAILED', 'SUCCESS']
