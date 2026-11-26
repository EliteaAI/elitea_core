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


def _last_answer_failed_condition():
    """The conversation's last message group (by id) has a tool call with is_error set.

    Both subqueries are correlated to the outer conversation through a dedicated alias, so the conversation is not
    re-added to their FROM clause (which would make every conversation match).
    """
    group = aliased(ConversationMessageGroup)
    last_group_id = (
        select(func.max(group.id))
        .where(group.conversation_id == Conversation.id)
        .correlate(Conversation)
        .scalar_subquery()
    )
    step = aliased(MessageTraceStep)
    failed_call = exists().where(
        step.message_group_id == last_group_id,
        step.kind == 'tool_call',
        step.is_error.is_(True),
    ).correlate(Conversation)
    # A run that failed before any tool call (an MCP that cannot connect) leaves an error on its group, not a step
    flagged_group = ConversationMessageGroup.id == last_group_id
    flagged_group_error = and_(
        flagged_group,
        ConversationMessageGroup.meta['is_error'].as_boolean().is_(True),
    )
    return or_(failed_call, exists().where(flagged_group_error).correlate(Conversation))


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
    plain_running = and_(_without_record_condition(), _has_live_run_condition())
    plain_failed = and_(_without_record_condition(), ~_has_live_run_condition(), _last_answer_failed_condition())
    plain_success = and_(
        _without_record_condition(), ~_has_live_run_condition(), ~_last_answer_failed_condition(),
    )
    if outcome == RUNNING:
        return or_(record_match, plain_running)
    if outcome == FAILED:
        return or_(record_match, plain_failed)
    if outcome == SUCCESS:
        return or_(record_match, plain_success)
    # Any other stored schedule status (skipped, blocked, waiting_for_approval) is matched by its record alone
    return record_match


def run_outcomes(session, conversation_ids, statuses_by_conversation) -> dict[int, str]:
    """The outcome of each conversation, for the display; one batched read bounded by the page.

    `statuses_by_conversation` maps a conversation id to its schedule run status, for rows that have a record.
    """
    ids = [i for i in conversation_ids if i is not None]
    if not ids:
        return {}
    running = set(
        row[0] for row in session.query(ConversationMessageGroup.conversation_id).filter(
            ConversationMessageGroup.conversation_id.in_(ids), is_live_run(),
        ).distinct().all()
    )
    failed = set(
        row[0] for row in session.query(ConversationMessageGroup.conversation_id).join(
            MessageTraceStep, MessageTraceStep.message_group_id == ConversationMessageGroup.id,
        ).filter(
            ConversationMessageGroup.conversation_id.in_(ids),
            ConversationMessageGroup.id.in_(
                select(func.max(ConversationMessageGroup.id)).where(
                    ConversationMessageGroup.conversation_id.in_(ids)
                ).group_by(ConversationMessageGroup.conversation_id)
            ),
            MessageTraceStep.kind == 'tool_call',
            MessageTraceStep.is_error.is_(True),
        ).distinct().all()
    )
    # The same error flag the filter reads: the last group of the conversation carries is_error in its meta
    flagged = set(
        row[0] for row in session.query(ConversationMessageGroup.conversation_id).filter(
            ConversationMessageGroup.conversation_id.in_(ids),
            ConversationMessageGroup.id.in_(
                select(func.max(ConversationMessageGroup.id)).where(
                    ConversationMessageGroup.conversation_id.in_(ids)
                ).group_by(ConversationMessageGroup.conversation_id)
            ),
            ConversationMessageGroup.meta['is_error'].as_boolean().is_(True),
        ).distinct().all()
    )
    failed = failed | flagged
    outcomes = {}
    for conv_id in ids:
        if conv_id in statuses_by_conversation:
            outcomes[conv_id] = statuses_by_conversation[conv_id]
        elif conv_id in running:
            outcomes[conv_id] = RUNNING
        elif conv_id in failed:
            outcomes[conv_id] = FAILED
        else:
            outcomes[conv_id] = SUCCESS
    return outcomes


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
