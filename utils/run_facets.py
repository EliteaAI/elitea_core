"""The options of the Run History filters: the values that have runs the caller can see (AC-9, AC-47a).

A filter lists every value that exists for the entity, not only values on the loaded rows. The options are limited to
the runs the caller may list (the same visibility as the run list), so a filter never reveals a name from a run the
caller cannot open. A deleted version or schedule that still has runs is listed with `deleted` set.
One read answers all the filters of a page.
"""
from sqlalchemy import select

from ..models.conversation import Conversation
from ..models.entity_schedule import EntitySchedule, EntityScheduleRun
from ..models.message_group import ConversationMessageGroup
from ..models.message_trace_step import MessageTraceStep
from ..utils.schedule_run_detail import version_labels


def _runs_of_entity(entity_id: int, visible_conversation_ids):
    """The schedule run rows of an agent or pipeline whose conversation the caller may list."""
    return select(EntityScheduleRun).join(
        EntitySchedule, EntitySchedule.id == EntityScheduleRun.schedule_id,
    ).where(
        EntitySchedule.application_id == entity_id,
        EntityScheduleRun.conversation_id.in_(visible_conversation_ids),
    )


def _version_facets(session, entity_id: int, visible_conversation_ids) -> list[dict]:
    """The versions the caller's visible runs of this entity used, by name; a deleted version is marked deleted."""
    runs = _runs_of_entity(entity_id, visible_conversation_ids).subquery()
    version_ids = {
        row[0] for row in session.execute(
            select(runs.c.version_id).where(runs.c.version_id.isnot(None)).distinct()
        ).all()
    }
    labels = version_labels(session, version_ids)
    return [
        {'id': version_id, 'name': labels.get(version_id), 'deleted': version_id not in labels}
        for version_id in sorted(version_ids)
    ]


def _schedule_facets(session, entity_id: int, visible_conversation_ids) -> list[dict]:
    """The schedules and webhooks the caller's visible runs of this entity came from, by name.

    Only schedules that still exist are listed. A deleted schedule that still has visible runs is not listed yet:
    the run row keeps only the schedule id, so its name cannot be shown (open gap, see the report).
    """
    rows = session.execute(
        select(EntitySchedule.id, EntitySchedule.name, EntitySchedule.trigger_kind)
        .join(EntityScheduleRun, EntityScheduleRun.schedule_id == EntitySchedule.id)
        .where(
            EntitySchedule.application_id == entity_id,
            EntityScheduleRun.conversation_id.in_(visible_conversation_ids),
        )
        .distinct()
        .order_by(EntitySchedule.name)
    ).all()
    return [{'id': sid, 'name': name, 'trigger_kind': kind, 'deleted': False} for sid, name, kind in rows]


def _tool_facets(session, visible_conversation_ids) -> list[str]:
    """The tool names called in the caller's visible runs (AC-45): the options of an MCP's Event filter."""
    names = session.execute(
        select(MessageTraceStep.tool_name)
        .join(ConversationMessageGroup, MessageTraceStep.message_group_id == ConversationMessageGroup.id)
        .where(
            ConversationMessageGroup.conversation_id.in_(visible_conversation_ids),
            MessageTraceStep.kind == 'tool_call',
            MessageTraceStep.tool_name.isnot(None),
        )
        .distinct()
        .order_by(MessageTraceStep.tool_name)
    ).scalars().all()
    return list(names)


def run_facets(session, entity_id: int, visible_conversation_ids) -> dict:
    """Every filter option of a page in one read, limited to the caller's visible conversations.

    `visible_conversation_ids` is a SELECT of the conversation ids the caller may list, used as a subquery so the
    page never holds those ids in memory.
    """
    return {
        'versions': _version_facets(session, entity_id, visible_conversation_ids),
        'schedules': _schedule_facets(session, entity_id, visible_conversation_ids),
        'tools': _tool_facets(session, visible_conversation_ids),
    }


__all__ = ['run_facets']
