from pylon.core.tools import web, log
from tools import db

from sqlalchemy import asc

from ..models.message_trace_step import MessageTraceStep
from ..models.message_group import ConversationMessageGroup
from ..models.pd.trace_step import TraceStepListItem, TraceStepDetail

# Defence-in-depth cap: a conversation's total steps are tiny (p99=20/group), but bound the
# flat list so a pathological conversation can't fan into an unbounded response.
TRACE_STEPS_MAX_LIMIT = 2000

# Light columns only — never select tool_output / text / thinking / attrs on the list path.
_LIST_COLUMNS = (
    MessageTraceStep.id,
    MessageTraceStep.message_group_id,
    MessageTraceStep.kind,
    MessageTraceStep.tool_name,
    MessageTraceStep.parent_agent_name,
    MessageTraceStep.parent_agent_call_id,
    MessageTraceStep.started_at,
    MessageTraceStep.finished_at,
    MessageTraceStep.is_error,
    MessageTraceStep.step_type,
    MessageTraceStep.model_name,
    MessageTraceStep.finish_reason,
)


class RPC:
    @web.rpc("chat_list_trace_steps", "list_trace_steps_rpc")
    def list_trace_steps_rpc(
        self,
        project_id: int,
        conversation_id: int,
        message_group_id: int = None,
        kind: str = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> dict:
        """Light, paginated trace-step list for a conversation (pins/labels/order).

        Batched by conversation in one query (no N+1). Ordered by (started_at, id) — render order
        is derived from timestamps, `seq` was dropped in TS-1. Rows carry message_group_id so the
        FE groups them per message. No heavy fields (tool_inputs/tool_output/text/thinking/attrs).
        """
        limit = min(max(int(limit or 0), 1), TRACE_STEPS_MAX_LIMIT)
        offset = max(int(offset or 0), 0)

        with db.get_session(project_id) as session:
            query = (
                session.query(*_LIST_COLUMNS)
                .join(
                    ConversationMessageGroup,
                    ConversationMessageGroup.id == MessageTraceStep.message_group_id,
                )
                .filter(ConversationMessageGroup.conversation_id == conversation_id)
            )
            if message_group_id is not None:
                query = query.filter(MessageTraceStep.message_group_id == message_group_id)
            if kind:
                query = query.filter(MessageTraceStep.kind == kind)

            total = query.count()
            rows = (
                query.order_by(asc(MessageTraceStep.started_at), asc(MessageTraceStep.id))
                .limit(limit)
                .offset(offset)
                .all()
            )
            items = [TraceStepListItem.model_validate(r).model_dump(mode='json') for r in rows]
            return {'total': total, 'rows': items}

    @web.rpc("chat_get_trace_step", "get_trace_step_rpc")
    def get_trace_step_rpc(self, project_id: int, step_id: int) -> dict | None:
        """Full single step (heavy fields incl. attrs), fetched on pin expand.

        One bounded row — no cumulative-size risk. Returns None if not found.
        """
        with db.get_session(project_id) as session:
            row = session.query(MessageTraceStep).filter(MessageTraceStep.id == step_id).first()
            if not row:
                return None
            return TraceStepDetail.model_validate(row).model_dump(mode='json')
