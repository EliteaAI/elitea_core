from pylon.core.tools import web, log
from tools import db

from sqlalchemy import asc, or_

from ..models.message_trace_step import MessageTraceStep
from ..models.message_group import ConversationMessageGroup
from ..models.pd.trace_step import TraceStepListItem, TraceStepDetail

# Defence-in-depth cap: a conversation's total steps are tiny (p99=20/group), but bound the
# flat list so a pathological conversation can't fan into an unbounded response.
TRACE_STEPS_MAX_LIMIT = 2000
TRACE_MESSAGE_GROUPS_MAX = 200
TRACE_STEP_KINDS = {'tool_call', 'thinking_step'}

# Light columns + the bounded attrs sidecar (chip icon/label the FE draws at rest). Never select the
# heavy fields (tool_output / text / thinking / tool_inputs) on the list path — those are detail-only.
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
    MessageTraceStep.attrs,
)


class RPC:
    @web.rpc("chat_list_trace_steps", "list_trace_steps_rpc")
    def list_trace_steps_rpc(
        self,
        project_id: int,
        conversation_id: int,
        message_group_id: int = None,
        message_group_ids: list[int] = None,
        kind: str = None,
        limit: int = 2000,
        offset: int = 0,
        include_total: bool = False,
    ) -> dict:
        """Light, paginated trace-step list for a conversation (pins/labels/order).

        Batched by conversation in one query (no N+1). Ordered by (started_at, id) — render order
        is derived from timestamps, `seq` was dropped in TS-1. Rows carry message_group_id so the
        FE groups them per message. Light fields + the bounded attrs sidecar only; no heavy fields
        (tool_inputs/tool_output/text/thinking).

        Blank thinking steps (transition markers the SDK emits with an action but no text, e.g. a
        tool-call-start) are excluded so the FE draws no empty pins — it renders every row it gets.
        """
        limit = min(max(int(limit or 0), 1), TRACE_STEPS_MAX_LIMIT)
        offset = max(int(offset or 0), 0)
        if kind and kind not in TRACE_STEP_KINDS:
            raise ValueError(f'Unsupported trace-step kind: {kind}')
        scoped_group_ids = list(dict.fromkeys(
            group_id for group_id in (message_group_ids or [])
            if isinstance(group_id, int) and group_id > 0
        ))
        if len(scoped_group_ids) > TRACE_MESSAGE_GROUPS_MAX:
            raise ValueError('Too many message groups requested')

        with db.get_session(project_id) as session:
            query = (
                session.query(*_LIST_COLUMNS)
                .join(
                    ConversationMessageGroup,
                    ConversationMessageGroup.id == MessageTraceStep.message_group_id,
                )
                .filter(ConversationMessageGroup.conversation_id == conversation_id)
                .filter(or_(
                    MessageTraceStep.kind != 'thinking_step',
                    MessageTraceStep.has_visible_content.is_(True),
                ))
            )
            if message_group_id is not None:
                query = query.filter(MessageTraceStep.message_group_id == message_group_id)
            if scoped_group_ids:
                query = query.filter(MessageTraceStep.message_group_id.in_(scoped_group_ids))
            if kind:
                query = query.filter(MessageTraceStep.kind == kind)

            total = query.count() if include_total else None
            rows = (
                query.order_by(
                    asc(MessageTraceStep.started_at).nullslast(),
                    asc(MessageTraceStep.id),
                )
                .limit(limit)
                .offset(offset)
                .all()
            )
            items = [TraceStepListItem.model_validate(r).model_dump(mode='json') for r in rows]
            return {'total': total, 'rows': items}

    @web.rpc("chat_get_trace_step", "get_trace_step_rpc")
    def get_trace_step_rpc(
        self,
        project_id: int,
        step_id: int,
        message_group_id: int,
    ) -> dict | None:
        """Full single step (heavy fields incl. attrs), fetched on pin expand.

        One bounded row — no cumulative-size risk. Returns None if not found.
        """
        with db.get_session(project_id) as session:
            row = session.query(MessageTraceStep).filter(
                MessageTraceStep.id == step_id,
                MessageTraceStep.message_group_id == message_group_id,
            ).first()
            if not row:
                return None
            return TraceStepDetail.model_validate(row).model_dump(mode='json')
