"""
User detail analytics endpoint.

Returns per-user KPIs, model/tool/agent breakdown, and recent activity.
"""

from pylon.core.tools import log

try:
    from tools import api_tools, auth, config as c
    _API_AVAILABLE = True
except ImportError:
    _API_AVAILABLE = False


if _API_AVAILABLE:
    from datetime import datetime, timedelta, timezone
    from flask import request
    from sqlalchemy import func, case, cast, Date, desc

    def _parse_dates(args):
        date_from = args.get("date_from")
        date_to = args.get("date_to")
        try:
            dt_from = datetime.fromisoformat(date_from) if date_from else None
        except (ValueError, TypeError):
            dt_from = None
        try:
            dt_to = datetime.fromisoformat(date_to) if date_to else None
        except (ValueError, TypeError):
            dt_to = None
        if not dt_from and not dt_to:
            dt_to = datetime.now(timezone.utc)
            dt_from = dt_to - timedelta(days=7)
        return dt_from, dt_to

    class PromptLibAPI(api_tools.APIModeHandler):
        """Per-user detail analytics."""

        @auth.decorators.check_api({
            "permissions": ["models.monitoring.tracing.view"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            }
        })
        def get(self, project_id: int, **kwargs):
            """
            GET /api/v2/elitea_core/analytics_user_detail/prompt_lib/<project_id>

            Query params:
                user_id (int): required
                date_from, date_to: ISO date range
            """
            from tools import db
            from ...models.audit_event import AuditEvent

            user_id = request.args.get("user_id")
            if not user_id:
                return {"error": "user_id is required"}, 400
            try:
                user_id = int(user_id)
            except (ValueError, TypeError):
                return {"error": "user_id must be an integer"}, 400

            dt_from, dt_to = _parse_dates(request.args)

            try:
                with db.with_project_schema_session(None) as session:
                    base = session.query(AuditEvent).filter(
                        AuditEvent.project_id == project_id,
                        AuditEvent.user_id == user_id,
                    )
                    if dt_from:
                        base = base.filter(AuditEvent.timestamp >= dt_from)
                    if dt_to:
                        base = base.filter(AuditEvent.timestamp <= dt_to)

                    # KPIs
                    kpi = base.with_entities(
                        AuditEvent.user_email,
                        func.count().label("total_events"),
                        func.count(func.distinct(
                            cast(AuditEvent.timestamp, Date)
                        )).label("active_days"),
                        func.sum(case(
                            (AuditEvent.event_type == "llm", 1), else_=0,
                        )).label("llm_calls"),
                        func.sum(case(
                            (AuditEvent.event_type == "tool", 1), else_=0,
                        )).label("tool_events"),
                        func.sum(case(
                            (AuditEvent.entity_type == "application", 1), else_=0,
                        )).label("agent_events"),
                        func.sum(case(
                            (AuditEvent.action == "SIO chat_predict", 1), else_=0,
                        )).label("chat_events"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                    ).group_by(AuditEvent.user_email).first()

                    if not kpi:
                        return {"error": "No data found for this user"}, 404

                    # Models used by this user
                    model_rows = base.with_entities(
                        AuditEvent.model_name,
                        func.count().label("calls"),
                    ).filter(
                        AuditEvent.model_name.isnot(None),
                        AuditEvent.model_name != "",
                    ).group_by(
                        AuditEvent.model_name,
                    ).order_by(func.count().desc()).all()

                    # Get display names for models via configurations RPC - build mapping once
                    model_display_names = {}
                    try:
                        from tools import rpc_tools
                        models_response = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_models(
                            project_id=project_id, 
                            section='llm', 
                            include_shared=True
                        )
                        items = models_response.get('items', []) if models_response else []
                        for item in items:
                            if isinstance(item, dict) and 'name' in item:
                                display = item.get('display_name', item['name'])
                                model_display_names[item['name']] = display
                    except Exception as e:
                        log.warning(f"Failed to get model configurations: {e}")

                    # Tools used by this user
                    tool_rows = base.with_entities(
                        AuditEvent.tool_name,
                        func.count().label("calls"),
                    ).filter(
                        AuditEvent.tool_name.isnot(None),
                        AuditEvent.tool_name != "",
                    ).group_by(
                        AuditEvent.tool_name,
                    ).order_by(func.count().desc()).all()

                    # Agents used by this user
                    agent_rows = base.with_entities(
                        AuditEvent.entity_name,
                        AuditEvent.entity_id,
                        func.count().label("runs"),
                    ).filter(
                        AuditEvent.entity_type == "application",
                        AuditEvent.entity_id.isnot(None),
                    ).group_by(
                        AuditEvent.entity_name,
                        AuditEvent.entity_id,
                    ).order_by(func.count().desc()).all()

                    # Daily activity by event type
                    daily_rows = base.with_entities(
                        cast(AuditEvent.timestamp, Date).label("day"),
                        func.sum(case(
                            (AuditEvent.event_type == "llm", 1), else_=0,
                        )).label("llm"),
                        func.sum(case(
                            (AuditEvent.event_type == "tool", 1), else_=0,
                        )).label("tool"),
                        func.sum(case(
                            (AuditEvent.action == "SIO chat_predict", 1), else_=0,
                        )).label("chat"),
                        func.sum(case(
                            (AuditEvent.entity_type == "application", 1), else_=0,
                        )).label("agent"),
                        func.count().label("total"),
                    ).group_by("day").order_by("day").all()

                    return {
                        "user_id": user_id,
                        "user_email": kpi.user_email,
                        "kpis": {
                            "total_events": kpi.total_events,
                            "active_days": kpi.active_days,
                            "llm_events": kpi.llm_calls or 0,
                            "tool_events": kpi.tool_events or 0,
                            "agent_events": kpi.agent_events or 0,
                            "chat_events": kpi.chat_events or 0,
                            "errors": kpi.errors or 0,
                        },
                        "models": [
                            {
                                "model_name": r.model_name,
                                "display_name": model_display_names.get(r.model_name, r.model_name),
                                "calls": r.calls,
                            }
                            for r in model_rows
                        ],
                        "tools": [
                            {"tool_name": r.tool_name, "calls": r.calls}
                            for r in tool_rows
                        ],
                        "agents": [
                            {
                                "entity_name": r.entity_name or f"Agent #{r.entity_id}",
                                "entity_id": r.entity_id,
                                "runs": r.runs,
                            }
                            for r in agent_rows
                        ],
                        "daily_activity": [
                            {
                                "date": r.day.isoformat() if r.day else None,
                                "llm": r.llm or 0,
                                "tool": r.tool or 0,
                                "chat": r.chat or 0,
                                "agent": r.agent or 0,
                                "total": r.total or 0,
                            }
                            for r in daily_rows
                        ],
                    }, 200

            except Exception as e:
                log.error(f"Analytics user detail query failed: {e}")
                return {"error": "Failed to query user detail"}, 500


    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '<int:project_id>',
        ])
        mode_handlers = {
            'prompt_lib': PromptLibAPI,
        }
else:
    API = None
