"""
Tool detail analytics endpoint.

Returns per-tool KPIs, user breakdown, and agent breakdown.
"""

from pylon.core.tools import log

try:
    from tools import api_tools, auth, config as c, register_openapi
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
        """Per-tool detail analytics."""

        @register_openapi(
            name="Get Tool Analytics Detail",
            description=(
                "Returns KPIs, per-user breakdown, associated agents, and daily usage trend "
                "for a single tool identified by tool_name."
            ),
            mcp_description="Use this tool when you need a full drill-down on one known tool, including who used it, which agents invoked it, and how its usage/errors changed over time. Do not use this tool to browse all tools in a project — use List Tool Analytics first. Do not use if you only know a partial tool name and still need discovery. This endpoint is best for 'investigate this exact tool.'",
            tags=["elitea_core/analytics"],
            parameters=[
                {
                    "name": "tool_name",
                    "in": "query",
                    "required": True,
                    "schema": {"type": "string"},
                    "description": "Exact tool name to inspect.",
                    "example": "jira_create_issue",
                },
                {
                    "name": "date_from",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string", "format": "date-time"},
                    "description": "Start datetime (ISO 8601). Defaults to 7 days ago.",
                    "example": "2025-01-01T00:00:00",
                },
                {
                    "name": "date_to",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string", "format": "date-time"},
                    "description": "End datetime (ISO 8601). Defaults to now.",
                    "example": "2025-01-31T23:59:59",
                },
            ],
            responses={
                "200": {
                    "description": "Tool detail analytics",
                    "content": {
                        "application/json": {
                            "example": {
                                "tool_name": "jira_create_issue",
                                "kpis": {
                                    "total_calls": 120,
                                    "unique_users": 6,
                                    "avg_duration_ms": 310.0,
                                    "errors": 3,
                                    "error_rate": 2.5,
                                },
                                "users": [
                                    {
                                        "user_id": 42,
                                        "user_email": "alice@example.com",
                                        "calls": 55,
                                        "avg_duration_ms": 290.0,
                                        "errors": 1,
                                    }
                                ],
                                "agents": [
                                    {
                                        "entity_name": "Code Review Bot",
                                        "entity_id": 7,
                                        "calls": 45,
                                    }
                                ],
                                "daily_usage": [
                                    {"date": "2025-01-15", "calls": 18, "errors": 0},
                                    {"date": "2025-01-16", "calls": 22, "errors": 1},
                                ],
                            }
                        }
                    },
                },
                "400": {"description": "tool_name is required"},
                "401": {"description": "Unauthorized"},
                "404": {"description": "No data found for this tool"},
                "500": {"description": "Internal server error"},
            },
            available_to_users=True,
        )
        @auth.decorators.check_api({
            "permissions": ["models.monitoring.tracing.view"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            }
        })
        def get(self, project_id: int, **kwargs):
            """
            GET /api/v2/elitea_core/analytics_tool_detail/prompt_lib/<project_id>

            Query params:
                tool_name (str): required
                date_from, date_to: ISO date range
            """
            from tools import db
            from ...models.audit_event import AuditEvent

            tool_name = request.args.get("tool_name")
            if not tool_name:
                return {"error": "tool_name is required"}, 400

            dt_from, dt_to = _parse_dates(request.args)

            try:
                with db.with_project_schema_session(None) as session:
                    base = session.query(AuditEvent).filter(
                        AuditEvent.project_id == project_id,
                        AuditEvent.tool_name == tool_name,
                        # Exclude system users from analytics
                        ~AuditEvent.user_email.in_([
                            'system@centry.user'
                        ]),
                        ~AuditEvent.user_email.like('system_user_%@centry.user'),
                    )
                    if dt_from:
                        base = base.filter(AuditEvent.timestamp >= dt_from)
                    if dt_to:
                        base = base.filter(AuditEvent.timestamp <= dt_to)

                    # KPIs
                    kpi = base.with_entities(
                        func.count().label("total_calls"),
                        func.count(func.distinct(AuditEvent.user_id)).label("unique_users"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                    ).first()

                    if not kpi or not kpi.total_calls:
                        return {"error": "No data found for this tool"}, 404

                    # Users who called this tool
                    user_rows = base.with_entities(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                        func.count().label("calls"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                    ).filter(
                        AuditEvent.user_id.isnot(None),
                    ).group_by(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                    ).order_by(func.count().desc()).all()

                    # Agents (applications) that used this tool
                    # Tool events share trace_id with the parent application RPC event.
                    # Use a subquery to find trace_ids, then look up agents.
                    trace_subq = base.with_entities(
                        AuditEvent.trace_id,
                    ).filter(
                        AuditEvent.trace_id.isnot(None),
                        AuditEvent.trace_id != "",
                    ).distinct().subquery()

                    agent_rows = session.query(
                        AuditEvent.entity_name,
                        AuditEvent.entity_id,
                        func.count(func.distinct(AuditEvent.trace_id)).label("calls"),
                    ).filter(
                        AuditEvent.trace_id.in_(
                            session.query(trace_subq.c.trace_id)
                        ),
                        AuditEvent.entity_type == "application",
                        AuditEvent.entity_id.isnot(None),
                    ).group_by(
                        AuditEvent.entity_name,
                        AuditEvent.entity_id,
                    ).order_by(func.count(func.distinct(AuditEvent.trace_id)).desc()).limit(20).all()

                    # Daily usage
                    daily_rows = base.with_entities(
                        cast(AuditEvent.timestamp, Date).label("day"),
                        func.count().label("calls"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                    ).group_by("day").order_by("day").all()

                    return {
                        "tool_name": tool_name,
                        "kpis": {
                            "total_calls": kpi.total_calls,
                            "unique_users": kpi.unique_users,
                            "avg_duration_ms": round(kpi.avg_duration_ms, 1) if kpi.avg_duration_ms else 0,
                            "errors": kpi.errors or 0,
                            "error_rate": round((kpi.errors or 0) / kpi.total_calls * 100, 2) if kpi.total_calls > 0 else 0,
                        },
                        "users": [
                            {
                                "user_id": r.user_id,
                                "user_email": r.user_email,
                                "calls": r.calls,
                                "avg_duration_ms": round(r.avg_duration_ms, 1) if r.avg_duration_ms else 0,
                                "errors": r.errors or 0,
                            }
                            for r in user_rows
                        ],
                        "agents": [
                            {
                                "entity_name": r.entity_name or f"Agent #{r.entity_id}",
                                "entity_id": r.entity_id,
                                "calls": r.calls,
                            }
                            for r in agent_rows
                        ],
                        "daily_usage": [
                            {
                                "date": r.day.isoformat() if r.day else None,
                                "calls": r.calls,
                                "errors": r.errors or 0,
                            }
                            for r in daily_rows
                        ],
                    }, 200

            except Exception as e:
                log.error(f"Analytics tool detail query failed: {e}")
                return {"error": "Failed to query tool detail"}, 500


    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '<int:project_id>',
        ])
        mode_handlers = {
            'prompt_lib': PromptLibAPI,
        }
else:
    API = None
