"""
Project-level tracing and health endpoint.

Aggregates audit_events data to provide event/error KPIs, event type breakdown, daily
activity, chat session stats and per-event-type health.

The AI adoption, token and cost half of this payload moved to the usage plugin over the
usage_event table (#6574); chat metrics stay here because they are socketio-derived and have
no usage_event equivalent.
"""

from pylon.core.tools import log

try:
    from tools import api_tools, auth, config as c, register_openapi
    _API_AVAILABLE = True
except ImportError:
    _API_AVAILABLE = False


if _API_AVAILABLE:
    from flask import request
    from sqlalchemy import func, case, cast, Date, or_

    from ...utils.constants import SYSTEM_USER_EMAILS, SYSTEM_USER_EMAIL_PATTERN
    from ...utils.date_range import parse_date_range as _parse_dates

    def _apply_base_filters(session, AuditEvent, project_id, dt_from, dt_to):
        """Build base query with project + date filters, excluding system users."""
        base = session.query(AuditEvent).filter(
            AuditEvent.project_id == project_id,
            or_(
                AuditEvent.user_email.is_(None),
                ~AuditEvent.user_email.in_(SYSTEM_USER_EMAILS),
            ),
            or_(
                AuditEvent.user_email.is_(None),
                ~AuditEvent.user_email.like(SYSTEM_USER_EMAIL_PATTERN),
            ),
        )
        if dt_from:
            base = base.filter(AuditEvent.timestamp >= dt_from)
        if dt_to:
            base = base.filter(AuditEvent.timestamp <= dt_to)
        return base

    class PromptLibAPI(api_tools.APIModeHandler):
        """Project-level tracing and health metrics."""

        @register_openapi(
            name="Get Project Analytics Overview",
            description=(
                "Returns project-level event/error KPIs, event type breakdown, "
                "daily activity trend, chat session stats, and per-event-type "
                "health metrics."
            ),
            mcp_tool=True,
            mcp_description="Use this tool when you need the tracing/health view for a project: event and error KPIs, event type breakdown, daily activity trend, chat session stats, and per-event-type health metrics in one call. Do not use this tool when you need AI adoption, cost, or token metrics — use the usage analytics endpoint for those. This is the best first-step endpoint for project tracing and health summaries.",
            tags=["elitea_core/analytics"],
            parameters=[
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
                    "description": "Project analytics overview",
                    "content": {
                        "application/json": {
                            "example": {
                                "kpis": {
                                    "total_events": 1250,
                                    "avg_duration_ms": 432.5,
                                    "error_rate": 2.4,
                                    "error_count": 30,
                                    "chat_msgs": 210,
                                },
                                "event_type_breakdown": [
                                    {"event_type": "llm", "count": 780},
                                    {"event_type": "tool", "count": 340},
                                    {"event_type": "socketio", "count": 130},
                                ],
                                "daily_activity": [
                                    {"date": "2025-01-15", "events": 85, "errors": 2},
                                    {"date": "2025-01-16", "events": 110, "errors": 1},
                                ],
                                "chat_sessions": [
                                    {
                                        "action": "SIO chat_predict",
                                        "sessions": 210,
                                        "users": 14,
                                        "avg_duration_ms": 850.0,
                                    }
                                ],
                                "health": [
                                    {
                                        "event_type": "llm",
                                        "total": 780,
                                        "errors": 18,
                                        "error_rate": 2.31,
                                        "avg_duration_ms": 520.0,
                                    }
                                ],
                            }
                        }
                    },
                },
                "401": {"description": "Unauthorized"},
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
            GET /api/v2/elitea_core/analytics/prompt_lib/<project_id>

            Query params:
                date_from (str): ISO datetime lower bound
                date_to (str): ISO datetime upper bound
            """
            from tools import db
            from ...models.audit_event import AuditEvent

            dt_from, dt_to = _parse_dates(request.args)

            try:
                with db.with_project_schema_session(None) as session:
                    base = _apply_base_filters(session, AuditEvent, project_id, dt_from, dt_to)

                    # 1. KPIs
                    kpi_row = base.with_entities(
                        func.count().label("total_events"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("error_count"),
                        func.sum(case(
                            (AuditEvent.action == "SIO chat_predict", 1), else_=0,
                        )).label("chat_msgs"),
                    ).first()

                    total_events = kpi_row.total_events or 0
                    error_count = kpi_row.error_count or 0

                    kpis = {
                        "total_events": total_events,
                        "avg_duration_ms": round(kpi_row.avg_duration_ms, 1) if kpi_row.avg_duration_ms else 0,
                        "error_rate": round(error_count / total_events * 100, 2) if total_events > 0 else 0,
                        "error_count": error_count,
                        "chat_msgs": kpi_row.chat_msgs or 0,
                    }

                    # 2. Event type breakdown
                    event_type_rows = base.with_entities(
                        AuditEvent.event_type,
                        func.count().label("count"),
                    ).group_by(AuditEvent.event_type).all()

                    event_type_breakdown = [
                        {"event_type": r.event_type, "count": r.count}
                        for r in event_type_rows
                    ]

                    # 3. Daily activity
                    daily_rows = base.with_entities(
                        cast(AuditEvent.timestamp, Date).label("day"),
                        func.count().label("events"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                    ).group_by("day").order_by("day").all()

                    daily_activity = [
                        {
                            "date": r.day.isoformat() if r.day else None,
                            "events": r.events,
                            "errors": r.errors or 0,
                        }
                        for r in daily_rows
                    ]

                    # 4. Chat session counts (from socketio predict events)
                    chat_session_rows = base.with_entities(
                        AuditEvent.action,
                        func.count().label("sessions"),
                        func.count(func.distinct(AuditEvent.user_id)).label("users"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                    ).filter(
                        AuditEvent.event_type == "socketio",
                        AuditEvent.action.in_(["SIO chat_predict", "SIO chat_continue_predict"]),
                    ).group_by(AuditEvent.action).all()

                    chat_sessions = [
                        {
                            "action": r.action,
                            "sessions": r.sessions,
                            "users": r.users,
                            "avg_duration_ms": round(r.avg_duration_ms, 1) if r.avg_duration_ms else 0,
                        }
                        for r in chat_session_rows
                    ]

                    # 5. Health
                    health_rows = base.with_entities(
                        AuditEvent.event_type,
                        func.count().label("total"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                    ).group_by(AuditEvent.event_type).all()

                    health = [
                        {
                            "event_type": r.event_type,
                            "total": r.total,
                            "errors": r.errors or 0,
                            "error_rate": round((r.errors or 0) / r.total * 100, 2) if r.total > 0 else 0,
                            "avg_duration_ms": round(r.avg_duration_ms, 1) if r.avg_duration_ms else 0,
                        }
                        for r in health_rows
                    ]

                    return {
                        "kpis": kpis,
                        "event_type_breakdown": event_type_breakdown,
                        "daily_activity": daily_activity,
                        "chat_sessions": chat_sessions,
                        "health": health,
                    }, 200

            except Exception:
                log.error("Analytics query failed", exc_info=True)
                return {"error": "Failed to query analytics"}, 500


    class AdminAPI(api_tools.APIModeHandler):
        """Admin-level analytics (same logic, no project filter required)."""

        @auth.decorators.check_api({
            "permissions": ["models.admin.audit_trail.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            }
        })
        def get(self, **kwargs):
            from tools import db
            from ...models.audit_event import AuditEvent

            project_id = request.args.get("project_id")
            all_projects = request.args.get("all_projects", "").lower() == "true"
            if not project_id and not all_projects:
                return {"error": "project_id is required unless all_projects=true"}, 400

            dt_from, dt_to = _parse_dates(request.args)

            try:
                with db.with_project_schema_session(None) as session:
                    base = session.query(AuditEvent).filter(
                        or_(
                            AuditEvent.user_email.is_(None),
                            ~AuditEvent.user_email.in_(SYSTEM_USER_EMAILS),
                        ),
                        or_(
                            AuditEvent.user_email.is_(None),
                            ~AuditEvent.user_email.like(SYSTEM_USER_EMAIL_PATTERN),
                        ),
                    )
                    if project_id:
                        base = base.filter(AuditEvent.project_id == int(project_id))
                    if dt_from:
                        base = base.filter(AuditEvent.timestamp >= dt_from)
                    if dt_to:
                        base = base.filter(AuditEvent.timestamp <= dt_to)

                    total = base.count()
                    unique_users = base.with_entities(
                        func.count(func.distinct(AuditEvent.user_id))
                    ).scalar() or 0

                    return {
                        "kpis": {
                            "total_events": total,
                            "unique_users": unique_users,
                        },
                    }, 200

            except Exception:
                log.error("Admin analytics query failed", exc_info=True)
                return {"error": "Failed to query analytics"}, 500


    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '',
            '<int:project_id>',
        ])
        mode_handlers = {
            'administration': AdminAPI,
            'prompt_lib': PromptLibAPI,
        }
else:
    API = None
