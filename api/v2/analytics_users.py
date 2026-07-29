"""
Paginated analytics users endpoint.

Provides server-side pagination, search, and sorting for user activity data.
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
    from sqlalchemy import func, case, cast, Date, desc, asc, or_

    from ...utils.constants import (
        SYSTEM_USER_EMAILS,
        SYSTEM_USER_EMAIL_PATTERN,
    )

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

    _SORT_WHITELIST = frozenset([
        "total_events", "active_days", "llm_events", "tool_events",
        "agent_events", "chat_events", "errors", "user_email",
        "total_tokens", "llm_cost",
    ])

    class PromptLibAPI(api_tools.APIModeHandler):
        """Paginated user activity for analytics."""

        @register_openapi(
            name="List User Analytics",
            description=(
                "Returns paginated user activity statistics broken down by LLM calls, "
                "tool runs, agent interactions, and chat events, with sorting and search."
            ),
            mcp_tool=True,
            mcp_description="Use this tool when you need a paginated leaderboard or searchable list of user activity across a project. Do not use this tool when you need the detailed model/tool/agent breakdown for one individual — use Get User Analytics Detail. Do not use for project-level KPI dashboards. This is the primary list/discovery endpoint for user analytics.",
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
                {
                    "name": "limit",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
                    "description": "Page size (max 100).",
                },
                {
                    "name": "offset",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "integer", "default": 0, "minimum": 0},
                    "description": "Pagination offset.",
                },
                {
                    "name": "search",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string"},
                    "description": "Filter by user email (case-insensitive partial match).",
                },
                {
                    "name": "sort_by",
                    "in": "query",
                    "required": False,
                    "schema": {
                        "type": "string",
                        "enum": [
                            "total_events", "active_days", "llm_events",
                            "tool_events", "agent_events", "chat_events",
                            "errors", "user_email", "total_tokens", "llm_cost",
                        ],
                        "default": "total_events",
                    },
                    "description": "Column to sort by.",
                },
                {
                    "name": "sort_order",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string", "enum": ["asc", "desc"], "default": "desc"},
                    "description": "Sort direction.",
                },
            ],
            responses={
                "200": {
                    "description": "Paginated user analytics",
                    "content": {
                        "application/json": {
                            "example": {
                                "total": 18,
                                "rows": [
                                    {
                                        "user_id": 42,
                                        "user_email": "alice@example.com",
                                        "total_events": 320,
                                        "active_days": 14,
                                        "llm_events": 200,
                                        "tool_events": 90,
                                        "agent_events": 60,
                                        "chat_events": 45,
                                        "errors": 5,
                                        "total_tokens": 84500,
                                        "llm_cost": 0.00845,
                                    },
                                    {
                                        "user_id": 55,
                                        "user_email": "bob@example.com",
                                        "total_events": 180,
                                        "active_days": 10,
                                        "llm_events": 120,
                                        "tool_events": 40,
                                        "agent_events": 30,
                                        "chat_events": 25,
                                        "errors": 2,
                                        "total_tokens": 52000,
                                        "llm_cost": 0.0052,
                                    },
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
            GET /api/v2/elitea_core/analytics_users/prompt_lib/<project_id>

            Query params:
                date_from, date_to: ISO date range
                limit (int): page size, default 20, max 100
                offset (int): pagination offset, default 0
                search (str): filter by email (ILIKE)
                sort_by (str): column to sort, default "total_events"
                sort_order (str): "asc" or "desc", default "desc"
            """
            from tools import db
            from ...models.audit_event import AuditEvent

            dt_from, dt_to = _parse_dates(request.args)

            try:
                limit = min(int(request.args.get("limit", 20)), 100)
            except (ValueError, TypeError):
                limit = 20
            try:
                offset = max(int(request.args.get("offset", 0)), 0)
            except (ValueError, TypeError):
                offset = 0

            sort_by = request.args.get("sort_by", "total_events")
            if sort_by not in _SORT_WHITELIST:
                sort_by = "total_events"
            sort_order = request.args.get("sort_order", "desc")
            search = request.args.get("search", "").strip()

            try:
                with db.with_project_schema_session(None) as session:
                    base = session.query(AuditEvent).filter(
                        AuditEvent.project_id == project_id,
                        AuditEvent.user_id.isnot(None),
                        # Exclude system users from analytics (NULL-safe)
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
                    if search:
                        base = base.filter(
                            AuditEvent.user_email.ilike(f"%{search}%")
                        )

                    # Build aggregated subquery
                    total_events_col = func.count().label("total_events")
                    active_days_col = func.count(func.distinct(
                        cast(AuditEvent.timestamp, Date)
                    )).label("active_days")
                    llm_col = func.sum(case(
                        (AuditEvent.event_type == "llm", 1), else_=0,
                    )).label("llm_events")
                    tool_col = func.sum(case(
                        (AuditEvent.event_type == "tool", 1), else_=0,
                    )).label("tool_events")
                    agent_col = func.sum(case(
                        (AuditEvent.entity_type == "application", 1), else_=0,
                    )).label("agent_events")
                    chat_col = func.sum(case(
                        (AuditEvent.action == "SIO chat_predict", 1), else_=0,
                    )).label("chat_events")
                    errors_col = func.sum(case(
                        (AuditEvent.is_error.is_(True), 1), else_=0,
                    )).label("errors")
                    total_tokens_col = func.sum(
                        func.coalesce(AuditEvent.input_tokens, 0)
                        + func.coalesce(AuditEvent.output_tokens, 0)
                    ).label("total_tokens")
                    llm_cost_col = func.sum(AuditEvent.llm_cost).label("llm_cost")

                    query = base.with_entities(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                        total_events_col,
                        active_days_col,
                        llm_col,
                        tool_col,
                        agent_col,
                        chat_col,
                        errors_col,
                        total_tokens_col,
                        llm_cost_col,
                    ).group_by(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                    )

                    # Count total distinct users (for pagination)
                    count_q = base.with_entities(
                        func.count(func.distinct(AuditEvent.user_id))
                    ).scalar() or 0

                    # Sort
                    sort_map = {
                        "total_events": total_events_col,
                        "active_days": active_days_col,
                        "llm_events": llm_col,
                        "tool_events": tool_col,
                        "agent_events": agent_col,
                        "chat_events": chat_col,
                        "errors": errors_col,
                        "user_email": AuditEvent.user_email,
                        "total_tokens": total_tokens_col,
                        "llm_cost": llm_cost_col,
                    }
                    col = sort_map.get(sort_by, total_events_col)
                    order_fn = desc if sort_order == "desc" else asc
                    query = query.order_by(order_fn(col))

                    rows = query.offset(offset).limit(limit).all()

                    return {
                        "total": count_q,
                        "rows": [
                            {
                                "user_id": r.user_id,
                                "user_email": r.user_email,
                                "total_events": r.total_events,
                                "active_days": r.active_days,
                                "llm_events": r.llm_events or 0,
                                "tool_events": r.tool_events or 0,
                                "agent_events": r.agent_events or 0,
                                "chat_events": r.chat_events or 0,
                                "errors": r.errors or 0,
                                "total_tokens": r.total_tokens or 0,
                                "llm_cost": float(r.llm_cost) if r.llm_cost else 0.0,
                            }
                            for r in rows
                        ],
                    }, 200

            except Exception as e:
                log.error(f"Analytics users query failed: {e}")
                return {"error": "Failed to query analytics users"}, 500


    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '<int:project_id>',
        ])
        mode_handlers = {
            'prompt_lib': PromptLibAPI,
        }
else:
    API = None
