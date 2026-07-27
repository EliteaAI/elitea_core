"""
Paginated analytics agents endpoint.

Provides server-side pagination, search, and sorting for agent/application usage data.
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
    from sqlalchemy import func, case, cast, Date, desc, asc
    from sqlalchemy.orm import aliased

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
        "events", "users", "avg_duration_ms", "errors", "entity_name",
        "total_tokens", "llm_cost",
    ])

    class PromptLibAPI(api_tools.APIModeHandler):
        """Paginated agent/application usage for analytics."""

        @register_openapi(
            name="List Agent Analytics",
            description=(
                "Returns paginated agent/application usage statistics with optional "
                "date filtering, search by name, sorting, and a daily chat-message trend."
            ),
            mcp_tool=True,
            mcp_description="Use this tool when you need a leaderboard or paginated comparison of agents/applications in a project, with search and sorting. Do not use this tool when you need the full breakdown for one specific agent — use Get Agent Analytics Detail. Do not use for overall project KPIs — use Get Project Analytics Overview. This is the primary discovery/list endpoint for agent-level analytics.",
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
                    "description": "Filter by agent name (case-insensitive partial match).",
                },
                {
                    "name": "sort_by",
                    "in": "query",
                    "required": False,
                    "schema": {
                        "type": "string",
                        "enum": ["events", "users", "avg_duration_ms", "errors", "entity_name", "total_tokens", "llm_cost"],
                        "default": "events",
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
                    "description": "Paginated agent analytics",
                    "content": {
                        "application/json": {
                            "example": {
                                "total": 5,
                                "rows": [
                                    {
                                        "entity_name": "Code Review Bot",
                                        "entity_id": 7,
                                        "events": 95,
                                        "users": 5,
                                        "avg_duration_ms": 1200.0,
                                        "errors": 4,
                                        "total_tokens": 84500,
                                        "llm_cost": 0.00845,
                                    },
                                    {
                                        "entity_name": "SQL Assistant",
                                        "entity_id": 12,
                                        "events": 60,
                                        "users": 3,
                                        "avg_duration_ms": 740.0,
                                        "errors": 1,
                                        "total_tokens": 42000,
                                        "llm_cost": 0.0042,
                                    },
                                ],
                                "chat_daily": [
                                    {"date": "2025-01-15", "messages": 32},
                                    {"date": "2025-01-16", "messages": 45},
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
            GET /api/v2/elitea_core/analytics_agents/prompt_lib/<project_id>

            Query params:
                date_from, date_to: ISO date range
                limit (int): page size, default 20, max 100
                offset (int): pagination offset, default 0
                search (str): filter by entity_name (ILIKE)
                sort_by (str): column to sort, default "events"
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

            sort_by = request.args.get("sort_by", "events")
            if sort_by not in _SORT_WHITELIST:
                sort_by = "events"
            sort_order = request.args.get("sort_order", "desc")
            search = request.args.get("search", "").strip()

            try:
                with db.with_project_schema_session(None) as session:
                    base = session.query(AuditEvent).filter(
                        AuditEvent.entity_type == "application",
                        AuditEvent.entity_id.isnot(None),
                    )
                    # Filter by project_id if provided (agents may span projects)
                    if project_id:
                        base = base.filter(AuditEvent.project_id == project_id)
                    if dt_from:
                        base = base.filter(AuditEvent.timestamp >= dt_from)
                    if dt_to:
                        base = base.filter(AuditEvent.timestamp <= dt_to)
                    if search:
                        base = base.filter(
                            AuditEvent.entity_name.ilike(f"%{search}%")
                        )

                    # Per-agent LLM cost/tokens can't be read off the application
                    # events themselves: cost/token data lives on generation spans
                    # that carry no entity_type/entity_id, only user + model attrs.
                    # We correlate each llm event to the agent that produced it via
                    # shared trace_id (same approach analytics_costs uses), building
                    # a per-entity cost map we LEFT JOIN below so the DB can still
                    # sort by llm_cost / total_tokens.
                    app_traces = session.query(
                        AuditEvent.trace_id.label("trace_id"),
                        func.min(AuditEvent.entity_id).label("entity_id"),
                    ).filter(
                        AuditEvent.entity_type == "application",
                        AuditEvent.entity_id.isnot(None),
                        AuditEvent.trace_id.isnot(None),
                        AuditEvent.trace_id != "",
                    )
                    if project_id:
                        app_traces = app_traces.filter(
                            AuditEvent.project_id == project_id
                        )
                    app_traces = app_traces.group_by(AuditEvent.trace_id).subquery()

                    llm_ev = aliased(AuditEvent)
                    cost_map = session.query(
                        app_traces.c.entity_id.label("entity_id"),
                        func.sum(llm_ev.llm_cost).label("llm_cost"),
                        func.sum(
                            func.coalesce(llm_ev.input_tokens, 0)
                            + func.coalesce(llm_ev.output_tokens, 0)
                        ).label("total_tokens"),
                    ).select_from(llm_ev).join(
                        app_traces, llm_ev.trace_id == app_traces.c.trace_id,
                    ).filter(
                        llm_ev.event_type == "llm",
                    )
                    if project_id:
                        cost_map = cost_map.filter(llm_ev.project_id == project_id)
                    cost_map = cost_map.group_by(app_traces.c.entity_id).subquery()

                    events_col = func.count().label("events")
                    users_col = func.count(
                        func.distinct(AuditEvent.user_id)
                    ).label("users")
                    avg_dur_col = func.avg(
                        AuditEvent.duration_ms
                    ).label("avg_duration_ms")
                    errors_col = func.sum(case(
                        (AuditEvent.is_error.is_(True), 1), else_=0,
                    )).label("errors")
                    # func.max over the joined cost-map columns: the LEFT JOIN pairs
                    # each of an entity's application events with that entity's single
                    # cost-map row, so max() reads the value without inflating it by
                    # the application-event count (sum() would multiply it).
                    total_tokens_col = func.coalesce(
                        func.max(cost_map.c.total_tokens), 0
                    ).label("total_tokens")
                    llm_cost_col = func.coalesce(
                        func.max(cost_map.c.llm_cost), 0
                    ).label("llm_cost")

                    query = base.outerjoin(
                        cost_map, AuditEvent.entity_id == cost_map.c.entity_id,
                    ).with_entities(
                        func.max(AuditEvent.entity_name).label("entity_name"),
                        AuditEvent.entity_id,
                        events_col,
                        users_col,
                        avg_dur_col,
                        errors_col,
                        total_tokens_col,
                        llm_cost_col,
                    ).group_by(
                        AuditEvent.entity_id,
                    )

                    # Count total distinct agents (for pagination)
                    count_q = base.with_entities(
                        func.count(func.distinct(AuditEvent.entity_id))
                    ).scalar() or 0

                    # Sort
                    name_col = func.max(AuditEvent.entity_name).label("entity_name")
                    sort_map = {
                        "events": events_col,
                        "users": users_col,
                        "avg_duration_ms": avg_dur_col,
                        "errors": errors_col,
                        "entity_name": name_col,
                        "total_tokens": total_tokens_col,
                        "llm_cost": llm_cost_col,
                    }
                    col = sort_map.get(sort_by, events_col)
                    order_fn = desc if sort_order == "desc" else asc
                    query = query.order_by(order_fn(col))

                    rows = query.offset(offset).limit(limit).all()

                    # Daily chat messages (SIO chat_predict) for the chart
                    chat_base = session.query(AuditEvent).filter(
                        AuditEvent.action == "SIO chat_predict",
                    )
                    if project_id:
                        chat_base = chat_base.filter(AuditEvent.project_id == project_id)
                    if dt_from:
                        chat_base = chat_base.filter(AuditEvent.timestamp >= dt_from)
                    if dt_to:
                        chat_base = chat_base.filter(AuditEvent.timestamp <= dt_to)

                    chat_daily_rows = chat_base.with_entities(
                        cast(AuditEvent.timestamp, Date).label("day"),
                        func.count().label("messages"),
                    ).group_by("day").order_by("day").all()

                    return {
                        "total": count_q,
                        "rows": [
                            {
                                "entity_name": r.entity_name or f"Agent #{r.entity_id}",
                                "entity_id": r.entity_id,
                                "events": r.events,
                                "users": r.users,
                                "avg_duration_ms": round(r.avg_duration_ms, 1) if r.avg_duration_ms else 0,
                                "errors": r.errors or 0,
                                "total_tokens": r.total_tokens or 0,
                                "llm_cost": float(r.llm_cost) if r.llm_cost else 0.0,
                            }
                            for r in rows
                        ],
                        "chat_daily": [
                            {
                                "date": r.day.isoformat() if r.day else None,
                                "messages": r.messages,
                            }
                            for r in chat_daily_rows
                        ],
                    }, 200

            except Exception as e:
                log.error(f"Analytics agents query failed: {e}")
                return {"error": "Failed to query analytics agents"}, 500


    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '<int:project_id>',
        ])
        mode_handlers = {
            'prompt_lib': PromptLibAPI,
        }
else:
    API = None
