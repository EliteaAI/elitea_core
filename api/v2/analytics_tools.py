"""
Paginated analytics tools endpoint.

Provides server-side pagination, search, and sorting for tool usage data.
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
    from sqlalchemy import func, case, cast, Date, desc, asc

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
        "calls", "users", "avg_duration_ms", "errors", "tool_name",
    ])

    class PromptLibAPI(api_tools.APIModeHandler):
        """Paginated tool usage for analytics."""

        @auth.decorators.check_api({
            "permissions": ["models.monitoring.tracing.view"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            }
        })
        def get(self, project_id: int, **kwargs):
            """
            GET /api/v2/elitea_core/analytics_tools/prompt_lib/<project_id>

            Query params:
                date_from, date_to: ISO date range
                limit (int): page size, default 20, max 100
                offset (int): pagination offset, default 0
                search (str): filter by tool_name (ILIKE)
                sort_by (str): column to sort, default "calls"
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

            sort_by = request.args.get("sort_by", "calls")
            if sort_by not in _SORT_WHITELIST:
                sort_by = "calls"
            sort_order = request.args.get("sort_order", "desc")
            search = request.args.get("search", "").strip()

            try:
                with db.with_project_schema_session(None) as session:
                    base = session.query(AuditEvent).filter(
                        AuditEvent.project_id == project_id,
                        AuditEvent.tool_name.isnot(None),
                        AuditEvent.tool_name != "",
                    )
                    if dt_from:
                        base = base.filter(AuditEvent.timestamp >= dt_from)
                    if dt_to:
                        base = base.filter(AuditEvent.timestamp <= dt_to)
                    if search:
                        base = base.filter(
                            AuditEvent.tool_name.ilike(f"%{search}%")
                        )

                    calls_col = func.count().label("calls")
                    users_col = func.count(
                        func.distinct(AuditEvent.user_id)
                    ).label("users")
                    avg_dur_col = func.avg(
                        AuditEvent.duration_ms
                    ).label("avg_duration_ms")
                    errors_col = func.sum(case(
                        (AuditEvent.is_error.is_(True), 1), else_=0,
                    )).label("errors")

                    query = base.with_entities(
                        AuditEvent.tool_name,
                        calls_col,
                        users_col,
                        avg_dur_col,
                        errors_col,
                    ).group_by(
                        AuditEvent.tool_name,
                    )

                    # Count total distinct tools (for pagination)
                    count_q = base.with_entities(
                        func.count(func.distinct(AuditEvent.tool_name))
                    ).scalar() or 0

                    # Sort
                    sort_map = {
                        "calls": calls_col,
                        "users": users_col,
                        "avg_duration_ms": avg_dur_col,
                        "errors": errors_col,
                        "tool_name": AuditEvent.tool_name,
                    }
                    col = sort_map.get(sort_by, calls_col)
                    order_fn = desc if sort_order == "desc" else asc
                    query = query.order_by(order_fn(col))

                    rows = query.offset(offset).limit(limit).all()

                    return {
                        "total": count_q,
                        "rows": [
                            {
                                "tool_name": r.tool_name,
                                "calls": r.calls,
                                "users": r.users,
                                "avg_duration_ms": round(r.avg_duration_ms, 1) if r.avg_duration_ms else 0,
                                "errors": r.errors or 0,
                            }
                            for r in rows
                        ],
                    }, 200

            except Exception as e:
                log.error(f"Analytics tools query failed: {e}")
                return {"error": "Failed to query analytics tools"}, 500


    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '<int:project_id>',
        ])
        mode_handlers = {
            'prompt_lib': PromptLibAPI,
        }
else:
    API = None
