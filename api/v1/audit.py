"""
Audit trail API endpoint.

Provides paginated, filterable access to audit events for the admin UI.
"""

from pylon.core.tools import log

try:
    from tools import api_tools, auth, config as c
    _API_AVAILABLE = True
except ImportError:
    _API_AVAILABLE = False


if _API_AVAILABLE:
    from flask import request
    from sqlalchemy import desc, asc

    # Allowed sort fields (whitelist to prevent SQL injection via column names)
    _SORT_WHITELIST = frozenset([
        "timestamp", "user_email", "event_type", "action", "http_method",
        "status_code", "duration_ms", "project_id",
    ])

    class AdminAPI(api_tools.APIModeHandler):
        """Admin API for querying audit trail events."""

        @auth.decorators.check_api({
            "permissions": ["models.admin.audit_trail.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            }
        })
        def get(self, **kwargs):
            """
            GET /api/v1/elitea_core/audit/administration

            Query params:
                limit (int): max rows, default 50, max 200
                offset (int): pagination offset, default 0
                sort_by (str): column to sort, default "timestamp"
                sort_order (str): "asc" or "desc", default "desc"
                search (str): free-text on action, tool_name, user_email
                event_type (str): filter by event_type (api, socketio, rpc, agent, tool, llm)
                http_method (str): filter by HTTP method
                is_error (str): "true" to show only errors
                user_id (int): filter by user
                project_id (int): filter by project
                trace_id (str): filter by trace
                date_from (str): ISO datetime lower bound
                date_to (str): ISO datetime upper bound
            """
            from tools import db
            from ...models.audit_event import AuditEvent

            # Parse pagination
            try:
                limit = min(int(request.args.get("limit", 50)), 200)
            except (ValueError, TypeError):
                limit = 50
            try:
                offset = max(int(request.args.get("offset", 0)), 0)
            except (ValueError, TypeError):
                offset = 0

            # Parse sort
            sort_by = request.args.get("sort_by", "timestamp")
            if sort_by not in _SORT_WHITELIST:
                sort_by = "timestamp"
            sort_order = request.args.get("sort_order", "desc")

            try:
                with db.with_project_schema_session(None) as db_session:
                    query = db_session.query(AuditEvent)

                    # -- Filters --
                    search = request.args.get("search")
                    if search:
                        pattern = f"%{search}%"
                        query = query.filter(
                            AuditEvent.action.ilike(pattern)
                            | AuditEvent.tool_name.ilike(pattern)
                            | AuditEvent.user_email.ilike(pattern)
                            | AuditEvent.model_name.ilike(pattern)
                        )

                    event_type = request.args.get("event_type")
                    if event_type:
                        types = [t.strip() for t in event_type.split(",")]
                        if len(types) == 1:
                            query = query.filter(AuditEvent.event_type == types[0])
                        else:
                            query = query.filter(AuditEvent.event_type.in_(types))

                    http_method = request.args.get("http_method")
                    if http_method:
                        query = query.filter(AuditEvent.http_method == http_method.upper())

                    is_error = request.args.get("is_error")
                    if is_error and is_error.lower() == "true":
                        query = query.filter(AuditEvent.is_error.is_(True))

                    user_id = request.args.get("user_id")
                    if user_id:
                        try:
                            query = query.filter(AuditEvent.user_id == int(user_id))
                        except (ValueError, TypeError):
                            pass

                    project_id = request.args.get("project_id")
                    if project_id:
                        try:
                            query = query.filter(AuditEvent.project_id == int(project_id))
                        except (ValueError, TypeError):
                            pass

                    trace_id = request.args.get("trace_id")
                    if trace_id:
                        query = query.filter(AuditEvent.trace_id == trace_id)

                    date_from = request.args.get("date_from")
                    if date_from:
                        from datetime import datetime
                        try:
                            dt = datetime.fromisoformat(date_from)
                            query = query.filter(AuditEvent.timestamp >= dt)
                        except (ValueError, TypeError):
                            pass

                    date_to = request.args.get("date_to")
                    if date_to:
                        from datetime import datetime
                        try:
                            dt = datetime.fromisoformat(date_to)
                            query = query.filter(AuditEvent.timestamp <= dt)
                        except (ValueError, TypeError):
                            pass

                    duration_min = request.args.get("duration_min")
                    if duration_min:
                        try:
                            query = query.filter(
                                AuditEvent.duration_ms >= float(duration_min)
                            )
                        except (ValueError, TypeError):
                            pass

                    duration_max = request.args.get("duration_max")
                    if duration_max:
                        try:
                            query = query.filter(
                                AuditEvent.duration_ms < float(duration_max)
                            )
                        except (ValueError, TypeError):
                            pass

                    # -- Count --
                    total = query.count()

                    # -- Sort --
                    sort_col = getattr(AuditEvent, sort_by, AuditEvent.timestamp)
                    order_fn = desc if sort_order == "desc" else asc
                    query = query.order_by(order_fn(sort_col))

                    # -- Paginate --
                    rows = query.offset(offset).limit(limit).all()

                    return {
                        "total": total,
                        "rows": [self._serialize(row) for row in rows],
                    }, 200

            except Exception as e:
                log.error(f"Audit trail query failed: {e}")
                return {"error": "Failed to query audit trail"}, 500

        @staticmethod
        def _serialize(row):
            """Convert AuditEvent ORM instance to dict."""
            return {
                "id": row.id,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "user_id": row.user_id,
                "user_email": row.user_email,
                "project_id": row.project_id,
                "event_type": row.event_type,
                "action": row.action,
                "http_method": row.http_method,
                "http_route": row.http_route,
                "status_code": row.status_code,
                "duration_ms": round(row.duration_ms, 2) if row.duration_ms is not None else None,
                "is_error": row.is_error,
                "tool_name": row.tool_name,
                "model_name": row.model_name,
                "trace_id": row.trace_id,
                "span_id": row.span_id,
                "parent_span_id": row.parent_span_id,
            }

    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '',
        ])
        mode_handlers = {
            'administration': AdminAPI,
        }
else:
    API = None
