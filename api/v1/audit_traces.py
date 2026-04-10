"""
Audit trail trace-level list endpoint.

Returns traces (grouped by trace_id) with aggregated metrics,
paginated and filterable for the admin UI.
"""

from pylon.core.tools import log

try:
    from tools import api_tools, auth, config as c
    _API_AVAILABLE = True
except ImportError:
    _API_AVAILABLE = False


if _API_AVAILABLE:
    from flask import request
    from sqlalchemy import desc, asc, func, case, literal_column, Float

    # Allowed sort fields for trace-level queries
    _SORT_WHITELIST = frozenset([
        "start_time", "duration_ms", "span_count", "user_email", "project_id",
    ])

    class AdminAPI(api_tools.APIModeHandler):
        """Admin API for querying audit trail traces (grouped by trace_id)."""

        @auth.decorators.check_api({
            "permissions": ["models.admin.audit_trail.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            }
        })
        def get(self, **kwargs):
            """
            GET /api/v1/elitea_core/audit_traces/administration

            Returns traces grouped by trace_id with aggregate metrics.

            Query params:
                limit (int): max rows, default 50, max 200
                offset (int): pagination offset, default 0
                sort_by (str): column to sort, default "start_time"
                sort_order (str): "asc" or "desc", default "desc"
                search (str): free-text on action, tool_name, user_email
                event_type (str): filter by event_type
                is_error (str): "true" to show only traces with errors
                user_id (int): filter by user
                project_id (int): filter by project
                trace_id (str): filter by specific trace
                date_from (str): ISO datetime lower bound
                date_to (str): ISO datetime upper bound
                duration_min (float): min trace duration in ms
                duration_max (float): max trace duration in ms
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
            sort_by = request.args.get("sort_by", "start_time")
            if sort_by not in _SORT_WHITELIST:
                sort_by = "start_time"
            sort_order = request.args.get("sort_order", "desc")

            try:
                with db.with_project_schema_session(None) as db_session:
                    # -- Build base query with filters --
                    query = db_session.query(AuditEvent).filter(
                        AuditEvent.trace_id.isnot(None)
                    )

                    # -- Filters (same as audit.py) --
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

                    trace_id_filter = request.args.get("trace_id")
                    if trace_id_filter:
                        query = query.filter(AuditEvent.trace_id == trace_id_filter)

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

                    # -- Aggregate by trace_id --
                    trace_duration = (
                        (func.max(
                            func.extract('epoch', AuditEvent.timestamp)
                            + func.coalesce(AuditEvent.duration_ms, literal_column('0'))
                            / literal_column('1000')
                        ) - func.min(
                            func.extract('epoch', AuditEvent.timestamp)
                        )) * literal_column('1000')
                    ).cast(Float).label('duration_ms')

                    trace_query = query.with_entities(
                        AuditEvent.trace_id,
                        func.min(AuditEvent.timestamp).label('start_time'),
                        trace_duration,
                        func.count().label('span_count'),
                        func.count(case(
                            (AuditEvent.is_error.is_(True), 1),
                        )).label('error_count'),
                        func.bool_or(AuditEvent.is_error).label('has_error'),
                        func.min(AuditEvent.user_email).label('user_email'),
                        func.min(AuditEvent.project_id).label('project_id'),
                        func.array_agg(func.distinct(AuditEvent.event_type)).label('event_types'),
                    ).group_by(AuditEvent.trace_id)

                    # -- Post-aggregation filters (duration) --
                    duration_min = request.args.get("duration_min")
                    duration_max = request.args.get("duration_max")

                    # Wrap in subquery for count + sort + pagination + duration filter
                    trace_subq = trace_query.subquery('traces')

                    outer_query = db_session.query(trace_subq)

                    if duration_min:
                        try:
                            outer_query = outer_query.filter(
                                trace_subq.c.duration_ms >= float(duration_min)
                            )
                        except (ValueError, TypeError):
                            pass

                    if duration_max:
                        try:
                            outer_query = outer_query.filter(
                                trace_subq.c.duration_ms < float(duration_max)
                            )
                        except (ValueError, TypeError):
                            pass

                    # -- Count --
                    total = outer_query.count()

                    # -- Sort --
                    sort_col = getattr(trace_subq.c, sort_by, trace_subq.c.start_time)
                    order_fn = desc if sort_order == "desc" else asc
                    outer_query = outer_query.order_by(order_fn(sort_col))

                    # -- Paginate --
                    rows = outer_query.offset(offset).limit(limit).all()

                    # -- Resolve root span info for current page --
                    trace_ids = [row.trace_id for row in rows]
                    root_map = {}

                    if trace_ids:
                        # Primary: spans with parent_span_id IS NULL (root spans)
                        root_spans = db_session.query(
                            AuditEvent.trace_id,
                            AuditEvent.action,
                            AuditEvent.event_type,
                            AuditEvent.http_method,
                            AuditEvent.status_code,
                        ).filter(
                            AuditEvent.trace_id.in_(trace_ids),
                            AuditEvent.parent_span_id.is_(None),
                        ).all()

                        for r in root_spans:
                            root_map[r.trace_id] = {
                                'action': r.action,
                                'event_type': r.event_type,
                                'http_method': r.http_method,
                                'status_code': r.status_code,
                            }

                        # Fallback: traces without a root span — use earliest span
                        missing = set(trace_ids) - set(root_map.keys())
                        if missing:
                            from sqlalchemy.orm import aliased
                            # Get the earliest span per missing trace
                            for tid in missing:
                                earliest = db_session.query(
                                    AuditEvent.action,
                                    AuditEvent.event_type,
                                    AuditEvent.http_method,
                                    AuditEvent.status_code,
                                ).filter(
                                    AuditEvent.trace_id == tid,
                                ).order_by(
                                    asc(AuditEvent.timestamp)
                                ).first()
                                if earliest:
                                    root_map[tid] = {
                                        'action': earliest.action,
                                        'event_type': earliest.event_type,
                                        'http_method': earliest.http_method,
                                        'status_code': earliest.status_code,
                                    }

                    return {
                        "total": total,
                        "rows": [self._serialize(row, root_map) for row in rows],
                    }, 200

            except Exception as e:
                log.error(f"Audit trace list query failed: {e}")
                return {"error": "Failed to query audit traces"}, 500

        @staticmethod
        def _serialize(row, root_map):
            """Convert aggregated trace row to dict."""
            root_info = root_map.get(row.trace_id, {})
            return {
                "trace_id": row.trace_id,
                "start_time": row.start_time.isoformat() if row.start_time else None,
                "duration_ms": round(row.duration_ms, 2) if row.duration_ms is not None else None,
                "span_count": row.span_count,
                "error_count": row.error_count,
                "has_error": bool(row.has_error),
                "user_email": row.user_email,
                "project_id": row.project_id,
                "event_types": sorted(row.event_types) if row.event_types else [],
                "root_action": root_info.get('action'),
                "root_event_type": root_info.get('event_type'),
                "root_http_method": root_info.get('http_method'),
                "root_status_code": root_info.get('status_code'),
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
