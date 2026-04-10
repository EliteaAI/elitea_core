"""
Paginated analytics users endpoint.

Provides server-side pagination, search, and sorting for user activity data.
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
        "total_events", "active_days", "llm_events", "tool_events",
        "agent_events", "chat_events", "errors", "user_email",
    ])

    class PromptLibAPI(api_tools.APIModeHandler):
        """Paginated user activity for analytics."""

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
                    # Get actual project members to filter cross-project events
                    actual_project_members = set()
                    try:
                        project_user_data = auth.list_project_users(project_id)
                        if project_user_data:
                            for user_id_item in project_user_data:
                                try:
                                    user_details = auth.get_user(user_id_item)
                                    user_email = user_details.get('email', '') if user_details else ''
                                    if user_email and not any([
                                        user_email in ['system@centry.user'],
                                        user_email.startswith('system_user_') and user_email.endswith('@centry.user')
                                    ]):
                                        actual_project_members.add(user_id_item)
                                except Exception:
                                    continue
                    except Exception as e:
                        log.error(f"Failed to get project users for project {project_id}: {e}")
                        from flask import g
                        try:
                            if hasattr(g, 'auth') and g.auth and hasattr(g.auth, 'user_id'):
                                actual_project_members = {g.auth.user_id}
                        except Exception:
                            actual_project_members = set()

                    base = session.query(AuditEvent).filter(
                        AuditEvent.project_id == project_id,
                        AuditEvent.user_id.isnot(None),
                        # Exclude system users from analytics
                        ~AuditEvent.user_email.in_([
                            'system@centry.user'
                        ]),
                        ~AuditEvent.user_email.like('system_user_%@centry.user'),
                    )

                    # Filter to only actual project members
                    if actual_project_members:
                        base = base.filter(AuditEvent.user_id.in_(actual_project_members))
                    else:
                        base = base.filter(AuditEvent.user_id.is_(None))

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
