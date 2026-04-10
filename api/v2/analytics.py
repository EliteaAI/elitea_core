"""
Project-level analytics endpoint.

Aggregates audit_events data to provide KPIs, breakdowns, and trends
for the AI Adoption Analytics dashboard.
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
    from sqlalchemy import func, case, cast, Float, Date, String, or_

    def _parse_dates(args):
        """Parse date_from / date_to from request args, default to last 30 days."""
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

    def _apply_base_filters(session, AuditEvent, project_id, dt_from, dt_to):
        """Build base query with project + date filters, excluding system users."""
        base = session.query(AuditEvent).filter(
            AuditEvent.project_id == project_id,
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
        return base

    class PromptLibAPI(api_tools.APIModeHandler):
        """Project-level analytics for AI adoption dashboard."""

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
                    # Get actual project members to filter cross-project events
                    try:
                        actual_project_members = set()
                        project_user_data = auth.list_project_users(project_id)
                        if project_user_data:
                            for user_id in project_user_data:
                                try:
                                    user_details = auth.get_user(user_id)
                                    user_email = user_details.get('email', '') if user_details else ''
                                    # Only include non-system users
                                    if user_email and not any([
                                        user_email in ['system@centry.user'],
                                        user_email.startswith('system_user_') and user_email.endswith('@centry.user')
                                    ]):
                                        actual_project_members.add(user_id)
                                except:
                                    continue
                    except Exception:
                        actual_project_members = set()

                    base = _apply_base_filters(session, AuditEvent, project_id, dt_from, dt_to)
                    
                    # Filter to only include events from actual project members
                    if actual_project_members:
                        base = base.filter(AuditEvent.user_id.in_(actual_project_members))

                    # 1. KPIs
                    kpi_row = base.with_entities(
                        func.count().label("total_events"),
                        func.count(func.distinct(AuditEvent.user_id)).label("unique_users"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("error_count"),
                        func.count(func.distinct(AuditEvent.tool_name)).label("unique_tools"),
                        func.count(func.distinct(AuditEvent.model_name)).label("unique_models"),
                        func.sum(case(
                            (AuditEvent.event_type == "llm", 1), else_=0,
                        )).label("llm_calls"),
                        func.sum(case(
                            (AuditEvent.event_type == "tool", 1), else_=0,
                        )).label("tool_runs"),
                        func.sum(case(
                            (AuditEvent.action == "SIO chat_predict", 1), else_=0,
                        )).label("chat_msgs"),
                        func.sum(case(
                            (AuditEvent.entity_type == "application", 1), else_=0,
                        )).label("agent_runs"),
                    ).first()

                    total_events = kpi_row.total_events or 0
                    error_count = kpi_row.error_count or 0
                    unique_users = kpi_row.unique_users or 0

                    # AI-active users: those with at least one llm/tool event or application interaction
                    ai_active = base.filter(
                        or_(
                            AuditEvent.event_type.in_(["llm", "tool"]),
                            AuditEvent.entity_type == "application",
                        ),
                    ).with_entities(
                        func.count(func.distinct(AuditEvent.user_id)),
                    ).scalar() or 0

                    adoption_rate = round(ai_active / unique_users * 100, 1) if unique_users > 0 else 0

                    # Total project members (from auth/role system, includes users who never logged in)
                    # Filter out system users to match analytics filtering
                    try:
                        project_user_data = auth.list_project_users(project_id)
                        if project_user_data:
                            # project_user_data is a list of user IDs, need to get user details
                            filtered_users = []
                            for user_id in project_user_data:
                                try:
                                    # Get user details including email
                                    user_details = auth.get_user(user_id)
                                    user_email = user_details.get('email', '') if user_details else ''
                                    
                                    # Filter out system users by their email patterns
                                    if user_email and not any([
                                        user_email in ['system@centry.user'],
                                        user_email.startswith('system_user_') and user_email.endswith('@centry.user')
                                    ]):
                                        filtered_users.append(user_id)
                                except:
                                    # If we can't get user details, skip this user
                                    continue
                            total_project_users = len(filtered_users)
                        else:
                            total_project_users = 0
                    except Exception:
                        total_project_users = 0

                    kpis = {
                        "total_events": total_events,
                        "unique_users": unique_users,
                        "total_project_users": total_project_users,
                        "ai_active_users": ai_active,
                        "adoption_rate": adoption_rate,
                        "avg_duration_ms": round(kpi_row.avg_duration_ms, 1) if kpi_row.avg_duration_ms else 0,
                        "error_rate": round(error_count / total_events * 100, 2) if total_events > 0 else 0,
                        "error_count": error_count,
                        "unique_tools": kpi_row.unique_tools or 0,
                        "unique_models": kpi_row.unique_models or 0,
                        "llm_calls": kpi_row.llm_calls or 0,
                        "tool_runs": kpi_row.tool_runs or 0,
                        "chat_msgs": kpi_row.chat_msgs or 0,
                        "agent_runs": kpi_row.agent_runs or 0,
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

                    # 2b. Top AI active users (leaderboard)
                    ai_base = base.filter(
                        or_(
                            AuditEvent.event_type.in_(["llm", "tool"]),
                            AuditEvent.entity_type == "application",
                        ),
                    )
                    top_user_rows = ai_base.with_entities(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                        func.count().label("ai_events"),
                        func.sum(case(
                            (AuditEvent.event_type == "llm", 1), else_=0,
                        )).label("llm_calls"),
                        func.sum(case(
                            (AuditEvent.event_type == "tool", 1), else_=0,
                        )).label("tool_runs"),
                        func.sum(case(
                            (AuditEvent.entity_type == "application", 1), else_=0,
                        )).label("agent_runs"),
                    ).group_by(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                    ).order_by(func.count().desc()).limit(5).all()

                    top_ai_users = [
                        {
                            "user_id": r.user_id,
                            "user_email": r.user_email,
                            "ai_events": r.ai_events,
                            "llm_calls": r.llm_calls or 0,
                            "tool_runs": r.tool_runs or 0,
                            "agent_runs": r.agent_runs or 0,
                        }
                        for r in top_user_rows
                    ]

                    # 3. Daily activity
                    daily_rows = base.with_entities(
                        cast(AuditEvent.timestamp, Date).label("day"),
                        func.count().label("events"),
                        func.count(func.distinct(AuditEvent.user_id)).label("users"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                    ).group_by("day").order_by("day").all()

                    daily_activity = [
                        {
                            "date": r.day.isoformat() if r.day else None,
                            "events": r.events,
                            "users": r.users,
                            "errors": r.errors or 0,
                        }
                        for r in daily_rows
                    ]

                    # 4. Top tools
                    tool_rows = base.with_entities(
                        AuditEvent.tool_name,
                        func.count().label("calls"),
                        func.count(func.distinct(AuditEvent.user_id)).label("users"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                    ).filter(
                        AuditEvent.tool_name.isnot(None),
                        AuditEvent.tool_name != "",
                    ).group_by(
                        AuditEvent.tool_name,
                    ).order_by(func.count().desc()).limit(30).all()

                    tools = [
                        {
                            "tool_name": r.tool_name,
                            "calls": r.calls,
                            "users": r.users,
                            "avg_duration_ms": round(r.avg_duration_ms, 1) if r.avg_duration_ms else 0,
                            "errors": r.errors or 0,
                        }
                        for r in tool_rows
                    ]

                    # 5. Model usage
                    model_rows = base.with_entities(
                        AuditEvent.model_name,
                        func.count().label("calls"),
                        func.count(func.distinct(AuditEvent.user_id)).label("users"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                    ).filter(
                        AuditEvent.model_name.isnot(None),
                        AuditEvent.model_name != "",
                    ).group_by(
                        AuditEvent.model_name,
                    ).order_by(func.count().desc()).limit(20).all()

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

                    models = [
                        {
                            "model_name": r.model_name,
                            "display_name": model_display_names.get(r.model_name, r.model_name),
                            "calls": r.calls,
                            "users": r.users,
                            "avg_duration_ms": round(r.avg_duration_ms, 1) if r.avg_duration_ms else 0,
                        }
                        for r in model_rows
                    ]

                    # 6. Agents / Applications activity
                    # Aggregate by entity_name for entity_type='application',
                    # and also capture agent-like actions from socketio events
                    agent_rows = base.with_entities(
                        AuditEvent.entity_name,
                        AuditEvent.entity_id,
                        func.count().label("events"),
                        func.count(func.distinct(AuditEvent.user_id)).label("users"),
                        func.avg(AuditEvent.duration_ms).label("avg_duration_ms"),
                        func.sum(case(
                            (AuditEvent.is_error.is_(True), 1), else_=0,
                        )).label("errors"),
                    ).filter(
                        AuditEvent.entity_type == "application",
                        AuditEvent.entity_id.isnot(None),
                    ).group_by(
                        AuditEvent.entity_name,
                        AuditEvent.entity_id,
                    ).order_by(func.count().desc()).limit(30).all()

                    agents = [
                        {
                            "entity_name": r.entity_name or f"Agent #{r.entity_id}",
                            "entity_id": r.entity_id,
                            "events": r.events,
                            "users": r.users,
                            "avg_duration_ms": round(r.avg_duration_ms, 1) if r.avg_duration_ms else 0,
                            "errors": r.errors or 0,
                        }
                        for r in agent_rows
                    ]

                    # Also get chat session counts per application (from socketio predict events)
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

                    # 7. Health
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
                        "top_ai_users": top_ai_users,
                        "daily_activity": daily_activity,
                        "tools": tools,
                        "models": models,
                        "agents": agents,
                        "chat_sessions": chat_sessions,
                        "health": health,
                    }, 200

            except Exception as e:
                log.error(f"Analytics query failed: {e}")
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
            dt_from, dt_to = _parse_dates(request.args)

            try:
                with db.with_project_schema_session(None) as session:
                    base = session.query(AuditEvent)
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

            except Exception as e:
                log.error(f"Admin analytics query failed: {e}")
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
