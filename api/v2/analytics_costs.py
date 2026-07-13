"""
Analytics cost breakdown endpoint.

Returns LLM cost distribution by model, by agent, by user, and daily trend.
All values are summed from the llm_cost column written at event time.
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

    from ...utils.constants import (
        DEFAULT_DATE_RANGE_DAYS,
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
            dt_from = dt_to - timedelta(days=DEFAULT_DATE_RANGE_DAYS)
        elif not dt_from:
            dt_from = dt_to - timedelta(days=DEFAULT_DATE_RANGE_DAYS)
        elif not dt_to:
            dt_to = datetime.now(timezone.utc)
        return dt_from, dt_to

    class PromptLibAPI(api_tools.APIModeHandler):
        """LLM cost breakdown analytics for the project."""

        @register_openapi(
            name="Get Analytics Cost Breakdown",
            description=(
                "Returns LLM cost distribution by model, by agent, by user, and a daily "
                "cost trend. Values are aggregated from llm_cost recorded per LLM call."
            ),
            tags=["elitea_core/analytics"],
            parameters=[
                {
                    "name": "date_from",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string", "format": "date-time"},
                    "description": "Start datetime (ISO 8601). Defaults to 7 days ago.",
                },
                {
                    "name": "date_to",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string", "format": "date-time"},
                    "description": "End datetime (ISO 8601). Defaults to now.",
                },
            ],
            responses={
                "200": {
                    "description": "LLM cost breakdown",
                    "content": {
                        "application/json": {
                            "example": {
                                "kpis": {
                                    "total_cost": 12.45,
                                    "total_input_tokens": 4500000,
                                    "total_output_tokens": 980000,
                                    "total_tokens": 5480000,
                                    "avg_cost_per_call": 0.0016,
                                },
                                "by_model": [
                                    {
                                        "model_name": "gpt-4o",
                                        "display_name": "GPT-4o",
                                        "calls": 450,
                                        "input_tokens": 3200000,
                                        "output_tokens": 720000,
                                        "total_cost": 9.80,
                                    }
                                ],
                                "by_agent": [
                                    {
                                        "entity_name": "Code Review Bot",
                                        "entity_id": 7,
                                        "total_cost": 4.20,
                                        "total_tokens": 2100000,
                                    }
                                ],
                                "by_user": [
                                    {
                                        "user_id": 42,
                                        "user_email": "alice@example.com",
                                        "total_cost": 3.10,
                                        "total_tokens": 1550000,
                                    }
                                ],
                                "daily": [
                                    {
                                        "date": "2025-01-15",
                                        "total_cost": 1.80,
                                        "total_tokens": 900000,
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
            from tools import db, rpc_tools
            from ...models.audit_event import AuditEvent

            dt_from, dt_to = _parse_dates(request.args)

            try:
                with db.with_project_schema_session(None) as session:
                    from sqlalchemy import or_
                    base = session.query(AuditEvent).filter(
                        AuditEvent.project_id == project_id,
                        AuditEvent.event_type == "llm",
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

                    # Overall KPIs
                    kpi = base.with_entities(
                        func.sum(AuditEvent.llm_cost).label("total_cost"),
                        func.sum(AuditEvent.input_tokens).label("total_input_tokens"),
                        func.sum(AuditEvent.output_tokens).label("total_output_tokens"),
                        func.sum(
                            func.coalesce(AuditEvent.input_tokens, 0)
                            + func.coalesce(AuditEvent.output_tokens, 0)
                        ).label("total_tokens"),
                        func.count().label("total_calls"),
                    ).first()

                    total_cost = float(kpi.total_cost) if kpi and kpi.total_cost else 0.0
                    total_calls = kpi.total_calls or 0

                    kpis = {
                        "total_cost": round(total_cost, 6),
                        "total_input_tokens": kpi.total_input_tokens or 0,
                        "total_output_tokens": kpi.total_output_tokens or 0,
                        "total_tokens": kpi.total_tokens or 0,
                        "avg_cost_per_call": round(total_cost / total_calls, 8) if total_calls > 0 else 0.0,
                    }

                    # Get model display names
                    model_display_names = {}
                    try:
                        models_response = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_models(
                            project_id=project_id, section='llm', include_shared=True
                        )
                        items = models_response.get('items', []) if models_response else []
                        for item in items:
                            if isinstance(item, dict) and 'name' in item:
                                model_display_names[item['name']] = item.get('display_name', item['name'])
                    except Exception as e:
                        log.warning(f"Failed to get model configurations for cost breakdown: {e}")

                    # Cost by model
                    model_rows = base.with_entities(
                        AuditEvent.model_name,
                        func.count().label("calls"),
                        func.sum(AuditEvent.input_tokens).label("input_tokens"),
                        func.sum(AuditEvent.output_tokens).label("output_tokens"),
                        func.sum(AuditEvent.llm_cost).label("total_cost"),
                    ).filter(
                        AuditEvent.model_name.isnot(None),
                        AuditEvent.model_name != "",
                    ).group_by(AuditEvent.model_name).order_by(
                        func.sum(AuditEvent.llm_cost).desc()
                    ).limit(30).all()

                    by_model = [
                        {
                            "model_name": r.model_name,
                            "display_name": model_display_names.get(r.model_name, r.model_name),
                            "calls": r.calls,
                            "input_tokens": r.input_tokens or 0,
                            "output_tokens": r.output_tokens or 0,
                            "total_cost": round(float(r.total_cost), 6) if r.total_cost else 0.0,
                        }
                        for r in model_rows
                    ]

                    # Cost by agent (application)
                    agent_rows = base.with_entities(
                        AuditEvent.entity_name,
                        AuditEvent.entity_id,
                        func.sum(AuditEvent.llm_cost).label("total_cost"),
                        func.sum(
                            func.coalesce(AuditEvent.input_tokens, 0)
                            + func.coalesce(AuditEvent.output_tokens, 0)
                        ).label("total_tokens"),
                    ).filter(
                        AuditEvent.entity_type == "application",
                        AuditEvent.entity_id.isnot(None),
                    ).group_by(
                        AuditEvent.entity_name, AuditEvent.entity_id
                    ).order_by(func.sum(AuditEvent.llm_cost).desc()).limit(20).all()

                    by_agent = [
                        {
                            "entity_name": r.entity_name or f"Agent #{r.entity_id}",
                            "entity_id": r.entity_id,
                            "total_cost": round(float(r.total_cost), 6) if r.total_cost else 0.0,
                            "total_tokens": r.total_tokens or 0,
                        }
                        for r in agent_rows
                    ]

                    # Cost by user
                    user_rows = base.with_entities(
                        AuditEvent.user_id,
                        AuditEvent.user_email,
                        func.sum(AuditEvent.llm_cost).label("total_cost"),
                        func.sum(
                            func.coalesce(AuditEvent.input_tokens, 0)
                            + func.coalesce(AuditEvent.output_tokens, 0)
                        ).label("total_tokens"),
                    ).filter(
                        AuditEvent.user_id.isnot(None),
                    ).group_by(
                        AuditEvent.user_id, AuditEvent.user_email
                    ).order_by(func.sum(AuditEvent.llm_cost).desc()).limit(20).all()

                    by_user = [
                        {
                            "user_id": r.user_id,
                            "user_email": r.user_email,
                            "total_cost": round(float(r.total_cost), 6) if r.total_cost else 0.0,
                            "total_tokens": r.total_tokens or 0,
                        }
                        for r in user_rows
                    ]

                    # Daily cost trend
                    daily_rows = base.with_entities(
                        cast(AuditEvent.timestamp, Date).label("day"),
                        func.sum(AuditEvent.llm_cost).label("total_cost"),
                        func.sum(
                            func.coalesce(AuditEvent.input_tokens, 0)
                            + func.coalesce(AuditEvent.output_tokens, 0)
                        ).label("total_tokens"),
                    ).group_by("day").order_by("day").all()

                    daily = [
                        {
                            "date": r.day.isoformat() if r.day else None,
                            "total_cost": round(float(r.total_cost), 6) if r.total_cost else 0.0,
                            "total_tokens": r.total_tokens or 0,
                        }
                        for r in daily_rows
                    ]

                    return {
                        "kpis": kpis,
                        "by_model": by_model,
                        "by_agent": by_agent,
                        "by_user": by_user,
                        "daily": daily,
                    }, 200

            except Exception as e:
                log.error(f"Analytics cost query failed: {e}")
                return {"error": "Failed to query analytics costs"}, 500


    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '<int:project_id>',
        ])
        mode_handlers = {
            'prompt_lib': PromptLibAPI,
        }
else:
    API = None
