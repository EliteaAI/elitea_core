"""
Generic + AI active-users trend (#5110), bucketed by day/week/month and optionally
restricted to one or more project roles.

The generic half is distinct users with at least one audit_events row in the bucket,
reusing analytics.py's own base filters (project scope, date range, project-member scope,
system/service-account exclusion). The AI-active-users half lives over the usage plugin's
usage_event table and is fetched through an RPC rather than an ORM import - #6574 forbids
one plugin defining another plugin's table - so it degrades to zero when that plugin is
disabled, slow, or erroring, rather than failing this endpoint. See
usage/api/v2/analytics_activity.py for the RPC's own bucketed endpoint over usage_event.
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
    from sqlalchemy import func, distinct

    from .analytics import _apply_base_filters, _project_member_ids
    from ...utils.date_range import parse_date_range as _parse_dates

    GRANULARITY_DAY = "day"
    GRANULARITY_WEEK = "week"
    GRANULARITY_MONTH = "month"
    _GRANULARITIES = (GRANULARITY_DAY, GRANULARITY_WEEK, GRANULARITY_MONTH)

    def _parse_granularity(args):
        """day | week | month, defaulted to day. An unrecognised value defaults rather than
        reaching _bucket_expr, so a bad query param can never select SQL by string.
        """
        value = (args.get("granularity") or GRANULARITY_DAY).strip().lower()
        return value if value in _GRANULARITIES else GRANULARITY_DAY

    def _bucket_expr(AuditEvent, granularity):
        """The group-by expression for an already-validated granularity: a lookup into a
        fixed dict of pre-built expressions, never a string interpolated into SQL.
        """
        exprs = {
            GRANULARITY_DAY: func.date_trunc("day", AuditEvent.timestamp),
            GRANULARITY_WEEK: func.date_trunc("week", AuditEvent.timestamp),
            GRANULARITY_MONTH: func.date_trunc("month", AuditEvent.timestamp),
        }
        return exprs[granularity]

    def _bucket_bounds(bucket_start, granularity):
        """[start, end) for one bucket, so the frontend never infers a week's or a month's
        length from the label alone.
        """
        if bucket_start is None:
            return None, None
        if granularity == GRANULARITY_WEEK:
            bucket_end = bucket_start + timedelta(days=7)
        elif granularity == GRANULARITY_MONTH:
            year = bucket_start.year + bucket_start.month // 12
            month = bucket_start.month % 12 + 1
            bucket_end = bucket_start.replace(year=year, month=month)
        else:
            bucket_end = bucket_start + timedelta(days=1)
        return bucket_start, bucket_end

    def _to_utc_key(value):
        """Normalize a bucket_start (datetime from the SQL side, or ISO string from the RPC
        side) to an aware UTC datetime, so both sides merge on the same key regardless of how
        each produced it.
        """
        if value is None:
            return None
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _role_params():
        """Role names from repeated ?roles=... params, each also split on comma so a single
        comma-separated value works too.
        """
        roles = []
        for raw in request.args.getlist("roles"):
            roles.extend(part.strip() for part in raw.split(",") if part.strip())
        return roles

    def _resolve_role_filter(project_id, roles):
        """user_id set for the given project role names, or None for "no filter" (today's
        behaviour, all roles).

        One bulk RPC pair for the whole project, never one lookup per role or per member -
        the same shape admin/rpc/roles.py's get_users_roles_in_project already uses. A role
        that exists but has no members returns an empty set, a legitimate zero rather than a
        reason to fall back to matching everyone.
        """
        wanted = {role for role in (roles or []) if role}
        if not wanted:
            return None
        try:
            project_roles = auth.list_project_roles(project_id) or []
            user_roles = auth.list_project_user_roles(project_id) or []
        except Exception:  # pylint: disable=W0703
            log.warning("Role lookup failed for project %s; analytics_activity role filter disabled", project_id, exc_info=True)
            return set()
        role_ids = {r["id"] for r in project_roles if r.get("name") in wanted}
        return {ur["user_id"] for ur in user_roles if ur.get("role_id") in role_ids}

    def _project_role_names(project_id):
        """The role names this project's filter can be set to.

        Returned with the trend rather than from a roles endpoint of its own: admin's
        Get Project Roles is gated on configuration.roles.roles.view, which a viewer who can
        read analytics does not hold, so a separate call would leave the picker empty for
        exactly the people the report is for. An empty list means "unknown", and the caller
        falls back to no filter rather than to a broken picker.
        """
        try:
            return sorted({
                role["name"] for role in (auth.list_project_roles(project_id) or [])
                if role.get("name")
            })
        except Exception:  # pylint: disable=W0703
            log.warning("Role list unavailable for project %s", project_id, exc_info=True)
            return []

    class PromptLibAPI(api_tools.APIModeHandler):
        """Bucketed active-users / AI-active-users trend, role-filterable."""

        @register_openapi(
            name="Get Activity Trend",
            description=(
                "Returns a bucketed trend of distinct active users (any audit event) and "
                "AI-active users (a metered LLM or tool call) for a date range, grouped by "
                "calendar day, week, or month, and optionally restricted to one or more "
                "project roles."
            ),
            mcp_tool=True,
            mcp_description="Use this tool when you need a day/week/month trend comparing generic activity against AI activity for a project, optionally scoped to one or more roles. Do not use this tool for a paginated per-user leaderboard - use List User Analytics instead. Do not use for project-level KPI dashboards - use Get Project Analytics Overview instead.",
            tags=["elitea_core/analytics"],
            parameters=[
                {
                    "name": "date_from",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string", "format": "date-time"},
                    "description": "Start datetime (ISO 8601). Defaults to 7 days ago.",
                    "example": "2026-01-01T00:00:00",
                },
                {
                    "name": "date_to",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string", "format": "date-time"},
                    "description": "End datetime (ISO 8601). Defaults to now.",
                    "example": "2026-05-31T23:59:59",
                },
                {
                    "name": "granularity",
                    "in": "query",
                    "required": False,
                    "schema": {
                        "type": "string",
                        "enum": ["day", "week", "month"],
                        "default": "day",
                    },
                    "description": (
                        "Bucket size. Weeks and months are calendar-aligned (Postgres "
                        "date_trunc: weeks start Monday). An unrecognised value defaults to day."
                    ),
                },
                {
                    "name": "roles",
                    "in": "query",
                    "required": False,
                    "style": "form",
                    "explode": True,
                    "schema": {"type": "array", "items": {"type": "string"}},
                    "description": (
                        "Zero or more project role names to filter by (repeat the parameter "
                        "for multiple values, e.g. roles=Viewer&roles=Editor; a single "
                        "comma-separated value is also accepted). Omitted or empty means all "
                        "roles."
                    ),
                },
            ],
            responses={
                "200": {
                    "description": "Bucketed active-users / AI-active-users trend",
                    "content": {
                        "application/json": {
                            "example": {
                                "granularity": "week",
                                "roles": ["viewer"],
                                "available_roles": ["admin", "editor", "viewer"],
                                "buckets": [
                                    {
                                        "bucket_start": "2026-01-05T00:00:00+00:00",
                                        "bucket_end": "2026-01-12T00:00:00+00:00",
                                        "active_users": 12,
                                        "ai_active_users": 4,
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
            GET /api/v2/elitea_core/analytics_activity/prompt_lib/<project_id>

            Query params:
                date_from, date_to: ISO date range
                granularity (str): "day" | "week" | "month", default "day"
                roles (list[str]): zero or more project role names; repeatable or
                    comma-separated. Omitted/empty means all roles.
            """
            from tools import db
            from ...models.audit_event import AuditEvent

            dt_from, dt_to = _parse_dates(request.args)
            granularity = _parse_granularity(request.args)
            roles = _role_params()

            try:
                role_user_ids = _resolve_role_filter(project_id, roles)
                member_ids = _project_member_ids(project_id)

                with db.with_project_schema_session(None) as session:
                    base = _apply_base_filters(
                        session, AuditEvent, project_id, dt_from, dt_to, member_ids,
                    )
                    if role_user_ids is not None:
                        # Possibly empty: a selected role with no members is a legitimate zero
                        base = base.filter(AuditEvent.user_id.in_(role_user_ids))

                    bucket_col = _bucket_expr(AuditEvent, granularity).label("bucket")
                    rows = base.with_entities(
                        bucket_col,
                        func.count(distinct(AuditEvent.user_id)).label("active_users"),
                    ).group_by(bucket_col).order_by(bucket_col).all()

                    active_by_key = {}
                    for r in rows:
                        key = _to_utc_key(r.bucket)
                        if key is None:
                            continue
                        active_by_key[key] = int(r.active_users or 0)

            except Exception:
                log.error("Analytics activity query failed", exc_info=True)
                return {"error": "Failed to query analytics activity"}, 500

            # The AI-active-users half lives over usage_event in the usage plugin (#6574). It
            # is one of two halves of this payload, so a slow or failing RPC degrades to zero
            # rather than taking the generic half down with it.
            try:
                ai_result = self.module.context.rpc_manager.timeout(5).usage_ai_active_users_trend(
                    project_id=project_id, date_from=dt_from, date_to=dt_to,
                    granularity=granularity, roles=roles,
                ) or {}
            except Exception as exc:  # pylint: disable=W0703
                log.warning("AI-active-users trend unavailable for project %s: %s", project_id, exc)
                ai_result = {}

            ai_by_key = {}
            for b in ai_result.get("buckets") or []:
                key = _to_utc_key(b.get("bucket_start"))
                if key is None:
                    continue
                ai_by_key[key] = int(b.get("ai_active_users") or 0)

            buckets = []
            for key in sorted(set(active_by_key) | set(ai_by_key)):
                _, bucket_end = _bucket_bounds(key, granularity)
                buckets.append({
                    "bucket_start": key.isoformat(),
                    "bucket_end": bucket_end.isoformat() if bucket_end else None,
                    "active_users": active_by_key.get(key, 0),
                    "ai_active_users": ai_by_key.get(key, 0),
                })

            return {
                "granularity": granularity,
                "roles": sorted(set(roles)),
                "available_roles": _project_role_names(project_id),
                "buckets": buckets,
            }, 200


    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '<int:project_id>',
        ])
        mode_handlers = {
            'prompt_lib': PromptLibAPI,
        }
else:
    API = None
