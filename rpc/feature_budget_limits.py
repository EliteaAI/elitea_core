""" Effective Tool Spend feature limits (#6980)

Same resolution order as rpc/budget_limits.py (kept byte-identical); a parity test pins the two.
"""

from pylon.core.tools import web

from ..utils.tool_spend_features import feature_defaults, validate_feature_period


def resolve_project_limit(row, default):
    """Explicit row limit, else the default; enabled=false is exempt (unlimited)."""
    if row is not None:
        if not row.get("enabled", True):
            return None
        if row.get("limit") is not None:
            return row["limit"]
    return default


def resolve_member_default(project_row, platform_default, exempt):
    if project_row and project_row.get("member_default_limit") is not None:
        return project_row["member_default_limit"]
    return None if exempt else platform_default


def resolve_member_limit(personal, member_row, project_row, platform_default):
    """Member row, then the project member default, then the platform default; none in personal projects."""
    if personal:
        return None
    exempt = member_row is not None and not member_row.get("enabled", True)
    if member_row is not None and not exempt and member_row.get("limit") is not None:
        return member_row["limit"]
    return resolve_member_default(project_row, platform_default, exempt)


def resolve_feature_enabled(defaults, project_row, personal):
    """Platform switch AND (project override, else the team/personal default)."""
    if not defaults.get("enabled", False):
        return False
    if project_row is not None and project_row.get("feature_enabled") is not None:
        return bool(project_row["feature_enabled"])
    return bool(defaults.get("enabled_personal" if personal else "enabled_team", True))


def limit_source(row, key="limit"):
    return "explicit" if row is not None and (row.get(key) is not None or not row.get("enabled", True)) else "default"


class RPC:
    @web.rpc("elitea_core_get_effective_feature_limits", "get_effective_feature_limits")
    def get_effective_feature_limits(self, project_id, user_id=None, feature=None, period_kind="month", **kwargs):
        """Feature limits in USD (None = unlimited). Raises on a failed read: callers fail closed."""
        validate_feature_period(feature, period_kind)
        personal = self.is_personal_project(project_id, strict=True)
        defaults = feature_defaults(self.descriptor.config, feature)
        project_row = self.get_project_feature_budget(project_id, feature, period_kind)
        member_row = None if user_id is None else self.get_user_feature_budget(
            project_id, user_id, feature, period_kind,
        )
        project_default = defaults.get(
            "personal_project_monthly_limit" if personal else "project_monthly_limit",
        )
        member_limit = None if user_id is None else resolve_member_limit(
            personal, member_row, project_row, defaults.get("user_monthly_limit"),
        )
        return {
            "feature": feature,
            "period_kind": period_kind,
            "project_limit_usd": resolve_project_limit(project_row, project_default),
            "member_limit_usd": member_limit,
            "project_source": limit_source(project_row),
            "member_source": limit_source(member_row),
            "feature_enabled": resolve_feature_enabled(defaults, project_row, personal),
            "is_personal_project": personal,
        }

    @web.rpc("elitea_core_get_effective_feature_project_limits", "get_effective_feature_project_limits")
    def get_effective_feature_project_limits(self, project_ids, feature, period_kind="month", **kwargs):
        """Effective feature project limits for many projects (admin listing), keyed by project id."""
        validate_feature_period(feature, period_kind)
        defaults = feature_defaults(self.descriptor.config, feature)
        rows = self.list_project_feature_budgets(feature, period_kind) or {}
        result = {}
        for project_id in project_ids:
            personal = self.is_personal_project(project_id)
            result[project_id] = resolve_project_limit(
                rows.get(project_id, rows.get(str(project_id))),
                defaults.get("personal_project_monthly_limit" if personal else "project_monthly_limit"),
            )
        return result

    @web.rpc("elitea_core_get_effective_feature_member_limits", "get_effective_feature_member_limits")
    def get_effective_feature_member_limits(self, project_id, user_ids, feature, period_kind="month", **kwargs):
        """Effective feature member limits within a project, keyed by user id."""
        validate_feature_period(feature, period_kind)
        defaults = feature_defaults(self.descriptor.config, feature)
        personal = self.is_personal_project(project_id)
        project_row = self.get_project_feature_budget(project_id, feature, period_kind)
        rows = {int(row["user_id"]): row for row in self.list_user_feature_budgets(project_id, feature, period_kind)}
        return {
            user_id: resolve_member_limit(
                personal, rows.get(int(user_id)), project_row, defaults.get("user_monthly_limit"),
            )
            for user_id in user_ids
        }
