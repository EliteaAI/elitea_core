""" Admin read/write of Tool Spend feature limits, behind the budget APIs' optional `feature` param """

from tools import rpc_tools  # pylint: disable=E0401

from .tool_spend_features import validate_feature_period

FEATURE_FIELDS = ("limit_usd", "member_default_limit_usd", "enabled", "feature_enabled")


def request_feature(request):
    """`feature` from the query string or the JSON body; validated, None for the LLM path."""
    body = request.get_json(silent=True) if request.method in ("PUT", "POST") else None
    feature = request.args.get("feature") or ((body or {}).get("feature") if isinstance(body, dict) else None)
    if feature:
        validate_feature_period(feature, "month")
    return feature or None


def _limit(raw, field):
    value = raw.get(field)
    if value is None:
        return None
    value = float(value)
    if value < 0:
        raise ValueError(f"{field} must be >= 0")
    return value


def parse_feature_payload(raw, member=False):
    """Only the fields that were sent; the UI sends just the active feature's fields."""
    allowed = ("limit_usd", "enabled") if member else FEATURE_FIELDS
    payload = {}
    for field in allowed:
        if field not in raw:
            continue
        if field in ("limit_usd", "member_default_limit_usd"):
            payload[field] = _limit(raw, field)
        elif field == "feature_enabled":
            payload[field] = None if raw[field] is None else bool(raw[field])
        else:
            payload[field] = bool(raw[field])
    return payload


def _spend_dollars(spend, key):
    return (spend or {}).get(key, 0) / 1_000_000_000 if (spend or {}).get("available") else 0.0


def feature_state(project_id, feature, user_id=None):
    """Stored row, effective limits (with source) and current-period feature spend."""
    rpc = rpc_tools.RpcMixin().rpc
    limits = rpc.timeout(10).elitea_core_get_effective_feature_limits(
        project_id=project_id, user_id=user_id, feature=feature,
    ) or {}
    if user_id is None:
        row = rpc.timeout(5).elitea_core_get_project_feature_budget(project_id=project_id, feature=feature)
        limit, source = limits.get("project_limit_usd"), limits.get("project_source")
    else:
        row = rpc.timeout(5).elitea_core_get_user_feature_budget(
            project_id=project_id, user_id=user_id, feature=feature,
        )
        limit, source = limits.get("member_limit_usd"), limits.get("member_source")
    try:
        spend = rpc.timeout(10).usage_get_feature_spend(project_id, user_id, feature)
    except Exception:  # pylint: disable=W0703
        spend = {"available": False}
    spent = _spend_dollars(spend, "project_spent_nano" if user_id is None else "member_spent_nano")
    row = row or {}
    return {
        "project_id": project_id,
        "user_id": user_id,
        "feature": feature,
        "period_kind": "month",
        "limit_usd": row.get("limit"),
        "member_default_limit_usd": row.get("member_default_limit") if user_id is None else None,
        "enabled": row.get("enabled", True),
        "feature_enabled": row.get("feature_enabled") if user_id is None else None,
        "feature_enabled_effective": limits.get("feature_enabled"),
        "effective_limit": limit,
        "limit_source": source,
        "spend": spent,
        "remaining": None if limit is None else max(0.0, limit - spent),
        "percent_used": None if not limit else round(spent / limit * 100, 2),
        "spend_available": bool(spend.get("available")),
    }


def feature_write(project_id, feature, raw, user_id=None):
    """Upsert the feature row, or delete it ("Reset to default") when `reset` is true."""
    rpc = rpc_tools.RpcMixin().rpc.timeout(20)
    if raw.get("reset"):
        if user_id is None:
            rpc.elitea_core_delete_project_feature_budget(project_id=project_id, feature=feature)
        else:
            rpc.elitea_core_delete_user_feature_budget(project_id=project_id, user_id=user_id, feature=feature)
        return feature_state(project_id, feature, user_id)
    payload = parse_feature_payload(raw, member=user_id is not None)
    if user_id is None:
        rpc.elitea_core_set_project_feature_budget(project_id=project_id, feature=feature, **payload)
    else:
        rpc.elitea_core_set_user_feature_budget(project_id=project_id, user_id=user_id, feature=feature, **payload)
    return feature_state(project_id, feature, user_id)
