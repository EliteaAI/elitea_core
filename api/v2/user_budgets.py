from flask import request
from pylon.core.tools import log

from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...utils.constants import (
    PROMPT_LIB_MODE, SYSTEM_USER_EMAILS, SYSTEM_USER_EMAIL_PATTERN,
)
from .project_budgets import _limit_source

OPENAPI_TAG = "elitea_core/usage"

# Every one of these is resolved before paging, so unlike the project listing they order
# the whole result rather than only the page that was already fetched.
SORTABLE_FIELDS = ("spend", "name", "user_id")
DEFAULT_SORT_FIELD = "spend"

DEFAULT_LIMIT = 20

# Bounds a single full-list read, which is what the Usage page export asks for
MAX_LIMIT = 1000

# Spend LiteLLM recorded against the project but not against any member: the project's own
# system user, and anything tagged before per-member tagging was in place.
SYSTEM_ROW_NAME = "System / unattributed"

# Below half a cent the residual is float noise from summing the member rows, not real spend
SYSTEM_ROW_EPSILON = 0.005

DEFAULT_WARNING_PCT = 80

PROJECT_ID_PARAM = {
    "name": "project_id",
    "in": "path",
    "required": True,
    "schema": {"type": "integer"},
    "description": "Project whose members to report on.",
    "example": 1,
}

QUERY_PARAMS = [
    {
        "name": "limit",
        "in": "query",
        "schema": {"type": "integer", "default": DEFAULT_LIMIT, "maximum": MAX_LIMIT},
        "description": "Rows per page.",
    },
    {
        "name": "offset",
        "in": "query",
        "schema": {"type": "integer", "default": 0},
        "description": "Rows to skip.",
    },
    {
        "name": "search",
        "in": "query",
        "schema": {"type": "string"},
        "description": "Match on member name or email.",
    },
    {
        "name": "sort_by",
        "in": "query",
        "schema": {"type": "string", "enum": list(SORTABLE_FIELDS), "default": DEFAULT_SORT_FIELD},
    },
    {
        "name": "sort_order",
        "in": "query",
        "schema": {"type": "string", "enum": ["asc", "desc"], "default": "desc"},
    },
]


def _safe_sort_field(sort_by):
    """Fall back to the default for anything not orderable, rather than silently ignoring it."""
    return sort_by if sort_by in SORTABLE_FIELDS else DEFAULT_SORT_FIELD


def _safe_rpc(rpc, name, timeout, default, *args, **kwargs):
    """Call an RPC, degrading just its own contribution if it fails.

    Every column here is optional detail on top of the member list, so one slow service
    must not turn the whole Usage page into a 500 — which is what it used to do.
    """
    try:
        return getattr(rpc.timeout(timeout), name)(*args, **kwargs)
    except Exception:  # pylint: disable=W0703
        log.warning("Member usage: %s failed, using fallback", name, exc_info=True)
        #
        return default


def _sort_key(sort_by):
    if sort_by == "name":
        return lambda row: (row.get("name") or "").lower()
    #
    if sort_by == "user_id":
        return lambda row: row["user_id"]
    #
    return lambda row: row.get("spend") or 0.0


def _matches(row, search):
    needle = search.lower()
    #
    return needle in (row.get("name") or "").lower() or needle in (row.get("email") or "").lower()


def _is_system_account(email):
    """True for platform or per-project system accounts, which are not people."""
    if not email:
        return False
    #
    prefix, _, suffix = SYSTEM_USER_EMAIL_PATTERN.partition("%")
    #
    return email in SYSTEM_USER_EMAILS or (
        email.startswith(prefix) and email.endswith(suffix)
    )


def _system_row(project_spend, rows, currency):
    """Project spend not attributable to any member, so the rows add up to the total."""
    if project_spend is None:
        return None
    #
    residual = project_spend - sum(row.get("spend") or 0.0 for row in rows)
    #
    if residual < SYSTEM_ROW_EPSILON:
        return None
    #
    return {
        "user_id": None,
        "name": SYSTEM_ROW_NAME,
        "email": None,
        "roles": [],
        "spend": residual,
        "currency": currency,
        "monthly_limit": None,
        "effective_limit": None,
        "limit_source": None,
        "remaining": None,
        "percent_used": None,
        "enabled": False,
    }


def _member_budget_rows(project_id: int, limit, offset, search, sort_by, sort_order):
    """Per-member spend and budget for a project's current month.

    Members come from recorded spend where that is available, so someone who spent and then
    left the project is still reported instead of their spend vanishing from the breakdown.
    """
    rpc = rpc_tools.RpcMixin().rpc
    #
    # Returns {user_id: [roles]}, so ids come from the keys
    user_roles = _safe_rpc(
        rpc, "admin_get_users_roles_in_project", 15, {}, project_id, filter_system_user=True,
    ) or {}
    # A role row without an individual owner (e.g. a group-level assignment) has user_id=None
    roles_by_id = {
        int(user_id): roles for user_id, roles in user_roles.items() if user_id is not None
    }
    #
    spend_data = _safe_rpc(rpc, "litellm_list_member_spend", 30, None, project_id=project_id)
    degraded = spend_data is None
    #
    if degraded:
        # Spend table unreachable: fall back to current membership, which is the only list
        # available, and say so rather than presenting a short list as complete.
        member_ids = set(roles_by_id)
        usage_by_id = {}
        project_spend = None
    else:
        usage_by_id = {
            int(user_id): entry
            for user_id, entry in (spend_data.get("members") or {}).items()
        }
        project_spend = float((spend_data.get("project") or {}).get("spend") or 0)
        #
        # Union: whoever spent (including former members) plus current members yet to spend
        member_ids = set(roles_by_id) | set(usage_by_id)
    #
    member_ids = sorted(member_ids)
    #
    # One bulk call for identities, so search and ordering cover every member rather than
    # only the page. Per-row lookups are what made this endpoint scale badly.
    users = _safe_rpc(rpc, "auth_list_users", 15, [], user_ids=member_ids) if member_ids else []
    by_id = {int(user["id"]): user for user in users or []}
    #
    # Service accounts are not people: dropping them here also folds their spend into the
    # system row, since the residual is the project total minus the rows that remain.
    system_ids = {
        user_id for user_id, user in by_id.items() if _is_system_account(user.get("email"))
    }
    #
    if system_ids:
        member_ids = [user_id for user_id in member_ids if user_id not in system_ids]
    #
    stored = _safe_rpc(rpc, "elitea_core_list_user_budgets", 10, [], project_id=project_id) or []
    by_user = {row["user_id"]: row for row in stored}
    #
    # The value every member with no limit of their own inherits, shown as its own source
    project_budget = _safe_rpc(
        rpc, "elitea_core_get_project_budget", 5, {}, project_id=project_id,
    ) or {}
    member_default = project_budget.get("member_default_limit")
    currency = project_budget.get("currency") or "USD"
    #
    if degraded:
        usage_by_id = {
            int(user_id): {"spend": float(spend or 0)}
            for user_id, spend in (_safe_rpc(
                rpc, "litellm_get_users_spend", 30, {},
                project_id=project_id, user_ids=member_ids,
            ) or {}).items()
        }
    #
    rows = []
    #
    for user_id in member_ids:
        user = by_id.get(user_id) or {}
        usage = usage_by_id.get(user_id) or {}
        #
        rows.append({
            "project_id": project_id,
            "user_id": user_id,
            # A former member may no longer be resolvable at all, so never render blank
            "name": user.get("name") or user.get("email") or f"User {user_id}",
            "email": user.get("email"),
            "roles": roles_by_id.get(user_id) or [],
            "spend": float(usage.get("spend") or 0),
            "requests": int(usage.get("requests") or 0),
        })
    #
    # Computed over every member, not the page, so it stays the true residual
    system_row = _system_row(project_spend, rows, currency)
    #
    if search:
        rows = [row for row in rows if _matches(row, search)]
    #
    rows.sort(key=_sort_key(sort_by), reverse=sort_order == "desc")
    #
    total = len(rows)
    page = rows[offset:offset + limit]
    #
    # Limits are resolved for the page alone: this is the one call that costs per member
    limit_map = _safe_rpc(
        rpc, "litellm_get_effective_user_limits", 20, {},
        project_id=project_id, user_ids=[row["user_id"] for row in page],
    ) or {}
    #
    for row in page:
        user_id = row["user_id"]
        stored_row = by_user.get(user_id) or {}
        effective = limit_map.get(user_id, limit_map.get(str(user_id)))
        spend = row["spend"]
        #
        row.update({
            "monthly_limit": stored_row.get("monthly_limit"),
            "effective_limit": effective,
            "limit_source": _limit_source(stored_row, effective, member_default),
            "currency": stored_row.get("currency") or currency,
            "enabled": stored_row.get("enabled", False),
            "remaining": None if effective is None else max(0.0, effective - spend),
            "percent_used": None if not effective else round(spend / effective * 100, 2),
        })
    #
    warning_pct = _safe_rpc(
        rpc, "litellm_get_warning_threshold", 5, DEFAULT_WARNING_PCT, scope="user",
    )
    #
    return {
        "rows": page,
        "total": total,
        "warning_pct": warning_pct or DEFAULT_WARNING_PCT,
        "member_default_limit": member_default,
        "system_row": system_row,
        "spend_source": "membership" if degraded else "spend_records",
        "degraded": degraded,
    }


def _listing_from_request(project_id: int):
    """Read the paging parameters and build the listing."""
    try:
        limit = min(MAX_LIMIT, max(1, int(request.args.get("limit", DEFAULT_LIMIT))))
        offset = max(0, int(request.args.get("offset", 0)))
    except (TypeError, ValueError):
        limit, offset = DEFAULT_LIMIT, 0
    #
    return _member_budget_rows(
        project_id,
        limit=limit,
        offset=offset,
        search=request.args.get("search") or None,
        sort_by=_safe_sort_field(request.args.get("sort_by", DEFAULT_SORT_FIELD)),
        sort_order=request.args.get("sort_order", "desc"),
    )


class PromptLibAPI(api_tools.APIModeHandler):
    """Project admins list their own project's per-member usage."""

    @register_openapi(
        name="List Project Member Budgets",
        description=(
            "Per-member spend and budget for the project's current month, paged and "
            "searchable. Backs the Members section of Settings -> Usage.\n\n"
            "Members are enumerated from recorded spend, so a user who spent and later left "
            "the project is still listed. Spend that belongs to no member — the project's "
            "system user, or traffic tagged before per-member tagging existed — is returned "
            "separately as system_row, so the rows reconcile to the project total.\n\n"
            "A member's own limit applies in addition to the project limit: a call is "
            "blocked when either is exceeded.\n\n"
            "If the spend records cannot be read, the response falls back to current "
            "membership and sets degraded to true.\n\n"
            "In prompt_lib mode this is restricted to admins of that project; ordinary "
            "members receive 403, since they may see only their own usage. In "
            "administration mode it is available to platform administrators regardless of "
            "membership."
        ),
        tags=[OPENAPI_TAG],
        parameters=[PROJECT_ID_PARAM] + QUERY_PARAMS,
        responses={
            "200": {"description": "One page of member rows, the total, and the warning threshold."},
            "403": {"description": "Caller is not an admin of this project."},
        },
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.project_context.view"],
            "recommended_roles": {
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        # Members see only their own usage, so the member list is admin-only
        is_admin = _safe_rpc(
            rpc_tools.RpcMixin().rpc, "admin_check_user_is_admin", 5, False,
            project_id, auth.current_user().get("id"),
        )
        #
        if not is_admin:
            return {"error": "Forbidden"}, 403
        #
        return _listing_from_request(project_id), 200


class AdminAPI(api_tools.APIModeHandler):
    """Platform-admin listing of per-user budgets and spend within one project."""

    # Documented on PromptLibAPI: both handlers share one path, so only one registration
    # survives. Same convention as analytics.py.
    @auth.decorators.check_api(
        {
            "permissions": ["models.admin.project_budgets.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        return _listing_from_request(project_id), 200


class API(api_tools.APIBase):
    url_params = [
        "<string:mode>/<int:project_id>",
    ]

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
        c.ADMINISTRATION_MODE: AdminAPI,
    }
