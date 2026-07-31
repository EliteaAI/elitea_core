from flask import request
from tools import api_tools, auth, config as c, register_openapi, rpc_tools

OPENAPI_TAG = "elitea_core/usage"


# Only real project columns can be ordered. Limit, spend and used are derived after the
# query — from stored budgets, config defaults and LiteLLM — so ordering by them would
# only ever sort the page that was already fetched. Ranking by cost is done in the export.
SORTABLE_FIELDS = ("name", "id")
DEFAULT_SORT_FIELD = "name"


def _safe_sort_field(sort_by):
    """Fall back to the default for anything not orderable in SQL.

    An unsupported field would otherwise reach the project listing's own silent default
    and appear to work while ordering by something else entirely.
    """
    return sort_by if sort_by in SORTABLE_FIELDS else DEFAULT_SORT_FIELD


def _owner_ids_for_search(rpc, search: str):
    """Ids of users whose name or email matches, so a search can find them by identity.

    A personal project is named project_user_N, which nobody searches for. The project
    query matches ids rather than joining users, so the caller resolves them first.
    """
    if not search:
        return None
    #
    try:
        matched = rpc.timeout(15).auth_search_users(search=search) or []
    except Exception:  # pylint: disable=W0703
        return None
    #
    return [user["id"] for user in matched] or None


def _limit_source(row: dict, effective, project_default=None):
    """Explain where the enforced limit came from, so an admin isn't surprised by it.

    project_default is the project's member default, passed only for member rows: it
    separates "capped by this project" from "capped platform-wide". A member marked exempt
    is still subject to it, so the label follows the resolved limit rather than the flag.
    """
    exempt = bool(row) and not row.get("enabled", True)
    #
    if row and not exempt and row.get("monthly_limit") is not None:
        return "explicit"
    #
    if effective is None:
        return "unlimited"
    #
    if project_default is not None and effective == project_default:
        return "project_default"
    #
    return "default"


class AdminAPI(api_tools.APIModeHandler):
    """Platform-admin listing of every project's budget and current-month spend."""

    @register_openapi(
        name="List Project Budgets (Admin)",
        description=(
            "Paginated list of projects with their effective budget and current-month "
            "spend. Backs Admin -> Budgets.\n\n"
            "Only name and id can be ordered. Limit, spend and usage are derived after the "
            "query - from stored budgets, configured defaults and the LLM proxy - so "
            "ordering by them would sort only the returned page; an unsupported sort_by "
            "falls back to name. Export the data to rank projects by cost.\n\n"
            "Personal projects can additionally be matched by their owner's name or email."
        ),
        tags=[OPENAPI_TAG],
        parameters=[
            {
                "name": "limit",
                "in": "query",
                "required": False,
                "schema": {"type": "integer", "default": 20},
                "description": "Page size.",
                "example": 20,
            },
            {
                "name": "offset",
                "in": "query",
                "required": False,
                "schema": {"type": "integer", "default": 0},
                "description": "Rows to skip.",
                "example": 0,
            },
            {
                "name": "search",
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": (
                    "Matches project name or id; for personal projects, also the owner's "
                    "name and email."
                ),
                "example": "analytics",
            },
            {
                "name": "project_type",
                "in": "query",
                "required": False,
                "schema": {"type": "string", "enum": ["team", "personal"]},
                "description": "Restrict to shared team projects or to users' own projects.",
                "example": "team",
            },
            {
                "name": "sort_by",
                "in": "query",
                "required": False,
                "schema": {"type": "string", "enum": list(SORTABLE_FIELDS), "default": DEFAULT_SORT_FIELD},
                "description": "Anything else falls back to name.",
                "example": DEFAULT_SORT_FIELD,
            },
            {
                "name": "sort_order",
                "in": "query",
                "required": False,
                "schema": {"type": "string", "enum": ["asc", "desc"], "default": "asc"},
                "description": "Sort direction.",
                "example": "asc",
            },
        ],
        responses={
            "200": {
                "description": (
                    "Rows for the requested page, the filtered total, and unfiltered "
                    "per-type counts for tab labels."
                ),
            },
        },
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.admin.project_budgets.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, **kwargs):
        rpc = rpc_tools.RpcMixin().rpc
        #
        limit = int(request.args.get("limit", 20))
        offset = int(request.args.get("offset", 0))
        search = request.args.get("search") or None
        project_type = request.args.get("project_type") or None
        sort_order = request.args.get("sort_order", "asc")
        #
        sort_by = _safe_sort_field(request.args.get("sort_by", DEFAULT_SORT_FIELD))
        #
        # Owner ids are OR'd into the project search, so resolve them only for personal
        # projects. Doing it always would let a team project match on its owner's email.
        owner_ids = _owner_ids_for_search(rpc, search) if project_type == "personal" else None
        #
        listing = rpc.timeout(15).project_list_paginated(
            limit=limit,
            offset=offset,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
            project_type=project_type,
            owner_ids=owner_ids,
        ) or {}
        #
        projects = listing.get("rows") or []
        project_ids = [p["id"] for p in projects]
        #
        budgets = rpc.timeout(10).elitea_core_list_project_budgets() or {}
        #
        # A personal project is really that user's own budget, so resolve the owner
        # to label it by identity rather than the opaque project_user_N name.
        owner_ids = [p["owner_id"] for p in projects if p.get("owner_id") is not None]
        #
        try:
            owners = rpc.timeout(15).auth_list_users(user_ids=owner_ids) if owner_ids else []
        except Exception:  # pylint: disable=W0703
            owners = []
        #
        owner_map = {int(user["id"]): user for user in owners or []}
        #
        # One LiteLLM call for the whole page rather than one per row
        try:
            spend_map = rpc.timeout(30).litellm_get_projects_spend(
                project_ids=project_ids,
            ) or {}
        except Exception:  # pylint: disable=W0703
            spend_map = {}
        #
        try:
            limit_map = rpc.timeout(20).litellm_get_effective_project_limits(
                project_ids=project_ids,
            ) or {}
        except Exception:  # pylint: disable=W0703
            limit_map = {}
        #
        rows = []
        #
        for project in projects:
            project_id = project["id"]
            row = budgets.get(project_id) or budgets.get(str(project_id)) or {}
            #
            effective = limit_map.get(project_id, limit_map.get(str(project_id)))
            spend = float(spend_map.get(project_id, spend_map.get(str(project_id), 0)) or 0)
            #
            project_name = project.get("name") or ""
            is_personal = project_name.startswith("project_user_")
            owner = owner_map.get(project.get("owner_id")) or {}
            owner_label = owner.get("email") or owner.get("name")
            #
            rows.append({
                "project_id": project_id,
                "name": project_name,
                "display_name": owner_label if (is_personal and owner_label) else project_name,
                # Falls back to email so the owner column is never blank; the two differ
                # only where the identity provider supplies a real display name.
                "owner_name": owner.get("name") or owner.get("email"),
                "owner_email": owner.get("email"),
                "is_personal": is_personal,
                "monthly_limit": row.get("monthly_limit"),
                # Carried so the edit dialog can prefill it without a second read
                "member_default_limit": row.get("member_default_limit"),
                "effective_limit": effective,
                "limit_source": _limit_source(row, effective),
                "currency": row.get("currency", "USD"),
                "enabled": row.get("enabled", False),
                "spend": spend,
                "remaining": None if effective is None else max(0.0, effective - spend),
                "percent_used": None if not effective else round(spend / effective * 100, 2),
            })
        #
        return {
            "rows": rows,
            "total": listing.get("total", len(rows)),
            "counts": listing.get("counts") or {},
        }, 200


class API(api_tools.APIBase):
    url_params = [
        "<string:mode>",
    ]

    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
