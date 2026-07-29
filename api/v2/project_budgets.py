from flask import request
from tools import api_tools, auth, config as c, rpc_tools


# Columns derived after the DB query, so they cannot be sorted in SQL
COMPUTED_SORT_FIELDS = ("spend", "percent_used", "effective_limit")


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


def _limit_source(row: dict, effective):
    """Explain where the enforced limit came from, so an admin isn't surprised by it."""
    if row and not row.get("enabled", True):
        return "unlimited"
    #
    if row and row.get("monthly_limit") is not None:
        return "explicit"
    #
    return "default" if effective is not None else "unlimited"


class AdminAPI(api_tools.APIModeHandler):
    """Platform-admin listing of every project's budget and current-month spend."""

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
        sort_by = request.args.get("sort_by", "name")
        sort_order = request.args.get("sort_order", "asc")
        #
        # Spend/limit are computed after the query, so the DB keeps a stable order
        # for those and the page is re-sorted below.
        is_computed_sort = sort_by in COMPUTED_SORT_FIELDS
        #
        # Owner ids are OR'd into the project search, so resolve them only for personal
        # projects. Doing it always would let a team project match on its owner's email.
        owner_ids = _owner_ids_for_search(rpc, search) if project_type == "personal" else None
        #
        listing = rpc.timeout(15).project_list_paginated(
            limit=limit,
            offset=offset,
            search=search,
            sort_by="name" if is_computed_sort else sort_by,
            sort_order="asc" if is_computed_sort else sort_order,
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
                "effective_limit": effective,
                "limit_source": _limit_source(row, effective),
                "currency": row.get("currency", "USD"),
                "enabled": row.get("enabled", False),
                "spend": spend,
                "remaining": None if effective is None else max(0.0, effective - spend),
                "percent_used": None if not effective else round(spend / effective * 100, 2),
            })
        #
        if is_computed_sort:
            reverse = sort_order.lower() == "desc"
            # Nulls (unlimited / no spend) sort last either way
            rows.sort(
                key=lambda row: (row.get(sort_by) is None, row.get(sort_by) or 0),
                reverse=reverse,
            )
        #
        return {
            "rows": rows,
            "total": listing.get("total", len(rows)),
            "counts": listing.get("counts") or {},
            "sorted_within_page": is_computed_sort,
        }, 200


class API(api_tools.APIBase):
    url_params = [
        "<string:mode>",
    ]

    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
