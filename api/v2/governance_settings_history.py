"""The change history of the credential governance switch (#7120, C-1009).

GET /api/v2/elitea_core/governance_settings_history/administration?limit=20&offset=0 - newest first, ``{total, rows}``.
Needs ``governance.settings.view`` (super administrators and platform administrators).
"""
from flask import request
from tools import api_tools, auth, config as c, register_openapi

from ...utils.governance_settings import (
    HISTORY_DEFAULT_LIMIT, HISTORY_MAX_LIMIT, SettingsError, history_page,
)

OPENAPI_TAG = "elitea_core/governance"


def _int_arg(name, default):
    raw = request.args.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise SettingsError("INVALID_REQUEST") from None


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List Governance Settings History",
        description=(
            "Every change of the credential governance switch, newest first: revision, mode, policy revision, flags, "
            f"environment, change note, who changed it and when. limit defaults to {HISTORY_DEFAULT_LIMIT} (at most "
            f"{HISTORY_MAX_LIMIT})."
        ),
        tags=[OPENAPI_TAG],
        parameters=[
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer"}},
            {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer"}},
        ],
        responses={"200": {"description": "total and rows."}},
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["governance.settings.view"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"super_admin": True, "admin": True, "viewer": False, "editor": False},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, **kwargs):
        try:
            return history_page(_int_arg("limit", HISTORY_DEFAULT_LIMIT), _int_arg("offset", 0)), 200
        except SettingsError as error:
            return error.body(), error.status


class API(api_tools.APIBase):
    url_params = ["<string:mode>"]

    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
