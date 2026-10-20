"""The platform-wide credential governance switch (#7120, C-1009): read it, change it.

GET /api/v2/elitea_core/governance_settings/administration  - what is in effect, what is stored, what the deployment
                                                              fixes, the preflight, and the texts the page shows
PUT /api/v2/elitea_core/governance_settings/administration  - change the mode, the policy revision and the two flags

Reading needs ``governance.settings.view`` (super administrators and platform administrators). Changing needs
``governance.settings.manage`` and, in addition to the permission, the ``super_admin`` role: a role matrix edit cannot
widen it. Every refusal is ``{error, code}`` with a fixed message.
"""
from flask import request
from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...utils.governance_settings import MAX_BODY_BYTES, SettingsError, apply_update, current_state, is_super_admin

OPENAPI_TAG = "elitea_core/governance"

PUT_BODY = {
    "required": True,
    "content": {"application/json": {
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["mode", "admit_legacy_pat", "allow_runtime_domain_binding", "change_note", "expected_revision"],
            "properties": {
                "mode": {"type": "string", "enum": ["off", "audit_only", "enforce"]},
                "policy_revision": {"type": "string", "nullable": True, "maxLength": 128,
                                    "description": "Required for audit_only and enforce."},
                "admit_legacy_pat": {"type": "boolean"},
                "allow_runtime_domain_binding": {"type": "boolean"},
                "environment": {"type": "string", "nullable": True, "maxLength": 64,
                                "description": "Refused with FIELD_LOCKED when the deployment sets the label."},
                "change_note": {"type": "string", "minLength": 1, "maxLength": 256},
                "expected_revision": {"type": "integer", "minimum": 0,
                                      "description": "The stored revision that was read; 0 when there was no row."},
            },
        },
        "example": {"mode": "audit_only", "policy_revision": "CHG-1042", "admit_legacy_pat": False,
                    "allow_runtime_domain_binding": False, "environment": None, "change_note": "Start measuring",
                    "expected_revision": 0},
    }},
}


def _roles(admin):
    return {c.ADMINISTRATION_MODE: {"super_admin": True, "admin": admin, "viewer": False, "editor": False}}


class AdminAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Governance Settings",
        description=(
            "The credential governance switch: the mode in effect (off, audit_only, enforce), the stored row, what the "
            "deployment fixes (the highest mode allowed and the environment label), the preflight checks that decide "
            "whether it can be turned on, and the texts the Admin UI shows. A fresh read, not the snapshot dispatch uses."
        ),
        tags=[OPENAPI_TAG],
        responses={"200": {"description": "Effective and stored settings, locks, ceiling, preflight."}},
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["governance.settings.view"],
        "recommended_roles": _roles(admin=True),
    })
    @api_tools.endpoint_metrics
    def get(self, **kwargs):
        return current_state(self.module), 200

    @register_openapi(
        name="Set Governance Settings",
        description=(
            "Change the credential governance switch. Super administrators only. The change is compare-and-set on "
            "expected_revision, refused above the deployment's highest allowed mode and while the preflight fails "
            "(for audit_only and enforce), and is recorded in the history and the security audit trail in one "
            "transaction. Returns the new state as the GET does."
        ),
        tags=[OPENAPI_TAG],
        request_body=PUT_BODY,
        responses={
            "200": {"description": "The state after the change."},
            "400": {"description": "INVALID_REQUEST or POLICY_REVISION_REQUIRED."},
            "403": {"description": "FORBIDDEN: not a super administrator."},
            "409": {"description": "REVISION_CONFLICT, FIELD_LOCKED, MODE_ABOVE_CEILING, PREFLIGHT_FAILED, "
                                   "MIGRATION_REQUIRED."},
            "503": {"description": "DEPENDENCY_UNAVAILABLE or AUDIT_UNAVAILABLE; nothing was changed."},
        },
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["governance.settings.manage"],
        "recommended_roles": _roles(admin=False),
    })
    @api_tools.endpoint_metrics
    def put(self, **kwargs):
        try:
            actor = auth.current_user().get("id")
        except Exception as exc:  # pylint: disable=W0703
            log.error("governance settings: the caller could not be read (%s)", type(exc).__name__)
            actor = None
        try:
            if isinstance(actor, bool) or not isinstance(actor, int) or actor <= 0 or not is_super_admin(actor):
                raise SettingsError("FORBIDDEN")
            if (request.content_length or 0) > MAX_BODY_BYTES:   # before anything is parsed
                raise SettingsError("INVALID_REQUEST")
            return apply_update(self.module, request.get_json(silent=True), actor), 200
        except SettingsError as error:
            return error.body(), error.status


class API(api_tools.APIBase):
    url_params = ["<string:mode>"]

    mode_handlers = {
        c.ADMINISTRATION_MODE: AdminAPI,
    }
