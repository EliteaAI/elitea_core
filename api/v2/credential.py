from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, rpc_tools, register_openapi
from pylon.core.tools import log

from ...models.pd.credential import CredentialCreateModel
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Create Credential",
        description="Create a credential (configuration) in the project for use by toolkits.",
        request_body=CredentialCreateModel,
        mcp_description="""
        USE to create a credential that a toolkit references (e.g. an integration login).

        Provide the credential `type`, a `label`, and `data` (the credential fields).
        Store sensitive values in Vault first (see the create-secret tool) and reference
        them in `data` as `{{secret.<key>}}` instead of passing raw secrets.

        `project_id` is taken from the request context. If a credential with the same
        `elitea_title` already exists it is returned as-is (HTTP 200) rather than
        duplicated (HTTP 201 on creation).
        """,
        tags=["elitea_core/toolkits"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.tools.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        try:
            parsed = CredentialCreateModel.model_validate(request.get_json(silent=True) or {})
        except ValidationError as e:
            return {"ok": False, "error": e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )}, 400

        try:
            result, created = rpc_tools.RpcMixin().rpc.timeout(5).configurations_create_if_not_exists(
                payload=parsed.to_payload(project_id),
            )
        except Exception:
            # Do not leak configurations/DB internals from the exception string.
            log.exception("Failed to create credential")
            return {"ok": False, "error": "Failed to create credential"}, 400

        if created:
            _fire_credential_audit(project_id, parsed.elitea_title, parsed.type)

        return {"ok": True, "created": created, "credential": result}, 201 if created else 200


def _fire_credential_audit(project_id: int, elitea_title: str, type_: str) -> None:
    """Best-effort audit trail for credential creation (who / when / what)."""
    try:
        user = auth.current_user() or {}
        rpc_tools.EventManagerMixin().event_manager.fire_event(
            'elitea_credential_created',
            {
                'project_id': project_id,
                'elitea_title': elitea_title,
                'type': type_,
                'requested_by_user_id': user.get('id'),
            },
        )
    except Exception as e:
        log.warning("Failed to fire credential audit event: %s", e)


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
