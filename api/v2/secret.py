from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, VaultClient, register_openapi
from pylon.core.tools import log

from ...models.pd.secret import SecretCreateModel
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Create Secret",
        description="Store a secret value in the project Vault and return its reference.",
        request_body=SecretCreateModel,
        mcp_description="""
        USE to store a sensitive value (token, password, api key) in the project Vault
        before creating a credential or toolkit that needs it.

        Returns a `reference` string of the form `{{secret.<key>}}`. Put that reference
        into a credential's `data` field (create_credential) or a toolkit's secret
        settings instead of the raw value.

        Note: an existing secret with the same `key` is overwritten.
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
            parsed = SecretCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return {"ok": False, "error": e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )}, 400

        try:
            vault_client = VaultClient(project_id)
            # set_secrets replaces the whole secret map, so merge with existing values.
            project_secrets = vault_client.get_secrets() or {}
            project_secrets[parsed.key] = parsed.value
            vault_client.set_secrets(project_secrets)
        except Exception as e:
            log.exception("Failed to store secret")
            return {"ok": False, "error": str(e)}, 400

        return {
            "ok": True,
            "key": parsed.key,
            "reference": f"{{{{secret.{parsed.key}}}}}",
        }, 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
