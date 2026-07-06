from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, this, rpc_tools, VaultClient, register_openapi
from pylon.core.tools import log

from ...models.pd.secret import SecretCreateModel
from ...utils.constants import PROMPT_LIB_MODE


# Per-project advisory lock so the read-modify-write below is serialized. VaultClient
# has no check-and-set primitive here and ``set_secrets`` rewrites the whole KV map,
# so concurrent writers (parallel MCP calls) could otherwise clobber each other.
_LOCK_TIMEOUT = 10  # seconds a held lock auto-expires after (crash safety)
_LOCK_BLOCKING_TIMEOUT = 8  # seconds to wait to acquire before giving up


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Create Secret",
        description="Store a secret value in the project Vault and return its reference.",
        request_body=SecretCreateModel,
        mcp_description="""
        USE to store a sensitive value (token, password, api key) in the project Vault
        before creating a credential or toolkit that needs it.

        Returns a `reference` string of the form `{{secret.<key>}}`. Put that reference
        into a credential's `data` field or a toolkit's secret settings instead of the
        raw value.

        By default an existing secret with the same `key` is NOT replaced: the call
        fails with HTTP 409. Pass `overwrite: true` explicitly to replace it.
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
            parsed = SecretCreateModel.model_validate(request.get_json(silent=True) or {})
        except ValidationError as e:
            return {"ok": False, "error": e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )}, 400

        lock = this.module.get_redis_client().lock(
            f"elitea_core:secret_write:{project_id}",
            timeout=_LOCK_TIMEOUT,
            blocking_timeout=_LOCK_BLOCKING_TIMEOUT,
        )
        if not lock.acquire():
            return {"ok": False, "error": "Secret store is busy, please retry"}, 409

        try:
            vault_client = VaultClient(project_id)
            # set_secrets replaces the whole secret map, so merge with existing values.
            project_secrets = vault_client.get_secrets() or {}
            if parsed.key in project_secrets and not parsed.overwrite:
                return {
                    "ok": False,
                    "error": f"Secret '{parsed.key}' already exists. "
                             "Pass overwrite=true to replace it.",
                }, 409
            project_secrets[parsed.key] = parsed.value
            vault_client.set_secrets(project_secrets)
        except Exception:
            # Do not leak Vault paths / tokens from the exception string to the caller.
            log.exception("Failed to store secret")
            return {"ok": False, "error": "Failed to store secret"}, 400
        finally:
            try:
                lock.release()
            except Exception:
                log.warning("Failed to release secret write lock for project %s", project_id)

        _fire_secret_audit(project_id, parsed.key)

        return {
            "ok": True,
            "key": parsed.key,
            "reference": f"{{{{secret.{parsed.key}}}}}",
        }, 201


def _fire_secret_audit(project_id: int, key: str) -> None:
    """Best-effort audit trail for secret writes (who / when / what)."""
    try:
        user = auth.current_user() or {}
        rpc_tools.EventManagerMixin().event_manager.fire_event(
            'elitea_secret_created',
            {
                'project_id': project_id,
                'key': key,
                'requested_by_user_id': user.get('id'),
            },
        )
    except Exception as e:
        log.warning("Failed to fire secret audit event: %s", e)


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
