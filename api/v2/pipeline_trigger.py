"""
API endpoint for Pipeline Trigger configuration.

Provides GET/PUT operations to read and update pipeline trigger settings
stored in ApplicationVersion.pipeline_settings['trigger'].
"""
import secrets
from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from tools import api_tools, auth, config as c, context, db, serialize, this, VaultClient

from ...models.all import ApplicationVersion, Application
from ...models.pd.pipeline_trigger import (
    UpdatePipelineTrigger,
    PipelineTriggerResponse,
    TriggerType,
    WebhookType,
    get_trigger_from_pipeline_settings,
    build_trigger_for_storage,
)
from ...utils.constants import PROMPT_LIB_MODE


# Webhook URL components - derived from routing configuration
# Full URL pattern: {url_prefix}/{module_name}/webhook/{mode}/{project_id}/{version_id}/{webhook_type}
WEBHOOK_API_PATH = "webhook"


def _generate_webhook_secret(webhook_type: str) -> str:
    """
    Generate a webhook secret appropriate for the webhook type.

    All webhook types use a raw token (no header prefix).
    """
    return secrets.token_urlsafe(32)


def _get_webhook_secret_for_display(project_id: int, trigger_data: dict, webhook_type: str) -> dict:
    """
    Get webhook secret from vault and format for display.

    Secret is stored per-version in trigger_data['webhook_secret'] (vault reference).

    Returns dict with:
    - secret_configured: bool - whether secret is set
    - secret_header: str - header name for custom webhook (e.g., 'X-Webhook-Token')
    - secret_value: str - the actual secret value (masked or full depending on context)
    - secret_instructions: str - instructions for configuring in external system
    """
    webhook_secret_ref = trigger_data.get("webhook_secret") if trigger_data else None
    if not webhook_secret_ref:
        return {
            "secret_configured": False,
            "secret_header": None,
            "secret_value": None,
            "secret_instructions": None,
        }

    secret = VaultClient(project_id).unsecret(webhook_secret_ref)
    if not secret:
        return {
            "secret_configured": False,
            "secret_header": None,
            "secret_value": None,
            "secret_instructions": None,
        }

    if webhook_type == WebhookType.custom.value:
        # Custom webhook uses raw token with fixed X-Webhook-Token header
        # Strip header prefix if present (legacy data)
        value = secret.split(":", 1)[1] if ":" in secret else secret
        return {
            "secret_configured": True,
            "secret_header": "X-Webhook-Token",
            "secret_value": value,
            "secret_instructions": f"Add header 'X-Webhook-Token' with value '{value}' to your webhook request",
        }
    elif webhook_type == WebhookType.github.value:
        # GitHub uses raw secret - strip header prefix if present (legacy data)
        secret_value = secret.split(":", 1)[1] if ":" in secret else secret
        return {
            "secret_configured": True,
            "secret_header": None,
            "secret_value": secret_value,
            "secret_instructions": "Enter this secret in your GitHub webhook configuration under 'Secret'",
        }
    elif webhook_type == WebhookType.gitlab.value:
        # GitLab uses raw token - strip header prefix if present (legacy data)
        secret_value = secret.split(":", 1)[1] if ":" in secret else secret
        return {
            "secret_configured": True,
            "secret_header": None,
            "secret_value": secret_value,
            "secret_instructions": "Enter this token in your GitLab webhook configuration under 'Secret token'",
        }

    return {
        "secret_configured": True,
        "secret_header": None,
        "secret_value": secret,
        "secret_instructions": None,
    }


def _build_webhook_url(project_id: int, version_id: int, webhook_type: str) -> str:
    """
    Build the webhook URL for a pipeline trigger.

    URL pattern: /api/v2/{module_name}/webhook/{mode}/{project_id}/{version_id}/{webhook_type}
    Example: /api/v2/elitea_core/webhook/prompt_lib/1/42/custom

    Note: Hardcoding /api/v2 because context.url_prefix may be empty in some contexts.
    """
    return f"/api/v2/{this.module_name}/{WEBHOOK_API_PATH}/{PROMPT_LIB_MODE}/{project_id}/{version_id}/{webhook_type}"


class PromptLibAPI(api_tools.APIModeHandler):
    """API handler for pipeline trigger configuration."""

    @auth.decorators.check_api({
        "permissions": ["models.applications.pipeline_trigger.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, version_id: int, **kwargs):
        """
        Get the current trigger configuration for a pipeline version.

        Returns the trigger type and schedule details (if applicable).
        Defaults to 'chat_message' for legacy pipelines without trigger config.
        """
        try:
            with db.get_session(project_id) as session:
                version = session.query(ApplicationVersion).options(
                    joinedload(ApplicationVersion.application)
                ).filter(
                    ApplicationVersion.id == version_id
                ).first()

                if not version:
                    return {"ok": False, "error": f"Version {version_id} not found"}, 404

                # Get trigger from pipeline_settings with fallback
                pipeline_settings = version.pipeline_settings or {}
                trigger_data = get_trigger_from_pipeline_settings(pipeline_settings)

                # Build webhook URL and secret info if applicable
                webhook_url = None
                secret_info = {}
                trigger_type = trigger_data.get("type", TriggerType.chat_message.value)
                webhook_type = trigger_data.get("webhook_type")
                if trigger_type == TriggerType.webhook.value and webhook_type:
                    webhook_url = _build_webhook_url(project_id, version_id, webhook_type)
                    secret_info = _get_webhook_secret_for_display(
                        project_id, trigger_data, webhook_type
                    )

                # Build response model
                response = PipelineTriggerResponse(
                    type=trigger_type,
                    cron=trigger_data.get("cron"),
                    timezone=trigger_data.get("timezone"),
                    last_run=trigger_data.get("last_run"),
                    created_by=trigger_data.get("created_by"),
                    webhook_type=webhook_type,
                    webhook_url=webhook_url,
                    **secret_info,
                )

                log.debug(f"Pipeline trigger GET: version_id={version_id}, trigger_data={trigger_data}, response={response.dict()}")
                return serialize(response.dict()), 200

        except Exception as e:
            log.exception(f"Error fetching pipeline trigger for version {version_id}: {e}")
            return {"ok": False, "error": "Error fetching pipeline trigger"}, 500

    @auth.decorators.check_api({
        "permissions": ["models.applications.pipeline_trigger.edit"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    def put(self, project_id: int, version_id: int, **kwargs):
        """
        Update the trigger configuration for a pipeline version.

        For 'chat_message' type: No additional fields required.
        For 'schedule' type: cron and timezone are required.
        For 'webhook' type: webhook_type is required, secret is auto-generated.
        """
        payload = dict(request.json)

        # Validate input
        try:
            update_data = UpdatePipelineTrigger.parse_obj(payload)
        except ValidationError as e:
            log.error(f"Validation error on pipeline trigger update: {e.errors()}")
            return {
                "ok": False,
                "error": f"Validation error: {e.errors()}"
            }, 400

        # Additional validation for schedule type
        # Note: type is a string due to use_enum_values=True in Config
        if update_data.type == TriggerType.schedule.value:
            if not update_data.cron:
                return {"ok": False, "error": "cron is required for schedule trigger"}, 400
            if not update_data.timezone:
                return {"ok": False, "error": "timezone is required for schedule trigger"}, 400

        # Additional validation for webhook type
        if update_data.type == TriggerType.webhook.value:
            if not update_data.webhook_type:
                return {"ok": False, "error": "webhook_type is required for webhook trigger"}, 400
            valid_webhook_types = ["github", "gitlab", "custom"]
            if update_data.webhook_type not in valid_webhook_types:
                return {"ok": False, "error": f"webhook_type must be one of: {valid_webhook_types}"}, 400

        try:
            with db.get_session(project_id) as session:
                version = session.query(ApplicationVersion).options(
                    joinedload(ApplicationVersion.application)
                ).filter(
                    ApplicationVersion.id == version_id
                ).first()

                if not version:
                    return {"ok": False, "error": f"Version {version_id} not found"}, 404

                application = version.application

                # Get current user ID
                current_user_id = auth.current_user().get("id")

                # Build trigger config for storage
                trigger_config = build_trigger_for_storage(update_data, current_user_id)

                # Handle webhook secret:
                # 1. If webhook_secret_value provided in payload - use it (user regenerated in UI)
                # 2. If no secret exists - generate one for the first time
                # 3. Otherwise keep existing secret
                # NOTE: We must merge with existing secrets to avoid overwriting other webhook secrets
                secret_info = {}
                webhook_type = trigger_config.get("webhook_type")
                if trigger_config.get("type") == TriggerType.webhook.value and webhook_type:
                    # Check if user provided a new secret value (regenerated in UI)
                    new_secret_from_ui = payload.get("webhook_secret_value")

                    # Check current trigger for existing secret
                    current_trigger = get_trigger_from_pipeline_settings(version.pipeline_settings or {})
                    existing_secret_ref = current_trigger.get("webhook_secret")
                    secret_key = f"webhook_secret_v{version_id}"

                    if new_secret_from_ui:
                        # User regenerated secret in UI - use raw token
                        new_secret = new_secret_from_ui
                        # Merge with existing project secrets to preserve other webhook secrets
                        vault_client = VaultClient(project_id)
                        project_secrets = vault_client.get_secrets() or {}
                        project_secrets[secret_key] = new_secret
                        vault_client.set_secrets(project_secrets)
                        trigger_config["webhook_secret"] = f"{{{{secret.{secret_key}}}}}"
                        log.info(f"Saved user-provided webhook secret for version {version_id} (type: {webhook_type})")
                    elif not existing_secret_ref:
                        # No secret exists - generate one for the first time
                        new_secret = _generate_webhook_secret(webhook_type)
                        # Merge with existing project secrets to preserve other webhook secrets
                        vault_client = VaultClient(project_id)
                        project_secrets = vault_client.get_secrets() or {}
                        project_secrets[secret_key] = new_secret
                        vault_client.set_secrets(project_secrets)
                        trigger_config["webhook_secret"] = f"{{{{secret.{secret_key}}}}}"
                        log.info(f"Generated initial webhook secret for version {version_id} (type: {webhook_type})")
                    else:
                        # Keep existing secret reference
                        trigger_config["webhook_secret"] = existing_secret_ref

                # Update pipeline_settings
                pipeline_settings = version.pipeline_settings or {}
                pipeline_settings["trigger"] = trigger_config
                version.pipeline_settings = pipeline_settings

                flag_modified(version, "pipeline_settings")
                session.commit()

                log.info(
                    f"Updated pipeline trigger for version {version_id}: "
                    f"type={update_data.type}"
                )

                # Build webhook URL and get secret info if applicable
                webhook_url = None
                if trigger_config.get("type") == TriggerType.webhook.value and webhook_type:
                    webhook_url = _build_webhook_url(project_id, version_id, webhook_type)
                    secret_info = _get_webhook_secret_for_display(
                        project_id, trigger_config, webhook_type
                    )

                # Return the updated trigger
                response = PipelineTriggerResponse(
                    type=trigger_config.get("type"),
                    cron=trigger_config.get("cron"),
                    timezone=trigger_config.get("timezone"),
                    last_run=trigger_config.get("last_run"),
                    created_by=trigger_config.get("created_by"),
                    webhook_type=webhook_type,
                    webhook_url=webhook_url,
                    **secret_info,
                )

                return serialize(response.dict()), 200

        except Exception as e:
            log.exception(f"Error updating pipeline trigger for version {version_id}: {e}")
            return {"ok": False, "error": "Error updating pipeline trigger"}, 500

    @auth.decorators.check_api({
        "permissions": ["models.applications.pipeline_trigger.edit"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, version_id: int, **kwargs):
        """
        Regenerate the webhook secret for a pipeline version.

        This is useful when the secret has been compromised.
        Only works when trigger type is 'webhook'.
        """
        try:
            with db.get_session(project_id) as session:
                version = session.query(ApplicationVersion).options(
                    joinedload(ApplicationVersion.application)
                ).filter(
                    ApplicationVersion.id == version_id
                ).first()

                if not version:
                    return {"ok": False, "error": f"Version {version_id} not found"}, 404

                # Check if current trigger is webhook
                pipeline_settings = version.pipeline_settings or {}
                trigger_data = get_trigger_from_pipeline_settings(pipeline_settings)
                trigger_type = trigger_data.get("type", TriggerType.chat_message.value)
                webhook_type = trigger_data.get("webhook_type")

                if trigger_type != TriggerType.webhook.value:
                    return {
                        "ok": False,
                        "error": "Cannot regenerate secret: trigger type is not webhook"
                    }, 400

                if not webhook_type:
                    return {
                        "ok": False,
                        "error": "Cannot regenerate secret: webhook_type not configured"
                    }, 400

                # Generate new secret
                new_secret = _generate_webhook_secret(webhook_type)
                secret_key = f"webhook_secret_v{version_id}"

                # Merge with existing project secrets to preserve other webhook secrets
                vault_client = VaultClient(project_id)
                project_secrets = vault_client.get_secrets() or {}
                project_secrets[secret_key] = new_secret
                vault_client.set_secrets(project_secrets)

                # Update trigger config with new secret reference
                trigger_data["webhook_secret"] = f"{{{{secret.{secret_key}}}}}"
                pipeline_settings["trigger"] = trigger_data
                version.pipeline_settings = pipeline_settings
                flag_modified(version, "pipeline_settings")
                session.commit()

                log.info(f"Regenerated webhook secret for version {version_id}")

                # Get updated secret info
                secret_info = _get_webhook_secret_for_display(
                    project_id, trigger_data, webhook_type
                )
                webhook_url = _build_webhook_url(project_id, version_id, webhook_type)

                # Return the updated trigger config with new secret
                response = PipelineTriggerResponse(
                    type=trigger_type,
                    cron=trigger_data.get("cron"),
                    timezone=trigger_data.get("timezone"),
                    last_run=trigger_data.get("last_run"),
                    created_by=trigger_data.get("created_by"),
                    webhook_type=webhook_type,
                    webhook_url=webhook_url,
                    **secret_info,
                )

                return serialize(response.dict()), 200

        except Exception as e:
            log.exception(f"Error regenerating webhook secret for version {version_id}: {e}")
            return {"ok": False, "error": "Error regenerating webhook secret"}, 500


class API(api_tools.APIBase):
    """
    Pipeline Trigger API.

    URL pattern: /api/v2/{mode}/{project_id}/pipeline/{version_id}/trigger
    """
    url_params = api_tools.with_modes([
        '<int:project_id>/pipeline/<int:version_id>/trigger',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
