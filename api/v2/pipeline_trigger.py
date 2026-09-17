"""
API endpoint for Pipeline Trigger configuration.

Provides GET/PUT/POST operations to read and update pipeline trigger settings
stored in ApplicationVersion.pipeline_settings['trigger'].
"""
from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from tools import api_tools, auth, config as c, db, serialize, register_openapi

from ...models.all import ApplicationVersion
from ...models.pd.pipeline_trigger import (
    UpdatePipelineTrigger,
    PipelineTriggerResponse,
    TriggerType,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.pipeline_trigger import (
    GITLAB_AUTH_SIGNING_TOKEN,
    generate_webhook_secret,
    store_webhook_secret,
    store_webhook_signing_secret,
    get_webhook_secret_for_display,
    build_webhook_url,
    get_trigger_from_pipeline_settings,
    build_trigger_for_storage,
    validate_webhook_secret_strength,
    validate_gitlab_signing_token_format,
)
from ...utils.folder_access import require_folder_access, APPLICATION_ENTITY_TYPES


class PromptLibAPI(api_tools.APIModeHandler):
    """API handler for pipeline trigger configuration."""

    @register_openapi(
        name="Get Pipeline Trigger",
        description="Get current trigger configuration for a pipeline version.",
        tags=["elitea_core/applications"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }
    })
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'version_id', via_version=True)
    def get(self, project_id: int, version_id: int, **kwargs):
        """
        Get the current trigger configuration for a pipeline version.

        Returns the trigger type and schedule details (if applicable).
        Defaults to 'chat_message' for legacy pipelines without trigger config.

        Note: Webhook secrets are masked for viewers (users without edit permission).
        """
        try:
            # Check if user has edit permission (viewers don't)
            # If they don't have edit permission, mask the secret
            user_permissions = auth.resolve_permissions(mode=c.DEFAULT_MODE, project_id=project_id)
            can_edit = bool(
                {"models.applications.version.update"}.intersection(user_permissions)
            )
            should_mask = not can_edit

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
                gitlab_auth_method = trigger_data.get("gitlab_auth_method")
                if trigger_type == TriggerType.webhook.value and webhook_type:
                    webhook_url = build_webhook_url(project_id, version_id, webhook_type)
                    secret_info = get_webhook_secret_for_display(
                        project_id, trigger_data, webhook_type,
                        should_mask=should_mask, auth_method=gitlab_auth_method,
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
                    gitlab_auth_method=gitlab_auth_method,
                    **secret_info,
                )

                log.debug(f"Pipeline trigger GET: version_id={version_id}, trigger_data={trigger_data}, response={response.dict()}")
                return serialize(response.dict()), 200

        except Exception as e:
            log.exception(f"Error fetching pipeline trigger for version {version_id}: {e}")
            return {"ok": False, "error": "Error fetching pipeline trigger"}, 500

    @register_openapi(
        name="Update Pipeline Trigger",
        description="Update trigger configuration for a pipeline version.",
        tags=["elitea_core/applications"],
        request_body=UpdatePipelineTrigger,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'version_id', write=True, via_version=True)
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
            if update_data.gitlab_auth_method and update_data.webhook_type != "gitlab":
                return {
                    "ok": False,
                    "error": "gitlab_auth_method is only valid for gitlab webhooks"
                }, 400

        try:
            with db.get_session(project_id) as session:
                version = session.query(ApplicationVersion).options(
                    joinedload(ApplicationVersion.application)
                ).filter(
                    ApplicationVersion.id == version_id
                ).first()

                if not version:
                    return {"ok": False, "error": f"Version {version_id} not found"}, 404

                # Get current user ID
                current_user_id = auth.current_user().get("id")

                # Build trigger config for storage
                trigger_config = build_trigger_for_storage(update_data, current_user_id)

                # Handle webhook secret using shared helper
                secret_info = {}
                webhook_type = trigger_config.get("webhook_type")
                if trigger_config.get("type") == TriggerType.webhook.value and webhook_type:
                    current_trigger = get_trigger_from_pipeline_settings(version.pipeline_settings or {})
                    existing_secret_ref = current_trigger.get("webhook_secret")
                    existing_signing_ref = current_trigger.get("webhook_signing_secret")

                    if trigger_config.get("gitlab_auth_method") == GITLAB_AUTH_SIGNING_TOKEN:
                        # GitLab issues the signing token and shows it once, so it can only
                        # ever be pasted in — there is nothing for Elitea to generate.
                        new_signing_token = payload.get("webhook_signing_secret_value")
                        if new_signing_token:
                            format_error = validate_gitlab_signing_token_format(new_signing_token)
                            if format_error:
                                return {"ok": False, "error": format_error}, 400
                            trigger_config["webhook_signing_secret"] = store_webhook_signing_secret(
                                project_id, version_id, new_signing_token
                            )
                        elif existing_signing_ref:
                            trigger_config["webhook_signing_secret"] = existing_signing_ref
                        else:
                            return {
                                "ok": False,
                                "error": "webhook_signing_secret_value is required to enable GitLab signing tokens"
                            }, 400
                    else:
                        new_secret_from_ui = payload.get("webhook_secret_value")
                        if new_secret_from_ui:
                            strength_error = validate_webhook_secret_strength(new_secret_from_ui)
                            if strength_error:
                                return {"ok": False, "error": strength_error}, 400

                        # Generate new secret if UI provided one or none exists yet
                        if new_secret_from_ui or not existing_secret_ref:
                            secret_value = new_secret_from_ui or generate_webhook_secret()
                            trigger_config["webhook_secret"] = store_webhook_secret(
                                project_id, version_id, secret_value
                            )
                        else:
                            trigger_config["webhook_secret"] = existing_secret_ref

                    # Carry the inactive secret across so switching auth methods back and
                    # forth does not silently invalidate what is configured in GitLab.
                    if "webhook_secret" not in trigger_config and existing_secret_ref:
                        trigger_config["webhook_secret"] = existing_secret_ref
                    if "webhook_signing_secret" not in trigger_config and existing_signing_ref:
                        trigger_config["webhook_signing_secret"] = existing_signing_ref

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
                    webhook_url = build_webhook_url(project_id, version_id, webhook_type)
                    secret_info = get_webhook_secret_for_display(
                        project_id, trigger_config, webhook_type,
                        auth_method=trigger_config.get("gitlab_auth_method"),
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
                    gitlab_auth_method=trigger_config.get("gitlab_auth_method"),
                    **secret_info,
                )

                return serialize(response.dict()), 200

        except Exception as e:
            log.exception(f"Error updating pipeline trigger for version {version_id}: {e}")
            return {"ok": False, "error": "Error updating pipeline trigger"}, 500

    @register_openapi(
        name="Regenerate Pipeline Webhook Secret",
        description="Regenerate webhook secret for a pipeline version configured with webhook trigger.",
        tags=["elitea_core/applications"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'version_id', via_version=True)
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
                gitlab_auth_method = trigger_data.get("gitlab_auth_method")

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

                if gitlab_auth_method == GITLAB_AUTH_SIGNING_TOKEN:
                    return {
                        "ok": False,
                        "error": "Cannot regenerate a GitLab signing token: rotate it in GitLab, "
                                 "then save the new token here"
                    }, 400

                # Generate and store new secret using shared helper
                new_secret = generate_webhook_secret()
                trigger_data["webhook_secret"] = store_webhook_secret(
                    project_id, version_id, new_secret
                )

                # Update trigger config
                pipeline_settings["trigger"] = trigger_data
                version.pipeline_settings = pipeline_settings
                flag_modified(version, "pipeline_settings")
                session.commit()

                log.info(f"Regenerated webhook secret for version {version_id}")

                # Get updated secret info
                secret_info = get_webhook_secret_for_display(
                    project_id, trigger_data, webhook_type, auth_method=gitlab_auth_method
                )
                webhook_url = build_webhook_url(project_id, version_id, webhook_type)

                # Return the updated trigger config with new secret
                response = PipelineTriggerResponse(
                    type=trigger_type,
                    cron=trigger_data.get("cron"),
                    timezone=trigger_data.get("timezone"),
                    last_run=trigger_data.get("last_run"),
                    created_by=trigger_data.get("created_by"),
                    webhook_type=webhook_type,
                    webhook_url=webhook_url,
                    gitlab_auth_method=gitlab_auth_method,
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
