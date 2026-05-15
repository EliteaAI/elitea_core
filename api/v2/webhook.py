#!/usr/bin/python3
# coding=utf-8
# pylint: disable=W0201

#   Copyright 2024-2025 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" API """

import json

from flask import request  # pylint: disable=E0401

from pydantic.v1 import ValidationError  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0401,E0611
from tools import api_tools, auth, db, VaultClient, rpc_tools  # pylint: disable=E0401

from ...models.all import ApplicationVersion, Application
from ...models.enums.all import AgentTypes
from ...models.pd.pipeline_trigger import TriggerType
from ...utils.constants import PROMPT_LIB_MODE  # pylint: disable=E0402
from ...utils.exceptions import VerifySignatureError  # pylint: disable=E0402
from ...utils.pipeline_trigger import (
    WEBHOOK_TYPE_CONFIG,
    normalize_secret_value,
    validate_webhook_secret,
    get_webhook_creator_id,
)
from ...rpc.pipeline_webhook import execute_pipeline_webhook


class WebHookAPI(api_tools.APIModeHandler):  # pylint: disable=R0903
    """ API """

    def _validate_webhook_signature(
        self,
        project_id: int,
        version_id: int,
        webhook_type: str,
        raw_data: bytes,
    ):
        """
        Validate webhook signature against stored secret.

        Args:
            project_id: Project ID
            version_id: Application version ID
            webhook_type: Type of webhook (github, gitlab, custom)
            raw_data: Raw request body for signature validation

        Returns:
            Tuple of (is_valid, error_response or None)
        """
        # Get webhook config for this type
        webhook_config = WEBHOOK_TYPE_CONFIG.get(webhook_type)
        if not webhook_config:
            return False, ({"error": "Bad signature type"}, 400)

        # Get signature from headers based on webhook type
        signature_header = webhook_config["signature_header"]
        if webhook_type == "custom":
            webhook_signature = request.headers
        else:
            webhook_signature = request.headers.get(signature_header)
            if webhook_signature is None:
                return False, ({"error": f"Missing request header {signature_header}"}, 400)

        # Get secret and validate
        with db.get_session(project_id) as session:
            from sqlalchemy.orm import joinedload

            version = session.query(ApplicationVersion).options(
                joinedload(ApplicationVersion.application)
            ).get(version_id)

            if not version:
                return False, ({"error": f"Version {version_id} not found"}, 404)

            # Get secret from version-specific trigger config
            pipeline_settings = version.pipeline_settings or {}
            trigger = pipeline_settings.get("trigger", {})
            webhook_secret_ref = trigger.get("webhook_secret")

            if not webhook_secret_ref:
                return False, ({"error": "Webhook secret not configured"}, 400)

            secret = VaultClient(project_id).unsecret(webhook_secret_ref)
            if not secret:
                return False, ({"error": "Webhook secret not configured"}, 400)

            # Normalize secret and validate using shared helper
            secret_value = normalize_secret_value(secret)
            is_valid, error_msg = validate_webhook_secret(
                webhook_type=webhook_type,
                secret_value=secret_value,
                signature=webhook_signature,
                raw_data=raw_data,
            )

            if not is_valid:
                return False, ({"error": error_msg}, 400)

            return True, (version, version.application)

    @api_tools.endpoint_metrics
    def post(self, project_id: int, version_id: int, webhook_type: str):  # pylint: disable=R0911
        """ POST """
        raw_data = request.data
        payload_str = raw_data.decode("utf-8")

        # Validate webhook signature first
        is_valid, result = self._validate_webhook_signature(
            project_id, version_id, webhook_type, raw_data
        )
        if not is_valid:
            return result  # Return error response

        version, application = result  # On success, result is the (ApplicationVersion, Application)

        # Check if this is a pipeline with webhook trigger
        if version.agent_type == AgentTypes.pipeline.value:
            pipeline_settings = version.pipeline_settings or {}
            trigger = pipeline_settings.get("trigger", {})

            if trigger.get("type") == TriggerType.webhook.value:
                # Validate webhook type matches configuration
                configured_webhook_type = trigger.get("webhook_type")
                if configured_webhook_type != webhook_type:
                    return {
                        "error": f"Webhook type mismatch: expected '{configured_webhook_type}', got '{webhook_type}'"
                    }, 400

                # Parse JSON payload
                try:
                    payload = json.loads(payload_str) if payload_str.strip() else {}
                except json.JSONDecodeError:
                    # If not valid JSON, wrap in a simple object
                    payload = {"raw_payload": payload_str}

                # Execute pipeline with History tracking
                # Use trigger creator as execution context so runs appear in their History
                user_id = get_webhook_creator_id(trigger, application)
                try:
                    result = execute_pipeline_webhook(
                        project_id=project_id,
                        version=version,
                        user_id=user_id,
                        webhook_type=webhook_type,
                        payload=payload,
                    )
                    if "error" in result:
                        return result, 400
                    return result, 200
                except Exception as exc:
                    log.exception(f"Pipeline webhook execution failed: {exc}")
                    return {"error": "Pipeline execution failed", "status": "error"}, 500

        # Fall back to existing behavior for non-pipeline apps or apps without webhook trigger
        payload_in = {
            "chat_history": [],
            "user_input": payload_str,
        }

        # Re-extract signature for do_predict (it does its own validation)
        if webhook_type == "github":
            webhook_signature = request.headers.get("x-hub-signature-256")
        elif webhook_type == "gitlab":
            webhook_signature = request.headers.get("x-gitlab-token")
        elif webhook_type == "custom":
            webhook_signature = request.headers
        else:
            webhook_signature = None

        # Use application owner as execution context for non-pipeline webhook (legacy)
        user_id = application.owner_id if application and application.owner_id else 1
        try:
            result = self.module.do_predict(
                project_id=project_id,
                user_id=user_id,
                version_id=version_id,
                payload_in=payload_in,
                raw=raw_data,
                webhook_signature=webhook_signature,
                webhook_type=webhook_type,
            )
            if "error" in result:
                return result, 400
        except ValidationError as e:
            return e.errors(), 400
        except VerifySignatureError as e:
            return e.value, 400
        except Exception as exc:
            log.error(exc)
            return {"error": "Can not do predict"}, 500

        return result, 200


class API(api_tools.APIBase):  # pylint: disable=R0903
    """ API """

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:version_id>/<webhook_type>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: WebHookAPI,
    }
