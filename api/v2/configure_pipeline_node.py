#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems
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

"""Configure a single pipeline node with AI.

POST /elitea_core/configure_pipeline_node/prompt_lib/<project_id>
"""

import json

from flask import request
from pydantic import ValidationError

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.configure_node import ConfigureNodeRequest, ConfigureNodeResponse
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.draft_llm_utils import (
    DraftSettingsError,
    answer_was_cut_short,
    describe_parse_failure,
    describe_predict_failure,
    extract_answer,
    resolve_draft_llm_settings,
    timeout_response,
)
from ...utils.predict_utils import PredictPayloadError
from ...utils.exceptions import PoolSaturationError
from ...utils.pipeline_prompt_utils import build_node_configure_system_prompt
from ...utils.pipeline_validator import KNOWN_NODE_TYPES
from ...utils.service_prompt_utils import get_service_prompt
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY_CONFIGURE = "configure_node_with_ai"
_SERVICE_PROMPT_KEY_REFERENCE = "pipeline_authoring_reference"
_AWAIT_TASK_TIMEOUT = 60
_DEFAULT_MAX_TOKENS = 4096
_DEFAULT_TEMPERATURE = 0.3


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Configure Pipeline Node with AI",
        description=(
            "Fill all fields of a pipeline node from a plain-text description of what "
            "the node should do. Returns a complete node configuration for the specified "
            "node type, ready to apply to the pipeline editor."
        ),
        request_body=ConfigureNodeRequest,
        tags=["elitea_core/applications"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        try:
            req = ConfigureNodeRequest.model_validate(request.json)
        except ValidationError as exc:
            return exc.errors(), 400

        node_type = req.node_type.lower().strip()
        if node_type not in KNOWN_NODE_TYPES:
            return {
                "error": (
                    f"Unknown node type '{req.node_type}'. "
                    f"Valid types: {', '.join(sorted(KNOWN_NODE_TYPES))}."
                )
            }, 400

        user_id = auth.current_user().get("id")

        try:
            llm_settings = resolve_draft_llm_settings(
                project_id,
                req.llm_settings,
                default_temperature=_DEFAULT_TEMPERATURE,
                default_max_tokens=_DEFAULT_MAX_TOKENS,
                log_prefix="configure_pipeline_node",
            )
        except DraftSettingsError as exc:
            return exc.body, exc.status

        # Fetch reference and configure template
        reference = get_service_prompt(_SERVICE_PROMPT_KEY_REFERENCE)
        if not reference:
            log.warning(
                "configure_pipeline_node: service prompt '%s' not configured; continuing without reference",
                _SERVICE_PROMPT_KEY_REFERENCE,
            )
            reference = ""

        template = get_service_prompt(_SERVICE_PROMPT_KEY_CONFIGURE)
        if not template:
            return {
                "error": f"Service prompt '{_SERVICE_PROMPT_KEY_CONFIGURE}' is not configured."
            }, 500

        system_prompt = build_node_configure_system_prompt(
            template,
            reference,
            node_type=node_type,
            state_variables=req.state_variables,
            available_nodes=req.available_nodes,
        )

        user_input = req.user_description
        if req.current_node_config:
            user_input = (
                f"{req.user_description}\n\nCurrent node configuration:\n"
                f"{json.dumps(req.current_node_config, indent=2)}"
            )

        log.debug("configure_pipeline_node: system_prompt length=%d", len(system_prompt))

        # Call LLM
        try:
            result = self.module.predict_sio_llm(
                sid=None,
                data={
                    "project_id": project_id,
                    "user_input": user_input,
                    "instructions": system_prompt,
                    "llm_settings": llm_settings,
                    "await_task_timeout": _AWAIT_TASK_TIMEOUT,
                },
                await_task_timeout=_AWAIT_TASK_TIMEOUT,
                user_id=user_id,
                skip_expansion=True,
                return_chat_history=True,
            )
        except PredictPayloadError as exc:
            return {"error": str(exc)}, 400
        except PoolSaturationError as exc:
            return {
                "error": "temporarily_unavailable",
                "message": "The service is busy. Please try again in a few seconds.",
                "retry_after": exc.retry_after,
            }, 503
        except Exception:
            log.exception("configure_pipeline_node: LLM call failed")
            return {"error": "LLM generation failed."}, 500

        timed_out = timeout_response(result, _AWAIT_TASK_TIMEOUT)
        if timed_out:
            return timed_out

        raw_text = extract_answer(result)
        if not raw_text:
            failure = describe_predict_failure(result)
            log.warning(
                "configure_pipeline_node: no response text; %s; full result=%s",
                failure or "no error reported", result,
            )
            return {"error": failure or "LLM returned an empty response."}, 500

        extracted = extract_json_from_text(raw_text)
        try:
            parsed = json.loads(extracted)
        except json.JSONDecodeError as exc:
            log.warning(
                "configure_pipeline_node: response did not parse: %s",
                describe_parse_failure(raw_text, extracted, exc, result),
            )
            if answer_was_cut_short(result, extracted):
                return {
                    "error": (
                        "LLM response was truncated. "
                        "Increase max_tokens in llm_settings (recommended: 4096+)."
                    )
                }, 422
            return {"error": "LLM returned unparseable output.", "parse_error": str(exc)}, 422

        if not isinstance(parsed, dict):
            return {"error": "LLM response was not a JSON object.", "raw": parsed}, 422

        # Build response — summary is optional; default to empty if LLM omitted it
        node_config = {k: v for k, v in parsed.items() if k != "summary"}
        summary = parsed.get("summary", f"Configured {node_type} node.")

        try:
            response = ConfigureNodeResponse.model_validate(
                {"node_config": node_config, "summary": summary}
            )
        except ValidationError as exc:
            log.warning("configure_pipeline_node: response model validation failed: %s", exc.errors())
            return {
                "error": "Generated node config failed validation.",
                "details": exc.errors(),
                "raw": parsed,
            }, 422

        return response.model_dump(), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
