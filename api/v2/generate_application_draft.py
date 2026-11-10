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

import json

from flask import request
from pydantic import ValidationError

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.generate_application_draft import (
    GenerateApplicationDraftRequest,
    GenerateApplicationDraftResponse,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.conversation_starters_utils import get_max_conversation_starters
from ...utils.draft_llm_utils import (
    DraftSettingsError,
    answer_was_cut_short,
    apply_offered_origins,
    describe_parse_failure,
    describe_predict_failure,
    extract_answer,
    resolve_draft_llm_settings,
    timeout_response,
)
from ...utils.predict_utils import PredictPayloadError
from ...utils.exceptions import PoolSaturationError
from ...utils.generate_application_utils import (
    fetch_project_resources,
    build_system_prompt,
    fetch_application_for_edit,
    build_edit_system_prompt,
)
from ...utils.service_prompt_utils import get_service_prompt
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY_CREATE = "generate_application_draft"
_SERVICE_PROMPT_KEY_EDIT = "edit_application_draft"
_AWAIT_TASK_TIMEOUT = 60
_DEFAULT_MAX_TOKENS = 4096
_DEFAULT_TEMPERATURE = 0.3



class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Generate Application Draft from Natural Language",
        description=(
            "Generate a draft agent configuration from a plain-text description. "
            "Uses the project's default LLM. Returns a validated JSON payload with "
            "name, instructions, welcome message, conversation starters, and resource suggestions. "
            "Add application_id and version_id to edit an existing application."
        ),
        request_body=GenerateApplicationDraftRequest,
        tags=["elitea_core/applications"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        try:
            req = GenerateApplicationDraftRequest.model_validate(request.json)
        except ValidationError as e:
            return e.errors(), 400

        user_id = auth.current_user().get("id")

        try:
            llm_settings = resolve_draft_llm_settings(
                project_id,
                req.llm_settings,
                default_temperature=_DEFAULT_TEMPERATURE,
                default_max_tokens=_DEFAULT_MAX_TOKENS,
                log_prefix="generate_application_draft",
            )
        except DraftSettingsError as exc:
            return exc.body, exc.status

        if req.is_edit_mode:
            # Edit mode: fetch existing application config, provide ALL available resources
            current_config = fetch_application_for_edit(
                project_id, req.application_id, req.version_id
            )
            if current_config is None:
                return {"error": "Application or version not found"}, 404

            log.debug("generate_application_draft: edit mode, current_config attached_toolkits=%s", current_config.get("attached_toolkits"))

            try:
                toolkits, mcp, agents, pipelines, skills = fetch_project_resources(project_id, req.user_description)
            except Exception:
                log.warning("generate_application_draft: failed to fetch project resources")
                toolkits, mcp, agents, pipelines, skills = [], [], [], [], []

            # LLM gets ALL available resources to decide what to keep/add/remove
            template = get_service_prompt(_SERVICE_PROMPT_KEY_EDIT)
            if not template:
                return {"error": "Service prompt 'edit_application_draft' is not configured"}, 500
            system_prompt = build_edit_system_prompt(
                template, project_id, current_config, toolkits, mcp, agents, pipelines, skills
            )
            log.debug("generate_application_draft: edit system_prompt length=%d", len(system_prompt))
        else:
            # Create mode: existing behavior
            try:
                toolkits, mcp, agents, pipelines, skills = fetch_project_resources(project_id, req.user_description)
            except Exception:
                log.warning("generate_application_draft: failed to fetch project resources")
                toolkits, mcp, agents, pipelines, skills = [], [], [], [], []

            template = get_service_prompt(_SERVICE_PROMPT_KEY_CREATE)
            if not template:
                return {"error": "Service prompt 'generate_application_draft' is not configured"}, 500
            system_prompt = build_system_prompt(template, project_id, toolkits, mcp, agents, pipelines, skills)

        try:
            result = self.module.predict_sio_llm(
                sid=None,
                data={
                    "project_id": project_id,
                    "user_input": req.user_description,
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
            log.exception("generate_application_draft: LLM call failed")
            return {"error": "LLM generation failed"}, 500

        timed_out = timeout_response(result, _AWAIT_TASK_TIMEOUT)
        if timed_out:
            return timed_out

        raw_text = extract_answer(result)
        if not raw_text:
            failure = describe_predict_failure(result)
            log.warning(
                "generate_application_draft: no draft text; %s; full result=%s",
                failure or "no error reported", result,
            )
            return {"error": failure or "LLM returned an empty response"}, 500

        extracted = extract_json_from_text(raw_text)
        try:
            parsed = json.loads(extracted)
        except json.JSONDecodeError as e:
            log.warning(
                "generate_application_draft: draft did not parse: %s",
                describe_parse_failure(raw_text, extracted, e, result),
            )
            if answer_was_cut_short(result, extracted):
                return {
                    "error": "LLM response was truncated. Increase max_tokens in llm_settings (recommended: 4096+)."
                }, 422
            return {"error": "LLM returned unparseable output", "parse_error": str(e)}, 422

        apply_offered_origins(parsed, project_id, agents, pipelines, skills)

        try:
            draft = GenerateApplicationDraftResponse.model_validate(
                parsed, context={'max_conversation_starters': get_max_conversation_starters()},
            )
        except ValidationError as e:
            log.warning("generate_application_draft: validation failed: %s", e.errors())
            return {"error": "Generated draft failed validation", "details": e.errors(), "raw": parsed}, 422

        return draft.model_dump(exclude_none=True), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
