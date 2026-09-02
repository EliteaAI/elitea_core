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
from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...models.pd.generate_project_context_draft import (
    GenerateProjectContextDraftRequest,
    GenerateProjectContextDraftResponse,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.draft_llm_utils import (
    caller_chose,
    describe_predict_failure,
    extract_draft_text,
    hit_token_limit,
    is_truncated_json,
    resolve_model,
    timeout_response,
)
from ...utils.predict_utils import PredictPayloadError
from ...utils.exceptions import PoolSaturationError
from ...utils.generate_project_context_utils import (
    build_create_project_context_system_prompt,
    build_edit_project_context_system_prompt,
)
from ...utils.service_prompt_utils import get_service_prompt
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY_CREATE = "project_context_generator"
_SERVICE_PROMPT_KEY_EDIT = "edit_project_context_draft"
_AWAIT_TASK_TIMEOUT = 60
_DEFAULT_MAX_TOKENS = 4096


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Generate Project Context Draft from Natural Language",
        description=(
            "Generate a draft Project Context from a plain-text description of the "
            "project. Uses the project's default LLM and the 'project_context_generator' service "
            "prompt. Returns a validated JSON payload; no toolkit/agent/pipeline/MCP/resource suggestions."
        ),
        request_body=GenerateProjectContextDraftRequest,
        tags=["elitea_core/project_context"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.project_context.generate"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        try:
            req = GenerateProjectContextDraftRequest.model_validate(request.json)
        except ValidationError as e:
            return e.errors(), 400

        user_id = auth.current_user().get("id")

        caller_set_max_tokens = caller_chose(req.llm_settings, "max_tokens")

        if req.llm_settings and req.llm_settings.model_name:
            llm_settings = req.llm_settings.model_dump(exclude_none=True)
            unavailable, owning_project_id = resolve_model(
                project_id, llm_settings["model_name"], llm_settings.get("model_project_id")
            )
            if unavailable:
                return {"error": unavailable}, 400
            if owning_project_id is not None:
                llm_settings["model_project_id"] = owning_project_id
        else:
            try:
                llm_settings = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_default_model(
                    project_id, section="llm"
                )
                if not llm_settings or not llm_settings.get("model_name"):
                    return {"error": "No default LLM model configured for this project"}, 400
                llm_settings.setdefault("temperature", 0.7)
                if req.llm_settings:
                    # model_project_id only qualifies a model_name; with none supplied there is
                    # nothing for it to qualify, and the default model brings its own
                    overrides = req.llm_settings.model_dump(
                        exclude_none=True, exclude={"model_name", "model_project_id"}
                    )
                    llm_settings.update(overrides)
            except Exception:
                log.exception("generate_project_context_draft: failed to get default model")
                return {"error": "Failed to resolve project default LLM model"}, 400

        if not caller_set_max_tokens:
            llm_settings["max_tokens"] = _DEFAULT_MAX_TOKENS

        if req.is_edit_mode:
            template = get_service_prompt(_SERVICE_PROMPT_KEY_EDIT)
            if not template:
                return {"error": "Service prompt 'edit_project_context_draft' is not configured"}, 500
            system_prompt = build_edit_project_context_system_prompt(
                template,
                req.current_project_background,
                req.current_activation_description,
            )
        else:
            template = get_service_prompt(_SERVICE_PROMPT_KEY_CREATE)
            if not template:
                return {"error": "Service prompt 'project_context_generator' is not configured"}, 500
            system_prompt = build_create_project_context_system_prompt(template)

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
            log.exception("generate_project_context_draft: LLM call failed")
            return {"error": "LLM generation failed"}, 500

        timed_out = timeout_response(result, _AWAIT_TASK_TIMEOUT)
        if timed_out:
            return timed_out

        raw_text = extract_draft_text(result)
        if not raw_text:
            failure = describe_predict_failure(result)
            log.warning(
                "generate_project_context_draft: no draft text; %s; full result=%s",
                failure or "no error reported", result,
            )
            return {"error": failure or "LLM returned an empty response"}, 500

        try:
            parsed = json.loads(extract_json_from_text(raw_text))
        except json.JSONDecodeError as e:
            log.debug("generate_project_context_draft: LLM output is not valid JSON: %s", raw_text[:300])
            if hit_token_limit(result) or is_truncated_json(raw_text):
                return {
                    "error": "LLM response was truncated. Increase max_tokens in llm_settings "
                             "(recommended: 4096+)."
                }, 422
            return {"error": "LLM returned unparseable output", "parse_error": str(e)}, 422

        try:
            draft = GenerateProjectContextDraftResponse.model_validate(parsed)
        except ValidationError as e:
            log.warning("generate_project_context_draft: validation failed: %s", e.errors())
            return {"error": "Generated draft failed validation", "details": e.errors(), "raw": parsed}, 422

        return draft.model_dump(), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
