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

from ...models.pd.generate_application_draft import (
    GenerateApplicationDraftRequest,
    GenerateApplicationDraftResponse,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.predict_utils import PredictPayloadError
from ...utils.exceptions import PoolSaturationError
from ...utils.generate_application_utils import fetch_project_resources, build_system_prompt
from ...utils.service_prompt_utils import get_service_prompt
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY = "generate_application_draft"


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Generate Application Draft from Natural Language",
        description=(
            "Generate a draft agent configuration from a plain-text description. "
            "Uses the project's default LLM. Returns a validated JSON payload with "
            "name, instructions, welcome message, conversation starters, and resource suggestions."
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

        # Resolve LLM settings — explicit override or project default
        if req.llm_settings and req.llm_settings.model_name:
            llm_settings = req.llm_settings.model_dump(exclude_none=True)
        else:
            try:
                llm_settings = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_default_model(
                    project_id, section="llm"
                )
                if not llm_settings or not llm_settings.get("model_name"):
                    return {"error": "No default LLM model configured for this project"}, 400
                # Structured JSON generation — favour determinism and low cost
                llm_settings.setdefault("temperature", 0.7)
                llm_settings.setdefault("max_tokens", 4096)
                if req.llm_settings:
                    overrides = req.llm_settings.model_dump(exclude_none=True, exclude={"model_name"})
                    llm_settings.update(overrides)
            except Exception:
                log.exception("generate_application_draft: failed to get default model")
                return {"error": "Failed to resolve project default LLM model"}, 400

        try:
            toolkits, agents, skills = fetch_project_resources(project_id, req.user_description)
        except Exception:
            log.warning("generate_application_draft: failed to fetch project resources")
            toolkits, agents, skills = [], [], []

        template = get_service_prompt(_SERVICE_PROMPT_KEY)
        if not template:
            return {"error": "Service prompt 'generate_application_draft' is not configured"}, 500
        system_prompt = build_system_prompt(template, toolkits, agents, skills)

        try:
            result = self.module.predict_sio_llm(
                sid=None,
                data={
                    "project_id": project_id,
                    "user_input": req.user_description,
                    "instructions": system_prompt,
                    "llm_settings": llm_settings,
                    "await_task_timeout": 60,
                },
                await_task_timeout=60,
                user_id=user_id,
                is_system_user=True,
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

        task_result = result.get("result") or {}
        thinking_steps = task_result.get("thinking_steps", []) if isinstance(task_result, dict) else []
        raw_text = next(
            (s["text"] for s in reversed(thinking_steps) if s.get("text")),
            "",
        )
        if not raw_text:
            return {"error": "LLM returned an empty response"}, 500

        try:
            parsed = json.loads(extract_json_from_text(raw_text))
        except json.JSONDecodeError:
            log.debug("generate_application_draft: LLM output is not valid JSON: %s", raw_text[:300])
            return {"error": "LLM returned unparseable output"}, 422

        try:
            draft = GenerateApplicationDraftResponse.model_validate(parsed)
        except ValidationError as e:
            log.warning("generate_application_draft: validation failed: %s", e.errors())
            return {"error": "Generated draft failed validation", "details": e.errors(), "raw": parsed}, 422

        return draft.model_dump(), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
