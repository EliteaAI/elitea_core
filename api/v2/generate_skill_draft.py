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

from ...models.pd.generate_skill_draft import (
    GenerateSkillDraftRequest,
    GenerateSkillDraftResponse,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.predict_utils import PredictPayloadError
from ...utils.exceptions import PoolSaturationError
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY = "skill_generator"


def _get_system_prompt_template() -> str:
    try:
        configs = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_filtered_public(
            filter_fields={"type": "service_prompt"}
        )
        for cfg in configs or []:
            if cfg.get("data", {}).get("key") == _SERVICE_PROMPT_KEY:
                return cfg["data"].get("prompt", "")
    except Exception:
        log.warning("generate_skill_draft: failed to fetch service prompt from configurations")
    return ""


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Generate Skill Draft from Natural Language",
        description=(
            "Generate a draft skill (name, description, instructions) from a plain-text "
            "description. Uses the project's default LLM and the 'skill_generator' service "
            "prompt. Returns a validated JSON payload; no toolkit/agent/pipeline/MCP suggestions."
        ),
        request_body=GenerateSkillDraftRequest,
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        try:
            req = GenerateSkillDraftRequest.model_validate(request.json)
        except ValidationError as e:
            return e.errors(), 400

        user_id = auth.current_user().get("id")

        if req.llm_settings and req.llm_settings.model_name:
            llm_settings = req.llm_settings.model_dump(exclude_none=True)
        else:
            try:
                llm_settings = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_default_model(
                    project_id, section="llm"
                )
                if not llm_settings or not llm_settings.get("model_name"):
                    return {"error": "No default LLM model configured for this project"}, 400
                llm_settings.setdefault("temperature", 0.7)
                llm_settings.setdefault("max_tokens", 4096)
                if req.llm_settings:
                    overrides = req.llm_settings.model_dump(exclude_none=True, exclude={"model_name"})
                    llm_settings.update(overrides)
            except Exception:
                log.exception("generate_skill_draft: failed to get default model")
                return {"error": "Failed to resolve project default LLM model"}, 400

        system_prompt = _get_system_prompt_template()
        if not system_prompt:
            return {"error": "Service prompt 'skill_generator' is not configured"}, 500

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
            log.exception("generate_skill_draft: LLM call failed")
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
            log.debug("generate_skill_draft: LLM output is not valid JSON: %s", raw_text[:300])
            return {"error": "LLM returned unparseable output"}, 422

        try:
            draft = GenerateSkillDraftResponse.model_validate(parsed)
        except ValidationError as e:
            log.warning("generate_skill_draft: validation failed: %s", e.errors())
            return {"error": "Generated draft failed validation", "details": e.errors(), "raw": parsed}, 422

        return draft.model_dump(), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
