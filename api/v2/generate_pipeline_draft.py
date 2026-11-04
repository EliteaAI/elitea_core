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

"""Generate or edit a pipeline draft from a plain-text description.

POST /elitea_core/generate_pipeline_draft/prompt_lib/<project_id>
"""

import json

from flask import request
from pydantic import ValidationError

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...models.pd.generate_pipeline_draft import (
    GeneratePipelineDraftRequest,
    GeneratePipelineDraftResponse,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.draft_llm_utils import (
    answer_was_cut_short,
    caller_chose,
    describe_parse_failure,
    describe_predict_failure,
    extract_answer,
    resolve_model,
    timeout_response,
)
from ...utils.predict_utils import PredictPayloadError
from ...utils.exceptions import PoolSaturationError
from ...utils.generate_application_utils import (
    fetch_project_resources,
    fetch_application_for_edit,
)
from ...utils.pipeline_prompt_utils import (
    build_pipeline_system_prompt,
    build_edit_pipeline_system_prompt,
)
from ...utils.pipeline_validator import fix_deprecated_end_node, validate_pipeline_yaml
from ...utils.service_prompt_utils import get_service_prompt
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY_CREATE = "generate_pipeline_draft"
_SERVICE_PROMPT_KEY_EDIT = "edit_pipeline_draft"
_SERVICE_PROMPT_KEY_REFERENCE = "pipeline_authoring_reference"
_AWAIT_TASK_TIMEOUT = 120
_DEFAULT_MAX_TOKENS = 8192
_DEFAULT_TEMPERATURE = 0.3


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Generate Pipeline Draft from Natural Language",
        description=(
            "Generate a new pipeline configuration or edit an existing one from a "
            "plain-text description. Uses the project's LLM. Returns a validated "
            "pipeline YAML with name, description, and resource suggestions. "
            "Add pipeline_id and version_id to edit an existing pipeline."
        ),
        request_body=GeneratePipelineDraftRequest,
        tags=["elitea_core/applications"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        try:
            req = GeneratePipelineDraftRequest.model_validate(request.json)
        except ValidationError as e:
            return e.errors(), 400

        user_id = auth.current_user().get("id")

        caller_set_max_tokens = caller_chose(req.llm_settings, "max_tokens")
        caller_set_temperature = caller_chose(req.llm_settings, "temperature")

        # Resolve LLM settings:
        # 1. explicit override in request
        # 2. pipeline's own llm_settings (edit mode only)
        # 3. project default
        current_config = None
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
            pipeline_llm_settings = {}
            if req.is_edit_mode:
                current_config = fetch_application_for_edit(
                    project_id, req.pipeline_id, req.version_id
                )
                if current_config is None:
                    return {"error": "Pipeline or version not found."}, 404
                pipeline_llm_settings = current_config.get("llm_settings") or {}

            if pipeline_llm_settings.get("model_name"):
                llm_settings = dict(pipeline_llm_settings)
                if req.llm_settings:
                    overrides = req.llm_settings.model_dump(
                        exclude_none=True, exclude={"model_name", "model_project_id"}
                    )
                    llm_settings.update(overrides)
            else:
                try:
                    llm_settings = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_default_model(
                        project_id, section="llm"
                    )
                    if not llm_settings or not llm_settings.get("model_name"):
                        return {
                            "error": (
                                "No LLM model configured for this project. "
                                "Go to Settings → Default model to set one."
                            )
                        }, 400
                    if req.llm_settings:
                        overrides = req.llm_settings.model_dump(
                            exclude_none=True, exclude={"model_name", "model_project_id"}
                        )
                        llm_settings.update(overrides)
                except Exception:
                    log.exception("generate_pipeline_draft: failed to get default model")
                    return {"error": "Failed to resolve project default LLM model."}, 400

        if not caller_set_temperature:
            llm_settings["temperature"] = _DEFAULT_TEMPERATURE
        if not caller_set_max_tokens:
            llm_settings["max_tokens"] = _DEFAULT_MAX_TOKENS

        # Fetch resources
        try:
            toolkits, mcp, agents, pipelines, skills = fetch_project_resources(
                project_id, req.user_description
            )
        except Exception:
            log.warning("generate_pipeline_draft: failed to fetch project resources")
            toolkits, mcp, agents, pipelines, skills = [], [], [], [], []

        # Fetch reference
        reference = get_service_prompt(_SERVICE_PROMPT_KEY_REFERENCE)
        if not reference:
            log.warning(
                "generate_pipeline_draft: service prompt '%s' not configured; continuing without reference",
                _SERVICE_PROMPT_KEY_REFERENCE,
            )
            reference = ""

        if req.is_edit_mode:
            if current_config is None:
                current_config = fetch_application_for_edit(
                    project_id, req.pipeline_id, req.version_id
                )
                if current_config is None:
                    return {"error": "Pipeline or version not found."}, 404

            template = get_service_prompt(_SERVICE_PROMPT_KEY_EDIT)
            if not template:
                return {
                    "error": f"Service prompt '{_SERVICE_PROMPT_KEY_EDIT}' is not configured."
                }, 500
            system_prompt = build_edit_pipeline_system_prompt(
                template, reference, current_config, toolkits, mcp, agents, pipelines, skills
            )
        else:
            template = get_service_prompt(_SERVICE_PROMPT_KEY_CREATE)
            if not template:
                return {
                    "error": f"Service prompt '{_SERVICE_PROMPT_KEY_CREATE}' is not configured."
                }, 500
            system_prompt = build_pipeline_system_prompt(
                template, reference, toolkits, mcp, agents, pipelines, skills
            )

        log.debug("generate_pipeline_draft: system_prompt length=%d", len(system_prompt))

        # Call LLM
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
            log.exception("generate_pipeline_draft: LLM call failed")
            return {"error": "LLM generation failed."}, 500

        timed_out = timeout_response(result, _AWAIT_TASK_TIMEOUT)
        if timed_out:
            return timed_out

        raw_text = extract_answer(result)
        if not raw_text:
            failure = describe_predict_failure(result)
            log.warning(
                "generate_pipeline_draft: no draft text; %s; full result=%s",
                failure or "no error reported", result,
            )
            return {"error": failure or "LLM returned an empty response."}, 500

        extracted = extract_json_from_text(raw_text)
        try:
            parsed = json.loads(extracted)
        except json.JSONDecodeError as exc:
            log.warning(
                "generate_pipeline_draft: draft did not parse: %s",
                describe_parse_failure(raw_text, extracted, exc, result),
            )
            if answer_was_cut_short(result, extracted):
                return {
                    "error": (
                        "LLM response was truncated. "
                        "Increase max_tokens in llm_settings (recommended: 8192+)."
                    )
                }, 422
            return {"error": "LLM returned unparseable output.", "parse_error": str(exc)}, 422

        # Validate the generated pipeline YAML
        pipeline_yaml = parsed.get("pipeline_yaml", "")
        if not pipeline_yaml or not isinstance(pipeline_yaml, str):
            return {
                "error": "LLM did not return a 'pipeline_yaml' field.",
                "raw": parsed,
            }, 422

        # Auto-fix the deprecated type: END / end_node pattern before validation
        pipeline_yaml = fix_deprecated_end_node(pipeline_yaml)
        parsed["pipeline_yaml"] = pipeline_yaml

        validation_result = validate_pipeline_yaml(pipeline_yaml, project_id=project_id)
        # An AI-generated pipeline must have nodes and a terminal path — treat
        # missing_nodes and no_end_node as hard failures here even though the
        # validator only warns (to allow draft saves).
        _draft_warning_codes = {"missing_nodes", "no_end_node"}
        structural_failures = validation_result.errors + [
            w for w in validation_result.warnings if w.code in _draft_warning_codes
        ]
        if not validation_result.valid or structural_failures:
            log.warning(
                "generate_pipeline_draft: generated YAML failed validation: %s",
                [e.model_dump() for e in structural_failures],
            )
            return {
                "error": "Generated pipeline YAML failed structural validation.",
                "validation_errors": [e.model_dump() for e in structural_failures],
                "raw": parsed,
            }, 422

        # Inject validation result into parsed dict for response model
        parsed["validation_result"] = validation_result.model_dump()

        # Inject project_id into suggested agents/pipelines/skills so catalog items
        # carry the public project ID and the frontend can link them correctly.
        _app_project_id = {
            a["application_id"]: a.get("project_id")
            for a in (agents + pipelines)
            if a.get("project_id") is not None
        }
        _skill_project_id = {
            s["id"]: s.get("project_id")
            for s in skills
            if s.get("project_id") is not None
        }
        for key in ("suggested_agents", "suggested_pipelines"):
            for item in parsed.get(key) or []:
                if isinstance(item, dict):
                    pid = _app_project_id.get(item.get("application_id"))
                    if pid is not None:
                        item["project_id"] = pid
        for item in parsed.get("suggested_skills") or []:
            if isinstance(item, dict):
                pid = _skill_project_id.get(item.get("id"))
                if pid is not None:
                    item["project_id"] = pid

        try:
            draft = GeneratePipelineDraftResponse.model_validate(parsed)
        except ValidationError as exc:
            log.warning("generate_pipeline_draft: response validation failed: %s", exc.errors())
            return {
                "error": "Generated draft failed validation.",
                "details": exc.errors(),
                "raw": parsed,
            }, 422

        return draft.model_dump(exclude_none=True), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
