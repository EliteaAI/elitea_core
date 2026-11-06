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
from tools import api_tools, auth, config as c, register_openapi

from ...models.pd.generate_pipeline_draft import (
    GeneratePipelineDraftRequest,
    GeneratePipelineDraftResponse,
)
from ...utils.constants import PROMPT_LIB_MODE
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
    fetch_application_for_edit,
)
from ...utils.pipeline_prompt_utils import (
    build_pipeline_system_prompt,
    build_edit_pipeline_system_prompt,
)
from ...utils.pipeline_validator import fix_deprecated_end_node, fix_missing_node_bindings, validate_pipeline_yaml
from ...utils.service_prompt_utils import get_service_prompt
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY_CREATE = "generate_pipeline_draft"
_SERVICE_PROMPT_KEY_EDIT = "edit_pipeline_draft"
_SERVICE_PROMPT_KEY_REFERENCE = "pipeline_authoring_reference"
_AWAIT_TASK_TIMEOUT = 120
_DEFAULT_MAX_TOKENS = 8192
_DEFAULT_TEMPERATURE = 0.3
# one repair call after a structurally invalid draft; each call can take up to _AWAIT_TASK_TIMEOUT
_MAX_REPAIR_ATTEMPTS = 1
_DRAFT_FAILURE_WARNINGS = frozenset({"missing_nodes", "no_end_node"})


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
        "permissions": ["models.applications.applications.details"],
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

        # Create mode requires create permission; edit mode requires update permission.
        caller_permissions = auth.resolve_permissions(mode=c.DEFAULT_MODE, project_id=project_id)
        if req.is_edit_mode:
            if "models.applications.applications.update" not in caller_permissions:
                return {"error": "Permission denied."}, 403
        else:
            if "models.applications.applications.create" not in caller_permissions:
                return {"error": "Permission denied."}, 403

        user_id = auth.current_user().get("id")

        current_config = None
        if req.is_edit_mode:
            current_config = fetch_application_for_edit(project_id, req.pipeline_id, req.version_id)
            if current_config is None:
                return {"error": "Pipeline or version not found."}, 404

        try:
            llm_settings = resolve_draft_llm_settings(
                project_id,
                req.llm_settings,
                default_temperature=_DEFAULT_TEMPERATURE,
                default_max_tokens=_DEFAULT_MAX_TOKENS,
                fallback_settings=(current_config or {}).get("llm_settings"),
                log_prefix="generate_pipeline_draft",
            )
        except DraftSettingsError as exc:
            return exc.body, exc.status

        try:
            toolkits, mcp, agents, pipelines, skills = fetch_project_resources(
                project_id, req.user_description
            )
        except Exception:
            log.warning("generate_pipeline_draft: failed to fetch project resources")
            toolkits, mcp, agents, pipelines, skills = [], [], [], [], []

        reference = get_service_prompt(_SERVICE_PROMPT_KEY_REFERENCE)
        if not reference:
            log.warning(
                "generate_pipeline_draft: service prompt '%s' not configured; continuing without reference",
                _SERVICE_PROMPT_KEY_REFERENCE,
            )
            reference = ""

        template_key = _SERVICE_PROMPT_KEY_EDIT if req.is_edit_mode else _SERVICE_PROMPT_KEY_CREATE
        template = get_service_prompt(template_key)
        if not template:
            return {"error": f"Service prompt '{template_key}' is not configured."}, 500
        if req.is_edit_mode:
            system_prompt = build_edit_pipeline_system_prompt(
                template, reference, project_id, current_config, toolkits, mcp, agents, pipelines, skills
            )
        else:
            system_prompt = build_pipeline_system_prompt(
                template, reference, project_id, toolkits, mcp, agents, pipelines, skills
            )
        log.debug("generate_pipeline_draft: system_prompt length=%d", len(system_prompt))

        user_input = req.user_description
        for attempt in range(_MAX_REPAIR_ATTEMPTS + 1):
            outcome = self._generate_once(project_id, user_id, user_input, system_prompt, llm_settings)
            if outcome.response is not None:
                return outcome.response
            if outcome.parsed is not None:
                break
            if attempt == _MAX_REPAIR_ATTEMPTS:
                return outcome.failure
            log.info(
                "generate_pipeline_draft: repair attempt %d with %d validator finding(s)",
                attempt + 1, len(outcome.findings),
            )
            user_input = _repair_request(req.user_description, outcome.pipeline_yaml, outcome.findings)

        parsed = outcome.parsed
        apply_offered_origins(parsed, project_id, agents, pipelines, skills)

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

    def _generate_once(self, project_id, user_id, user_input, system_prompt, llm_settings) -> "_Attempt":
        """One LLM call plus the parse, auto-fix and structural validation of its pipeline YAML.

        Returns a terminal ``response`` for failures no retry can fix (transport, timeout,
        unparseable or truncated output), or ``findings`` when only the pipeline structure is wrong,
        which the caller may feed back to the model once.
        """
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
            return _Attempt.stop(({"error": str(exc)}, 400))
        except PoolSaturationError as exc:
            return _Attempt.stop(({
                "error": "temporarily_unavailable",
                "message": "The service is busy. Please try again in a few seconds.",
                "retry_after": exc.retry_after,
            }, 503))
        except Exception:
            log.exception("generate_pipeline_draft: LLM call failed")
            return _Attempt.stop(({"error": "LLM generation failed."}, 500))

        timed_out = timeout_response(result, _AWAIT_TASK_TIMEOUT)
        if timed_out:
            return _Attempt.stop(timed_out)

        raw_text = extract_answer(result)
        if not raw_text:
            failure = describe_predict_failure(result)
            log.warning(
                "generate_pipeline_draft: no draft text; %s; full result=%s",
                failure or "no error reported", result,
            )
            return _Attempt.stop(({"error": failure or "LLM returned an empty response."}, 500))

        extracted = extract_json_from_text(raw_text)
        try:
            parsed = json.loads(extracted)
        except json.JSONDecodeError as exc:
            log.warning(
                "generate_pipeline_draft: draft did not parse: %s",
                describe_parse_failure(raw_text, extracted, exc, result),
            )
            if answer_was_cut_short(result, extracted):
                return _Attempt.stop(({
                    "error": (
                        "LLM response was truncated. "
                        "Increase max_tokens in llm_settings (recommended: 8192+)."
                    )
                }, 422))
            return _Attempt.stop(({"error": "LLM returned unparseable output.", "parse_error": str(exc)}, 422))

        pipeline_yaml = parsed.get("pipeline_yaml", "") if isinstance(parsed, dict) else ""
        if not pipeline_yaml or not isinstance(pipeline_yaml, str):
            return _Attempt.stop(({
                "error": "LLM did not return a 'pipeline_yaml' field.",
                "raw": parsed,
            }, 422))

        pipeline_yaml = fix_deprecated_end_node(pipeline_yaml)
        pipeline_yaml = fix_missing_node_bindings(pipeline_yaml)
        parsed["pipeline_yaml"] = pipeline_yaml

        validation_result = validate_pipeline_yaml(pipeline_yaml, project_id=project_id)
        # An AI-generated pipeline must have nodes and a terminal path, so these two warnings are
        # treated as failures here even though a manual draft save accepts them.
        structural_failures = validation_result.errors + [
            w for w in validation_result.warnings if w.code in _DRAFT_FAILURE_WARNINGS
        ]
        if not validation_result.valid or structural_failures:
            findings = [e.model_dump() for e in structural_failures]
            log.warning("generate_pipeline_draft: generated YAML failed validation: %s", findings)
            return _Attempt.repairable(pipeline_yaml, findings, ({
                "error": "Generated pipeline YAML failed structural validation.",
                "validation_errors": findings,
                "raw": parsed,
            }, 422))

        parsed["validation_result"] = validation_result.model_dump()
        return _Attempt.done(parsed)


class _Attempt:
    """Outcome of one generation: a final ``response``, a valid ``parsed`` draft, or repairable ``findings``."""

    def __init__(self, response=None, parsed=None, pipeline_yaml="", findings=(), failure=None):
        self.response = response
        self.parsed = parsed
        self.pipeline_yaml = pipeline_yaml
        self.findings = list(findings)
        self.failure = failure

    @classmethod
    def stop(cls, response):
        return cls(response=response)

    @classmethod
    def done(cls, parsed):
        return cls(parsed=parsed)

    @classmethod
    def repairable(cls, pipeline_yaml, findings, failure):
        return cls(pipeline_yaml=pipeline_yaml, findings=findings, failure=failure)


def _repair_request(user_description: str, pipeline_yaml: str, findings: list) -> str:
    """The follow-up request that hands the validator's findings back to the model.

    The validator messages are written for the model to act on, so they are passed through as-is
    together with the YAML they describe.
    """
    lines = "\n".join(
        f"- [{f.get('code')}]"
        + (f" node '{f['node_id']}'" if f.get("node_id") else "")
        + f": {f.get('message')}"
        for f in findings
    )
    return (
        f"{user_description}\n\n"
        "Your previous answer produced this pipeline_yaml:\n"
        f"```yaml\n{pipeline_yaml}\n```\n"
        "It failed structural validation with these findings:\n"
        f"{lines}\n\n"
        "Return the complete JSON answer again in the same format, with pipeline_yaml fixed so that "
        "none of these findings remain. Keep everything else that was correct."
    )


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
