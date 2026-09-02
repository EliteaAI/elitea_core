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

"""AI-analyse a finished eval run and propose fixes (ENH-4, §5).

One sync LLM call, nothing persisted — the same shape as ``generate_eval_dimensions.py``. The
client holds the proposal and saves only the items the user accepts, through the existing
instruction-patch/fork and eval CRUD endpoints (§6).

The two decisions this file owns rather than delegating:

* **The version is the run's pinned one, never the agent's current draft.** Diagnosing text that
  was not under test is the easiest way to produce a confidently wrong proposal (§7.1). The
  response carries the version id and the instructions hash so the apply path can refuse a patch
  built against instructions that have since changed.
* **Gap selection happens on the server, deterministically.** The LLM is asked about a ranked,
  capped subset (§3.1), and ``coverage`` reports what was left out so a sampled analysis is never
  presented to the user as a complete one.
"""

import json

from flask import request
from pydantic import ValidationError

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...models.pd.enhance_from_eval import (
    EnhanceFromEvalRequest,
    EnhanceFromEvalResponse,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.enhancement_gap_selection import select_gaps
from ...utils.enhancement_prompt import (
    EnhancePromptTemplateError,
    build_enhance_system_prompt,
)
from ...utils.enhancement_validation import ground_proposal
from ...utils.enhancement_utils import (
    EvalRunNotFinishedError,
    fetch_evaluated_version,
    fetch_run_for_enhancement,
)
from ...utils.evaluation_human_score_utils import EvalRunNotFoundError
from ...utils.exceptions import PoolSaturationError
from ...utils.predict_utils import PredictPayloadError
from ...utils.service_prompt_utils import get_service_prompt
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY = "enhance_agent_from_eval"

# The brief carries full agent instructions plus the gap payload, and the response is two lists of
# instruction spans, so both directions are large. 60s matches generate_eval_dimensions.
_AWAIT_TASK_TIMEOUT = 90
_DEFAULT_MAX_TOKENS = 8192


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Propose agent and evaluation fixes from a finished eval run",
        description=(
            "Analyses the gaps in a finished evaluation run and proposes both instruction edits "
            "and evaluation fixes, attributing each gap to the agent or to the measurement. "
            "Returns a proposal only — nothing is persisted; the client applies accepted items "
            "through the instruction-patch/fork and eval CRUD endpoints."
        ),
        request_body=EnhanceFromEvalRequest,
        tags=["elitea_core/evaluation"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        try:
            req = EnhanceFromEvalRequest.model_validate(request.json)
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        user_id = auth.current_user().get("id")

        try:
            run = fetch_run_for_enhancement(project_id, req.run_id)
        except EvalRunNotFoundError:
            return {"error": f"Evaluation run {req.run_id} not found"}, 404
        except EvalRunNotFinishedError as exc:
            return {"error": str(exc)}, 409

        agent = fetch_evaluated_version(project_id, run["application_id"], run["version_id"])
        if agent is None:
            return {
                "error": (
                    f"Version {run['version_id']} the run was pinned to no longer exists, so its "
                    "instructions cannot be analysed"
                )
            }, 409

        selection = select_gaps(
            run["snapshot"],
            run["results"],
            run["human_scores"],
        )
        gaps = self._filter_gaps(selection["gaps"], req.dimension_ids)
        if not gaps:
            # A clean run is a valid answer, not an error — and asking the model to find fault
            # in a run with no misses is how an ungrounded proposal gets generated.
            return EnhanceFromEvalResponse(
                run_id=run["run_id"],
                version_id=agent["version_id"],
                instructions_sha256=agent["instructions_sha256"],
                diagnosis=(
                    "No dimension missed its target in this run, so there is nothing to diagnose."
                ),
                coverage=selection["coverage"],
            ).model_dump(), 200

        llm_settings, error = self._resolve_llm_settings(project_id, req)
        if error:
            return error

        template = get_service_prompt(_SERVICE_PROMPT_KEY)
        if not template:
            return {"error": f"Service prompt '{_SERVICE_PROMPT_KEY}' is not configured"}, 500

        try:
            system_prompt = build_enhance_system_prompt(
                template,
                application_name=agent["application_name"],
                instructions=agent["instructions"],
                gaps=gaps,
                coverage=selection["coverage"],
                agent_context=agent["agent_context"],
            )
        except EnhancePromptTemplateError as exc:
            log.exception("enhance_from_eval: %s", exc)
            return {"error": f"Service prompt '{_SERVICE_PROMPT_KEY}' template is malformed"}, 500

        raw_text, error = self._call_llm(project_id, user_id, system_prompt, llm_settings)
        if error:
            return error

        parsed, error = self._parse_json(raw_text)
        if error:
            return error

        # Server-owned fields: overwrite whatever the model invented before validating, so the
        # pin the caller applies against is the version the instructions were actually read from.
        parsed["run_id"] = run["run_id"]
        parsed["version_id"] = agent["version_id"]
        parsed["instructions_sha256"] = agent["instructions_sha256"]
        parsed["coverage"] = selection["coverage"]

        try:
            proposal = EnhanceFromEvalResponse.model_validate(parsed)
        except ValidationError as e:
            log.warning("enhance_from_eval: validation failed: %s", e.errors())
            return {
                "error": "Generated proposal failed validation",
                "details": e.errors(),
                "raw": parsed,
            }, 422

        # Grounding (ENH-5) runs after type validation and before the response: an item whose anchor
        # is absent or whose citations are invented would otherwise reach a one-click accept button
        # and fail at 409 in front of a user who had already judged it correct.
        dropped = ground_proposal(
            proposal,
            instructions=agent["instructions"],
            snapshot=run["snapshot"],
            results=run["results"],
        )
        if any(dropped.values()):
            # The anchors are logged with the counts: a count alone cannot distinguish a model that
            # invented a span from a prompt that stopped quoting the instructions verbatim, and those
            # have different fixes.
            log.warning(
                "enhance_from_eval: dropped ungrounded items for run %s: %s, proposed anchors=%s",
                run["run_id"],
                dropped,
                [
                    (fix.get("old_text"), fix.get("cited_dimension_ids"), fix.get("cited_case_ids"))
                    for fix in (parsed.get("agent_fixes") or [])
                ],
            )

        return proposal.model_dump(), 200

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _filter_gaps(gaps: list, dimension_ids) -> list:
        """Narrow the brief to the dimensions the caller asked about.

        Applied after ranking, so an explicit selection is honoured exactly rather than competing
        with the impact ordering.
        """
        if not dimension_ids:
            return gaps
        wanted = set(dimension_ids)
        return [gap for gap in gaps if gap.get('dimension_id') in wanted]

    @staticmethod
    def _resolve_llm_settings(project_id: int, req):
        if req.llm_settings and req.llm_settings.model_name:
            return req.llm_settings.model_dump(exclude_none=True), None

        try:
            llm_settings = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_default_model(
                project_id, section="llm"
            )
        except Exception:
            log.exception("enhance_from_eval: failed to get default model")
            return None, ({"error": "Failed to resolve project default LLM model"}, 400)

        if not llm_settings or not llm_settings.get("model_name"):
            return None, ({"error": "No default LLM model configured for this project"}, 400)
        if req.llm_settings:
            llm_settings.update(
                req.llm_settings.model_dump(exclude_none=True, exclude={"model_name"})
            )

        # A low temperature on both paths: this endpoint quotes existing instruction text back as
        # patch anchors, and a creative paraphrase of an anchor is a patch that cannot apply.
        llm_settings.setdefault("temperature", 0.2)
        llm_settings.setdefault("max_tokens", _DEFAULT_MAX_TOKENS)
        return llm_settings, None

    def _call_llm(self, project_id: int, user_id, system_prompt: str, llm_settings: dict):
        try:
            result = self.module.predict_sio_llm(
                sid=None,
                data={
                    "project_id": project_id,
                    "user_input": "Analyse the gaps above and return the JSON proposal.",
                    "instructions": system_prompt,
                    "llm_settings": llm_settings,
                    "await_task_timeout": _AWAIT_TASK_TIMEOUT,
                },
                await_task_timeout=_AWAIT_TASK_TIMEOUT,
                user_id=user_id,
                is_system_user=True,
            )
        except PredictPayloadError as exc:
            return None, ({"error": str(exc)}, 400)
        except PoolSaturationError as exc:
            return None, ({
                "error": "temporarily_unavailable",
                "message": "The service is busy. Please try again in a few seconds.",
                "retry_after": exc.retry_after,
            }, 503)
        except Exception:
            log.exception("enhance_from_eval: LLM call failed")
            return None, ({"error": "LLM analysis failed"}, 500)

        task_result = result.get("result") or {}
        steps = task_result.get("thinking_steps", []) if isinstance(task_result, dict) else []
        raw_text = next((s["text"] for s in reversed(steps) if s.get("text")), "")
        if not raw_text:
            log.warning("enhance_from_eval: empty response, full result=%s", result)
            return None, ({"error": "LLM returned an empty response"}, 500)
        return raw_text, None

    @staticmethod
    def _parse_json(raw_text: str):
        try:
            parsed = json.loads(extract_json_from_text(raw_text))
        except json.JSONDecodeError as exc:
            log.debug("enhance_from_eval: LLM output is not valid JSON: %s", raw_text[:500])
            # Unbalanced brackets mean the model ran out of tokens mid-object. Reported distinctly
            # because the fix is a setting the caller controls, not a retry.
            if raw_text.count('{') > raw_text.count('}') or raw_text.count('[') > raw_text.count(']'):
                return None, ({
                    "error": (
                        "LLM response was truncated. Increase max_tokens in llm_settings "
                        f"(recommended: {_DEFAULT_MAX_TOKENS}+)."
                    )
                }, 422)
            return None, ({"error": "LLM returned unparseable output", "parse_error": str(exc)}, 422)

        if not isinstance(parsed, dict):
            return None, ({"error": "LLM returned a JSON value that is not an object"}, 422)
        return parsed, None


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
