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

"""Build with AI for a schedule form (EL-7011, US-21): the model drafts, the server checks, nothing is saved."""
import json
from typing import ClassVar

from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from tools import api_tools, auth, register_openapi, rpc_tools
from tools import config as c

from ...models.pd.generate_schedule_draft import (
    GenerateScheduleDraftRequest,
    GenerateScheduleDraftResponse,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.draft_llm_utils import (
    describe_predict_failure,
    extract_answer,
    timeout_response,
)
from ...utils.exceptions import PoolSaturationError
from ...utils.predict_utils import PredictPayloadError
from ...utils.schedule_draft_utils import (  # pylint: disable=W0212
    build_schedule_draft_prompt,
    schedule_roster,
    validate_schedule_draft,
)
from ...utils.service_prompt_utils import get_service_prompt
from ...utils.utils import extract_json_from_text

_SERVICE_PROMPT_KEY = "generate_schedule_draft"
_AWAIT_TASK_TIMEOUT = 60
_DEFAULT_MAX_TOKENS = 4096
_DEFAULT_TEMPERATURE = 0.3


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Generate Schedule Draft from Natural Language",
        description=(
            "Draft the fields of a schedule form from a plain-text description and the current form "
            "values. Uses the project's default LLM. The server checks the draft and returns it with "
            "warnings for every field it could not use. Nothing is saved."
        ),
        request_body=GenerateScheduleDraftRequest,
        responses={
            "200": {"description": "Success: the checked `draft` and its `warnings`"},
            "400": {"description": "Invalid body, or no default LLM in the project"},
            "403": {"description": "No permission to create schedules"},
            "500": {"description": "The service prompt is not configured"},
            "502": {"description": "The model failed or its answer could not be used"},
            "503": {"description": "The service is busy; retry after `retry_after` seconds"},
        },
        tags=["elitea_core/schedules"],
        mcp_tool=False,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.schedules.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        try:
            req = GenerateScheduleDraftRequest.model_validate(request.json)
        except ValidationError as e:
            return e.errors(), 400

        template = get_service_prompt(_SERVICE_PROMPT_KEY)
        if not template:
            return {"error": f"Service prompt '{_SERVICE_PROMPT_KEY}' is not configured"}, 500

        try:
            llm_settings = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_default_model(
                project_id, section="llm"
            )
        except Exception:  # noqa: BLE001 -- no model answer means "no default model" (R1.6)
            log.exception("generate_schedule_draft: failed to get default model")
            llm_settings = None
        if not llm_settings or not llm_settings.get("model_name"):
            return {"error": "No default LLM model configured for this project"}, 400
        llm_settings.update(temperature=_DEFAULT_TEMPERATURE, max_tokens=_DEFAULT_MAX_TOKENS)

        locked = req.locked_entity.model_dump() if req.locked_entity else None
        roster = schedule_roster(project_id)
        system_prompt = build_schedule_draft_prompt(
            template, req.current, roster, req.timezone, locked,
        )

        try:
            result = self.module.predict_sio_llm(
                sid=None,
                data={
                    "project_id": project_id,
                    "user_input": req.prompt,
                    "instructions": system_prompt,
                    "llm_settings": llm_settings,
                    "await_task_timeout": _AWAIT_TASK_TIMEOUT,
                },
                await_task_timeout=_AWAIT_TASK_TIMEOUT,
                user_id=auth.current_user().get("id"),
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
        except Exception:  # noqa: BLE001 -- any model failure is a 502 with a short reason (R1.6)
            log.exception("generate_schedule_draft: LLM call failed")
            return {"error": "LLM generation failed"}, 502

        timed_out = timeout_response(result, _AWAIT_TASK_TIMEOUT)
        if timed_out:
            return timed_out[0], 502

        raw_text = extract_answer(result)
        if not raw_text:
            return {"error": describe_predict_failure(result) or "LLM returned an empty response"}, 502
        try:
            parsed = json.loads(extract_json_from_text(raw_text))
        except json.JSONDecodeError:
            return {"error": "LLM returned no JSON draft"}, 502
        if not isinstance(parsed, dict):
            return {"error": "LLM returned no JSON draft"}, 502

        try:
            draft, warnings = validate_schedule_draft(project_id, parsed, locked, roster)
        except Exception:  # noqa: BLE001 -- the model answer was paid for; answer a short reason, not a 500
            log.exception("generate_schedule_draft: the draft could not be checked")
            return {"error": "The draft could not be checked"}, 502
        return GenerateScheduleDraftResponse(draft=draft, warnings=warnings).model_dump(mode="json"), 200


class API(api_tools.APIBase):
    url_params: ClassVar[list] = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers: ClassVar[dict] = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
