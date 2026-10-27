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

"""Build with AI for a schedule form (EL-7011, US-21): the model drafts, the server checks, nothing is saved.

`api/v2/schedules.py` serves the handler; this module registers no route.
"""
import json
import math
import threading
import time
from collections import deque

from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from tools import auth, rpc_tools

from ..models.pd.generate_schedule_draft import (
    GenerateScheduleDraftRequest,
    GenerateScheduleDraftResponse,
)
from .draft_llm_utils import (
    describe_predict_failure,
    extract_answer,
    timeout_response,
)
from .exceptions import PoolSaturationError
from .predict_utils import PredictPayloadError
from .schedule_access import schedule_check_spec
from .schedule_draft_utils import (  # pylint: disable=W0212
    build_schedule_draft_prompt,
    schedule_roster,
    validate_schedule_draft,
)
from .service_prompt_utils import get_service_prompt
from .utils import extract_json_from_text

_SERVICE_PROMPT_KEY = "generate_schedule_draft"
_AWAIT_TASK_TIMEOUT = 60
_DEFAULT_MAX_TOKENS = 4096
_DEFAULT_TEMPERATURE = 0.3
_MODEL_FAILED = "model_failed"
PAYLOAD_REFUSED = "The draft request could not be sent to the model"
_DRAFTS_PER_WINDOW = 10
_WINDOW_SECONDS = 60
_recent_drafts: dict[tuple[int, int], deque] = {}
_recent_lock = threading.Lock()


def _drop_idle_keys(now: float) -> None:
    """Remove users whose last draft left the window, so the dict does not grow with every user. Hold `_recent_lock`."""
    for key in [k for k, v in _recent_drafts.items() if not v or now - v[-1] >= _WINDOW_SECONDS]:
        del _recent_drafts[key]


def _wait_seconds(project_id: int, user_id: int) -> int:
    """0 and the call is counted; else the seconds until the user may draft again.

    lean: a sliding window per worker process, so the limit is per worker; move it to Redis if the draft
    endpoint needs one limit across workers.
    """
    now = time.monotonic()
    with _recent_lock:
        hits = _recent_drafts.setdefault((project_id, user_id), deque())
        while hits and now - hits[0] >= _WINDOW_SECONDS:
            hits.popleft()
        if len(hits) >= _DRAFTS_PER_WINDOW:
            return max(1, math.ceil(_WINDOW_SECONDS - (now - hits[0])))
        hits.append(now)
        _drop_idle_keys(now)
    return 0


def _refusal(code: str, text: str, status: int, **extra) -> tuple[dict, int]:
    """The body of `schedule_service.refusal`; the draft tests load this module without the service."""
    return {"code": code, "error": text, **extra}, status


class ScheduleDraftHandler:
    """Draft a schedule with AI; `self.module` is the plugin module that runs the model call."""

    @auth.decorators.check_api(schedule_check_spec("create"))
    def _draft(self, project_id: int):
        """Draft the fields of a schedule form from plain text and the current form values; nothing is saved.

        Who may call: a project member with `schedules.create`.
        Errors: 400 invalid body or no default LLM; 403; 429 too many drafts, retry after `retry_after` seconds; 500 the service prompt is not configured;
        502 the model failed or its answer cannot be used; 503 busy, retry after `retry_after` seconds.
        """
        try:
            req = GenerateScheduleDraftRequest.model_validate(request.get_json(silent=True))
        except ValidationError as e:
            first = e.errors()[0]
            return _refusal("invalid_input", first["msg"], 400, field=first["loc"][0] if first["loc"] else "body")

        user_id = auth.current_user().get("id")
        wait = _wait_seconds(project_id, user_id)
        if wait:
            return _refusal("too_many_drafts", "Too many drafts. Please try again in a moment.", 429, retry_after=wait)

        template = get_service_prompt(_SERVICE_PROMPT_KEY)
        if not template:
            return _refusal("service_prompt_missing", f"Service prompt '{_SERVICE_PROMPT_KEY}' is not configured", 500)

        try:
            llm_settings = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_default_model(
                project_id, section="llm"
            )
        except Exception:  # noqa: BLE001 -- no model answer means "no default model" (R1.6)
            log.exception("generate_schedule_draft: failed to get default model")
            llm_settings = None
        if not llm_settings or not llm_settings.get("model_name"):
            return _refusal("no_default_model", "No default LLM model configured for this project", 400)
        llm_settings.update(temperature=_DEFAULT_TEMPERATURE, max_tokens=_DEFAULT_MAX_TOKENS)

        locked = req.locked_entity.model_dump() if req.locked_entity else None
        roster = schedule_roster(project_id, user_id)
        system_prompt = build_schedule_draft_prompt(
            template, req.current.model_dump(exclude_unset=True), roster, req.timezone, locked,
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
                user_id=user_id,
                skip_expansion=True,
                return_chat_history=True,
            )
        except PredictPayloadError:
            log.exception("generate_schedule_draft: the model call was refused")  # the text can name internals
            return _refusal("invalid_input", PAYLOAD_REFUSED, 400)
        except PoolSaturationError as exc:
            busy = "The service is busy. Please try again in a few seconds."
            return _refusal("temporarily_unavailable", busy, 503, message=busy, retry_after=exc.retry_after)
        except Exception:  # noqa: BLE001 -- any model failure is a 502 with a short reason (R1.6)
            log.exception("generate_schedule_draft: LLM call failed")
            return _refusal(_MODEL_FAILED, "LLM generation failed", 502)

        timed_out = timeout_response(result, _AWAIT_TASK_TIMEOUT)
        if timed_out:
            return _refusal("model_timeout", timed_out[0]["error"], 502)

        raw_text = extract_answer(result)
        if not raw_text:
            return _refusal(_MODEL_FAILED, describe_predict_failure(result) or "LLM returned an empty response", 502)
        try:
            parsed = json.loads(extract_json_from_text(raw_text))
        except json.JSONDecodeError:
            return _refusal(_MODEL_FAILED, "LLM returned no JSON draft", 502)
        if not isinstance(parsed, dict):
            return _refusal(_MODEL_FAILED, "LLM returned no JSON draft", 502)

        try:
            draft, warnings = validate_schedule_draft(
                project_id, parsed, locked, roster, owner_id=user_id)
        except Exception:  # noqa: BLE001 -- the model answer was paid for; answer a short reason, not a 500
            log.exception("generate_schedule_draft: the draft could not be checked")
            return _refusal("draft_check_failed", "The draft could not be checked", 502)
        return GenerateScheduleDraftResponse(draft=draft, warnings=warnings).model_dump(mode="json"), 200
