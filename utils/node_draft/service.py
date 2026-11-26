"""One node draft: call the model, repair once on an invalid answer, map failures to responses.

The model call and the clock are injected so the budget and retry rules are testable without a worker.
"""
import json
import time
from typing import Callable, List, Optional, Tuple

from ...models.pd.node_draft import LIMITS, DraftTarget, ModelReply
from .validate import DraftInvalid, finalize

CallModel = Callable[[str, List[dict], dict, float], ModelReply]


def draft_llm_settings(requested: Optional[dict], node_type: str, supports_reasoning: bool) -> dict:
    """The pipeline version's model with this endpoint's limits. No fallback to another model (AC-31)."""
    if not requested or not requested.get('model_name'):
        raise ValueError('Select a model for this pipeline to use AI.')
    settings = {k: v for k, v in requested.items() if k not in ('temperature', 'max_tokens', 'reasoning_effort')}
    if supports_reasoning:
        settings.update(max_tokens=LIMITS.reasoning_max_tokens, reasoning_effort=LIMITS.reasoning_effort)
    else:
        settings.update(max_tokens=LIMITS.max_tokens_for(node_type), temperature=LIMITS.temperature)
    return settings


def run_draft(
    system: str,
    user: str,
    target: DraftTarget,
    llm_settings: dict,
    call_model: CallModel,
    extract_json: Callable[[str], str],
    clock: Callable[[], float] = time.monotonic,
) -> Tuple[dict, int]:
    deadline = clock() + LIMITS.budget_seconds
    messages = [{'role': 'user', 'content': user}]
    details: List[dict] = []

    for attempt in range(2):
        remaining = deadline - clock()
        if attempt and remaining < LIMITS.min_repair_seconds:
            break
        reply = call_model(system, list(messages), llm_settings, remaining)
        if reply.timed_out or clock() >= deadline:
            return {'error': 'Generating took too long. Try again or simplify the request.'}, 504
        if reply.error:
            return {'error': reply.error}, 500
        try:
            parsed = json.loads(extract_json(reply.text or ''))
        except ValueError:
            if reply.cut_short:
                return {'error': "The draft was too long for the model's output limit. Simplify the request."}, 422
            details = [{'path': 'answer', 'message': 'is not one valid JSON object'}]
        else:
            try:
                return finalize(parsed, target).model_dump(), 200
            except DraftInvalid as exc:
                details = exc.details
        messages += [{'role': 'assistant', 'content': reply.text or ''},
                     {'role': 'user', 'content': _repair_message(details)}]

    return {'error': "The AI couldn't produce a valid node. Try a more specific description or another model.",
            'details': details}, 422


def _repair_message(details: List[dict]) -> str:
    text = '; '.join(f"{d['path']} {d['message']}" for d in details)
    if len(text) > LIMITS.repair_details_limit:
        text = text[:LIMITS.repair_details_limit - 1] + '…'
    return f'Your previous answer was invalid: {text} Return the corrected JSON only.'
