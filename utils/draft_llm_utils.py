"""Shared failure reporting for the "generate draft with AI" endpoints.

The draft generators dispatch one blocking ``predict_sio_llm`` call and read the answer out of
``thinking_steps``. A worker exception, a join timeout, an exhausted token budget and a genuinely
empty completion all leave that text empty, so without these helpers they collapse into one
indistinguishable "empty response" string.
"""

from typing import Optional

from pylon.core.tools import log
from tools import rpc_tools

from .utils import get_public_project_id

ERROR_LINE_MAX_LENGTH = 300
MAX_LISTED_MODEL_NAMES = 20


def _available_llm_models(project_id: int) -> Optional[dict]:
    """Return the ``{(project_id, model_name): config}`` map, or None when the lookup failed."""
    try:
        return rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_available_models(
            project_id=project_id, section='llm', include_shared=True
        )
    except Exception as exc:  # pylint: disable=W0703
        log.warning(
            "draft_llm_utils: model availability lookup failed for project %s: %s", project_id, exc
        )
        return None


def _available_names_hint(available: dict) -> str:
    names = sorted({name for (_, name) in available})
    if not names:
        return "No LLM models are configured for this project."
    listed = ", ".join(names[:MAX_LISTED_MODEL_NAMES])
    remaining = len(names) - MAX_LISTED_MODEL_NAMES
    if remaining > 0:
        return f"Available models: {listed} (+{remaining} more)."
    return f"Available models: {listed}."


def caller_chose(llm_settings, field: str) -> bool:
    """Whether the request really asked for this ``llm_settings`` value.

    ``LLMSettingsRequest`` defaults ``max_tokens`` and ``temperature``, and those defaults survive
    ``model_dump(exclude_none=True)``, so a present key proves nothing. An explicit ``null`` is
    dropped by the dump, so it counts as unset too — the endpoint's own default has to win.
    """
    return bool(
        llm_settings
        and field in llm_settings.model_fields_set
        and getattr(llm_settings, field) is not None
    )


def _public_project_id() -> Optional[int]:
    try:
        return get_public_project_id()
    except Exception as exc:  # pylint: disable=W0703
        log.warning("draft_llm_utils: public project lookup failed: %s", exc)
        return None


def _configured_in(project_id: Optional[int], model_name: str) -> Optional[bool]:
    """Whether the per-project lookup ``generate_predict_payload`` performs would find the model.

    Needed only for projects the available-models set cannot speak for: it carries the public
    project's *shared* rows only (``get_public_filters``), and nothing at all about a third project
    named by ``model_project_id``. The caller's own project needs no such call — that side of the
    set runs the same query with no extra filter (``get_private_filters`` returns ``[]``).

    ``None`` means the question could not be answered — the lookup raised, or there was no project
    id to ask about — and each caller decides what to make of that. ``False`` is reserved for a
    definitive absence. Nothing here can see a model registered in LiteLLM with no configuration
    row at all — ``_map_model_name``'s third fallback — so those come back ``False`` and are
    rejected with a message listing what is configured.
    """
    if project_id is None:
        return None
    try:
        configuration = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_configuration_model(
            project_id, model_name
        )
    except Exception as exc:  # pylint: disable=W0703
        log.warning(
            "draft_llm_utils: model configuration lookup failed for project %s: %s", project_id, exc
        )
        return None
    return bool(configuration)


def resolve_model(
    project_id: int, model_name: str, model_project_id: Optional[int] = None
) -> tuple[Optional[str], Optional[int]]:
    """Return a user-facing reason the model cannot be used, and the project that owns it.

    Answers the question the LLM proxy will ask: the caller's own models, the public project's, and
    — when the request names one — a third project's. Only a definitive absence is reported; if the
    availability set cannot be read at all, generation proceeds.

    An owning project is reported only when the caller left ``model_project_id`` unset, and only
    when the lookup was conclusive. ``generate_predict_payload`` otherwise reads the model
    configuration out of the *caller's* project (``predict_utils.py``: ``model_project_id or
    parsed.project_id``), so a model shared from elsewhere resolves to ``{}`` and every capability
    silently takes its default — ``openai_compatible`` to False, which on a Bedrock-backed
    credential routes the name down a path that cannot resolve it.

    The one place an unanswerable lookup still yields a message is an unreadable ``model_project_id``
    (see below), because passing that project on is fatal further down.
    """
    available = _available_llm_models(project_id)
    if available is None:
        return None, None

    unavailable = (
        f"Model {model_name!r} is not available in project {project_id}. "
        f"{_available_names_hint(available)}"
    )

    if model_project_id is None:
        # private before public: fetch_private_configurations inserts its keys first, so the
        # caller's own model wins a name collision - the ordering validate_and_resolve_llm_settings
        # relies on too
        owning_project_id = next((pid for (pid, name) in available if name == model_name), None)
        if owning_project_id is not None:
            return None, owning_project_id
        # the public project's non-shared rows are invisible here, so only a definitive "no" counts.
        # A hit is deliberately not stamped: `_configured_in` filters on name alone, so it also
        # answers for rows the public project chose not to share, and naming that project would
        # resolve capabilities and secrets against a configuration the caller was never given.
        if _configured_in(_public_project_id(), model_name) is False:
            return unavailable, None
        return None, None

    if (model_project_id, model_name) in available:
        return None, None
    # Anything short of a confirmed hit is treated as a miss. A general configurations outage would
    # already have emptied `available` and returned above, so a lookup that fails only for this
    # project points at the project id - and a wrong one costs a clear 400 here versus a generic 500
    # further down, where capabilities and secrets resolve against it.
    if _configured_in(model_project_id, model_name):
        return None, None

    owning_projects = [pid for (pid, name) in available if name == model_name]
    if owning_projects:
        return (
            f"Model {model_name!r} is not configured in project {model_project_id}. "
            f"It is available in project {owning_projects[0]} - pass that as model_project_id."
        ), None
    return unavailable, None


def _last_exception_line(error_text) -> str:
    lines = [line.strip() for line in str(error_text).splitlines() if line.strip()]
    line = lines[-1] if lines else str(error_text).strip()
    if len(line) > ERROR_LINE_MAX_LENGTH:
        return line[:ERROR_LINE_MAX_LENGTH] + "..."
    return line


def hit_token_limit(result) -> bool:
    """Whether any part of the generation ran out of budget.

    Authoritative where brace-counting is not: a truncated draft often ends mid-prose with its
    braces balanced. *Any* step counts, not the last one — the worker continues after a cut-off,
    so a run that lost its JSON to the budget still ends ``stop`` (observed: ``length, length,
    stop`` for one skill draft at ``max_tokens=400``). Only consulted once the draft is missing or
    unparseable, so a run that recovered and produced valid JSON never reaches it.
    """
    inner = result.get("result") if isinstance(result, dict) else None
    steps = inner.get("thinking_steps") if isinstance(inner, dict) else None
    if not isinstance(steps, list):
        return False
    return any(
        isinstance(step, dict)
        and (step.get("generation_info") or {}).get("finish_reason") == "length"
        for step in steps
    )


def _already_states(curated: str, detail: str) -> bool:
    """Whether the curated message already carries the exception's own wording.

    The worker's specific branches interpolate the exception into their text ("Authentication error
    with the AI provider: {e}", "Details: …"); only its catch-alls do not, and those are the ones
    that need the detail appended.
    """
    if detail in curated:
        return True
    _, separator, message = detail.partition(": ")
    return bool(separator) and message in curated


def _failure_text(container: dict) -> Optional[str]:
    """Compose one sentence from whatever the container reports about the failure."""
    if container.get("message"):
        return container["message"]

    curated = container.get("human_readable")
    detail = _last_exception_line(container["error"]) if container.get("error") else None
    if curated and detail and not _already_states(curated, detail):
        return f"{curated} ({detail})"
    return curated or detail


def describe_predict_failure(result) -> Optional[str]:
    """Return why the predict call produced no output, or None when it reported no failure.

    Deliberately not special-cased by condition: a refusal the platform words itself — maintenance
    being the one that exists today — arrives as a top-level ``{"error": …, "message": …}`` envelope
    and is reported through the same path as any other failure, since the sentence it carries is
    already the answer. Only a join timeout is taken out first, by ``timeout_response``, because it
    warrants a different status rather than a different message.
    """
    if not isinstance(result, dict):
        return "LLM generation failed: the worker returned an unexpected response."

    for container in (result, result.get("result")):
        if not isinstance(container, dict):
            continue
        text = _failure_text(container)
        if text:
            return f"LLM generation failed: {text}"

    if hit_token_limit(result):
        return (
            "LLM response was cut off before any output was produced. "
            "Increase max_tokens in llm_settings (recommended: 4096+)."
        )

    return None


def extract_draft_text(result) -> str:
    """Return the last non-empty assistant text from a blocking predict result."""
    task_result = result.get("result") if isinstance(result, dict) else None
    thinking_steps = task_result.get("thinking_steps", []) if isinstance(task_result, dict) else []
    return next(
        (
            step["text"] for step in reversed(thinking_steps)
            if isinstance(step, dict) and step.get("text")
        ),
        "",
    )


def timeout_response(result, timeout_seconds: int) -> Optional[tuple]:
    """Return the (body, status) when the blocking join gave up, else None.

    ``predict_sio_llm`` reports this by returning only a ``task_id``, having asked the worker to
    cancel the run. 504 separates "the model took too long" from "the model failed", so a
    retry-on-5xx policy does not silently re-dispatch another full-length generation; the message
    does not invite an identical retry either, since it would take just as long and cancellation is
    best-effort (``cancel_on_timeout``).
    """
    if not isinstance(result, dict):
        return None

    if "result" not in result and result.get("task_id"):
        return {
            "error": (
                f"LLM generation timed out after {timeout_seconds}s. "
                f"Try a shorter description, or a faster model."
            ),
        }, 504

    return None


def is_truncated_json(raw_text: str) -> bool:
    return (
        raw_text.count('{') > raw_text.count('}')
        or raw_text.count('[') > raw_text.count(']')
    )
