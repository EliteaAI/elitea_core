"""Shared failure reporting for the "generate draft with AI" endpoints.

The draft generators dispatch one blocking ``predict_sio_llm`` call and read the answer out of the
result it returns. A worker exception, a join timeout, an exhausted token budget and a genuinely
empty completion all leave that answer empty, so without these helpers they collapse into one
indistinguishable "empty response" string.
"""

from typing import Optional

from pylon.core.tools import log
from tools import rpc_tools

from .utils import get_public_project_id

ERROR_LINE_MAX_LENGTH = 300
MAX_LISTED_MODEL_NAMES = 20
PARSE_FAILURE_WINDOW = 120
PARSE_FAILURE_TAIL = 80
LENGTH_RULES = (("max_length", "maximum"), ("min_length", "minimum"))
VALIDATION_FAILURE_FALLBACK = "Generated draft failed validation"
# mirrors llm_judge and evaluation_agent_runner, which keep their own copies to stay ORM-free
ASSISTANT_ROLES = ("assistant", "ai")


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


def _thinking_steps(result) -> list:
    inner = result.get("result") if isinstance(result, dict) else None
    steps = inner.get("thinking_steps") if isinstance(inner, dict) else None
    return [step for step in steps if isinstance(step, dict)] if isinstance(steps, list) else []


def finish_reasons(result) -> list:
    """One entry per step, None where the worker reported no usable ``generation_info``.

    Read from inside the parse-failure handler, where raising would turn a described 422 into a
    bare 500 - so a step whose ``generation_info`` is not a mapping counts as unknown, not fatal.
    """
    reasons = []
    for step in _thinking_steps(result):
        generation_info = step.get("generation_info")
        reasons.append(
            generation_info.get("finish_reason") if isinstance(generation_info, dict) else None
        )
    return reasons


def hit_token_limit(result) -> bool:
    """Whether any part of the generation ran out of budget.

    *Any* step counts, because this is only asked when the run produced no readable text at all:
    with nothing to show for it, a step that stopped on ``length`` is the explanation. A run that
    produced an answer must be judged by :func:`answer_was_cut_short` instead, where a ``length``
    step means something quite different.
    """
    return "length" in finish_reasons(result)


def answer_was_cut_short(result, answer: str) -> bool:
    """Whether output was lost, as opposed to merely assembled from several calls.

    ``length, length, stop`` is the ordinary signature of a *complete* answer: the SDK continues a
    completion that hit the output limit and merges the rounds. Only the final round matters, and
    when the model reported one, it settles the question — ``is_truncated_json`` counts braces
    without regard for strings, so a draft whose Markdown contains a ``{`` reads as unbalanced and
    would send a caller after ``max_tokens`` when the real fault was in the content.
    """
    reasons = finish_reasons(result)
    last_reason = reasons[-1] if reasons else None
    if last_reason is not None:
        return last_reason == "length"
    return is_truncated_json(answer)


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
    """Return the last non-empty assistant text from the run's *trace*.

    Kept only as :func:`extract_answer`'s fallback — see there for why a trace step is not the
    answer.
    """
    return next(
        (step["text"] for step in reversed(_thinking_steps(result)) if step.get("text")),
        "",
    )


def extract_answer(result) -> str:
    """Return the model's answer, preferring the result channel over the trace channel.

    ``thinking_steps`` holds one entry per LLM call. When a completion stops on ``length`` the SDK
    asks for the rest and merges the rounds outside any callback, so the merged answer is never
    traced and the last step is a fragment starting mid-sentence. ``chat_history`` is where that
    merged text reaches a blocking caller; the trace is the fallback for a worker that omits it.
    """
    task_result = result.get("result") if isinstance(result, dict) else None
    history = task_result.get("chat_history") if isinstance(task_result, dict) else None
    if isinstance(history, list):
        answer = next(
            (
                entry["content"] for entry in reversed(history)
                if isinstance(entry, dict)
                and (entry.get("role") in ASSISTANT_ROLES or entry.get("type") == "ai")
                and isinstance(entry.get("content"), str)
                and entry["content"].strip()
            ),
            None,
        )
        if answer:
            return answer
    return extract_draft_text(result)


def describe_parse_failure(raw_text: str, candidate: str, error, result) -> str:
    """Why a draft would not parse, in the detail needed to tell the causes apart.

    A completion cut off mid-string and a control character the model emitted inside one produce
    the same 422 and the same truncated prefix in the log. The **message class** separates them —
    ``Unterminated string starting at`` against ``Invalid control character at`` — and the rest of
    the line is what makes that classification checkable rather than taken on faith.

    ``pos`` is labelled per class because it means something different in each: for an unterminated
    string it marks the opening quote, which for a long value sits hundreds of characters from the
    cut, so a distance "from the end" there would read as a mid-draft failure for exactly the
    truncation it identifies. That offset is reported only where ``pos`` marks the failure itself.

    The window is ``repr``'d because the log formatter would otherwise render the offending
    character as ordinary whitespace; the tail shows a cut draft ending mid-prose with nothing
    closing it; the finish reasons say whether the model thought it had finished. All are bounded
    slices rather than the whole draft — they carry the evidence, and the draft is user content.
    """
    if error.msg.startswith("Unterminated string"):
        location = f"pos {error.pos}, never closed"
    else:
        location = f"pos {error.pos}, {len(candidate) - error.pos} chars from the end"
    window_start = max(0, error.pos - PARSE_FAILURE_WINDOW)
    return (
        f"{error.msg} ({location}; {len(candidate)} extracted chars, {len(raw_text)} raw); "
        f"finish_reasons={finish_reasons(result)}; "
        f"window={candidate[window_start:error.pos + PARSE_FAILURE_WINDOW]!r}; "
        f"tail={candidate[-PARSE_FAILURE_TAIL:]!r}"
    )


def _describe_validation_error(error: dict) -> str:
    """One field's failure, or "" when the error is about the payload rather than a field.

    A model answering with a bare JSON string parses, so the payload reaching the response model is
    a ``str`` and pydantic reports one ``loc``-less error whose wording names the model class. That
    class name says nothing to whoever has to produce a valid draft and the modals render this
    sentence to end users verbatim, so those fall back to the generic label.

    A broken length rule is stated as the measurement rather than in pydantic's own wording: the
    Settings modal renders this sentence to a person, and "String should have at most 2500
    characters" describes the constraint where what both a person and a regenerating model need is
    how far over the draft actually went. Every other rule keeps pydantic's wording, and no length
    is quoted against it — beside a pattern or a type it would read as the thing that failed.
    """
    location = ".".join(str(part) for part in error.get("loc") or ())
    if not location:
        return ""
    value = error.get("input")
    context = error.get("ctx") or {}
    if isinstance(value, str):
        for key, label in LENGTH_RULES:
            if key in context:
                return f"{location} is {len(value)} characters, the {label} is {context[key]}"
    return f"{location}: {_last_exception_line(error.get('msg') or error.get('type') or 'is invalid')}"


def describe_validation_failure(errors) -> str:
    """Why a parsed draft failed its response model, in the terms needed to produce a valid one.

    A caller that has to regenerate needs the field, the rule it broke and how far past it the
    draft went; a cap stated without the actual length gives no sense of how much to cut. The
    offending value itself is reported only by its length — the draft is user content, it already
    travels back whole under ``raw``, and neither this message nor the log line it feeds is a
    place for it, the same bound :func:`describe_parse_failure` keeps on its windows.

    The semicolon separates one field's failure from the next, so no clause may spend one itself:
    two broken fields would otherwise arrive as four peer clauses with nothing saying where the
    first field's failure ends.
    """
    described = [_describe_validation_error(error) for error in errors if isinstance(error, dict)]
    return "; ".join(part for part in described if part) or VALIDATION_FAILURE_FALLBACK


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
