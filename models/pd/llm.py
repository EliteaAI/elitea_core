from typing import Optional, Annotated

from pydantic import BaseModel, Field, model_validator


def llm_settings_family_conflict(temperature: Optional[float], reasoning_effort: Optional[str]) -> bool:
    """True when temperature and an active reasoning_effort are both set — invalid combo for
    reasoning models (Anthropic extended thinking, OpenAI o1/gpt-5). Single source of truth,
    shared by the read (auto-correct) and write (reject) LLM settings variants below, and by
    validate_and_resolve_llm_settings (utils/application_utils.py)."""
    return temperature is not None and reasoning_effort not in (None, "none")


def _normalize_llm_settings_family(llm_settings: dict, supports_reasoning: bool) -> dict:
    """Reset temperature/reasoning_effort to match a model's actual reasoning support.

    Shared by all branches of validate_and_resolve_llm_settings (available model, unavailable
    model, and unavailable-model-with-no-name-at-all fallback) so the reset logic lives in one
    place (issue #5821). Lives here (dep-free) rather than in application_utils so it can be
    reused by the heal admin task and unit-tested without the pylon runtime.
    """
    resolved = dict(llm_settings)
    if supports_reasoning:
        # Reasoning models ignore temperature; promote to reasoning_effort if not already set.
        resolved['temperature'] = None
        if not resolved.get('reasoning_effort'):
            resolved['reasoning_effort'] = 'medium'
    else:
        # Non-reasoning models ignore reasoning_effort.
        resolved['reasoning_effort'] = None
        if resolved.get('temperature') is None:
            resolved['temperature'] = 0.7
    return resolved


def decide_family_heal(llm_settings: dict, supports_reasoning: bool) -> Optional[dict]:
    """Per-row arm selector for the heal_llm_settings_family_conflicts admin task (#5860).

    ``supports_reasoning`` is the resolved model's real capability (looked up per project via
    RPC by the caller), so the reasoning/non-reasoning family is known — not guessed. Returns the
    normalized llm_settings when the row should be healed, or ``None`` when it is already aligned.
    Heal arms:
      - reasoning model + active reasoning_effort + temperature set  -> strip temperature (#5821)
      - non-reasoning model + active reasoning_effort                -> strip effort (impossible config)
      - reasoning model + null reasoning_effort                      -> set effort (#5858)

    Two rows are never touched:
      - explicit ``reasoning_effort='none'`` — the deliberate thinking-off escape hatch (a bare
        null is the unset/stale default, which IS a defect on a reasoning model).
      - non-reasoning model + null effort — already valid; normalizing it would spuriously inject
        a default temperature.
    """
    reasoning_effort = llm_settings.get('reasoning_effort')

    if reasoning_effort == 'none':
        return None
    if reasoning_effort is None and not supports_reasoning:
        return None

    healed = _normalize_llm_settings_family(llm_settings, supports_reasoning)
    return healed if healed != llm_settings else None


class LLMSettingsBase(BaseModel):
    temperature: Optional[Annotated[float, Field(gt=0, le=1)]] = None
    reasoning_effort: Optional[str] = None
    max_tokens: Optional[int] = None
    model_name: Optional[str] = None
    model_project_id: Optional[int] = None


class LLMSettingsModel(LLMSettingsBase):
    """Read/response variant — self-heals a conflicting combo instead of failing. Used for
    GET/predict/export paths that consume stored or expanded data rather than author it."""

    @model_validator(mode="after")
    def _auto_correct_family_conflict(self) -> "LLMSettingsModel":
        if llm_settings_family_conflict(self.temperature, self.reasoning_effort):
            self.temperature = None
        return self


class LLMSettingsWriteModel(LLMSettingsBase):
    """Create/Update variant — rejects a conflicting combo with a clear validation error
    instead of silently persisting it. Used by API models that accept caller-authored
    llm_settings (agent version create/update)."""

    @model_validator(mode="after")
    def _reject_family_conflict(self) -> "LLMSettingsWriteModel":
        if llm_settings_family_conflict(self.temperature, self.reasoning_effort):
            raise ValueError(
                "temperature is not allowed together with a reasoning_effort (other than "
                "'none') — reasoning models reject a custom temperature"
            )
        return self
