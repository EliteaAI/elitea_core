from typing import Optional, Annotated

from pydantic import BaseModel, Field, model_validator


def llm_settings_family_conflict(temperature: Optional[float], reasoning_effort: Optional[str]) -> bool:
    """True when temperature and an active reasoning_effort are both set — invalid combo for
    reasoning models (Anthropic extended thinking, OpenAI o1/gpt-5). Single source of truth,
    shared by the read (auto-correct) and write (reject) LLM settings variants below, and by
    validate_and_resolve_llm_settings (utils/application_utils.py)."""
    return temperature is not None and reasoning_effort not in (None, "none")


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
