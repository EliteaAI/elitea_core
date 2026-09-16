from typing import Optional, Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator


class _SelectionValue(BaseModel):
    """Strict routing intent; authorization belongs to runtime admission."""

    model_config = ConfigDict(extra='forbid', strict=True)


class RoutingModelRef(_SelectionValue):
    name: Annotated[str, Field(min_length=1, max_length=256, pattern=r'\S')]
    project_id: Annotated[int, Field(ge=1)]


class RoutingProfileRef(_SelectionValue):
    id: Annotated[str, Field(min_length=1, max_length=128, pattern=r'\S')]
    revision: Annotated[int, Field(ge=1)]


class ExplicitReasoning(_SelectionValue):
    mode: Literal['explicit']
    preset: Annotated[str, Field(min_length=1, max_length=64, pattern=r'\S')]


class AutoReasoning(_SelectionValue):
    mode: Literal['auto']


class FixedSelection(_SelectionValue):
    mode: Literal['fixed']
    model_ref: RoutingModelRef
    reasoning: Optional[ExplicitReasoning] = None


class AutoSelection(_SelectionValue):
    mode: Literal['auto']
    profile_ref: RoutingProfileRef
    scope_mode: Literal['task_episode', 'agent_task', 'run_locked']
    reasoning: Annotated[Union[AutoReasoning, ExplicitReasoning], Field(discriminator='mode')]


ModelSelection = Annotated[
    Union[FixedSelection, AutoSelection], Field(discriminator='mode')
]


def merge_llm_selection_override(baseline: dict | None, override: dict) -> dict:
    """Replace model-selection intent while retaining omitted generation settings."""
    result = dict(baseline or {})
    if override.get('selection') is not None:
        # Typed Auto/fixed selection owns its binding and effort as one value.
        # Legacy fields from the saved model must not conflict with that value.
        for name in ('model_name', 'model_project_id', 'reasoning_effort'):
            result.pop(name, None)
    elif override.get('model_name'):
        # Existing pickers send concrete model fields without a selection object.
        result.pop('selection', None)
    result.update(override)
    return result


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
    selection: Optional[ModelSelection] = None

    @model_validator(mode='after')
    def _validate_selection_binding(self):
        """Do not let legacy fields become a second conflicting model binding.

        An absent selection retains the legacy fixed/inherited behavior. Runtime
        admission still validates feature availability, surface and parent scope.
        """
        selection = self.selection
        if selection is None:
            return self
        if isinstance(selection, FixedSelection):
            ref = selection.model_ref
            if self.model_name not in (None, ref.name) or self.model_project_id not in (None, ref.project_id):
                raise ValueError('selection.model_ref conflicts with the explicit model settings')
            self.model_name, self.model_project_id = ref.name, ref.project_id
        elif self.model_name is not None or self.model_project_id is not None:
            raise ValueError('Auto selection cannot contain a fixed model binding')

        reasoning = getattr(selection, 'reasoning', None)
        if isinstance(reasoning, ExplicitReasoning):
            if self.reasoning_effort not in (None, reasoning.preset):
                raise ValueError('selection.reasoning conflicts with reasoning_effort')
            self.reasoning_effort = reasoning.preset
        elif self.reasoning_effort is not None:
            raise ValueError('Put an explicit effort in selection.reasoning')
        return self

    @model_serializer(mode='wrap')
    def _serialize_selection(self, handler):
        value = handler(self)
        if value.get('selection') is None:
            # Existing fixed records and SDK wire fixtures retain their exact keys.
            value.pop('selection', None)
        return value


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


def validate_model_selection_surface(llm_settings, *, agent_type=None, surface=None):
    """First-release surface gate, independent of environment/project flags.

    A Pipeline's ordinary-Agent child is admitted using the child's own type.
    Missing settings keep their existing behavior and never imply Auto.
    """
    selection = (llm_settings.get('selection') if isinstance(llm_settings, dict)
                 else getattr(llm_settings, 'selection', None))
    mode = selection.get('mode') if isinstance(selection, dict) else getattr(selection, 'mode', None)
    if mode == 'auto' and (agent_type == 'pipeline' or surface in {'pipeline', 'pipeline_llm_node'}):
        raise ValueError('Auto is not available for Pipelines in this release; select a fixed model')
