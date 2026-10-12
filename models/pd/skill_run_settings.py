from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .llm import LLMSettingsModel, LLMSettingsWriteModel, validate_model_selection_surface


SKILL_RUN_SURFACE = 'skill'


class SkillRunSettingsWriteModel(BaseModel):
    llm_settings: Optional[LLMSettingsWriteModel] = Field(
        default=None,
        description="Default model and generation settings. Omit to use the running project's default model.",
    )
    ignore_project_context: bool = Field(
        default=False,
        description="Leave the project context out of the skill's system prompt.",
    )

    model_config = ConfigDict(extra='forbid')

    @model_validator(mode='after')
    def _check_selection_surface(self):
        if self.llm_settings is not None:
            validate_model_selection_surface(self.llm_settings, surface=SKILL_RUN_SURFACE)
        return self


class SkillRunSettingsModel(BaseModel):
    llm_settings: Optional[LLMSettingsModel] = None
    ignore_project_context: bool = False

    model_config = ConfigDict(from_attributes=True)


def dump_run_settings(settings: Optional[SkillRunSettingsWriteModel]) -> Optional[dict]:
    if settings is None:
        return None
    return settings.model_dump(exclude_none=True)
