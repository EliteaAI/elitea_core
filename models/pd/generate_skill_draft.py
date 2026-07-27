import re
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

from .predict_llm import LLMSettingsRequest
from .skill import validate_skill_name

NAME_MAX_LENGTH = 64
DESCRIPTION_MAX_LENGTH = 2304
INSTRUCTIONS_MAX_LENGTH = 5000


def _slugify_skill_name(value: str) -> str:
    s = value.strip().lower()
    s = re.sub(r"[^a-z0-9-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:NAME_MAX_LENGTH].strip("-")


class GenerateSkillDraftRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_description": "A skill that reviews GitHub pull requests for security issues",
                "llm_settings": {
                    "model_name": "gpt-5-mini",
                    "max_tokens": 2048,
                    "temperature": 0,
                },
            }
        }
    )

    user_description: str = Field(
        description="Natural-language description of the desired skill"
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description="LLM model override. If not provided, "
        "uses the project's default model with temperature=0.7 and max_tokens=4096.",
    )
    skill_id: Optional[int] = Field(
        default=None,
        description="Skill ID to edit. When provided with version_id, enables edit mode.",
    )
    version_id: Optional[int] = Field(
        default=None,
        description="Version ID to edit. Required when skill_id is provided.",
    )

    @model_validator(mode="after")
    def validate_edit_params(self):
        """Ensure both skill_id and version_id are provided together."""
        if (self.skill_id is None) != (self.version_id is None):
            raise ValueError("Both skill_id and version_id must be provided for edit mode")
        return self

    @property
    def is_edit_mode(self) -> bool:
        return self.skill_id is not None and self.version_id is not None


class GenerateSkillDraftResponse(BaseModel):
    """AI-generated skill draft, coerced to be creatable.

    The fields are normalized to fit the skill entity's constraints rather than
    rejected when slightly off, so a usable draft always reaches the review form:
    ``name`` is slugified then checked against :func:`validate_skill_name` (the
    same rule the skill create API enforces — single source of truth), and
    ``description``/``instructions`` are truncated to their caps. A 422 is only
    raised when a required field is missing/empty or the name cannot be salvaged
    into a valid slug — i.e. a genuine generation failure the user retries (AC9).
    There are deliberately NO suggested toolkits/agents/pipelines/MCPs for skills.
    """

    name: str = Field(
        min_length=1,
        max_length=NAME_MAX_LENGTH,
        description="Skill name, slugified (lowercase letters/digits/hyphens, no leading/trailing hyphen)",
    )
    description: str = Field(
        min_length=1,
        max_length=DESCRIPTION_MAX_LENGTH,
        description=f"Skill description (truncated to {DESCRIPTION_MAX_LENGTH} characters)",
    )
    instructions: str = Field(
        min_length=1,
        max_length=INSTRUCTIONS_MAX_LENGTH,
        description=f"Skill instructions in Markdown (truncated to {INSTRUCTIONS_MAX_LENGTH} characters)",
    )

    @field_validator("name", mode="before")
    @classmethod
    def _normalize_name(cls, v):
        return validate_skill_name(_slugify_skill_name(v)) if isinstance(v, str) else v

    @field_validator("description", mode="before")
    @classmethod
    def _truncate_description(cls, v):
        return v[:DESCRIPTION_MAX_LENGTH].rstrip() if isinstance(v, str) else v

    @field_validator("instructions", mode="before")
    @classmethod
    def _truncate_instructions(cls, v):
        return v[:INSTRUCTIONS_MAX_LENGTH].rstrip() if isinstance(v, str) else v
