import re
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict, field_validator

from .predict_llm import LLMSettingsRequest

NAME_MAX_LENGTH = 64
DESCRIPTION_MAX_LENGTH = 2304
INSTRUCTIONS_MAX_LENGTH = 2500


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


class GenerateSkillDraftResponse(BaseModel):
    """AI-generated skill draft, coerced to be creatable.

    The fields are normalized to fit the skill entity's constraints rather than
    rejected when slightly off, so a usable draft always reaches the review form:
    ``name`` is slugified, ``description``/``instructions`` are truncated to their
    caps. A 422 is only raised when a required field is missing/empty — i.e. a
    genuine generation failure the user retries (AC9). There are deliberately NO
    suggested toolkits/agents/pipelines/MCPs for skills.
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
        return _slugify_skill_name(v) if isinstance(v, str) else v

    @field_validator("description", mode="before")
    @classmethod
    def _truncate_description(cls, v):
        return v[:DESCRIPTION_MAX_LENGTH].rstrip() if isinstance(v, str) else v

    @field_validator("instructions", mode="before")
    @classmethod
    def _truncate_instructions(cls, v):
        return v[:INSTRUCTIONS_MAX_LENGTH].rstrip() if isinstance(v, str) else v
