from typing import Optional

from pydantic import BaseModel, Field, ConfigDict

from .predict_llm import LLMSettingsRequest
from .skill import SkillName


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
    """Validated AI-generated skill draft.

    Constraints mirror the skill create model (``models/pd/skill.py``):
    ``name`` reuses :data:`SkillName` (lowercase letters/digits/hyphens, no
    leading/trailing hyphen, <=64 chars, no "claude"/"anthropic"); description
    and instructions use the same caps as ``SkillVersionCreateModel``. There are
    deliberately NO suggested toolkits/agents/pipelines/MCPs for skills.
    """

    name: SkillName = Field(
        min_length=1,
        max_length=64,
        description="Skill name (lowercase letters/digits/hyphens, no leading/trailing hyphen)",
    )
    description: str = Field(
        min_length=1,
        max_length=2304,
        description="Skill description (1–2304 characters)",
    )
    instructions: str = Field(
        min_length=1,
        max_length=2500,
        description="Skill instructions in Markdown (1–2500 characters)",
    )
