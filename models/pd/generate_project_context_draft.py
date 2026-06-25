from typing import Optional

from pydantic import BaseModel, Field, ConfigDict, field_validator

from .predict_llm import LLMSettingsRequest
from .project_context import PROJECT_CONTEXT_MAX_LEN

PROJECT_BACKGROUND_MAX_LENGTH = PROJECT_CONTEXT_MAX_LEN


class GenerateProjectContextDraftRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_description": (
                    "A React + FastAPI app backed by Postgres. Trunk-based development, "
                    "pytest and Playwright for testing, deployed via GitHub Actions."
                ),
                "llm_settings": {
                    "model_name": "gpt-5-mini",
                    "max_tokens": 2048,
                    "temperature": 0,
                },
            }
        }
    )

    user_description: str = Field(
        description="Natural-language description of the project (architecture, processes, constraints, etc.)"
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description="LLM model override. If not provided, "
        "uses the project's default model with temperature=0.7 and max_tokens=4096.",
    )


class GenerateProjectContextDraftResponse(BaseModel):
    """AI-generated Project Background draft.

    The single ``project_background`` field is truncated to its cap rather than
    rejected when slightly over, so a usable draft always reaches the review form.
    A 422 is only raised when the field is missing/empty.
    There are deliberately NO suggested tools/agents/pipelines/MCPs/resources.
    """

    project_background: str = Field(
        min_length=1,
        max_length=PROJECT_BACKGROUND_MAX_LENGTH,
        description=f"Project Background in Markdown (truncated to {PROJECT_BACKGROUND_MAX_LENGTH} characters)",
    )

    @field_validator("project_background", mode="before")
    @classmethod
    def _truncate_project_background(cls, v):
        return v[:PROJECT_BACKGROUND_MAX_LENGTH].rstrip() if isinstance(v, str) else v
