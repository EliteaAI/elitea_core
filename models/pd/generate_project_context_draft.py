from typing import Optional

from pydantic import BaseModel, Field, ConfigDict, constr, field_validator

from .predict_llm import LLMSettingsRequest
from .project_context import (
    PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN,
    PROJECT_CONTEXT_MAX_LEN,
)

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

    user_description: constr(strip_whitespace=True, min_length=1) = Field(
        description="Natural-language description of the project (architecture, processes, constraints, etc.)"
    )
    current_project_background: Optional[str] = Field(
        default=None,
        description="Existing Project Background to refine. When provided, the draft is generated in "
        "edit mode (the suggestion refines this content) instead of create mode.",
    )
    current_activation_description: Optional[str] = Field(
        default=None,
        max_length=PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN,
        description="Existing activation description to refine together with the Project Background.",
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description="LLM model override. If not provided, "
        "uses the project's default model with temperature=0.7 and max_tokens=4096.",
    )

    @property
    def is_edit_mode(self) -> bool:
        return self.current_project_background is not None


class GenerateProjectContextDraftResponse(BaseModel):
    """AI-generated Project Context draft.

    An over-long Project Background is rejected rather than trimmed: a silent cut
    reaches the caller as a draft that ends mid-sentence with nothing to say content
    was lost, and the same content is refused again by the save endpoint.
    There are deliberately NO suggested tools/agents/pipelines/MCPs/resources.
    """

    project_background: str = Field(
        min_length=1,
        max_length=PROJECT_BACKGROUND_MAX_LENGTH,
        description=f"Project Background in Markdown "
                    f"(rejected when longer than {PROJECT_BACKGROUND_MAX_LENGTH} characters)",
    )
    activation_description: str = Field(
        min_length=1,
        max_length=PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN,
        description="Concise description of the user intents that should activate this Project Context.",
    )

    @field_validator("activation_description", mode="before")
    @classmethod
    def _normalize_activation_description(cls, v):
        if not isinstance(v, str):
            return v
        return ' '.join(v.split())[:PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN].rstrip()
