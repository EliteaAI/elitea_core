from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .predict_llm import LLMSettingsRequest
from .evaluation import EvalDimensionCreateModel, EvalBindingBaseModel


class GenerateEvalDimensionsRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "application_id": 1,
                    "llm_settings": {
                        "model_name": "gpt-5-mini",
                        "max_tokens": 4096,
                        "temperature": 0.7,
                    },
                },
            ]
        }
    )

    application_id: int = Field(description="Agent to generate eval dimensions for")
    version_id: Optional[int] = Field(
        default=None,
        description="Application version to read instructions from. "
        "Defaults to the agent's default/base version.",
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description="LLM model override. If not provided, uses the project's default model.",
    )
    count_hint: Optional[int] = Field(
        default=None,
        ge=1,
        le=20,
        description="Soft cap on how many dimensions to propose.",
    )


class GeneratedDimensionDraft(EvalDimensionCreateModel, EvalBindingBaseModel):
    """One proposed dimension + its suggested binding knobs, in a single flat draft item.

    Fields are the exact union of ``EvalDimensionCreateModel`` (the dimension definition) and
    ``EvalBindingBaseModel`` (how it should be scored for this agent) so a kept draft item maps
    1:1 onto a ``create_dimension`` call followed by a ``create_binding`` call with no renaming.
    """


class GenerateEvalDimensionsResponse(BaseModel):
    dimensions: List[GeneratedDimensionDraft] = Field(default_factory=list)
