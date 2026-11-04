#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Pydantic models for the generate_pipeline_draft endpoint."""

from typing import Optional, List

from pydantic import BaseModel, ConfigDict, Field, constr, model_validator

from .predict_llm import LLMSettingsRequest
from .generate_application_draft import (
    ApplicationSuggestion,
    SkillSuggestion,
    ToolkitSuggestion,
)
from ...utils.pipeline_validator import PipelineValidationResult


class GeneratePipelineDraftRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "user_description": "A pipeline that takes a GitHub PR, reviews it with an LLM, and posts a comment",
                    "llm_settings": {
                        "model_name": "gpt-4o",
                        "max_tokens": 8192,
                        "temperature": 0.3,
                    },
                },
                {
                    "user_description": "Add an approval step before the Jira issue is created",
                    "pipeline_id": 42,
                    "version_id": 7,
                },
            ]
        }
    )

    user_description: constr(strip_whitespace=True, min_length=1) = Field(
        description="Natural-language description of the desired pipeline or the changes to make."
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description=(
            "LLM model override. If not provided, uses the pipeline's llm_settings "
            "(edit mode) or the project's default model (create mode)."
        ),
    )
    pipeline_id: Optional[int] = Field(
        default=None,
        description="Pipeline application ID to edit. Required together with version_id for edit mode.",
    )
    version_id: Optional[int] = Field(
        default=None,
        description="Version ID to edit. Required together with pipeline_id for edit mode.",
    )

    @model_validator(mode="after")
    def validate_edit_params(self):
        if (self.pipeline_id is None) != (self.version_id is None):
            raise ValueError("Both pipeline_id and version_id must be provided for edit mode.")
        return self

    @property
    def is_edit_mode(self) -> bool:
        return self.pipeline_id is not None and self.version_id is not None


class GeneratePipelineDraftResponse(BaseModel):
    """Response for both create and edit modes of pipeline draft generation."""

    name: str = Field(
        min_length=1, max_length=32, description="Pipeline name (1–32 characters)."
    )
    description: str = Field(
        min_length=0,
        max_length=2304,
        description="Pipeline description (0–2304 characters).",
    )
    pipeline_yaml: str = Field(
        description="Complete, validated YAML string for the pipeline nodes."
    )
    validation_result: PipelineValidationResult = Field(
        description="Structural validation result for the generated pipeline_yaml."
    )

    # Resource suggestions — same shape as GenerateApplicationDraftResponse
    suggested_toolkits: List[ToolkitSuggestion] = Field(default_factory=list)
    suggested_mcp: List[ToolkitSuggestion] = Field(default_factory=list)
    suggested_agents: List[ApplicationSuggestion] = Field(default_factory=list)
    suggested_pipelines: List[ApplicationSuggestion] = Field(default_factory=list)
    suggested_skills: List[SkillSuggestion] = Field(default_factory=list)
