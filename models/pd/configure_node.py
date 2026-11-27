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

"""Pydantic models for the configure_pipeline_node endpoint."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .predict_llm import LLMSettingsRequest


class ConfigureNodeRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "node_type": "llm",
                    "user_description": "Summarise the pull request diff in 3 bullet points",
                    "state_variables": ["pr_diff", "pr_title"],
                    "available_nodes": ["fetch_pr", "summarise", "post_comment", "end_node"],
                    "current_node_config": {
                        "system": "You are a helpful assistant.",
                        "task": "{state.pr_diff}",
                    },
                }
            ]
        }
    )

    node_type: str = Field(
        description="The pipeline node type to configure (e.g. 'llm', 'code', 'router')."
    )
    user_description: str = Field(
        min_length=1,
        description="Plain-language description of what this node should do.",
    )
    current_node_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The node's current field values (omit to configure from scratch).",
    )
    state_variables: List[str] = Field(
        default_factory=list,
        description="Names of pipeline state variables available to this node.",
    )
    available_nodes: List[str] = Field(
        default_factory=list,
        description="Node IDs in the pipeline (used for transition targets).",
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description=(
            "LLM override for the generation call. "
            "Falls back to the pipeline's llm_settings, then the project default."
        ),
    )


class ConfigureNodeResponse(BaseModel):
    """Suggested field values for the configured node."""

    node_config: Dict[str, Any] = Field(
        description=(
            "Complete field values for the node. Does not include 'id' or 'type' — "
            "those are set by the caller. Apply only the fields the user selects."
        )
    )
    summary: str = Field(
        description="One-sentence description of what was configured and why."
    )
