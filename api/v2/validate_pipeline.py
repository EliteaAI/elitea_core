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

"""Validate a pipeline YAML string against the structural rules.

POST /elitea_core/validate_pipeline/prompt_lib/<project_id>

This endpoint is exposed as an internal MCP tool so that the Agent &
Pipeline Builder can validate before saving, enabling a self-correction
loop: generate → validate → errors? → fix → validate → save.
"""

from flask import request
from pydantic import BaseModel, ValidationError

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.pipeline_validator import validate_pipeline_yaml


class ValidatePipelineRequest(BaseModel):
    pipeline_yaml: str


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Validate Pipeline YAML",
        description=(
            "Validate a pipeline YAML string against the structural rules. "
            "Returns errors and warnings without saving anything. "
            "Call this before create_version or update_version to confirm the YAML is valid."
        ),
        request_body=ValidatePipelineRequest,
        tags=["elitea_core/applications"],
        mcp_tool=True,
        available_to_users=True,
        mcp_description="""
        Validate a pipeline YAML string before saving it.

        Call this tool BEFORE create_version or update_version for any pipeline you generated or
        edited. If the result has errors, fix them and validate again before saving.

        Input: { "pipeline_yaml": "<yaml string>" }

        Output: { "valid": true/false, "errors": [...], "warnings": [...] }
        Each error/warning has: code, node_id, field, message, severity.

        Common fixes:
        - missing_nodes: add a top-level 'nodes' list with at least one node
        - missing_node_id: add unique 'id' to each node
        - unknown_node_type: set 'type' to one of: llm, code, agent, toolkit, mcp,
          router, decision, state_modifier, printer, hitl, custom
        - no_end_node: set `transition: END` on the last node of each execution path
          (do NOT add a separate node with type: end — that is deprecated)
        - end_node_unreachable: wire the graph so all paths reach a node with `transition: END`
        - unknown_transition_target: fix the target node id spelling
        - missing_tool_name: add 'name' to agent/toolkit/mcp nodes
        - missing_code_field: add a 'code' field to code nodes
        """,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        try:
            req = ValidatePipelineRequest.model_validate(
                request.get_json(silent=True) or {}
            )
        except ValidationError as exc:
            return exc.errors(include_url=False), 400

        result = validate_pipeline_yaml(req.pipeline_yaml, project_id=project_id)
        return result.model_dump(), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
