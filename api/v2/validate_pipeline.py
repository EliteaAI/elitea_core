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

"""Validate a pipeline YAML string before it is saved.

POST /elitea_core/validate_pipeline/prompt_lib/<project_id>

Exposed as an internal MCP tool so the Agent & Pipeline Builder can check a draft before
saving it: generate -> validate -> fix -> validate -> save. The rules are the SDK's
(``utils/pipeline_authoring``): node types and required fields come from the node
registry, and each finding names the node, the field and a fix.
"""

from typing import Optional

from flask import request
from pydantic import BaseModel, ValidationError

from tools import api_tools, auth, config as c, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.pipeline_authoring import VALIDATE_TOOL_DESCRIPTION, validate_for_builder


class ValidatePipelineRequest(BaseModel):
    pipeline_yaml: str
    trigger: Optional[str] = None


def with_legacy_keys(result: dict) -> dict:
    """Findings also carry the former ``code`` and ``severity`` keys for existing readers."""
    for severity, key in (("error", "errors"), ("warning", "warnings")):
        for finding in result[key]:
            finding.setdefault("code", finding["rule"])
            finding.setdefault("severity", severity)
    return result


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Validate Pipeline YAML",
        description=(
            "Validate a pipeline YAML string against the pipeline rules. Returns errors and "
            "warnings without saving anything."
        ),
        request_body=ValidatePipelineRequest,
        tags=["elitea_core/applications"],
        mcp_tool=True,
        available_to_users=True,
        mcp_description=VALIDATE_TOOL_DESCRIPTION,
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

        return with_legacy_keys(validate_for_builder(req.pipeline_yaml, trigger=req.trigger)), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
