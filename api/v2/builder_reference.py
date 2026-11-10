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

"""Serve the pipeline authoring reference document.

GET /elitea_core/builder_reference/prompt_lib/<project_id>

Returns the pipeline authoring reference (node types, required fields,
transition syntax, wiring patterns). Used by the Builder and by AI
prompts that need the full schema context on demand.
"""

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.pipeline_validator import KNOWN_NODE_TYPES
from ...utils.service_prompt_utils import get_service_prompt

_SERVICE_PROMPT_KEY_REFERENCE = "pipeline_authoring_reference"


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Pipeline Builder Reference",
        description=(
            "Return the pipeline authoring reference document: node types, "
            "required fields, transition syntax, and wiring patterns. "
            "Use this when authoring or debugging pipeline YAML."
        ),
        tags=["elitea_core/applications"],
        mcp_tool=True,
        available_to_users=True,
        mcp_description="""
        Return the complete pipeline authoring reference.

        Call this tool at the start of a pipeline authoring session, or whenever
        you need to look up a node type's required fields, transition syntax, or
        a wiring pattern.

        Returns: { "reference": "<full reference text>",
                   "node_types": ["llm", "code", ...],
                   "version": "1.0" }
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
    def get(self, project_id: int):
        reference = get_service_prompt(_SERVICE_PROMPT_KEY_REFERENCE)
        if not reference:
            log.warning(
                "builder_reference: service prompt '%s' not configured",
                _SERVICE_PROMPT_KEY_REFERENCE,
            )
            return {
                "error": (
                    f"Service prompt '{_SERVICE_PROMPT_KEY_REFERENCE}' is not configured. "
                    f"Seed it via Settings → Service Prompts."
                )
            }, 500

        return {
            "reference": reference,
            "node_types": sorted(KNOWN_NODE_TYPES),
            "version": "1.0",
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
