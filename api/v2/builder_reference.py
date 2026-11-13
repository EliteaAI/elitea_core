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

"""Serve the pipeline authoring reference, one topic at a time.

GET /elitea_core/builder_reference/prompt_lib/<project_id>?topics=overview,node:llm

Returns the requested pages of the pipeline authoring reference (#6388): an overview of the
pipeline YAML, one page per node type with its required fields from the node registry,
patterns, design notes, runtime limits and troubleshooting. The content is the SDK's, kept in
``utils/pipeline_authoring``. With no ``topics`` the overview is returned.
"""

from flask import request

from tools import api_tools, auth, config as c, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.pipeline_authoring import builder_reference, node_types, reference_tool_description


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get Pipeline Builder Reference",
        description=(
            "Return pages of the pipeline authoring reference: the YAML shape, each node "
            "type's fields and behaviour, patterns, runtime limits and troubleshooting."
        ),
        tags=["elitea_core/applications"],
        mcp_tool=True,
        available_to_users=True,
        mcp_description=reference_tool_description(),
        parameters=[
            {"name": "topics", "in": "query", "required": False, "schema": {"type": "string"},
             "description": "Comma-separated topics, e.g. overview,node:llm,node:router. Default: overview."},
        ],
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
        return {
            "reference": builder_reference(request.args.get("topics") or None),
            "node_types": node_types("current"),
            "version": "2.0",
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
