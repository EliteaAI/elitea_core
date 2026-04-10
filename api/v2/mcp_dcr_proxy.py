#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
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

""" API for MCP OAuth Dynamic Client Registration (DCR) Proxy """
from flask import request
from tools import api_tools, auth, config as c

from pylon.core.tools import log

from ...models.pd.mcp_oauth import McpDynamicClientRegistrationRequest
from ...utils.mcp_oauth import register_dynamic_client


class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.patch"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        """
        Proxy Dynamic Client Registration request to avoid CORS issues.
        
        This endpoint proxies RFC 7591 Dynamic Client Registration requests
        to OAuth authorization servers that support DCR.
        """
        try:
            data = McpDynamicClientRegistrationRequest.model_validate(request.json)
        except Exception as e:
            log.error(f"MCP DCR proxy validation error: {e}")
            return {"error": "Invalid request", "details": str(e)}, 400

        try:
            log.debug(f"MCP DCR proxy: registering client at {data.registration_endpoint}")
            log.debug(f"MCP DCR proxy request: redirect_uris={data.redirect_uris}, "
                     f"client_name={data.client_name}, grant_types={data.grant_types}")
            
            registration_data = register_dynamic_client(
                registration_endpoint=data.registration_endpoint,
                redirect_uris=data.redirect_uris,
                client_name=data.client_name,
                grant_types=data.grant_types,
                response_types=data.response_types,
                token_endpoint_auth_method=data.token_endpoint_auth_method,
                application_type=data.application_type,
                scope=data.scope,
                software_id=data.software_id,
                software_version=data.software_version,
            )
            
            log.debug(f"MCP DCR proxy: registration successful, client_id={registration_data.get('client_id')}")
            return registration_data, 200

        except ValueError as e:
            log.error(f"MCP DCR proxy: registration failed - {e}")
            return {"error": "registration_failed", "error_description": str(e)}, 400
        except Exception as e:
            log.error(f"MCP DCR proxy: unexpected error - {e}")
            return {"error": "internal_error", "error_description": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        c.DEFAULT_MODE: ProjectAPI,
    }
