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

"""API for Syncing/Fetching Tools from Remote MCP Server (v2)"""
from flask import request
from pydantic import ValidationError
from tools import api_tools, auth, config as c, serialize, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.mcp_config import is_mcp_exposure_enabled
from ...utils.sio_utils import SioValidationError
from ...utils.exceptions import PoolSaturationError

from ...models.pd.mcp_sync_tools import McpSyncToolsInputModel

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="MCP Sync Tools",
        description="Discover and sync available tools from a remote MCP server. Supports async mode via Socket.IO.",
        tags=["elitea_core/mcp"],
        parameters=[
            {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Project ID."},
            {"name": "await_response", "in": "query", "required": False, "schema": {"type": "boolean", "default": True}, "description": "Wait for the result synchronously (default: true)."},
            {"name": "timeout", "in": "query", "required": False, "schema": {"type": "integer", "default": 120}, "description": "Timeout in seconds (default: 120 for sync, -1 for async)."},
        ],
        request_body=McpSyncToolsInputModel,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.patch"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        """
        Sync/fetch tools from a remote MCP server.
        
        This endpoint discovers available tools from a remote MCP server.
        If the server requires OAuth authorization, it will emit an
        'mcp_authorization_required' socket event with the OAuth metadata.
        
        Args:
            project_id: Project ID
            
        Returns:
            Dictionary with tools list or task ID for async operation
        """
        if not is_mcp_exposure_enabled():
            return {"error": "MCP exposure is disabled on this deployment"}, 403

        raw = dict(request.json)
        await_response = request.args.get('await_response', 'true').lower() == 'true'
        # Configurable timeout with sensible default (2 minutes for sync, no timeout for async)
        timeout = int(request.args.get('timeout', '120' if await_response else '-1'))
        
        # Add project_id to the request data from URL parameter
        raw['project_id'] = project_id
        # handle pre-built MCP toolkit settings
        if raw.get('toolkit_type', '').startswith('mcp_'):
            log.debug(f"Resolving MCP toolkit settings for toolkit_type={raw.get('toolkit_type')}")
            # This will fill in any missing settings from the pylon config for pre-built MCP toolkits
            raw = self.module.resolve_mcp_prebuilt_settings(raw)

        try:
            sync_data = McpSyncToolsInputModel.model_validate(raw)
        except ValidationError as e:
            return {"error": e.errors()}, 400
        
        log.debug(f'MCP sync tools request: url={sync_data.url}, project_id={project_id}, ssl_verify={sync_data.ssl_verify}, raw_ssl_verify={raw.get("ssl_verify")}')
        
        # SID is optional for async calls - it's only needed for Socket.IO streaming
        # If not provided, the caller will need to poll the task status
        
        if await_response:
            sync_data.sid = None
        
        try:
            result = self.module.mcp_sync_tools_sio(
                sync_data.sid, sync_data.model_dump(), "mcp_sync_tools",
                await_task_timeout=timeout
            )
        except SioValidationError as e:
            return {'error': str(e.error)}, 400
        except PoolSaturationError as e:
            return {
                "error": "temporarily_unavailable",
                "message": "The service is busy processing other requests. Please try again in a few seconds.",
                "retry_after": e.retry_after,
            }, 503
        except Exception as e:
            log.error(f"Error in mcp_sync_tools API: {str(e)}")
            return {'error': str(e)}, 500
        
        task_id = result.get('task_id')
        if await_response:
            if not result.get('result'):
                # Stop the task if it didn't complete in time
                try:
                    self.module.task_node.stop_task(task_id)
                except:
                    pass
                return {"error": "Timeout", "task_id": task_id}, 408
        
        return serialize(result), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
