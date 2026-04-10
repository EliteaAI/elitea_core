"""
API endpoint for MCP toolkit tool discovery.

This endpoint discovers available tools from MCP servers by calling the SDK's
check_connection method, which connects to the MCP server and lists its tools.

Used by the "Load Tools" button in the toolkit configuration UI.
"""

from copy import deepcopy
from flask import request

from tools import api_tools, auth, config as c, db
from pylon.core.tools import log

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.application_tools import (
    expand_toolkit_settings,
    ValidatorNotSupportedError,
    ConfigurationExpandError,
)


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, toolkit_type: str, **kwargs):
        """
        Discover tools from an MCP server.

        Args:
            project_id: Project ID
            toolkit_type: MCP toolkit type (e.g., 'mcp_github_copilot')

        Request Body:
            settings: dict - Toolkit settings (credentials, etc.)

        Returns:
            {
                "success": True,
                "tools": [{"name": str, "description": str, "inputSchema": dict}],
                "args_schemas": {"tool_name": <json schema dict>}
            }
        """
        _ = kwargs

        current_user = auth.current_user()
        user_id = current_user.get('id') if current_user else None

        data = deepcopy(request.json or {})
        settings = data.get('settings', data)  # Support both {settings: {...}} and direct settings

        try:
            # Expand settings to resolve configuration references
            if user_id:
                try:
                    settings = expand_toolkit_settings(toolkit_type, settings, project_id, user_id)
                except (ValidatorNotSupportedError, ConfigurationExpandError) as e:
                    log.warning(f"Could not expand settings for {toolkit_type}: {e}")
                except Exception as e:
                    log.warning(f"Error expanding settings for {toolkit_type}: {e}")

            # Call RPC method which dispatches to indexer_worker
            result = self.module.discover_mcp_tools(
                toolkit_type=toolkit_type,
                settings=settings,
            )

            if isinstance(result, str):
                # Error message returned
                return {"success": False, "error": result}, 400

            if isinstance(result, dict):
                if result.get('error'):
                    return {"success": False, "error": result.get('error')}, 400
                # Success with tools data
                return {"success": True, **result}, 200

            # Unexpected result
            return {"success": False, "error": "Unexpected response from tool discovery"}, 500

        except Exception as e:
            log.exception(f"Error discovering tools for {toolkit_type}")
            return {"success": False, "error": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:toolkit_type>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
