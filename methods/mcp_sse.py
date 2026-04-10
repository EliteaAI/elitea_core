#!/usr/bin/python3
# coding=utf-8

#   Copyright 2025 EPAM Systems
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

""" MCP SSE Methods """

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611
from tools import context  # pylint: disable=E0401


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        MCP SSE Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def get_tool_schemas_mcp_sse(self, project_id: int, user_id: int):
        """Get MCP server tool schemas for project/user"""
        mcp_servers = self.get_registered_servers_private_and_current(project_id, user_id)
        return {
            server.name: _mcp_server_to_toolkit(server) for server in mcp_servers
        }

    @web.method()
    def get_registered_servers_private_and_current(self, project_id: int, user_id: int = None):
        """Get MCP servers for both private and current project"""
        private_project_id = context.rpc_manager.call.projects_get_personal_project_id(user_id)
        private_servers = self.servers_storage.get_servers_dict(private_project_id)
        current_servers = self.servers_storage.get_servers_dict(project_id)
        result = list({**current_servers, **private_servers}.values())
        #
        log.debug(f"[MCP_CLIENT] All Mcp Servers {self.servers_storage.status()}:")
        log.debug(f"[MCP_CLIENT] Collect Mcp Servers for private {private_project_id} ({user_id}) and current {project_id}:")
        log.debug(f"[MCP_CLIENT] Mcp Servers for private :\n{_str_servers(private_servers.values())}")
        log.debug(f"[MCP_CLIENT] Mcp Servers for current :\n{_str_servers(current_servers.values())}")
        log.debug(f"[MCP_CLIENT] Mcp Servers joined :\n{_str_servers(result)}")
        #
        return result


def _mcp_server_to_toolkit(mcp_server):
    """Convert MCP server to toolkit format"""
    from ..models.mcp import (
        EliteaToolkitArgsSchema, EliteaMcpToolkit, EliteaToolkitSelectedTools,
        EliteaToolkitItems, EliteaToolkitMetadata
    )
    
    args_schemas = {
        tool.name: EliteaToolkitArgsSchema(
            title=tool.name,
            **tool.model_dump(),
        )
        for tool in mcp_server.tools
    }
    #
    selected_tools = EliteaToolkitSelectedTools(
        title="Selected Tools",
        args_schemas=args_schemas,
        items=EliteaToolkitItems(enum=[tool.name for tool in mcp_server.tools])
    )
    #
    return EliteaMcpToolkit(
        title=mcp_server.name,
        properties={"selected_tools": selected_tools},
        metadata=EliteaToolkitMetadata(label=mcp_server.name),
    ).model_dump()


def _str_servers(servers_list):
    """Format servers list for logging"""
    return "\n".join(
        f"Mcp Server '{server.name}'\n   {len(server.tools)} tools : {[tool.name for tool in server.tools]}"
        for server in servers_list
    )
