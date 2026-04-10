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

"""Pydantic models for MCP sync tools API"""
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field


class McpSyncToolsInputModel(BaseModel):
    """Input model for syncing/fetching tools from a remote MCP server"""

    url: str = Field(
        ...,
        description="MCP server HTTP URL (http:// or https://)"
    )
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="HTTP headers for authentication and configuration"
    )
    timeout: Optional[int] = Field(
        default=60,
        description="Request timeout in seconds"
    )
    mcp_tokens: Optional[Dict[str, Any]] = Field(
        default=None,
        description="MCP OAuth tokens for authentication (keyed by server URL)"
    )

    # Communication
    sid: Optional[str] = Field(
        None,
        description="Socket ID for real-time communication"
    )

    # Project context (will be set from URL parameter)
    project_id: Optional[int] = Field(
        None,
        description="Project ID (set from URL parameter)"
    )

    ssl_verify: bool = True

    toolkit_type: Optional[str] = Field(
        None,
        description="Type of the toolkit (used for pre-built MCP)"
    )


class McpSyncToolsResponseModel(BaseModel):
    """Response model for MCP sync tools API"""

    success: bool = Field(..., description="Whether the operation was successful")
    tools: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="List of discovered tools from the MCP server"
    )
    count: Optional[int] = Field(
        default=None,
        description="Number of tools discovered"
    )
    server_url: Optional[str] = Field(
        default=None,
        description="URL of the MCP server"
    )
    error: Optional[str] = Field(
        default=None,
        description="Error message if the operation failed"
    )
    requires_authorization: Optional[bool] = Field(
        default=None,
        description="True if the MCP server requires OAuth authorization"
    )
    task_id: Optional[str] = Field(
        default=None,
        description="Task ID for async operations"
    )
