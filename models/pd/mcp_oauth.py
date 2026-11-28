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

"""Pydantic models for MCP OAuth operations."""

import re
from typing import Optional, Literal, List
from pydantic import BaseModel, Field, field_validator, ConfigDict

from .schedule_list import DbId

# Regex pattern to match vault secret references like {{secret.my_secret}}
VAULT_SECRET_PATTERN = re.compile(r'^\{\{secret\.[^}]+\}\}$')
# A reference anywhere in a value: the vault replaces it wherever it stands (shared/tools/vault_tools.py
# `_secret_pattern`, a narrower name set), so a guard that must catch every value the vault would resolve
# searches with this one and never with the anchored pattern above
VAULT_REFERENCE_IN_TEXT = re.compile(r'\{\{secret\.[^}]+\}\}')


class McpOAuthTokenRequest(BaseModel):
    """Request model for MCP OAuth token exchange (authorization_code or refresh_token)."""
    token_endpoint: str = Field(..., description="OAuth token endpoint URL")
    grant_type: Optional[Literal['authorization_code', 'refresh_token']] = Field(
        default='authorization_code',
        description="OAuth grant type"
    )
    code: Optional[str] = Field(default=None, description="Authorization code (required for authorization_code grant)")
    redirect_uri: Optional[str] = Field(default=None, description="Redirect URI used in authorization request")
    code_verifier: Optional[str] = Field(default=None, description="PKCE code verifier")
    refresh_token: Optional[str] = Field(default=None, description="Refresh token (required for refresh_token grant)")
    client_id: Optional[str] = Field(default=None, description="OAuth client ID")
    client_secret: Optional[str] = Field(default=None, description="OAuth client secret or vault reference (e.g., {{secret.my_secret}})")
    scope: Optional[str] = Field(default=None, description="OAuth scope")
    resource: Optional[str] = Field(default=None, description="RFC 8707 resource indicator: the MCP server the token is for")
    toolkit_id: Optional[int] = Field(default=None, description="Toolkit ID to fetch credentials from DB")
    toolkit_type: Optional[str] = Field(default=None, description="Toolkit type for fetching credentials (e.g., mcp_github, etc.)")
    configuration_uuid: str | None = Field(default=None, max_length=64, description="Saved credential to sign in with: the server loads its client and stored secret and sends the secret only to the sign-in address of that credential type. With save_login it is the mcp_login credential that keeps the login")
    configuration_project_id: DbId | None = Field(default=None, description="Project that holds the credential of configuration_uuid (the personal project for a private credential); the credential must be visible to the caller")
    oauth_app_id: Optional[str] = Field(default=None, description="Admin-managed OAuth app (Default app mode); its client is resolved server-side")
    credential_type: str | None = Field(default=None, max_length=100, description="Credential type signing in: with oauth_app_id (teams, outlook, sharepoint, ms_graph) the app must serve it; with configuration_uuid it must equal the type of the saved credential")
    used_dcr: bool = Field(default=False, description="True when the client_id was obtained via Dynamic Client Registration (RFC 7591). Prevents loading client_secret from toolkit DB settings since DCR-registered public clients have no secret.")
    save_login: bool = Field(default=False, description="Save the login on the mcp_login credential `configuration_uuid` for unattended runs; the refresh token is kept server-side only")

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "token_endpoint": "https://oauth.example.com/token",
                    "grant_type": "authorization_code",
                    "code": "abc123",
                    "redirect_uri": "https://dev.elitea.ai/mcp/callback",
                    "client_id": "my-client-id",
                }
            ]
        }
    )

    @field_validator('client_secret')
    @classmethod
    def validate_client_secret(cls, v):
        """
        Validate client_secret format.
        If it looks like a vault reference (starts with {{ and ends with }}), 
        it must match the exact pattern {{secret.name}}.
        """
        if v is None:
            return v
        
        # Check if it looks like a vault reference (contains {{ }})
        if '{{' in v and '}}' in v:
            if not VAULT_SECRET_PATTERN.match(v):
                raise ValueError(
                    f"Invalid vault secret format: '{v}'. "
                    "Expected format: {{secret.secret_name}}"
                )
        return v


class McpDynamicClientRegistrationRequest(BaseModel):
    """Request model for MCP OAuth Dynamic Client Registration (RFC 7591)."""
    registration_endpoint: str = Field(..., description="OAuth client registration endpoint URL")
    redirect_uris: List[str] = Field(..., description="List of redirect URIs for the client")
    client_name: Optional[str] = Field(default=None, description="Human-readable name for the client")
    grant_types: Optional[List[str]] = Field(
        default=None,
        description="List of grant types (default: authorization_code, refresh_token)"
    )
    response_types: Optional[List[str]] = Field(
        default=None,
        description="List of response types (default: code)"
    )
    token_endpoint_auth_method: Optional[str] = Field(
        default=None,
        description="Authentication method for token endpoint (default: none for public clients)"
    )
    application_type: Optional[str] = Field(
        default=None,
        description="Application type per RFC 7591 (web or native, default: web)"
    )
    scope: Optional[str] = Field(default=None, description="Space-separated scopes the client will request")
    software_id: Optional[str] = Field(default=None, description="Unique identifier for the client software")
    software_version: Optional[str] = Field(default=None, description="Version of the client software")

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "registration_endpoint": "https://oauth.example.com/register",
                    "redirect_uris": ["https://dev.elitea.ai/mcp/callback"],
                    "client_name": "Elitea MCP Client",
                    "grant_types": ["authorization_code", "refresh_token"],
                }
            ]
        }
    )

