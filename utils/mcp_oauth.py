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

"""Utility functions for MCP OAuth operations."""

from typing import Optional

import requests
from urllib.parse import parse_qs


def exchange_token(
    token_endpoint: str,
    code: str,
    redirect_uri: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    code_verifier: Optional[str] = None,
    scope: Optional[str] = None,
    timeout: int = 30,
):
    """
    Exchange an authorization code for OAuth tokens.
    
    Args:
        token_endpoint: OAuth token endpoint URL
        code: Authorization code
        redirect_uri: Redirect URI used in authorization request
        client_id: OAuth client ID (optional for DCR/public clients)
        client_secret: OAuth client secret (optional)
        code_verifier: PKCE code verifier (optional)
        scope: OAuth scope (optional)
        timeout: Request timeout in seconds
        
    Returns:
        Token data dictionary containing access_token and other OAuth response fields
        
    Raises:
        ValueError: If token exchange fails
    
    Note:
        client_id may be optional for:
        - Dynamic Client Registration (DCR): client_id may be in the code
        - OIDC public clients: some providers don't require it
        - Some MCP servers handle auth differently
    """
    token_body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    
    if client_id:
        token_body["client_id"] = client_id
    if client_secret:
        token_body["client_secret"] = client_secret
    if code_verifier:
        token_body["code_verifier"] = code_verifier
    if scope:
        token_body["scope"] = scope

    response = requests.post(
        token_endpoint,
        data=token_body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        timeout=timeout
    )
    
    try:
        token_data = response.json()
    except Exception:
        token_data = {k: v[0] if len(v) == 1 else v 
                     for k, v in parse_qs(response.text).items()}
    
    if response.ok:
        return token_data
    else:
        error_msg = token_data.get("error_description") or token_data.get("error") or response.text
        raise ValueError(f"Token exchange failed: {error_msg}")


def refresh_token(
    token_endpoint: str,
    refresh_token_value: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    scope: Optional[str] = None,
    timeout: int = 30,
):
    """
    Refresh an OAuth access token using a refresh token.
    
    Args:
        token_endpoint: OAuth token endpoint URL
        refresh_token_value: Refresh token to use
        client_id: OAuth client ID (optional for DCR/public clients)
        client_secret: OAuth client secret (optional)
        scope: OAuth scope (optional)
        timeout: Request timeout in seconds
        
    Returns:
        Token data dictionary containing access_token and other OAuth response fields
        
    Raises:
        ValueError: If token refresh fails
    
    Note:
        client_id may be optional for:
        - Dynamic Client Registration (DCR): client_id embedded in refresh_token
        - OIDC public clients: some providers don't require it
        - Some MCP servers handle auth differently
    """
    token_body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token_value,
    }
    
    if client_id:
        token_body["client_id"] = client_id
    if client_secret:
        token_body["client_secret"] = client_secret
    if scope:
        token_body["scope"] = scope

    response = requests.post(
        token_endpoint,
        data=token_body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        timeout=timeout
    )
    
    try:
        token_data = response.json()
    except Exception:
        token_data = {k: v[0] if len(v) == 1 else v 
                     for k, v in parse_qs(response.text).items()}
    
    if response.ok:
        return token_data
    else:
        error_msg = token_data.get("error_description") or token_data.get("error") or response.text
        raise ValueError(f"Token refresh failed: {error_msg}")


def register_dynamic_client(
    registration_endpoint: str,
    redirect_uris: list,
    client_name: Optional[str] = None,
    grant_types: Optional[list] = None,
    response_types: Optional[list] = None,
    token_endpoint_auth_method: Optional[str] = None,
    application_type: Optional[str] = None,
    scope: Optional[str] = None,
    software_id: Optional[str] = None,
    software_version: Optional[str] = None,
    timeout: int = 30,
):
    """
    Register a dynamic OAuth client with the authorization server.
    
    This implements RFC 7591 (OAuth 2.0 Dynamic Client Registration).
    Used when the MCP server requires DCR instead of pre-registered clients.
    
    Args:
        registration_endpoint: OAuth client registration endpoint URL
        redirect_uris: List of redirect URIs for the client
        client_name: Human-readable name for the client (optional)
        grant_types: List of grant types (default: authorization_code, refresh_token)
        response_types: List of response types (default: code)
        token_endpoint_auth_method: Auth method for token endpoint (default: none)
        application_type: Application type per RFC 7591 (default: web)
        scope: Space-separated scopes the client will request (optional)
        software_id: Unique identifier for the client software (optional)
        software_version: Version of the client software (optional)
        timeout: Request timeout in seconds
        
    Returns:
        Registration response dict containing client_id, client_secret (if issued), etc.
        
    Raises:
        ValueError: If registration fails
    """
    registration_body = {
        "redirect_uris": redirect_uris,
        "grant_types": grant_types or ["authorization_code", "refresh_token"],
        "response_types": response_types or ["code"],
        "token_endpoint_auth_method": token_endpoint_auth_method or "none",
        "application_type": application_type or "web",
    }
    
    if client_name:
        registration_body["client_name"] = client_name
    if scope:
        registration_body["scope"] = scope
    if software_id:
        registration_body["software_id"] = software_id
    if software_version:
        registration_body["software_version"] = software_version

    response = requests.post(
        registration_endpoint,
        json=registration_body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout=timeout
    )
    
    try:
        registration_data = response.json()
    except Exception:
        raise ValueError(f"DCR failed: invalid response format - {response.text[:500]}")
    
    if response.ok:
        return registration_data
    else:
        error_msg = registration_data.get("error_description") or registration_data.get("error") or response.text
        raise ValueError(f"Dynamic client registration failed: {error_msg}")
