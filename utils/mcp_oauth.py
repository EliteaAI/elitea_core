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

import re
from typing import Optional

import requests
from urllib.parse import parse_qs, urlsplit

from .entra_obo import AUTHORITY_HOST


# Toolkit settings keys that reference a credential carrying the OAuth client
# (client_id / client_secret / scopes) for the delegated flow. Order is lookup priority.
OAUTH_CONFIGURATION_KEYS = (
    'sharepoint_configuration',
    'openapi_configuration',
    'teams_configuration',
    'outlook_configuration',
    'ms_graph_configuration',
    'dataverse_configuration',  # its token endpoint is pinned: see token_endpoint_allowed
)


DATAVERSE_CONFIGURATION_KEY = 'dataverse_configuration'
# Credentials whose sign-in always runs at the Microsoft login host
_MICROSOFT_LOGIN_KEYS = ('teams_configuration', 'outlook_configuration', 'ms_graph_configuration')
# Same shape the SDK accepts for a tenant: a GUID or a domain name, never a path or a host
_TENANT_PATTERN = re.compile(r'[A-Za-z0-9][A-Za-z0-9.-]{0,99}')


def dataverse_token_endpoint(settings: dict) -> Optional[str]:
    """The only token endpoint a Dataverse toolkit may sign in at, built from its credential's tenant.

    None when the toolkit has no Dataverse credential. An empty string when it has one but the tenant is
    missing or malformed: no endpoint is then acceptable.
    """
    config = settings.get(DATAVERSE_CONFIGURATION_KEY)
    if not isinstance(config, dict) or not config:
        return None
    tenant = str(config.get('tenant_id') or '').strip()
    if not _TENANT_PATTERN.fullmatch(tenant):
        return ''
    return f'https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token'


def is_same_endpoint(expected: str, given: Optional[str]) -> bool:
    """Whether ``given`` is ``expected``, ignoring a trailing slash; an empty ``expected`` matches nothing."""
    return bool(expected) and expected.rstrip('/') == (given or '').strip().rstrip('/')


def is_microsoft_login_url(url: str | None) -> bool:
    """Whether ``url`` is an https address on the Microsoft login host, with no user info and port 443 at most.

    Only the host is pinned, so the tenant (a GUID or a domain name) and the path may be of any shape.
    Copy of the host check of ``entra_obo._post``: the two must agree.
    """
    if not isinstance(url, str) or any(char.isspace() or char == '\\' or ord(char) < 32 for char in url):
        return False
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return False
    return (parts.scheme == 'https' and parts.hostname == AUTHORITY_HOST
            and '@' not in parts.netloc and port in (None, 443))


def token_endpoint_allowed(settings: dict, token_endpoint: str | None) -> bool | None:
    """Whether the stored secret of every OAuth credential of the toolkit may go to ``token_endpoint``.

    None when the settings hold no credential of OAUTH_CONFIGURATION_KEYS: no rule applies. The caller's
    rights play no part here, so the answer is the same for a viewer, an editor and an admin.
    A key with no rule of its own (one added to OAUTH_CONFIGURATION_KEYS later) accepts only the endpoint
    saved in its credential, never a caller-chosen one.
    """
    verdicts = []
    for key in OAUTH_CONFIGURATION_KEYS:
        config = settings.get(key)
        if not isinstance(config, dict) or not config:
            continue
        if key == DATAVERSE_CONFIGURATION_KEY:
            verdicts.append(is_same_endpoint(dataverse_token_endpoint(settings), token_endpoint))
        elif key in _MICROSOFT_LOGIN_KEYS:
            verdicts.append(is_microsoft_login_url(token_endpoint))
        elif key == 'sharepoint_configuration':
            # Without a discovery endpoint or an app it is ACS app-only: no sign-in, so no endpoint
            delegated = bool(config.get('oauth_discovery_endpoint') or config.get('oauth_app_id'))
            verdicts.append(delegated and is_microsoft_login_url(token_endpoint))
        else:
            saved = config.get('token_url') or config.get('token_endpoint')
            verdicts.append(is_same_endpoint(saved if isinstance(saved, str) else '', token_endpoint))
    return all(verdicts) if verdicts else None


class TokenRefreshError(ValueError):
    """A refresh refused by the token endpoint; `error` is the OAuth error code, when it sent one."""

    def __init__(self, message: str, error: Optional[str] = None, status: Optional[int] = None):
        super().__init__(message)
        self.error, self.status = error, status


def get_oauth_configurations(settings: dict) -> list:
    """Return the OAuth credential configurations referenced by toolkit settings (same dict objects)."""
    configs = []
    for key in OAUTH_CONFIGURATION_KEYS:
        config = settings.get(key)
        if isinstance(config, dict) and config:
            configs.append(config)
    return configs


def pick_oauth_setting(sources: list, *keys: str):
    """Return the first truthy value of any of ``keys`` across ``sources``, in order."""
    for source in sources:
        for key in keys:
            value = source.get(key)
            if value:
                return value
    return None


def exchange_token(
    token_endpoint: str,
    code: str,
    redirect_uri: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    code_verifier: Optional[str] = None,
    scope: Optional[str] = None,
    resource: Optional[str] = None,
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
        resource: RFC 8707 resource indicator (optional)
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
    if resource:
        token_body["resource"] = resource

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
    resource: Optional[str] = None,
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
        resource: RFC 8707 resource indicator (optional)
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
    if resource:
        token_body["resource"] = resource

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
        error = token_data.get("error") if isinstance(token_data.get("error"), str) else None
        raise TokenRefreshError(f"Token refresh failed: {error_msg}", error, response.status_code)


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
