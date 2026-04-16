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

""" API for MCP OAuth Token Exchange Proxy """
from flask import request
from tools import api_tools, auth, config as c, db, VaultClient

from pylon.core.tools import log

from ...models.elitea_tools import EliteATool
from ...models.pd.mcp_oauth import McpOAuthTokenRequest
from ...utils.mcp_oauth import exchange_token, refresh_token
from ....configurations.utils import expand_configuration


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
        Proxy OAuth token exchange request to avoid CORS issues.
        
        Uses elitea_sdk's exchange_oauth_token function to perform the token exchange.
        If toolkit_id is provided, credentials are fetched from the database.
        """
        try:
            data = McpOAuthTokenRequest.model_validate(request.json)
        except Exception as e:
            log.error(f"MCP OAuth proxy validation error: {e}")
            return {"error": "Invalid request", "details": str(e)}, 400
        client_id = data.client_id
        client_secret = data.client_secret
        scope = data.scope

        log.debug(f"MCP OAuth proxy request: toolkit_id={data.toolkit_id}, has_client_secret={bool(client_secret)}, client_secret_preview={client_secret[:10] if client_secret else None}")

        # Helper function to detect masked secrets (e.g., "*****", "****6afP")
        def is_masked_secret(value):
            """Detect if a value is a masked secret pattern."""
            import re
            return isinstance(value, str) and bool(re.match(r'^\*+', value))

        # Treat masked secrets as None so we fetch from database
        if is_masked_secret(client_secret):
            log.debug(f"MCP OAuth proxy: detected masked client_secret, will fetch from database")
            client_secret = None

        # Unsecret any vault references in the request data (e.g., {{secret.my_secret}})
        vault_client = VaultClient(project_id)
        if client_secret:
            client_secret = vault_client.unsecret(client_secret)

        # If toolkit_id is provided, fetch credentials from database
        if data.toolkit_id:
            try:
                with db.with_project_schema_session(project_id) as session:
                    toolkit = session.query(EliteATool).filter(
                        EliteATool.id == data.toolkit_id
                    ).first()
                    
                    if not toolkit:
                        return {"error": "Toolkit not found", "details": f"No toolkit with id {data.toolkit_id}"}, 404
                    
                    settings = toolkit.settings or {}
                    server_name = settings.get('server_name', '')

                    # Unsecret the settings to resolve any vault secret references (e.g., {{secret.my_secret}})
                    try:
                        settings = vault_client.unsecret(settings)
                        log.debug(f"MCP OAuth proxy: unsecreted toolkit settings, keys: {list(settings.keys())}")
                    except Exception as e:
                        log.warning(f"MCP OAuth proxy: failed to unsecret toolkit settings - {e}")

                    # this flow is used to exchange OAuth token
                    # For pre-built MCP toolkits (type starts with 'mcp_'), resolve settings from pylon config
                    # This merges pylon config defaults with toolkit settings (toolkit settings take priority)
                    if server_name:
                        log.debug(f"MCP OAuth proxy: pre-built mcp settings resolution for server '{server_name}'")
                        settings['toolkit_type'] = data.toolkit_type or server_name  # Add toolkit_type to settings for resolution
                        settings = self.module.resolve_mcp_prebuilt_settings(settings)

                    # Use DB/resolved credentials if not provided in request (or if request value was just a vault reference)
                    # For SharePoint toolkit, sharepoint_configuration may be a reference to a configuration
                    sp_config = settings.get('sharepoint_configuration', {})
                    log.debug(f"MCP OAuth proxy: sp_config keys: {list(sp_config.keys()) if sp_config else 'None'}")

                    # If sharepoint_configuration only has reference fields (elitea_title, private), expand it
                    config_title = sp_config.get('elitea_title')
                    if config_title and not sp_config.get('client_id'):
                        log.debug(f"MCP OAuth proxy: expanding configuration by title: {config_title}")
                        try:
                            # Get user_id from auth context for private configuration lookup
                            user_id = auth.current_user().get('id') if sp_config.get('private') else None
                            # expand_configuration modifies sp_config in place and unsecretes the data
                            expand_configuration(sp_config, current_project_id=project_id, user_id=user_id, unsecret=True)
                            log.debug(f"MCP OAuth proxy: expanded configuration data, keys: {list(sp_config.keys())}")
                        except Exception as e:
                            log.error(f"MCP OAuth proxy: failed to expand configuration '{config_title}' - {e}")

                    if not client_id:
                        client_id = settings.get('client_id') or sp_config.get('client_id')
                    if not client_secret:
                        client_secret = settings.get('client_secret') or sp_config.get('client_secret')
                        log.debug(f"MCP OAuth proxy: extracted client_secret from DB: {bool(client_secret)}, preview: {client_secret[:8] if client_secret else 'None'}")
                    if not scope:
                        scope = settings.get('scopes') or sp_config.get('scopes')
                        if isinstance(scope, list):
                            scope = ' '.join(scope)

                    log.debug(f"MCP OAuth proxy: using credentials from toolkit {data.toolkit_id} (type: {data.toolkit_type})")
            except Exception as e:
                log.error(f"MCP OAuth proxy: error fetching toolkit - {e}")
                return {"error": "Database error", "details": str(e)}, 500

        # Note: client_id may be optional for some OAuth flows:
        # - Dynamic Client Registration (DCR): client_id obtained during registration
        # - OIDC public clients: may not require client_id in token request
        # - Some MCP servers handle auth differently
        # We only require client_id if the OAuth provider requires it (will fail at provider level)

        grant_type = data.grant_type or 'authorization_code'

        # Handle refresh_token grant
        if grant_type == 'refresh_token':
            if not data.refresh_token:
                return {"error": "Missing refresh_token", "details": "refresh_token is required for refresh_token grant"}, 400
            
            try:
                log.debug(f"MCP OAuth proxy: refreshing token at {data.token_endpoint}")
                log.debug(f"MCP OAuth proxy refresh request: client_id={client_id}, has_secret={bool(client_secret)}, scope={scope}")
                token_data = refresh_token(
                    token_endpoint=data.token_endpoint,
                    refresh_token_value=data.refresh_token,
                    client_id=client_id,
                    client_secret=client_secret,
                    scope=scope,
                )
                log.debug(f"MCP OAuth proxy: token refresh successful")
                log.debug(f"MCP OAuth proxy refresh response: has_access_token={bool(token_data.get('access_token'))}, "
                         f"has_refresh_token={bool(token_data.get('refresh_token'))}, "
                         f"expires_in={token_data.get('expires_in')}, "
                         f"token_type={token_data.get('token_type')}, "
                         f"scope={token_data.get('scope')}")
                return token_data, 200
            except ValueError as e:
                log.error(f"MCP OAuth proxy: token refresh failed - {e}")
                return {"error": "token_refresh_failed", "error_description": str(e)}, 400
            except Exception as e:
                log.error(f"MCP OAuth proxy: unexpected error during refresh - {e}")
                return {"error": "internal_error", "error_description": str(e)}, 500

        # Handle authorization_code grant (default)
        if not data.code:
            return {"error": "Missing code", "details": "code is required for authorization_code grant"}, 400
        if not data.redirect_uri:
            return {"error": "Missing redirect_uri", "details": "redirect_uri is required for authorization_code grant"}, 400

        try:
            log.debug(f"MCP OAuth proxy: exchanging code at {data.token_endpoint}")
            log.debug(f"MCP OAuth proxy exchange request: client_id={client_id}, has_secret={bool(client_secret)}, "
                     f"redirect_uri={data.redirect_uri}, has_code_verifier={bool(data.code_verifier)}, scope={scope}")
            token_data = exchange_token(
                token_endpoint=data.token_endpoint,
                code=data.code,
                redirect_uri=data.redirect_uri,
                client_id=client_id,
                client_secret=client_secret,
                code_verifier=data.code_verifier,
                scope=scope,
            )
            log.debug(f"MCP OAuth proxy: token exchange successful")
            log.debug(f"MCP OAuth proxy exchange response: has_access_token={bool(token_data.get('access_token'))}, "
                     f"has_refresh_token={bool(token_data.get('refresh_token'))}, "
                     f"expires_in={token_data.get('expires_in')}, "
                     f"token_type={token_data.get('token_type')}, "
                     f"scope={token_data.get('scope')}")
            return token_data, 200

        except ValueError as e:
            log.error(f"MCP OAuth proxy: token exchange failed - {e}")
            return {"error": "token_exchange_failed", "error_description": str(e)}, 400
        except Exception as e:
            log.error(f"MCP OAuth proxy: unexpected error - {e}")
            return {"error": "internal_error", "error_description": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        c.DEFAULT_MODE: ProjectAPI,
    }
