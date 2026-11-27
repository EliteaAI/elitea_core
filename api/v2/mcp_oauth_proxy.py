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
from tools import api_tools, auth, config as c, db, VaultClient, register_openapi

from pylon.core.tools import log

from ...models.elitea_tools import EliteATool
from ...models.pd.mcp_oauth import VAULT_SECRET_PATTERN, McpOAuthTokenRequest
from ...utils.mcp_config import is_mcp_exposure_enabled
from ...utils.mcp_oauth import (
    credential_token_endpoint_allowed,
    dataverse_token_endpoint,
    exchange_token,
    get_oauth_configurations,
    pick_oauth_setting,
    refresh_token,
    token_endpoint_allowed,
)
from ....configurations.utils import expand_configuration

TOKEN_ENDPOINT_REFUSED = "token_endpoint is not the sign-in address of this credential"
VAULT_REFERENCE_REFUSED = "a vault reference needs a saved credential or toolkit"
# The fields that name a shared credential: the same set a toolkit may hold for it
# (utils/application_tools.py `_expand_toolkit_settings`)
CREDENTIAL_REFERENCE_KEYS = frozenset({'elitea_title', 'private', 'configuration_uuid', 'configuration_project_id'})


def _holds_reference(value, reference: str) -> bool:
    """Whether the saved settings hold exactly ``reference`` as a value, at any depth."""
    if isinstance(value, str):
        return value == reference
    if isinstance(value, dict):
        return any(_holds_reference(item, reference) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_holds_reference(item, reference) for item in value)
    return False


def _resolve_oauth_app_client(oauth_app_id: str, project_id: int, credential_type: str):
    """Client ID, secret, token endpoint and sign-in scope of an admin-managed app.

    Both ids come from the browser: the app must serve this credential type in this project. The
    secret only ever goes to the app's own token endpoint, so the caller's endpoint is ignored.
    """
    from ....configurations.oauth_apps import resolve_app_client  # pylint: disable=C0415
    app = resolve_app_client(oauth_app_id, project_id, credential_type)
    scopes = [s for s in app.get('scopes') or [] if s != 'offline_access']
    # offline_access (a refresh token) only while the app's Auto refresh token is on (#7415)
    if app.get('auto_refresh_token', True):
        scopes = ['offline_access', *scopes]
    return app['client_id'], app['client_secret'], app['token_endpoint'], ' '.join(scopes)


def _may_send_stored_secret(project_id: int, token_endpoint: str, credential_sources: list,
                            dataverse_own_endpoint: bool = True) -> bool:
    """token_endpoint is caller-supplied: only send a stored secret to the endpoint the toolkit
    declares, or for callers who can edit toolkits anyway.

    dataverse_own_endpoint: a Dataverse credential saves no endpoint, so its tenant endpoint counts as
    the declared one. A schedule owner may lack the edit right but must still sign in, and the stored
    secret goes only to the tenant of its own credential. A vault reference sent by the caller is not
    that stored secret: it keeps the edit-right rule, so pass False there.
    """
    stored_endpoint = pick_oauth_setting(credential_sources, 'token_endpoint', 'token_url')
    if not stored_endpoint and dataverse_own_endpoint and credential_sources:
        # the first source is the toolkit settings
        stored_endpoint = dataverse_token_endpoint(credential_sources[0])
    if stored_endpoint and stored_endpoint.rstrip('/') == (token_endpoint or '').rstrip('/'):
        return True
    user_permissions = auth.resolve_permissions(mode=c.DEFAULT_MODE, project_id=project_id)
    return "models.applications.tool.patch" in user_permissions


class ProjectAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="MCP OAuth Token Proxy",
        description="Proxy OAuth token exchange (authorization_code or refresh_token grant) to avoid CORS issues. Optionally fetches credentials from a stored toolkit.",
        tags=["elitea_core/mcp"],
        parameters=[
            {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Project ID."},
        ],
        request_body=McpOAuthTokenRequest,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.mcp_auth.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
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
        # Default app sign-in (Teams/Outlook/SharePoint credentials) is not MCP: it works whatever
        # the MCP exposure setting; the app is fully checked below
        if not is_mcp_exposure_enabled() and not data.oauth_app_id:
            return {"error": "MCP exposure is disabled on this deployment"}, 403
        client_id = data.client_id
        client_secret = data.client_secret
        scope = data.scope
        saved_login = None
        if data.save_login:
            from ...utils.mcp_saved_login import (  # pylint: disable=C0415
                SavedLoginRefused, check_login_target, load_login_credential,
            )
            if (data.grant_type or 'authorization_code') != 'authorization_code':
                return {"error": "invalid_request", "error_description": "save_login needs an authorization code"}, 400
            try:
                saved_login = load_login_credential(project_id, data.configuration_uuid, auth.current_user().get('id'))
                # Checked before any secret is loaded or sent: the browser chooses token_endpoint
                login_url = (saved_login.get('data') or {}).get('url')
                check_login_target(login_url, data.token_endpoint, data.resource or login_url)
            except SavedLoginRefused as e:
                return {"error": e.code, "error_description": str(e)}, e.status
            # A non-DCR client configured on the credential is used as is; the browser cannot swap it
            login_data = saved_login.get('data') or {}
            # A client registered by DCR for an earlier login is not a configured one; only typed-in clients pin
            if not data.used_dcr and login_data.get('client_id') and not login_data.get('used_dcr'):
                client_id, client_secret = login_data['client_id'], login_data.get('client_secret')
            scope = scope or login_data.get('scopes')
            # The client comes only from the credential (or DCR): no toolkit or vault-reference secrets here
            data.toolkit_id, data.oauth_app_id = None, None
            if client_secret and client_secret != login_data.get('client_secret') and not data.used_dcr:
                client_secret = None


        # Helper function to detect masked secrets (e.g., "*****", "****6afP")
        def is_masked_secret(value):
            """Detect if a value is a masked secret pattern."""
            import re
            return isinstance(value, str) and bool(re.match(r'^\*+', value))

        # Treat masked secrets as None so we fetch from database
        if is_masked_secret(client_secret):
            log.debug(f"MCP OAuth proxy: detected masked client_secret, will fetch from database")
            client_secret = None

        vault_client = VaultClient(project_id)
        credential_sources = []
        endpoint_from_app = False

        # Default app mode: the browser knows only the app reference, never its secret
        if data.oauth_app_id and not data.used_dcr:
            try:
                # The platform's client is used only for the admin's scopes, never the caller's
                client_id, client_secret, app_token_endpoint, scope = _resolve_oauth_app_client(
                    data.oauth_app_id, project_id, data.credential_type)
                data.token_endpoint = app_token_endpoint
                endpoint_from_app = True
            except LookupError as e:  # OAuthAppUnavailable
                return {"error": "app_unavailable", "error_description": str(e)}, 400

        # A sign-in that names a saved credential is served from that credential alone, a saved toolkit next to
        # it included: the form may have picked another credential than the one the toolkit row still holds
        use_credential = bool(data.configuration_uuid) and saved_login is None and not endpoint_from_app
        toolkit_holds_reference = False

        # If toolkit_id is provided, fetch credentials from database
        if data.toolkit_id and not use_credential:
            try:
                with db.with_project_schema_session(project_id) as session:
                    toolkit = session.query(EliteATool).filter(
                        EliteATool.id == data.toolkit_id
                    ).first()
                    
                    if not toolkit:
                        return {"error": "Toolkit not found", "details": f"No toolkit with id {data.toolkit_id}"}, 404
                    
                    settings = toolkit.settings or {}
                    server_name = settings.get('server_name', '')
                    # Before the vault is read: a reference the caller sends counts only when the row holds it
                    toolkit_holds_reference = bool(
                        data.client_secret and _holds_reference(settings, data.client_secret))

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
                    # Delegated-OAuth toolkits (SharePoint, OpenAPI, Teams, Outlook) keep the OAuth client
                    # in a referenced credential, e.g. {"elitea_title": ..., "private": ...}; expand it in place
                    oauth_configs = get_oauth_configurations(settings)
                    for oauth_config in oauth_configs:
                        config_title = oauth_config.get('elitea_title')
                        # A reference by uuid alone is a reference too (the form saves it without a title)
                        if config_title or oauth_config.get('configuration_uuid'):
                            log.debug(f"MCP OAuth proxy: expanding configuration: {config_title or oauth_config.get('configuration_uuid')}")
                            # The expansion overlays the shared credential on this dict, so a key it does not
                            # hold would stay: an editor could plant the endpoint the secret is sent to, or
                            # a key that decides the sign-in kind. Only the reference stays, an inline
                            # client_id included: a toolkit type with no credential field in its schema is
                            # saved without the reference-only check.
                            for key in [key for key in oauth_config if key not in CREDENTIAL_REFERENCE_KEYS]:
                                del oauth_config[key]
                            try:
                                # Get user_id from auth context for private configuration lookup
                                user_id = auth.current_user().get('id') if oauth_config.get('private') else None
                                # expand_configuration modifies oauth_config in place and unsecretes the data
                                expand_configuration(oauth_config, current_project_id=project_id, user_id=user_id, unsecret=True)
                                log.debug(f"MCP OAuth proxy: expanded configuration data, keys: {list(oauth_config.keys())}")
                            except Exception as e:
                                log.error(f"MCP OAuth proxy: failed to expand configuration '{config_title}' - {e}")

                    credential_sources = [settings, *oauth_configs]

                    # A stored credential can be shared with every toolkit editor, so its secret never leaves
                    # for a caller-chosen host: only the sign-in address of its own identity provider is
                    # served, whatever the caller's rights.
                    # An endpoint taken from the admin app is the app's own: the admin may run a sovereign
                    # cloud or a custom authority, and the app secret goes only there.
                    if not endpoint_from_app and token_endpoint_allowed(settings, data.token_endpoint) is False:
                        log.warning(f"MCP OAuth proxy: token_endpoint refused for toolkit {data.toolkit_id}")
                        return {"error": "access_denied",
                                "error_description": TOKEN_ENDPOINT_REFUSED}, 403

                    # FE always sends the DCR client_id in the same request, so the guard below
                    # is not hit in practice, but is included for symmetry with the secret guard
                    # to prevent a stale DB client_id from overwriting a DCR-registered one.
                    if not client_id and not data.used_dcr:
                        client_id = pick_oauth_setting(credential_sources, 'client_id')
                    # When the client_id was obtained via DCR, the registered client is public
                    # (token_endpoint_auth_method=none). Never load a client_secret from DB in
                    # that case — sending a secret for a public client causes Aha (and others)
                    # to reject the token request with "unknown client".
                    if not client_secret and not data.used_dcr:
                        client_secret = pick_oauth_setting(credential_sources, 'client_secret')
                        if client_secret and not _may_send_stored_secret(project_id, data.token_endpoint, credential_sources):
                            log.warning(f"MCP OAuth proxy: stored client_secret withheld for toolkit {data.toolkit_id}, "
                                        f"token_endpoint does not match the toolkit and caller cannot edit toolkits")
                            client_secret = None
                        log.debug(f"MCP OAuth proxy: extracted client_secret from DB: {bool(client_secret)}")
                    if not scope:
                        # Both spellings are in use: admin MCP server definitions declare `scope`,
                        # toolkit settings and configurations use `scopes`.
                        for source in credential_sources:
                            scope = source.get('scopes') or source.get('scope')
                            if scope:
                                break
                        if isinstance(scope, list):
                            scope = ' '.join(scope)

                    log.debug(f"MCP OAuth proxy: using credentials from toolkit {data.toolkit_id} (type: {data.toolkit_type})")
            except Exception as e:
                log.error(f"MCP OAuth proxy: error fetching toolkit - {e}")
                return {"error": "Database error", "details": str(e)}, 500

        # The server loads the credential itself, so a vault reference sent next to it is never resolved and the
        # browser's endpoint is checked against the type of the loaded credential
        if use_credential:
            if client_secret and VAULT_SECRET_PATTERN.match(client_secret):
                client_secret = None
            credential = {'configuration_uuid': data.configuration_uuid}
            if data.configuration_project_id:
                # A credential of another project is visible only as the caller's own personal one or a shared
                # one; `private` makes the expansion count the personal project
                credential.update(configuration_project_id=data.configuration_project_id, private=True)
            try:
                expand_configuration(credential, current_project_id=project_id,
                                     user_id=auth.current_user().get('id'), unsecret=True)
            except Exception as e:  # noqa: BLE001  pylint: disable=W0703
                # Any failure, a missing project or schema included, gives the same fixed 404, so the status
                # does not tell which project ids exist; only the type of the error is logged
                log.warning(f"MCP OAuth proxy: credential lookup failed ({type(e).__name__})")
                return {"error": "not_found", "error_description": "Credential not found"}, 404
            credential_type = credential.get('configuration_type')
            if data.credential_type is not None and data.credential_type != credential_type:
                return {"error": "invalid_request",
                        "error_description": "credential_type is not the type of this credential"}, 400
            # Before any secret is read: the caller chooses token_endpoint, the credential decides where its secret goes
            if not credential_token_endpoint_allowed(credential_type, credential, data.token_endpoint):
                log.warning(f"MCP OAuth proxy: token_endpoint refused for a {credential_type} credential")
                return {"error": "access_denied", "error_description": TOKEN_ENDPOINT_REFUSED}, 403
            if not client_id and not data.used_dcr:
                client_id = credential.get('client_id')
            if not client_secret and not data.used_dcr:
                client_secret = credential.get('client_secret')
                # the wrapper first: a Dataverse credential saves no endpoint, its tenant endpoint counts as the declared one
                sources = [{f'{credential_type}_configuration': credential}, credential]
                if client_secret and not _may_send_stored_secret(project_id, data.token_endpoint, sources):
                    log.warning(f"MCP OAuth proxy: stored client_secret withheld for a {credential_type} credential, "
                                f"token_endpoint is not its saved one and caller cannot edit toolkits")
                    client_secret = None
            if not scope:
                scope = credential.get('scopes') or credential.get('scope')
                if isinstance(scope, list):
                    scope = ' '.join(scope)

        # A vault reference needs a saved credential (served above, the reference dropped) or a saved toolkit that
        # holds that exact reference: any other names a secret the caller never proved to own
        if (client_secret and client_secret == data.client_secret and VAULT_SECRET_PATTERN.match(client_secret)
                and not data.oauth_app_id and not toolkit_holds_reference):
            return {"error": "access_denied", "error_description": VAULT_REFERENCE_REFUSED}, 403

        # Unsecret vault references sent in the request (e.g., {{secret.my_secret}})
        if client_secret and data.client_secret and client_secret == data.client_secret and not data.oauth_app_id:
            resolved_secret = vault_client.unsecret(client_secret)
            if resolved_secret != client_secret and not _may_send_stored_secret(
                    project_id, data.token_endpoint, credential_sources, dataverse_own_endpoint=False):
                return {"error": "access_denied",
                        "error_description": "Vault secret references require toolkit edit permission"}, 403
            client_secret = resolved_secret

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
                    resource=data.resource,
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
                resource=data.resource,
            )
            log.debug(f"MCP OAuth proxy: token exchange successful")
            log.debug(f"MCP OAuth proxy exchange response: has_access_token={bool(token_data.get('access_token'))}, "
                     f"has_refresh_token={bool(token_data.get('refresh_token'))}, "
                     f"expires_in={token_data.get('expires_in')}, "
                     f"token_type={token_data.get('token_type')}, "
                     f"scope={token_data.get('scope')}")
            if saved_login is not None:
                from ...utils.mcp_saved_login import SavedLoginRefused, save_exchanged_login  # pylint: disable=C0415
                try:
                    return save_exchanged_login(project_id, saved_login, token_data, data,
                                                client_id=client_id, client_secret=client_secret, scope=scope), 200
                except SavedLoginRefused as e:
                    return {"error": e.code, "error_description": str(e)}, e.status
            return token_data, 200

        except ValueError as e:
            log.error(f"MCP OAuth proxy: token exchange failed - {e}")
            return {"error": "token_exchange_failed", "error_description": str(e)}, 400
        except Exception as e:
            log.error(f"MCP OAuth proxy: unexpected error - {e}")
            return {"error": "internal_error", "error_description": str(e)}, 500


    @register_openapi(
        name="Log out MCP saved login",
        description="Remove the saved MCP login of an mcp_login credential and its cached access tokens.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "configuration_uuid", "in": "query", "schema": {"type": "string"}},
        ],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.mcp_auth.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, **kwargs):
        from ...utils.mcp_saved_login import (  # pylint: disable=C0415
            MCP_LOGIN_TYPE, SavedLoginRefused, forget_cached_tokens, load_login_credential,
        )
        configuration_uuid = request.args.get('configuration_uuid')
        try:
            load_login_credential(project_id, configuration_uuid, auth.current_user().get('id'))
        except SavedLoginRefused as e:
            return {"error": e.code, "error_description": str(e)}, e.status
        self.module.context.rpc_manager.timeout(10).configurations_clear_stored_login(
            project_id=project_id, configuration_uuid=configuration_uuid, expected_type=MCP_LOGIN_TYPE)
        forget_cached_tokens(configuration_uuid)
        return {"ok": True}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        c.DEFAULT_MODE: ProjectAPI,
    }
