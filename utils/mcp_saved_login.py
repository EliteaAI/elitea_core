"""Server-side save of an MCP OAuth login (#7643): the refresh token goes to the vault, never to the browser."""
import base64
import json
import re
from typing import Optional
from urllib.parse import urlparse

from pylon.core.tools import log

from .stored_oauth_tokens import CACHE_PREFIX, canonical_resource

MCP_LOGIN_TYPE = "mcp_login"
NO_REFRESH_TOKEN_TEXT = ("This MCP server does not support saved logins (no refresh token). "
                         "Choose \"Browser only\" for this toolkit; it then works in chats only.")


class SavedLoginRefused(Exception):
    def __init__(self, code: str, message: str, status: int):
        super().__init__(message)
        self.code, self.status = code, status


def _rpc(timeout: int = 10):
    from tools import rpc_tools  # pylint: disable=C0415
    return rpc_tools.RpcMixin().rpc.timeout(timeout)


def load_login_credential(project_id: int, configuration_uuid: Optional[str], user_id) -> dict:
    """The mcp_login credential of its own project: team = project admins, private = only its owner (personal project)."""
    if not configuration_uuid:
        raise SavedLoginRefused("invalid_request", "configuration_uuid is required", 400)
    rpc = _rpc()
    if not rpc.configurations_can_manage_stored_login(project_id=project_id, user_id=user_id):
        raise SavedLoginRefused("forbidden", "Only project admins can log in or out on a team MCP login, "
                                             "and only its owner on a private one", 403)
    found = rpc.configurations_get_by_uuid_in_project(project_id=project_id, configuration_uuid=configuration_uuid,
                                                      expected_type=MCP_LOGIN_TYPE)
    if not found:
        raise SavedLoginRefused("not_found", "MCP login credential not found in this project", 404)
    return found


def _get_json(url: str, timeout: int = 10) -> Optional[dict]:
    import requests  # pylint: disable=C0415
    try:
        response = requests.get(url, timeout=timeout, headers={"Accept": "application/json"})
        return response.json() if response.ok else None
    except Exception:  # pylint: disable=W0703
        return None


def _resource_metadata_urls(server_url: str) -> list:
    import requests  # pylint: disable=C0415
    urls = []
    try:
        challenge = requests.post(server_url, json={"jsonrpc": "2.0", "id": 0, "method": "ping"}, timeout=10,
                                  headers={"Accept": "application/json, text/event-stream"}).headers.get(
            "WWW-Authenticate", "")
        match = re.search(r'resource_metadata\s*=\s*"?([^", ]+)"?', challenge)
        if match:
            urls.append(match.group(1))
    except Exception:  # pylint: disable=W0703
        pass
    parsed = urlparse(server_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.rstrip("/")
    urls += [f"{origin}/.well-known/oauth-protected-resource{path}", f"{origin}/.well-known/oauth-protected-resource"]
    return urls


def discovered_token_endpoints(server_url: str) -> set:
    """Token endpoints the MCP server itself advertises (RFC 9728 -> RFC 8414 / OIDC discovery)."""
    issuers = []
    for url in _resource_metadata_urls(server_url):
        metadata = _get_json(url)
        if metadata and metadata.get("authorization_servers"):
            issuers = metadata["authorization_servers"]
            break
    if not issuers:  # Servers without RFC 9728 act as their own authorization server
        parsed = urlparse(server_url)
        issuers = [f"{parsed.scheme}://{parsed.netloc}"]
    endpoints = set()
    for issuer in issuers[:3]:
        issuer = issuer.rstrip("/")
        parsed = urlparse(issuer)
        origin, path = f"{parsed.scheme}://{parsed.netloc}", parsed.path
        for url in (f"{origin}/.well-known/oauth-authorization-server{path}",
                    f"{origin}/.well-known/openid-configuration{path}", f"{issuer}/.well-known/openid-configuration"):
            metadata = _get_json(url)
            if metadata and metadata.get("token_endpoint"):
                endpoints.add(metadata["token_endpoint"])
                break
    return endpoints


def check_login_target(server_url: str, token_endpoint: Optional[str], resource: Optional[str]) -> None:
    """The secret and the saved login go only to the authorization server of the credential's own MCP server."""
    if not server_url:
        raise SavedLoginRefused("invalid_request", "The MCP login credential has no server URL", 400)
    if not resource or canonical_resource(resource) != canonical_resource(server_url):
        raise SavedLoginRefused("resource_mismatch", "This login is for a different MCP server than the credential", 400)
    if not token_endpoint or token_endpoint not in discovered_token_endpoints(server_url):
        raise SavedLoginRefused("access_denied",
                                "token_endpoint is not the one this MCP server advertises", 403)


def _account_from(token_data: dict) -> Optional[str]:
    # Display only: id_token claims are read without verification and never trusted for access
    id_token = token_data.get("id_token")
    if not isinstance(id_token, str) or id_token.count(".") < 2:
        return None
    try:
        part = id_token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
    except (ValueError, TypeError):
        return None
    for key in ("preferred_username", "email", "upn", "name", "sub"):
        if isinstance(claims.get(key), str) and claims[key]:
            return claims[key][:200]
    return None


def save_exchanged_login(project_id: int, credential: dict, token_data: dict, request_data, client_id: Optional[str],
                         client_secret: Optional[str], scope: Optional[str]) -> dict:
    """Persist the exchanged login; returns only what the browser may keep (no refresh/id token)."""
    refresh = token_data.get("refresh_token")
    if not refresh:
        raise SavedLoginRefused("no_refresh_token", NO_REFRESH_TOKEN_TEXT, 422)
    account = _account_from(token_data)
    fields = {
        "refresh_token": refresh,
        "token_endpoint": request_data.token_endpoint,
        "resource": request_data.resource or (credential.get("data") or {}).get("url"),
        "used_dcr": bool(request_data.used_dcr),
        "account": account,
        "client_id": client_id,
        "client_secret": client_secret or None,
    }
    if scope:
        fields["scopes"] = scope
    saved = _rpc().configurations_save_stored_login(
        project_id=project_id, configuration_uuid=credential["uuid"], expected_type=MCP_LOGIN_TYPE, fields=fields)
    if not saved:
        raise SavedLoginRefused("conflict", "The credential changed meanwhile; try the login again", 409)
    forget_cached_tokens(credential["uuid"])
    log.info("MCP login saved on credential %s", credential["uuid"])
    return {"access_token": token_data.get("access_token"), "token_type": token_data.get("token_type"),
            "expires_in": token_data.get("expires_in"), "account": account, "saved": True}


def forget_cached_tokens(configuration_uuid: str) -> None:
    from tools import this  # pylint: disable=C0415
    try:
        redis = this.module.get_redis_client()
        for key in redis.scan_iter(match=f"{CACHE_PREFIX}{configuration_uuid}:*", count=100):
            redis.delete(key)
    except Exception as e:  # pylint: disable=W0703
        log.warning("Cached tokens of credential %s not cleared: %s", configuration_uuid, type(e).__name__)
