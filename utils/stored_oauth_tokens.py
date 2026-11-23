"""Access tokens for credentials holding a stored OAuth login, so runs without browser tokens can use them."""

import hashlib
import time
from typing import Callable, Optional
from urllib.parse import urlparse

from pylon.core.tools import log

ENTRA_AUTHORITY = "https://login.microsoftonline.com/"
GRAPH_DEFAULT_SCOPE = "https://graph.microsoft.com/.default"
CACHE_PREFIX = "oauth:stored:at:"
LOCK_PREFIX = "oauth:stored:lock:"
LOCK_SECONDS = 30
LOCK_WAIT_SECONDS = 5
LOCK_POLL_SECONDS = 0.25
EXPIRY_MARGIN_SECONDS = 120
GRAPH_CONFIGURATION_KEYS = ("sharepoint_configuration", "outlook_configuration", "teams_configuration",
                            "ms_graph_configuration")
# The login can never be refreshed again (revoked, expired, or its app is gone); anything else is transient
EXPIRED_ERRORS = frozenset({"invalid_grant", "invalid_client", "unauthorized_client"})


class StoredLoginExpired(Exception):
    """The saved login of a credential was refused for good: the owner must sign in again."""

    def __init__(self, configuration_uuid: str, error: str):
        super().__init__(f"stored login of {configuration_uuid} expired: {error}")
        self.configuration_uuid, self.error = configuration_uuid, error


def _graph_login(config: dict) -> Optional[dict]:
    authority = (config.get("oauth_discovery_endpoint") or "").rstrip("/")
    authority = authority.removesuffix("/v2.0")  # the token path already carries v2.0
    if not authority.startswith(ENTRA_AUTHORITY):
        return None
    scopes = config.get("scopes") or [GRAPH_DEFAULT_SCOPE]
    if isinstance(scopes, str):
        scopes = scopes.split()
    return {"authority": authority, "token_key": config.get("oauth_discovery_endpoint"),
            "token_endpoint": f"{authority}/oauth2/v2.0/token",
            "scope": " ".join(["offline_access", *[s for s in scopes if s != "offline_access"]])}


def _dataverse_login(config: dict) -> Optional[dict]:
    tenant = (config.get("tenant_id") or "").strip()
    host = (urlparse((config.get("url") or "").strip()).hostname or "").lower()
    if not tenant or not host.endswith(".dynamics.com"):
        return None
    authority = f"{ENTRA_AUTHORITY}{tenant}"
    return {"authority": authority, "token_key": authority, "token_endpoint": f"{authority}/oauth2/v2.0/token",
            "scope": f"https://{host}/user_impersonation offline_access"}


def canonical_resource(server_url: str) -> str:
    """Same key the SDK looks MCP tokens up by (lowercase scheme/host, no trailing slash)."""
    parsed = urlparse((server_url or "").strip())
    resource = parsed._replace(scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower()).geturl()
    return resource[:-1] if resource.endswith("/") else resource


def _mcp_login(config: dict) -> Optional[dict]:
    url, token_endpoint = (config.get("url") or "").strip(), (config.get("token_endpoint") or "").strip()
    if not url.startswith(("https://", "http://")) or not token_endpoint.startswith(("https://", "http://")):
        return None
    scopes = config.get("scopes") or ""
    scope = " ".join(scopes) if isinstance(scopes, list) else str(scopes)
    return {"authority": token_endpoint, "token_endpoint": token_endpoint, "token_key": canonical_resource(url),
            "scope": scope.strip() or None, "resource": config.get("resource") or None, "mcp": True}


MCP_LOGIN_KEY = "mcp_login_configuration"


def mcp_login_fits_tool(tool: dict, login_url: Optional[str]) -> bool:
    """A saved MCP login serves only the server it was made for; the SDK keys tokens by the toolkit's URL."""
    tool_url = ((tool or {}).get("settings") or {}).get("url")
    return bool(tool_url and login_url) and canonical_resource(tool_url) == canonical_resource(login_url)
LOGIN_RESOLVERS = {**{key: _graph_login for key in GRAPH_CONFIGURATION_KEYS}, "dataverse_configuration": _dataverse_login,
                   MCP_LOGIN_KEY: _mcp_login}
# Only these toolkits save a login that Main can refresh; add a type when it implements the same flow
UNATTENDED_LOGIN_TYPES = frozenset(LOGIN_RESOLVERS)


def iter_credential_refs(tools: list):
    """(configuration_uuid, credential type, resolver, tool) per tool credential; only pointers are taken."""
    for tool in tools or []:
        settings = (tool or {}).get("settings") or {}
        for settings_key, resolve in LOGIN_RESOLVERS.items():
            config = settings.get(settings_key)
            if not isinstance(config, dict) or not config.get("configuration_uuid"):
                continue
            yield config["configuration_uuid"], settings_key.removesuffix("_configuration"), resolve, tool


def _browser_mcp_token_key(tokens: dict, token_key: str, tool: dict) -> Optional[str]:
    # The SDK also matches the toolkit type (pre-built MCPs); only a live browser token wins over the saved login
    for key in (token_key, (tool or {}).get("type") or ""):
        value = tokens.get(key) if key else None
        if (isinstance(value, dict) and value.get("access_token")) or (isinstance(value, str) and value):
            return key
    return None


def _login_fingerprint(config: dict, login: dict) -> str:
    # Ties the cache entry to this exact login, so a different or re-done login never reads another's token
    material = "|".join([str(config.get("refresh_token")), str(config.get("client_id")),
                         login["authority"], str(login.get("scope"))])
    return hashlib.sha256(material.encode()).hexdigest()[:32]


ENDPOINT_OK_PREFIX = "oauth:stored:ep:"
ENDPOINT_OK_SECONDS = 3600


def _mcp_endpoint_trusted(redis, login: dict, server_url: str, discover: Callable) -> bool:
    """The saved token endpoint is still the one the MCP server advertises; checked hourly per server+endpoint."""
    key = ENDPOINT_OK_PREFIX + hashlib.sha256(f"{server_url}|{login['token_endpoint']}".encode()).hexdigest()[:32]
    if redis.get(key):
        return True
    try:
        trusted = login["token_endpoint"] in discover(server_url)
    except Exception:  # pylint: disable=W0703
        return False
    if trusted:
        redis.setex(key, ENDPOINT_OK_SECONDS, "1")
    return trusted


def get_access_token(redis, config: dict, login: dict, refresh_fn: Callable, sleep: Callable = time.sleep,
                     save_fn: Optional[Callable] = None, discover_fn: Optional[Callable] = None) -> Optional[str]:
    """Cached access token for the stored login, refreshing it at most once per lifetime; None on a transient error."""
    suffix = f"{config['configuration_uuid']}:{_login_fingerprint(config, login)}"
    cache_key, lock_key = f"{CACHE_PREFIX}{suffix}", f"{LOCK_PREFIX}{suffix}"
    cached = redis.get(cache_key)
    if cached:
        return cached
    if not redis.set(lock_key, "1", nx=True, ex=LOCK_SECONDS):
        # Another run is refreshing this login; wait for its result instead of reporting a false logout
        for _ in range(int(LOCK_WAIT_SECONDS / LOCK_POLL_SECONDS)):
            sleep(LOCK_POLL_SECONDS)
            cached = redis.get(cache_key)
            if cached:
                return cached
        return None
    try:
        cached = redis.get(cache_key)
        if cached:
            return cached
        if login.get("mcp") and discover_fn and not _mcp_endpoint_trusted(redis, login, config.get("url"), discover_fn):
            # The secret and refresh token never go to an endpoint the server does not advertise (edited credential)
            log.warning("Saved MCP login %s: token endpoint not advertised by its server; not refreshed",
                        config.get("configuration_uuid"))
            return None
        extra = {"resource": login["resource"]} if login.get("resource") else {}
        data = refresh_fn(
            token_endpoint=login["token_endpoint"],
            refresh_token_value=config["refresh_token"],
            client_id=config.get("client_id"),
            client_secret=config.get("client_secret"),
            scope=login.get("scope"),
            **extra,
        )
        token = data.get("access_token")
        if not token:
            return None
        lifetime = int(data.get("expires_in") or 3600)
        # Short-lived tokens (some MCP servers: 1-5 min) still get cached, else every run refreshes
        ttl = lifetime - min(EXPIRY_MARGIN_SECONDS, lifetime // 4)
        if ttl > 0:
            redis.setex(cache_key, ttl, token)
        # Cached first: runs waiting on the lock read the token without waiting for the slower vault write
        rotated = data.get("refresh_token")
        if rotated and rotated != config["refresh_token"] and _save_rotated(save_fn, config, rotated) and ttl > 0:
            # The next run loads the rotated token: cache under its fingerprint too, or every run refreshes again
            rotated_fp = _login_fingerprint({**config, "refresh_token": rotated}, login)
            redis.setex(f"{CACHE_PREFIX}{config['configuration_uuid']}:{rotated_fp}", ttl, token)
        return token
    except Exception as e:  # pylint: disable=W0703
        if getattr(e, "error", None) in EXPIRED_ERRORS:
            raise StoredLoginExpired(config["configuration_uuid"], e.error) from None
        log.warning("Stored OAuth login refresh failed for credential %s: %s",
                    config.get("configuration_uuid"), type(e).__name__)
        return None
    finally:
        redis.delete(lock_key)


def make_rotation_saver(rpc) -> Callable:
    """save_fn writing a rotated refresh token back to the credential's own project, unless the login changed."""
    def save(config: dict, rotated: str) -> bool:
        if not config.get("project_id") or not config.get("type"):
            return False
        return rpc.configurations_save_stored_login(
            project_id=config["project_id"], configuration_uuid=config["configuration_uuid"],
            expected_type=config["type"], fields={"refresh_token": rotated},
            expected_refresh_token=config["refresh_token"])
    return save


def _save_rotated(save_fn: Optional[Callable], config: dict, rotated: str) -> bool:
    # Providers that rotate on every use invalidate the old token, so a lost write-back means a re-login later
    if save_fn is None:
        log.warning("Stored OAuth login of credential %s rotated but cannot be saved here",
                    config.get("configuration_uuid"))
        return False
    try:
        if save_fn(config, rotated):
            return True
        log.warning("Rotated refresh token of credential %s not saved: the login changed meanwhile",
                    config.get("configuration_uuid"))
    except Exception as e:  # pylint: disable=W0703
        log.warning("Rotated refresh token of credential %s not saved: %s", config.get("configuration_uuid"),
                    type(e).__name__)
    return False


def collect_payload_tools(payload: dict) -> list:
    tools = list(payload.get("tools") or [])
    application = payload.get("application") if isinstance(payload.get("application"), dict) else {}
    for entry in (application.get("subagent_version_details") or {}).values():
        tools.extend(((entry or {}).get("version_details") or {}).get("tools") or [])
    return tools


MCP_LOGIN_SECRET_FIELDS = ("refresh_token", "client_secret")


def strip_mcp_login_secrets(tools: list) -> None:
    # The runtime only needs the access token; the long-lived login never leaves Main
    for tool in tools or []:
        config = ((tool or {}).get("settings") or {}).get(MCP_LOGIN_KEY)
        if isinstance(config, dict):
            for field in MCP_LOGIN_SECRET_FIELDS:
                config.pop(field, None)


def inject_stored_oauth_tokens(payload: dict, redis, refresh_fn: Callable, load_fn: Callable,
                               save_fn: Optional[Callable] = None, discover_fn: Optional[Callable] = None) -> int:
    """Add access tokens for stored logins to payload['mcp_tokens']; browser tokens always win."""
    # Payload tool dicts can be client-supplied: only their uuid is used, the login comes from load_fn (DB)
    tokens = payload.setdefault("mcp_tokens", {})
    strip_mcp_login_secrets(collect_payload_tools(payload))
    added, seen = 0, set()
    for configuration_uuid, credential_type, resolve, tool in iter_credential_refs(collect_payload_tools(payload)):
        if configuration_uuid in seen:
            continue
        seen.add(configuration_uuid)
        config = load_fn(configuration_uuid, credential_type)
        if not config or not config.get("refresh_token") or config.get("configuration_uuid") != configuration_uuid:
            continue
        login = resolve(config)
        if not login:
            continue
        if login.get("mcp"):
            if not mcp_login_fits_tool(tool, config.get("url")):
                log.warning("Saved MCP login %s is for another server than its toolkit; not used", configuration_uuid)
                continue
            # The SDK sends a token keyed by canonical URL only to that server, so it cannot leak elsewhere
            token_key = login["token_key"]
            browser_key = _browser_mcp_token_key(tokens, token_key, tool)
        else:
            token_key = f"{configuration_uuid}:{login['token_key']}"
            if token_key in tokens:
                continue
        try:
            token = get_access_token(redis, config, login, refresh_fn, save_fn=save_fn, discover_fn=discover_fn)
        except StoredLoginExpired:
            log.warning("Stored OAuth login of credential %s expired", configuration_uuid)
            continue
        if token and login.get("mcp") and browser_key:
            # Browser token still goes first; the SDK retries with this one if the server rejects it
            entry = tokens[browser_key]
            tokens[browser_key] = {**(entry if isinstance(entry, dict) else {"access_token": entry}),
                                   "fallback_access_token": token}
        elif token:
            tokens[token_key] = {"access_token": token}
            added += 1
    return added
