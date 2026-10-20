"""Access tokens for credentials holding a stored OAuth login, so runs without browser tokens can use them."""

from typing import Callable, Optional
from urllib.parse import urlparse

from pylon.core.tools import log

ENTRA_AUTHORITY = "https://login.microsoftonline.com/"
GRAPH_DEFAULT_SCOPE = "https://graph.microsoft.com/.default"
CACHE_PREFIX = "oauth:stored:at:"
LOCK_PREFIX = "oauth:stored:lock:"
LOCK_SECONDS = 30
EXPIRY_MARGIN_SECONDS = 120
GRAPH_CONFIGURATION_KEYS = ("sharepoint_configuration", "outlook_configuration", "teams_configuration")


def _graph_login(config: dict) -> Optional[dict]:
    authority = (config.get("oauth_discovery_endpoint") or "").rstrip("/")
    if not authority.startswith(ENTRA_AUTHORITY):
        return None
    scopes = config.get("scopes") or [GRAPH_DEFAULT_SCOPE]
    if isinstance(scopes, str):
        scopes = scopes.split()
    return {"authority": authority, "token_key": config.get("oauth_discovery_endpoint"),
            "scope": " ".join(["offline_access", *[s for s in scopes if s != "offline_access"]])}


def _dataverse_login(config: dict) -> Optional[dict]:
    tenant = (config.get("tenant_id") or "").strip()
    host = (urlparse((config.get("url") or "").strip()).hostname or "").lower()
    if not tenant or not host.endswith(".dynamics.com"):
        return None
    authority = f"{ENTRA_AUTHORITY}{tenant}"
    return {"authority": authority, "token_key": authority,
            "scope": f"https://{host}/user_impersonation offline_access"}


LOGIN_RESOLVERS = {**{key: _graph_login for key in GRAPH_CONFIGURATION_KEYS}, "dataverse_configuration": _dataverse_login}


def iter_stored_logins(tools: list):
    """(token map key, configuration, login) for every tool credential that carries a stored login."""
    for tool in tools or []:
        settings = (tool or {}).get("settings") or {}
        for settings_key, resolve in LOGIN_RESOLVERS.items():
            config = settings.get(settings_key)
            if not isinstance(config, dict) or not config.get("refresh_token") or not config.get("configuration_uuid"):
                continue
            login = resolve(config)
            if login:
                yield f"{config['configuration_uuid']}:{login['token_key']}", config, login


def get_access_token(redis, config: dict, login: dict, refresh_fn: Callable) -> Optional[str]:
    """Cached access token for the credential, refreshing its stored login at most once per lifetime."""
    cache_key = f"{CACHE_PREFIX}{config['configuration_uuid']}"
    cached = redis.get(cache_key)
    if cached:
        return cached
    lock_key = f"{LOCK_PREFIX}{config['configuration_uuid']}"
    if not redis.set(lock_key, "1", nx=True, ex=LOCK_SECONDS):
        # Another run is refreshing; use its result if it already landed, otherwise go without
        return redis.get(cache_key)
    try:
        cached = redis.get(cache_key)
        if cached:
            return cached
        data = refresh_fn(
            token_endpoint=f"{login['authority']}/oauth2/v2.0/token",
            refresh_token_value=config["refresh_token"],
            client_id=config.get("client_id"),
            client_secret=config.get("client_secret"),
            scope=login["scope"],
        )
        token = data.get("access_token")
        if not token:
            return None
        ttl = int(data.get("expires_in") or 3600) - EXPIRY_MARGIN_SECONDS
        if ttl > 0:
            redis.setex(cache_key, ttl, token)
        return token
    except Exception as e:  # pylint: disable=W0703
        log.warning("Stored OAuth login refresh failed for credential %s: %s",
                    config.get("configuration_uuid"), type(e).__name__)
        return None
    finally:
        redis.delete(lock_key)


def collect_payload_tools(payload: dict) -> list:
    tools = list(payload.get("tools") or [])
    application = payload.get("application") if isinstance(payload.get("application"), dict) else {}
    for entry in (application.get("subagent_version_details") or {}).values():
        tools.extend(((entry or {}).get("version_details") or {}).get("tools") or [])
    return tools


def inject_stored_oauth_tokens(payload: dict, redis, refresh_fn: Callable) -> int:
    """Add access tokens for stored logins to payload['mcp_tokens']; browser tokens always win."""
    tokens = payload.setdefault("mcp_tokens", {})
    added = 0
    for token_key, config, login in iter_stored_logins(collect_payload_tools(payload)):
        if token_key in tokens:
            continue
        token = get_access_token(redis, config, login, refresh_fn)
        if token:
            tokens[token_key] = {"access_token": token}
            added += 1
    return added
