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


def iter_credential_refs(tools: list):
    """(configuration_uuid, credential type, payload token key) per tool credential; only pointers are taken."""
    for tool in tools or []:
        settings = (tool or {}).get("settings") or {}
        for settings_key, resolve in LOGIN_RESOLVERS.items():
            config = settings.get(settings_key)
            if not isinstance(config, dict) or not config.get("configuration_uuid"):
                continue
            yield config["configuration_uuid"], settings_key.removesuffix("_configuration"), resolve


def _login_fingerprint(config: dict, login: dict) -> str:
    # Ties the cache entry to this exact login, so a different or re-done login never reads another's token
    material = "|".join([str(config.get("refresh_token")), str(config.get("client_id")),
                         login["authority"], login["scope"]])
    return hashlib.sha256(material.encode()).hexdigest()[:32]


def get_access_token(redis, config: dict, login: dict, refresh_fn: Callable, sleep: Callable = time.sleep) -> Optional[str]:
    """Cached access token for the stored login, refreshing it at most once per lifetime."""
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


def inject_stored_oauth_tokens(payload: dict, redis, refresh_fn: Callable, load_fn: Callable) -> int:
    """Add access tokens for stored logins to payload['mcp_tokens']; browser tokens always win."""
    # Payload tool dicts can be client-supplied: only their uuid is used, the login comes from load_fn (DB)
    tokens = payload.setdefault("mcp_tokens", {})
    added, seen = 0, set()
    for configuration_uuid, credential_type, resolve in iter_credential_refs(collect_payload_tools(payload)):
        if configuration_uuid in seen:
            continue
        seen.add(configuration_uuid)
        config = load_fn(configuration_uuid, credential_type)
        if not config or not config.get("refresh_token") or config.get("configuration_uuid") != configuration_uuid:
            continue
        login = resolve(config)
        if not login:
            continue
        token_key = f"{configuration_uuid}:{login['token_key']}"
        if token_key in tokens:
            continue
        token = get_access_token(redis, config, login, refresh_fn)
        if token:
            tokens[token_key] = {"access_token": token}
            added += 1
    return added
