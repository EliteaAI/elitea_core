"""#7643: a saved MCP OAuth login refreshed by Main for runs without a browser token.

Scenarios: the MCP resolver builds the token key the SDK reads (canonical server URL), a browser
token still wins, a rotated refresh token is written back once (and the Microsoft path gets the same
write-back), an expired login raises StoredLoginExpired, and the long-lived secrets never reach the
runtime payload or the logs.
"""

import logging

import pytest

from .test_stored_oauth_tokens import FakeRedis, Refresh, svc  # noqa: F401  (fixture reuse)
from .test_stored_oauth_tokens import outlook_tool

pytestmark = pytest.mark.integration

SERVER = "https://MCP.Example.com/mcp/"
CANONICAL = "https://mcp.example.com/mcp"
TOKEN_ENDPOINT = "https://auth.example.com/oauth/token"


def mcp_tool(uuid="uuid-m", url=SERVER, tool_type="mcp"):
    return {"type": tool_type, "settings": {"url": url, "mcp_login_configuration": {
        "configuration_uuid": uuid, "url": url, "token_endpoint": TOKEN_ENDPOINT, "client_id": "dcr-client",
        "client_secret": "s3cret-client", "refresh_token": "rt-old", "resource": CANONICAL,
        "scopes": "read offline_access"}}}


def stored(tool, key="mcp_login_configuration", **extra):
    # What configurations_get_stored_login returns: the DB row, including the credential's own project
    config = dict(tool["settings"][key])
    config.update(project_id=3, type=key.removesuffix("_configuration"), **extra)
    return config


class Load:
    def __init__(self, *configs):
        self.rows = {c["configuration_uuid"]: c for c in configs}

    def __call__(self, uuid_, type_):
        row = self.rows.get(uuid_)
        return dict(row) if row and row["type"] == type_ else None


class Saver:
    def __init__(self, result=True, error=None):
        self.calls, self.result, self.error = [], result, error

    def __call__(self, config, rotated):
        self.calls.append((config["configuration_uuid"], config["refresh_token"], rotated))
        if self.error:
            raise self.error
        return self.result


def test_mcp_login_resolves_to_canonical_server_key(svc):
    login = svc._mcp_login(stored(mcp_tool()))

    assert login["token_key"] == CANONICAL
    assert login["token_endpoint"] == TOKEN_ENDPOINT
    assert login["resource"] == CANONICAL
    assert svc.MCP_LOGIN_KEY in svc.UNATTENDED_LOGIN_TYPES


@pytest.mark.parametrize("config", [
    {"url": SERVER},  # no token endpoint saved yet: never logged in
    {"url": "ftp://x", "token_endpoint": TOKEN_ENDPOINT},
    {"url": SERVER, "token_endpoint": "javascript:alert(1)"},
])
def test_mcp_login_rejects_incomplete_or_odd_logins(svc, config):
    assert svc._mcp_login(config) is None


def test_injects_access_token_under_canonical_url_and_strips_secrets(svc):
    tool = mcp_tool()
    payload, refresh = {"tools": [tool], "mcp_tokens": {}}, Refresh()

    assert svc.inject_stored_oauth_tokens(payload, FakeRedis(), refresh, Load(stored(tool))) == 1

    assert payload["mcp_tokens"] == {CANONICAL: {"access_token": "at-1"}}
    call = refresh.calls[0]
    assert call["token_endpoint"] == TOKEN_ENDPOINT
    assert call["resource"] == CANONICAL
    assert call["client_id"] == "dcr-client"
    # The runtime payload keeps the pointer but never the long-lived secrets
    login_ref = payload["tools"][0]["settings"]["mcp_login_configuration"]
    assert "refresh_token" not in login_ref and "client_secret" not in login_ref


def test_browser_token_goes_first_and_the_saved_login_rides_along_as_fallback(svc):
    # The browser token may be stale or revoked: the SDK retries with the fallback on a 401
    tool = mcp_tool()
    payload = {"tools": [tool], "mcp_tokens": {CANONICAL: {"access_token": "browser", "session_id": "s1"}}}

    assert svc.inject_stored_oauth_tokens(payload, FakeRedis(), Refresh(), Load(stored(tool))) == 0

    assert payload["mcp_tokens"][CANONICAL] == {"access_token": "browser", "session_id": "s1",
                                                "fallback_access_token": "at-1"}


def test_prebuilt_browser_token_under_toolkit_type_gets_the_fallback(svc):
    tool = mcp_tool(tool_type="mcp_github")
    payload = {"tools": [tool], "mcp_tokens": {"mcp_github": "legacy-browser"}}

    assert svc.inject_stored_oauth_tokens(payload, FakeRedis(), Refresh(), Load(stored(tool))) == 0

    assert payload["mcp_tokens"]["mcp_github"] == {"access_token": "legacy-browser", "fallback_access_token": "at-1"}
    assert CANONICAL not in payload["mcp_tokens"]


def test_expired_saved_login_adds_no_fallback(svc):
    error = type("TokenRefreshError", (Exception,), {})("refused")
    error.error = "invalid_grant"
    payload = {"tools": [mcp_tool()], "mcp_tokens": {CANONICAL: {"access_token": "browser"}}}

    svc.inject_stored_oauth_tokens(payload, FakeRedis(), Refresh(error=error), Load(stored(mcp_tool())))

    assert payload["mcp_tokens"][CANONICAL] == {"access_token": "browser"}


def test_expired_browser_entry_does_not_block_saved_login(svc):
    # The UI sends access_token:null for an expired tab token; the saved login must fill in
    tool = mcp_tool()
    payload = {"tools": [tool], "mcp_tokens": {CANONICAL: {"access_token": None, "refresh_token": "x"}}}

    assert svc.inject_stored_oauth_tokens(payload, FakeRedis(), Refresh(), Load(stored(tool))) == 1
    assert payload["mcp_tokens"][CANONICAL] == {"access_token": "at-1"}


def test_payload_login_fields_are_never_trusted(svc):
    # A forged payload points at the uuid but carries its own endpoint: only the DB login is used
    tool = mcp_tool()
    tool["settings"]["mcp_login_configuration"]["token_endpoint"] = "https://evil.example.com/token"
    refresh = Refresh()

    svc.inject_stored_oauth_tokens({"tools": [tool], "mcp_tokens": {}}, FakeRedis(), refresh,
                                   Load(stored(mcp_tool())))

    assert refresh.calls[0]["token_endpoint"] == TOKEN_ENDPOINT


def test_rotated_refresh_token_is_written_back_once(svc):
    tool, saver = mcp_tool(), Saver()
    refresh = Refresh({"access_token": "at-2", "refresh_token": "rt-new", "expires_in": 3600})
    redis = FakeRedis()

    for _ in range(2):  # the second run reads the cached access token: no second refresh, no second save
        svc.inject_stored_oauth_tokens({"tools": [mcp_tool()], "mcp_tokens": {}}, redis, refresh,
                                       Load(stored(tool)), save_fn=saver)

    assert len(refresh.calls) == 1
    assert saver.calls == [("uuid-m", "rt-old", "rt-new")]


def test_same_refresh_token_is_not_written_back(svc):
    saver = Saver()
    refresh = Refresh({"access_token": "at-2", "refresh_token": "rt-old", "expires_in": 3600})

    svc.inject_stored_oauth_tokens({"tools": [mcp_tool()], "mcp_tokens": {}}, FakeRedis(), refresh,
                                   Load(stored(mcp_tool())), save_fn=saver)

    assert saver.calls == []


@pytest.mark.parametrize("saver", [Saver(result=False), Saver(error=RuntimeError("rpc down"))])
def test_failed_write_back_still_returns_the_token(svc, saver):
    refresh = Refresh({"access_token": "at-2", "refresh_token": "rt-new", "expires_in": 3600})
    payload = {"tools": [mcp_tool()], "mcp_tokens": {}}

    assert svc.inject_stored_oauth_tokens(payload, FakeRedis(), refresh, Load(stored(mcp_tool())),
                                          save_fn=saver) == 1
    assert payload["mcp_tokens"][CANONICAL] == {"access_token": "at-2"}


def test_concurrent_run_waits_for_the_refresh_in_flight(svc):
    tool = mcp_tool()
    config, login = stored(tool), svc._mcp_login(stored(tool))
    redis, refresh = FakeRedis(), Refresh()
    suffix = f"uuid-m:{svc._login_fingerprint(config, login)}"
    redis.set(f"{svc.LOCK_PREFIX}{suffix}", "1", nx=True, ex=30)  # another run holds the lock

    def other_run_finishes(_seconds):
        redis.setex(f"{svc.CACHE_PREFIX}{suffix}", 60, "at-from-other-run")

    token = svc.get_access_token(redis, config, login, refresh, sleep=other_run_finishes)

    assert token == "at-from-other-run"
    assert refresh.calls == []


def test_invalid_grant_raises_expired(svc):
    error = type("TokenRefreshError", (Exception,), {})("refused")
    error.error = "invalid_grant"
    tool = mcp_tool()

    with pytest.raises(svc.StoredLoginExpired):
        svc.get_access_token(FakeRedis(), stored(tool), svc._mcp_login(stored(tool)), Refresh(error=error))


def test_microsoft_rotation_is_written_back_too(svc):
    tool, saver = outlook_tool(), Saver()
    refresh = Refresh({"access_token": "at-o", "refresh_token": "refresh-o-2", "expires_in": 3600})

    svc.inject_stored_oauth_tokens({"tools": [tool], "mcp_tokens": {}}, FakeRedis(), refresh,
                                   Load(stored(tool, "outlook_configuration")), save_fn=saver)

    assert saver.calls == [("uuid-o", "refresh-o", "refresh-o-2")]
    assert refresh.calls[0]["token_endpoint"] == "https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token"


def test_rotation_saver_writes_to_the_credentials_own_project(svc):
    calls = []

    class Rpc:
        def configurations_save_stored_login(self, **kwargs):
            calls.append(kwargs)
            return True

    assert svc.make_rotation_saver(Rpc())({**stored(mcp_tool()), "project_id": 77}, "rt-new") is True
    assert calls == [{"project_id": 77, "configuration_uuid": "uuid-m", "expected_type": "mcp_login",
                      "fields": {"refresh_token": "rt-new"}, "expected_refresh_token": "rt-old"}]


def test_no_token_values_in_logs(svc, caplog):
    saver = Saver(error=RuntimeError("boom rt-new"))
    refresh = Refresh({"access_token": "at-secret-value", "refresh_token": "rt-new", "expires_in": 3600})
    caplog.set_level(logging.DEBUG)

    svc.inject_stored_oauth_tokens({"tools": [mcp_tool()], "mcp_tokens": {}}, FakeRedis(), refresh,
                                   Load(stored(mcp_tool())), save_fn=saver)
    error = type("TokenRefreshError", (Exception,), {})("rt-old was refused")
    error.error = "temporarily_unavailable"
    svc.inject_stored_oauth_tokens({"tools": [mcp_tool(uuid="uuid-2")], "mcp_tokens": {}}, FakeRedis(),
                                   Refresh(error=error), Load(stored(mcp_tool(uuid="uuid-2"))))

    for secret in ("at-secret-value", "rt-new", "rt-old", "s3cret-client"):
        assert secret not in caplog.text


def test_short_lived_token_is_still_cached(svc):
    # Found live: a 60 s token got ttl = 60 - 120 < 0, so nothing was cached and parallel runs
    # raced on a rotating refresh token (one of them then got invalid_grant)
    redis, refresh = FakeRedis(), Refresh({"access_token": "at-short", "refresh_token": "rt-2", "expires_in": 60})
    tool = mcp_tool()

    svc.get_access_token(redis, stored(tool), svc._mcp_login(stored(tool)), refresh, save_fn=Saver())

    # Two entries after a saved rotation (used login + rotated login), same short ttl
    assert redis.ttl and all(0 < ttl < 60 for ttl in redis.ttl.values())


def test_token_is_cached_before_the_rotation_write_back(svc):
    redis, order = FakeRedis(), []
    original = redis.setex
    redis.setex = lambda *a: (order.append("cache"), original(*a))
    refresh = Refresh({"access_token": "at", "refresh_token": "rt-2", "expires_in": 3600})
    tool = mcp_tool()

    svc.get_access_token(redis, stored(tool), svc._mcp_login(stored(tool)), refresh,
                         save_fn=lambda c, r: order.append("save") or True)

    assert order == ["cache", "save", "cache"]  # used login first, rotated login only after the save


# --- code-review fixes -------------------------------------------------------------------------------------------

def test_rotation_caches_under_the_rotated_fingerprint_so_the_next_run_does_not_refresh(svc):
    # A provider that rotates on every use: without this every run refreshed (and rotated) again
    redis, saver = FakeRedis(), Saver()
    refresh = Refresh({"access_token": "at-2", "refresh_token": "rt-new", "expires_in": 3600})
    first = stored(mcp_tool())

    svc.inject_stored_oauth_tokens({"tools": [mcp_tool()], "mcp_tokens": {}}, redis, refresh, Load(first),
                                   save_fn=saver)
    # The next run loads the credential as written back: refresh token rt-new
    second = {**first, "refresh_token": "rt-new"}
    payload = {"tools": [mcp_tool()], "mcp_tokens": {}}
    svc.inject_stored_oauth_tokens(payload, redis, refresh, Load(second), save_fn=saver)

    assert len(refresh.calls) == 1
    assert payload["mcp_tokens"][CANONICAL] == {"access_token": "at-2"}


def test_failed_write_back_does_not_cache_under_the_rotated_fingerprint(svc):
    redis = FakeRedis()
    refresh = Refresh({"access_token": "at-2", "refresh_token": "rt-new", "expires_in": 3600})
    svc.get_access_token(redis, stored(mcp_tool()), svc._mcp_login(stored(mcp_tool())), refresh,
                         save_fn=Saver(result=False))
    assert len(redis.data) == 1  # only the entry of the login that was actually used


def test_login_for_another_server_is_not_injected(svc):
    tool = mcp_tool()
    tool["settings"]["url"] = "https://other.example.com/mcp"
    refresh = Refresh()

    payload = {"tools": [tool], "mcp_tokens": {}}
    assert svc.inject_stored_oauth_tokens(payload, FakeRedis(), refresh, Load(stored(mcp_tool()))) == 0
    assert payload["mcp_tokens"] == {} and refresh.calls == []


@pytest.mark.parametrize("tool_url, login_url, fits", [
    ("https://MCP.example.com/mcp/", "https://mcp.example.com/mcp", True),
    ("https://mcp.example.com/mcp", "https://mcp.example.com/other", False),
    (None, "https://mcp.example.com/mcp", False),
    ("https://mcp.example.com/mcp", None, False),
])
def test_mcp_login_fits_tool(svc, tool_url, login_url, fits):
    assert svc.mcp_login_fits_tool({"settings": {"url": tool_url}}, login_url) is fits


# --- review #116: the refresh never goes to a token endpoint the MCP server does not advertise -------------------

def test_edited_token_endpoint_is_not_refreshed(svc):
    tool = mcp_tool()
    evil = {**stored(tool), "token_endpoint": "https://attacker.example/token"}
    refresh = Refresh()

    payload = {"tools": [tool], "mcp_tokens": {}}
    added = svc.inject_stored_oauth_tokens(payload, FakeRedis(), refresh, Load(evil),
                                           discover_fn=lambda url: {TOKEN_ENDPOINT})

    assert added == 0 and refresh.calls == [] and payload["mcp_tokens"] == {}


def test_advertised_endpoint_is_checked_once_then_cached(svc):
    redis, calls = FakeRedis(), []

    def discover(url):
        calls.append(url)
        return {TOKEN_ENDPOINT}

    for _ in range(2):
        redis.data = {k: v for k, v in redis.data.items() if not k.startswith(svc.CACHE_PREFIX)}
        assert svc.inject_stored_oauth_tokens({"tools": [mcp_tool()], "mcp_tokens": {}}, redis, Refresh(),
                                              Load(stored(mcp_tool())), discover_fn=discover) == 1
    assert calls == [SERVER]


def test_discovery_outage_does_not_refresh(svc):
    def down(url):
        raise TimeoutError("metadata down")

    refresh = Refresh()
    svc.inject_stored_oauth_tokens({"tools": [mcp_tool()], "mcp_tokens": {}}, FakeRedis(), refresh,
                                   Load(stored(mcp_tool())), discover_fn=down)
    assert refresh.calls == []
