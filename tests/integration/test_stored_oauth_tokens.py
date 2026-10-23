"""Stored OAuth logins turned into access tokens for runs without browser tokens (scheduled pipelines).

A credential's hidden ``refresh_token`` (saved by the toolkit page's Login bar) is exchanged for an
access token at most once per token lifetime, cached in Redis, and injected into the predict
payload's ``mcp_tokens`` under the key each toolkit already reads. A token the browser sent always
wins; failures leave the payload alone so the toolkit raises its normal "log in" signal.

Security model: predict payload tool dicts can be client-supplied, so the only thing taken from them
is the credential uuid. The login itself (refresh token, client, authority, scopes) always comes from
``load_fn`` - the DB, limited to credentials the run could expand anyway - and the cache key is bound
to that exact login, so a forged or stale login can never read or poison another login's token.
"""

import importlib.util
import logging
import pathlib
import sys
import types

import pytest

pytestmark = pytest.mark.integration

_PATH = pathlib.Path(__file__).resolve().parents[2] / "utils" / "stored_oauth_tokens.py"


@pytest.fixture
def svc(monkeypatch):
    # The module only needs pylon's logger; give it a stdlib one.
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools = types.ModuleType("pylon.core.tools")
    tools.log = logging.getLogger("stored_oauth_tokens_test")
    for name, module in (("pylon", pylon), ("pylon.core", core), ("pylon.core.tools", tools)):
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("stored_oauth_tokens_under_test", _PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeRedis:
    def __init__(self):
        self.data, self.ttl = {}, {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, *, nx=False, ex=None):
        assert nx and ex
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def setex(self, key, ttl, value):
        self.data[key], self.ttl[key] = value, ttl

    def delete(self, key):
        self.data.pop(key, None)


class Db:
    """Stands in for configurations_get_stored_login: the credentials as they are stored on the server."""

    def __init__(self, *configs):
        self.rows = {(c["configuration_uuid"], t): dict(c) for t, c in configs}
        self.calls = []

    def __call__(self, configuration_uuid, credential_type):
        self.calls.append((configuration_uuid, credential_type))
        return self.rows.get((configuration_uuid, credential_type))

    @classmethod
    def of(cls, *tools):
        configs = []
        for tool in tools:
            for key, config in tool["settings"].items():
                if config.get("refresh_token") and config.get("configuration_uuid"):
                    configs.append((key.removesuffix("_configuration"), config))
        return cls(*configs)


def keys_for(svc, redis, prefix):
    return [k for k in redis.data if k.startswith(f"{prefix}uuid-o:")]


def inject(svc, payload, redis, refresh, db=None):
    return svc.inject_stored_oauth_tokens(payload, redis, refresh, db or Db.of(*svc.collect_payload_tools(payload)))


class Refresh:
    def __init__(self, response=None, error=None):
        self.calls, self.response, self.error = [], response or {"access_token": "at-1", "expires_in": 3600}, error

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.response


AUTHORITY = "https://login.microsoftonline.com/tenant-1"


def outlook_tool(uuid="uuid-o", refresh="refresh-o", scopes=("Mail.Read",)):
    return {"type": "outlook", "settings": {"outlook_configuration": {
        "configuration_uuid": uuid, "oauth_discovery_endpoint": AUTHORITY, "client_id": "client-o",
        "client_secret": "secret-o", "refresh_token": refresh, "scopes": list(scopes)}}}


def dataverse_tool(uuid="uuid-d"):
    return {"type": "dataverse", "settings": {"dataverse_configuration": {
        "configuration_uuid": uuid, "url": "https://org.crm4.dynamics.com", "tenant_id": "tenant-1",
        "client_id": "client-d", "client_secret": "secret-d", "refresh_token": "refresh-d"}}}


def test_graph_credential_gets_token_under_the_key_the_toolkit_reads(svc):
    payload, redis, refresh = {"tools": [outlook_tool()], "mcp_tokens": {}}, FakeRedis(), Refresh()

    assert inject(svc, payload, redis, refresh) == 1

    assert payload["mcp_tokens"] == {f"uuid-o:{AUTHORITY}": {"access_token": "at-1"}}
    call = refresh.calls[0]
    assert call["token_endpoint"] == f"{AUTHORITY}/oauth2/v2.0/token"
    assert call["refresh_token_value"] == "refresh-o"
    assert call["client_secret"] == "secret-o"
    assert call["scope"] == "offline_access Mail.Read"


def test_dataverse_credential_uses_its_tenant_and_org_scope(svc):
    payload, refresh = {"tools": [dataverse_tool()]}, Refresh()

    inject(svc, payload, FakeRedis(), refresh)

    assert payload["mcp_tokens"] == {f"uuid-d:{AUTHORITY}": {"access_token": "at-1"}}
    assert refresh.calls[0]["scope"] == "https://org.crm4.dynamics.com/user_impersonation offline_access"


def test_cached_token_is_reused_until_near_expiry(svc):
    redis, refresh = FakeRedis(), Refresh()
    inject(svc, {"tools": [outlook_tool()]}, redis, refresh)
    payload = {"tools": [outlook_tool()]}

    inject(svc, payload, redis, refresh)

    assert len(refresh.calls) == 1
    assert payload["mcp_tokens"][f"uuid-o:{AUTHORITY}"] == {"access_token": "at-1"}
    (cache_key,) = keys_for(svc, redis, svc.CACHE_PREFIX)
    assert redis.ttl[cache_key] == 3600 - svc.EXPIRY_MARGIN_SECONDS


def test_browser_token_is_never_overridden(svc):
    payload = {"tools": [outlook_tool()], "mcp_tokens": {f"uuid-o:{AUTHORITY}": {"access_token": "browser"}}}
    refresh = Refresh()

    assert inject(svc, payload, FakeRedis(), refresh) == 0

    assert payload["mcp_tokens"][f"uuid-o:{AUTHORITY}"] == {"access_token": "browser"}
    assert refresh.calls == []


class HeldLockRedis(FakeRedis):
    """Every lock is already held by another run; optionally that run's token lands after a few polls."""

    def __init__(self, lands_after=None):
        super().__init__()
        self.polls, self.lands_after = 0, lands_after

    def set(self, key, value, *, nx=False, ex=None):
        return False

    def get(self, key):
        if key.startswith("oauth:stored:at:"):
            self.polls += 1
            if self.lands_after is not None and self.polls > self.lands_after:
                return "at-from-other-run"
        return super().get(key)


def test_another_run_refreshing_means_no_second_refresh(svc, monkeypatch):
    # A held lock means a concurrent run is refreshing; never double-refresh, give up after the wait.
    monkeypatch.setattr(svc.time, "sleep", lambda _s: None)
    redis, refresh = HeldLockRedis(), Refresh()
    payload = {"tools": [outlook_tool()]}

    inject(svc, payload, redis, refresh)

    assert refresh.calls == []
    assert payload["mcp_tokens"] == {}


def test_waits_for_a_concurrent_refresh_instead_of_asking_to_log_in(svc, monkeypatch):
    # Cold cache, many runs at once: the losers re-read the cache rather than reporting a false logout.
    monkeypatch.setattr(svc.time, "sleep", lambda _s: None)
    redis, refresh = HeldLockRedis(lands_after=3), Refresh()
    payload = {"tools": [outlook_tool()]}

    inject(svc, payload, redis, refresh)

    assert refresh.calls == []
    assert payload["mcp_tokens"][f"uuid-o:{AUTHORITY}"] == {"access_token": "at-from-other-run"}


def test_failed_refresh_leaves_payload_alone_and_logs_no_secrets(svc, caplog):
    refresh = Refresh(error=ValueError("Token refresh failed: AADSTS700082 refresh-o"))
    redis, payload = FakeRedis(), {"tools": [outlook_tool()]}

    with caplog.at_level(logging.WARNING):
        assert inject(svc, payload, redis, refresh) == 0

    assert payload["mcp_tokens"] == {}
    assert "refresh-o" not in caplog.text and "secret-o" not in caplog.text
    assert keys_for(svc, redis, svc.LOCK_PREFIX) == []


@pytest.mark.parametrize("tool", [
    outlook_tool(refresh=None),
    {"settings": {"outlook_configuration": {"refresh_token": "r", "oauth_discovery_endpoint": AUTHORITY}}},
    {"settings": {"outlook_configuration": {"configuration_uuid": "u", "refresh_token": "r",
                                            "oauth_discovery_endpoint": "https://evil.example.com/t"}}},
    {"settings": {"dataverse_configuration": {"configuration_uuid": "u", "refresh_token": "r", "tenant_id": "t",
                                              "url": "https://evil.example.com"}}},
    {"settings": {"jira_configuration": {"configuration_uuid": "u", "refresh_token": "r"}}},
])
def test_credentials_without_a_usable_stored_login_are_skipped(svc, tool):
    # No stored login, no credential id, a non-Entra authority, a non-Dataverse host, or an unknown type.
    refresh = Refresh()
    assert inject(svc, {"tools": [tool]}, FakeRedis(), refresh) == 0
    assert refresh.calls == []


def test_sub_agent_tools_from_prefetch_are_covered(svc):
    payload = {"tools": [], "application": {"subagent_version_details": {
        "12:34": {"version_details": {"tools": [outlook_tool(uuid="uuid-sub")]}}}}}

    inject(svc, payload, FakeRedis(), Refresh())

    assert f"uuid-sub:{AUTHORITY}" in payload["mcp_tokens"]


def test_graph_scope_defaults_when_credential_has_none(svc):
    refresh = Refresh()
    inject(svc, {"tools": [outlook_tool(scopes=())]}, FakeRedis(), refresh)
    assert refresh.calls[0]["scope"] == f"offline_access {svc.GRAPH_DEFAULT_SCOPE}"


def test_predict_payload_calls_the_injection_after_prefetch():
    # Wiring guard: the hook lives right after attach_subagent_prefetch and can never fail the predict.
    source = (_PATH.parents[0] / "predict_utils.py").read_text()
    prefetch = source.index("attach_subagent_prefetch(payload, parsed.project_id, expansion_user_id)")
    call = source.index("inject_stored_logins(payload, parsed.project_id, expansion_user_id)")
    assert prefetch < call
    helper = source[source.index("def inject_stored_logins("):source.index("def generate_predict_payload(")]
    assert "configurations_get_stored_login(" in helper and "user_id=expansion_user_id" in helper
    assert "except Exception" in helper


def test_validation_runs_for_delegated_credentials_with_a_stored_login():
    # Validation used to skip every delegated credential without an access token. With a saved login the
    # SDK can refresh by itself, so the check must run and report an expired login to the toolkit page.
    source = (_PATH.parents[0] / "application_utils.py").read_text()
    skip = source.index("if (config_data.get('oauth_discovery_endpoint') and not config_data.get('access_token')")
    assert "not config_data.get('refresh_token')" in source[skip:skip + 200]


def test_connection_errors_sentinel_is_read_from_the_raised_value_error():
    # pydantic stores the raised ValueError (not its dict argument) in ctx.error, so comparing the exception
    # to a dict never matched and the validator API returned empty connection_errors.
    from pydantic import BaseModel, ValidationError, model_validator

    sentinel = "__connection_errors__"

    class Model(BaseModel):
        @model_validator(mode="after")
        def check(self):
            raise ValueError({sentinel: [{"configuration_type": "outlook", "message": "log in"}]})

    with pytest.raises(ValidationError) as caught:
        Model()
    err = caught.value.errors()[0]
    assert not isinstance(err["ctx"]["error"], dict)

    source = (_PATH.parents[0] / "application_utils.py").read_text()
    intercept = source.index("ctx_error = err.get('ctx', {}).get('error')")
    block = source[intercept:intercept + 500]
    assert "ctx_error.args[0]" in block and "CONNECTION_ERROR_SENTINEL" in block

    ctx_error = err["ctx"]["error"]
    payload = ctx_error.args[0] if getattr(ctx_error, "args", None) else ctx_error
    assert payload[sentinel][0]["message"] == "log in"


# --- Forged or tampered payloads (reviewer scenarios) -----------------------------------------
# The attacker controls the predict payload's tool dicts, but not the DB. Neither forging another
# credential's uuid nor sending their own refresh token may read or poison that credential's token.

def test_forged_uuid_with_attacker_refresh_token_uses_only_the_stored_login(svc):
    victim = outlook_tool(uuid="uuid-o", refresh="victim-refresh")
    db = Db.of(victim)
    forged = outlook_tool(uuid="uuid-o", refresh="attacker-refresh")
    forged["settings"]["outlook_configuration"]["client_secret"] = "attacker-secret"
    refresh = Refresh()

    inject(svc, {"tools": [forged]}, FakeRedis(), refresh, db)

    assert [c["refresh_token_value"] for c in refresh.calls] == ["victim-refresh"]
    assert refresh.calls[0]["client_secret"] == "secret-o"


def test_attacker_login_never_lands_in_the_victims_cache_entry(svc):
    # Cache poisoning: the attacker's refresh token is never exchanged, so nothing of theirs is cached.
    db, redis = Db.of(outlook_tool(refresh="victim-refresh")), FakeRedis()
    attacker_refresh = Refresh(response={"access_token": "attacker-at", "expires_in": 3600})

    inject(svc, {"tools": [outlook_tool(refresh="attacker-refresh")]}, redis, attacker_refresh, db)
    victim_payload = {"tools": [outlook_tool()]}
    inject(svc, victim_payload, redis, Refresh(), db)

    assert attacker_refresh.calls[0]["refresh_token_value"] == "victim-refresh"
    assert "attacker-refresh" not in str(redis.data)


def test_credential_the_run_cannot_reach_gets_nothing(svc):
    # load_fn returns None for a uuid outside the run's projects (or not shared from the public project).
    refresh = Refresh()
    payload = {"tools": [outlook_tool(uuid="someone-elses")]}

    assert inject(svc, payload, FakeRedis(), refresh, Db()) == 0
    assert refresh.calls == [] and payload["mcp_tokens"] == {}


def test_uuid_of_a_different_credential_type_is_not_used(svc):
    # A Dataverse uuid sent under outlook_configuration must not borrow the Dataverse login.
    db = Db.of(dataverse_tool(uuid="uuid-x"))
    payload = {"tools": [outlook_tool(uuid="uuid-x")]}

    assert inject(svc, payload, FakeRedis(), Refresh(), db) == 0
    assert db.calls == [("uuid-x", "outlook")]


def test_payload_tool_without_refresh_token_still_gets_the_stored_login(svc):
    # Pointer-only: the payload need not carry the secret for the server to resolve the login.
    db = Db.of(outlook_tool())
    payload = {"tools": [outlook_tool(refresh=None)]}

    assert inject(svc, payload, FakeRedis(), Refresh(), db) == 1


def test_a_new_login_never_reads_the_previous_logins_token(svc):
    # Logout or logging in as someone else changes the stored refresh token, so the old token is not served.
    redis = FakeRedis()
    inject(svc, {"tools": [outlook_tool()]}, redis, Refresh(), Db.of(outlook_tool(refresh="first")))
    second = Refresh(response={"access_token": "at-2", "expires_in": 3600})
    payload = {"tools": [outlook_tool()]}

    inject(svc, payload, redis, second, Db.of(outlook_tool(refresh="second")))

    assert len(second.calls) == 1
    assert payload["mcp_tokens"][f"uuid-o:{AUTHORITY}"] == {"access_token": "at-2"}
    assert len(keys_for(svc, redis, svc.CACHE_PREFIX)) == 2
