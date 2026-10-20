"""Stored OAuth logins turned into access tokens for runs without browser tokens (scheduled pipelines).

A credential's hidden ``refresh_token`` (saved by the toolkit page's Login bar) is exchanged for an
access token at most once per token lifetime, cached in Redis, and injected into the predict
payload's ``mcp_tokens`` under the key each toolkit already reads. A token the browser sent always
wins; failures leave the payload alone so the toolkit raises its normal "log in" signal.
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

    assert svc.inject_stored_oauth_tokens(payload, redis, refresh) == 1

    assert payload["mcp_tokens"] == {f"uuid-o:{AUTHORITY}": {"access_token": "at-1"}}
    call = refresh.calls[0]
    assert call["token_endpoint"] == f"{AUTHORITY}/oauth2/v2.0/token"
    assert call["refresh_token_value"] == "refresh-o"
    assert call["client_secret"] == "secret-o"
    assert call["scope"] == "offline_access Mail.Read"


def test_dataverse_credential_uses_its_tenant_and_org_scope(svc):
    payload, refresh = {"tools": [dataverse_tool()]}, Refresh()

    svc.inject_stored_oauth_tokens(payload, FakeRedis(), refresh)

    assert payload["mcp_tokens"] == {f"uuid-d:{AUTHORITY}": {"access_token": "at-1"}}
    assert refresh.calls[0]["scope"] == "https://org.crm4.dynamics.com/user_impersonation offline_access"


def test_cached_token_is_reused_until_near_expiry(svc):
    redis, refresh = FakeRedis(), Refresh()
    svc.inject_stored_oauth_tokens({"tools": [outlook_tool()]}, redis, refresh)
    payload = {"tools": [outlook_tool()]}

    svc.inject_stored_oauth_tokens(payload, redis, refresh)

    assert len(refresh.calls) == 1
    assert payload["mcp_tokens"][f"uuid-o:{AUTHORITY}"] == {"access_token": "at-1"}
    assert redis.ttl[f"{svc.CACHE_PREFIX}uuid-o"] == 3600 - svc.EXPIRY_MARGIN_SECONDS


def test_browser_token_is_never_overridden(svc):
    payload = {"tools": [outlook_tool()], "mcp_tokens": {f"uuid-o:{AUTHORITY}": {"access_token": "browser"}}}
    refresh = Refresh()

    assert svc.inject_stored_oauth_tokens(payload, FakeRedis(), refresh) == 0

    assert payload["mcp_tokens"][f"uuid-o:{AUTHORITY}"] == {"access_token": "browser"}
    assert refresh.calls == []


def test_another_run_refreshing_means_no_second_refresh(svc):
    # A held lock means a concurrent run is refreshing this credential; skip rather than double-refresh.
    redis, refresh = FakeRedis(), Refresh()
    redis.data[f"{svc.LOCK_PREFIX}uuid-o"] = "1"
    payload = {"tools": [outlook_tool()]}

    svc.inject_stored_oauth_tokens(payload, redis, refresh)

    assert refresh.calls == []
    assert payload["mcp_tokens"] == {}


def test_failed_refresh_leaves_payload_alone_and_logs_no_secrets(svc, caplog):
    refresh = Refresh(error=ValueError("Token refresh failed: AADSTS700082 refresh-o"))
    redis, payload = FakeRedis(), {"tools": [outlook_tool()]}

    with caplog.at_level(logging.WARNING):
        assert svc.inject_stored_oauth_tokens(payload, redis, refresh) == 0

    assert payload["mcp_tokens"] == {}
    assert "refresh-o" not in caplog.text and "secret-o" not in caplog.text
    assert f"{svc.LOCK_PREFIX}uuid-o" not in redis.data


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
    assert svc.inject_stored_oauth_tokens({"tools": [tool]}, FakeRedis(), refresh) == 0
    assert refresh.calls == []


def test_sub_agent_tools_from_prefetch_are_covered(svc):
    payload = {"tools": [], "application": {"subagent_version_details": {
        "12:34": {"version_details": {"tools": [outlook_tool(uuid="uuid-sub")]}}}}}

    svc.inject_stored_oauth_tokens(payload, FakeRedis(), Refresh())

    assert f"uuid-sub:{AUTHORITY}" in payload["mcp_tokens"]


def test_graph_scope_defaults_when_credential_has_none(svc):
    refresh = Refresh()
    svc.inject_stored_oauth_tokens({"tools": [outlook_tool(scopes=())]}, FakeRedis(), refresh)
    assert refresh.calls[0]["scope"] == f"offline_access {svc.GRAPH_DEFAULT_SCOPE}"


def test_predict_payload_calls_the_injection_after_prefetch():
    # Wiring guard: the hook lives right after attach_subagent_prefetch and can never fail the predict.
    source = (_PATH.parents[0] / "predict_utils.py").read_text()
    prefetch = source.index("attach_subagent_prefetch(payload, parsed.project_id, expansion_user_id)")
    inject = source.index("inject_stored_oauth_tokens(payload, this.module.get_redis_client(), refresh_token)")
    assert prefetch < inject
    assert "except Exception" in source[inject:inject + 300]


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
