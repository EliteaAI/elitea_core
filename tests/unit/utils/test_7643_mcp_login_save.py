"""#7643: server-side save of an MCP login from the token proxy (utils/mcp_saved_login.py).

Only admins may save or log out; a response without a refresh token is refused with 422 (the server
cannot keep a login); the browser gets the access token only, never the refresh or id token; the
DCR client and endpoint are stored with the login; a login for another server is refused.
"""
import base64
import json
import sys
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

SERVER = "https://mcp.example.com/mcp"


def id_token(claims):
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"h.{body}.sig"


@pytest.fixture
def env(monkeypatch):
    state = SimpleNamespace(admin=True, credential={"id": 9, "uuid": "uuid-m", "data": {"url": SERVER}},
                            saves=[], save_result=True, redis_keys={"oauth:stored:at:uuid-m:fp1", "other"})

    rpc = SimpleNamespace(
        configurations_can_manage_stored_login=lambda project_id, user_id: state.admin,
        configurations_get_by_uuid_in_project=lambda project_id, configuration_uuid, expected_type:
            state.credential if configuration_uuid == "uuid-m" and expected_type == "mcp_login" else None,
        configurations_save_stored_login=lambda **kw: state.saves.append(kw) or state.save_result,
    )

    class Redis:
        def scan_iter(self, match, count):
            return [k for k in list(state.redis_keys) if k.startswith(match.rstrip("*"))]

        def delete(self, key):
            state.redis_keys.discard(key)

    tools = sys.modules["tools"]
    monkeypatch.setattr(tools, "rpc_tools", SimpleNamespace(RpcMixin=lambda: SimpleNamespace(
        rpc=SimpleNamespace(timeout=lambda s: rpc))), raising=False)
    monkeypatch.setattr(tools, "this", SimpleNamespace(module=SimpleNamespace(get_redis_client=Redis)), raising=False)
    load("utils/stored_oauth_tokens.py", fresh=True)
    state.mod = load("utils/mcp_saved_login.py", fresh=True)
    state.request = SimpleNamespace(token_endpoint="https://auth.example.com/token", resource=SERVER, used_dcr=True)
    return state


def test_7643_non_admin_cannot_save_or_log_out(env):
    env.admin = False
    with pytest.raises(env.mod.SavedLoginRefused) as e:
        env.mod.load_login_credential(3, "uuid-m", 6)
    assert e.value.status == 403


@pytest.mark.parametrize("uuid_, status", [(None, 400), ("uuid-other", 404)])
def test_7643_missing_or_unknown_credential(env, uuid_, status):
    with pytest.raises(env.mod.SavedLoginRefused) as e:
        env.mod.load_login_credential(3, uuid_, 3)
    assert e.value.status == status


def test_7643_save_keeps_the_refresh_token_server_side(env):
    token_data = {"access_token": "at", "refresh_token": "rt", "expires_in": 60, "token_type": "Bearer",
                  "id_token": id_token({"preferred_username": "bot@example.com"})}

    result = env.mod.save_exchanged_login(3, env.credential, token_data, env.request,
                                          client_id="dcr-client", client_secret=None, scope="read")

    assert result == {"access_token": "at", "token_type": "Bearer", "expires_in": 60,
                      "account": "bot@example.com", "saved": True}
    [save] = env.saves
    assert save["configuration_uuid"] == "uuid-m" and save["expected_type"] == "mcp_login"
    assert save["fields"] == {"refresh_token": "rt", "token_endpoint": "https://auth.example.com/token",
                              "resource": SERVER, "used_dcr": True, "account": "bot@example.com",
                              "client_id": "dcr-client", "client_secret": None, "scopes": "read"}
    assert env.redis_keys == {"other"}  # the previous login's cached access token is gone


def test_7643_no_refresh_token_is_refused_and_nothing_saved(env):
    with pytest.raises(env.mod.SavedLoginRefused) as e:
        env.mod.save_exchanged_login(3, env.credential, {"access_token": "at"}, env.request, "c", None, None)
    assert (e.value.code, e.value.status) == ("no_refresh_token", 422) and env.saves == []


def test_7643_concurrent_change_is_a_conflict(env):
    env.save_result = False
    with pytest.raises(env.mod.SavedLoginRefused) as e:
        env.mod.save_exchanged_login(3, env.credential, {"access_token": "a", "refresh_token": "r"},
                                     env.request, "c", None, None)
    assert e.value.status == 409


@pytest.mark.parametrize("token, account", [
    (None, None), ("not-a-jwt", None), ("a.!!!.b", None), (id_token({"email": "e@x"}), "e@x"),
])
def test_7643_account_is_display_only_and_tolerant(env, token, account):
    assert env.mod._account_from({"id_token": token}) == account


# --- code review: the browser-chosen token_endpoint must be the one the credential's server advertises ----------

@pytest.fixture
def target(env, monkeypatch):
    advertised = {"https://auth.example.com/token"}
    monkeypatch.setattr(env.mod, "discovered_token_endpoints", lambda url: advertised if url == SERVER else set())
    return env.mod.check_login_target


def test_7643_advertised_endpoint_and_matching_resource_pass(target):
    target(SERVER, "https://auth.example.com/token", SERVER + "/")


@pytest.mark.parametrize("endpoint, resource, code", [
    ("https://attacker.example/token", SERVER, "access_denied"),
    (None, SERVER, "access_denied"),
    ("https://auth.example.com/token", None, "resource_mismatch"),  # resource is now required
    ("https://auth.example.com/token", "https://other.example.com/mcp", "resource_mismatch"),
])
def test_7643_secret_never_goes_to_another_endpoint(env, target, endpoint, resource, code):
    with pytest.raises(env.mod.SavedLoginRefused) as e:
        target(SERVER, endpoint, resource)
    assert e.value.code == code


def test_7643_discovery_follows_resource_metadata_to_the_issuer(env, monkeypatch):
    docs = {
        "https://mcp.example.com/.well-known/oauth-protected-resource/mcp": {
            "authorization_servers": ["https://idp.example.com/tenant"]},
        "https://idp.example.com/.well-known/oauth-authorization-server/tenant": {
            "token_endpoint": "https://idp.example.com/tenant/token"},
    }
    monkeypatch.setattr(env.mod, "_resource_metadata_urls", lambda url: [
        "https://mcp.example.com/.well-known/oauth-protected-resource/mcp"])
    monkeypatch.setattr(env.mod, "_get_json", lambda url, timeout=10: docs.get(url))

    assert env.mod.discovered_token_endpoints(SERVER) == {"https://idp.example.com/tenant/token"}
