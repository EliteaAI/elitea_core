"""#7401: the Connections check and the saved-login switch-off of utils/schedule_prerun.py.

A webhook may only use toolkits that run with no person signed in. A saved Microsoft login gets a real
refresh of the login stored on the server (the refresh itself is mocked); an expired or removed one
switches off the owner's triggers using that credential, with one notice. A transient error switches
nothing off. The module loads alone; the stored-token module is the real one, the RPC, Redis and the
writers are stand-ins.
"""
import re
import sys
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

UTILS = "plugins.elitea_core.utils"
OWNER = 12
AUTHORITY = "https://login.microsoftonline.com/tenant-1"


class Refused(ValueError):
    """mcp_oauth.TokenRefreshError: the OAuth `error` code rides on the exception."""

    def __init__(self, error):
        super().__init__(f"Token refresh failed: {error}")
        self.error = error


class FakeRedis:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, nx=False, ex=None):
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def setex(self, key, ttl, value):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


def outlook(uuid="uuid-o", title="team_outlook", private=False):
    return {"type": "outlook", "name": "Mail", "settings": {"outlook_configuration": {
        "configuration_uuid": uuid, "elitea_title": title, "private": private,
        "oauth_discovery_endpoint": AUTHORITY, "client_id": "c"}}}


def dataverse(uuid="uuid-d"):
    return {"type": "dataverse", "name": "CRM", "settings": {"dataverse_configuration": {
        "configuration_uuid": uuid, "elitea_title": "crm", "private": True, "tenant_id": "tenant-1",
        "url": "https://org.crm4.dynamics.com"}}}


def sharepoint(uuid="uuid-s", refresh_token=None, delegated=False):
    config = {"configuration_uuid": uuid, "elitea_title": "docs", "oauth_discovery_endpoint": AUTHORITY}
    if refresh_token:
        config["refresh_token"] = refresh_token
    if delegated:
        config["auth_mode"] = "entra_delegated"
    return {"type": "sharepoint", "name": "Docs", "settings": {"sharepoint_configuration": config}}


@pytest.fixture
def env(monkeypatch):
    state = SimpleNamespace(stored={}, refresh_error=None, refreshes=[], expanded={"tools": []}, subagents={},
                            switched=[], lookup_error=None, redis=FakeRedis())

    def get_stored_login(project_id, user_id, configuration_uuid, expected_type):
        if state.lookup_error:
            raise state.lookup_error
        return state.stored.get(configuration_uuid)

    rpc = SimpleNamespace(timeout=lambda seconds: SimpleNamespace(configurations_get_stored_login=get_stored_login))
    tools = sys.modules["tools"]
    monkeypatch.setattr(tools, "rpc_tools", SimpleNamespace(RpcMixin=lambda: SimpleNamespace(rpc=rpc)), raising=False)
    monkeypatch.setattr(tools, "this", SimpleNamespace(module=SimpleNamespace(get_redis_client=lambda: state.redis)),
                        raising=False)

    def refresh_token(**kwargs):
        state.refreshes.append(kwargs["refresh_token_value"])
        if state.refresh_error:
            raise state.refresh_error
        return {"access_token": "at", "expires_in": 3600}

    monkeypatch.setitem(sys.modules, f"{UTILS}.mcp_oauth", SimpleNamespace(refresh_token=refresh_token))
    monkeypatch.setitem(sys.modules, f"{UTILS}.subagent_prefetch", SimpleNamespace(
        expand_version_for_runtime=lambda *a: state.expanded,
        collect_subagent_version_details=lambda *a, **k: state.subagents))
    monkeypatch.setitem(sys.modules, f"{UTILS}.credential_decision", SimpleNamespace(DELEGATED_AUTH_MODE="entra_delegated"))
    monkeypatch.setitem(sys.modules, f"{UTILS}.internal_tools", SimpleNamespace(
        _INTERNAL_MCP_ENDPOINT_RE=re.compile(r"/app/(?:\{project_id\}|\d+)/mcp/(?P<suffix>[^?#]+)")))
    monkeypatch.setitem(sys.modules, f"{UTILS}.schedule_runs", SimpleNamespace(
        disable_by_credential=lambda *args: state.switched.append(args)))
    load("utils/stored_oauth_tokens.py", fresh=True)
    state.prerun = load("utils/schedule_prerun.py", fresh=True)

    def save_login(uuid, **extra):
        state.stored[uuid] = {"configuration_uuid": uuid, "refresh_token": f"rt-{uuid}", "client_id": "c",
                              "oauth_discovery_endpoint": AUTHORITY, "tenant_id": "tenant-1",
                              "url": "https://org.crm4.dynamics.com", **extra}
    state.save_login = save_login
    state.check = lambda refresh=True: state.prerun.check_connections(34, 5, 7, OWNER, refresh)
    return state


def statuses(result):
    return [(row["toolkit"], row["status"]) for row in result["rows"]]


# --- one row per toolkit --------------------------------------------------------------------------------------------

def test_7401_saved_logins_are_ok_after_a_real_refresh_and_the_token_is_cached(env):
    env.expanded = {"tools": [outlook(), dataverse(), {"type": "github", "name": "GH", "settings": {}}]}
    env.save_login("uuid-o")
    env.save_login("uuid-d")

    result = env.check()

    assert statuses(result) == [("Mail", "ok"), ("CRM", "ok"), ("GH", "ok")]
    assert env.refreshes == ["rt-uuid-o", "rt-uuid-d"]
    assert any(key.startswith("oauth:stored:at:uuid-o:") for key in env.redis.data)  # the run reuses it
    assert result["credential_refs"] == ["uuid-d", "uuid-o"]
    assert env.prerun.connections_ready(result)


def test_7401_a_credential_without_a_saved_login_needs_a_sign_in(env):
    env.expanded = {"tools": [outlook()]}

    [row] = env.check()["rows"]

    assert (row["status"], row["login"], row["credential_title"]) == ("sign_in_required", "gone", "team_outlook")
    assert env.refreshes == []


@pytest.mark.parametrize("error, login", [
    (Refused("invalid_grant"), "expired"),
    (Refused("invalid_client"), "expired"),  # moved to another admin-managed app (#7149)
    (Refused("temporarily_unavailable"), "transient"),
    (TimeoutError("read timed out"), "transient"),
])
def test_7401_an_expired_login_is_told_apart_from_a_transient_error(env, error, login):
    env.expanded = {"tools": [outlook()]}
    env.save_login("uuid-o")
    env.refresh_error = error

    result = env.check()

    assert result["rows"][0]["status"] == "sign_in_required" and result["rows"][0]["login"] == login
    assert [row["login"] for row in env.prerun.lost_logins(result)] == (["expired"] if login == "expired" else [])


def test_7401_a_lookup_outage_is_transient_never_a_logout(env):
    env.expanded = {"tools": [outlook()]}
    env.lookup_error = RuntimeError("rpc down")

    result = env.check()

    assert result["rows"][0]["login"] == "transient" and env.prerun.lost_logins(result) == []


def test_7401_a_version_save_checks_without_calling_the_token_endpoint(env):
    env.expanded = {"tools": [outlook()]}
    env.save_login("uuid-o")

    assert statuses(env.check(refresh=False)) == [("Mail", "ok")] and env.refreshes == []


@pytest.mark.parametrize("tool", [
    {"type": "mcp", "name": "Remote", "settings": {"url": "https://mcp.example.com/sse"}},
    {"type": "mcp_github", "name": "Remote GH", "settings": {"url": "https://api.example.com/mcp"}},
    {"type": "openapi", "name": "API", "settings": {"openapi_configuration": {
        "configuration_uuid": "uuid-a", "oauth_discovery_endpoint": "https://idp.example.com"}}},
], ids=["remote-mcp", "mcp-type", "openapi-oauth"])
def test_7401_a_browser_only_sign_in_is_unsupported(env, tool):
    env.expanded = {"tools": [tool]}

    [row] = env.check()["rows"]

    assert row["status"] == "unsupported" and row["reason"] == env.prerun.BROWSER_ONLY_TEXT


def test_7401_a_sharepoint_credential_without_a_saved_login_offers_a_sign_in(env):
    # Refused at save like the browser-only toolkits, but the owner can fix it from the panel (it saves the login)
    env.expanded = {"tools": [sharepoint()]}

    [row] = env.check()["rows"]

    assert (row["status"], row["login"]) == ("sign_in_required", "gone")


def test_7401_a_sharepoint_credential_with_a_saved_login_is_refreshed(env):
    env.expanded = {"tools": [sharepoint(refresh_token="stored")]}
    env.save_login("uuid-s")

    assert statuses(env.check()) == [("Docs", "ok")] and env.refreshes == ["rt-uuid-s"]


def test_7401_the_platform_mcp_servers_run_with_the_run_token(env):
    env.expanded = {"tools": [{"type": "mcp", "name": "ELITEA", "meta": {"mcp": True},
                               "settings": {"url": "http://pylon_main:8080/app/34/mcp/agents"}}]}

    assert statuses(env.check()) == [("ELITEA", "ok")]


def test_7401_an_entra_delegated_credential_is_unsupported(env):
    env.expanded = {"tools": [sharepoint(refresh_token="stored", delegated=True)]}

    [row] = env.check()["rows"]

    assert row["status"] == "unsupported" and "signed-in person" in row["reason"] and env.refreshes == []


def test_7401_sub_agent_toolkits_are_checked_too(env):
    env.expanded = {"tools": [{"type": "application", "name": "Helper", "settings": {}}]}
    env.subagents = {"9:19": {"version_details": {"tools": [outlook(uuid="uuid-sub")]}}}

    result = env.check()

    assert statuses(result) == [("Mail", "sign_in_required")] and result["credential_refs"] == ["uuid-sub"]


def test_7401_a_credential_error_is_a_sign_in_row_with_no_refs(env):
    env.expanded = {"error": "x", "credential_errors": [{"error_type": "credential_not_found", "credential_id": "jira"}]}

    result = env.check()

    assert result == {"rows": [{"toolkit": None, "credential_title": "jira", "status": "sign_in_required",
                                "reason": "credential jira not found"}], "credential_refs": None}


# --- pre-run: an expired or removed login switches the owner's triggers off ------------------------------------------

def schedule(id_=3, kind="webhook", refs=()):
    return SimpleNamespace(id=id_, created_by=OWNER, application_id=5, version_id=7, trigger_kind=kind,
                           credential_refs=list(refs))


@pytest.mark.parametrize("private, title", [(True, "mine"), (False, "team_outlook")])
def test_7401_an_expired_login_before_a_run_switches_off_the_triggers_using_it(env, private, title):
    env.save_login("uuid-o")
    env.refresh_error = Refused("invalid_grant")

    reason = env.prerun.check_saved_logins(34, schedule(), {"tools": [outlook(title=title, private=private)]})

    assert isinstance(reason, env.prerun.LoginLost) and title in reason
    assert env.switched == [(34, OWNER, "uuid-o", "credential_expired", title, private, 3)]


def test_7401_a_removed_login_logout_switches_off_like_an_expired_one(env):
    reason = env.prerun.check_saved_logins(34, schedule(), {"tools": [outlook()]})

    assert isinstance(reason, env.prerun.LoginLost) and env.switched[0][2:4] == ("uuid-o", "credential_expired")


def test_7401_a_cron_schedule_whose_login_was_never_saved_keeps_waiting_for_the_owner(env):
    # #7304 / #7495: under "Pause and ask me" the run waits for a sign-in; nothing is switched off
    assert env.prerun.check_saved_logins(34, schedule(kind="cron"), {"tools": [outlook()]}) is None
    assert env.switched == []


def test_7401_a_cron_schedule_whose_saved_login_was_removed_is_switched_off(env):
    # The refs hold the credential: it had a login when the trigger was saved or checked, so Logout removed it
    reason = env.prerun.check_saved_logins(34, schedule(kind="cron", refs=["uuid-o"]), {"tools": [outlook()]})

    assert isinstance(reason, env.prerun.LoginLost) and env.switched[0][2] == "uuid-o"


def test_7401_a_version_without_saved_logins_makes_no_lookup(env):
    env.lookup_error = AssertionError("no stored-login lookup for a plain toolkit")

    assert env.prerun.check_saved_logins(34, schedule(), {"tools": [{"type": "github", "settings": {}}]}) is None


def test_7401_a_transient_refresh_error_lets_the_run_go_on(env):
    env.save_login("uuid-o")
    env.refresh_error = Refused("temporarily_unavailable")

    assert env.prerun.check_saved_logins(34, schedule(), {"tools": [outlook()]}) is None and env.switched == []


def test_7401_a_test_run_never_switches_anything_off(env):
    assert env.prerun.check_saved_logins(34, schedule(None), {"tools": [outlook()]}) is None and env.switched == []


def test_7401_a_lost_login_is_not_counted_and_not_parked(env, monkeypatch):
    # The switch-off already happened and sent its one notice: no streak step, no wait, no second notice
    prerun = env.prerun
    lost = prerun.LoginLost("the saved login of credential x stopped working")
    failed, notices, counted = [], [], []
    monkeypatch.setattr(prerun, "check_principal", lambda s: None)
    monkeypatch.setattr(prerun, "check_owner_access", lambda p, o: None)
    monkeypatch.setattr(prerun, "check_owner_rights", lambda *a: None)
    monkeypatch.setattr(prerun, "check_owner_credentials", lambda p, s: lost)
    monkeypatch.setattr(prerun, "_park", lambda *a: pytest.fail("a lost login never waits"))
    monkeypatch.setitem(sys.modules, f"{UTILS}.schedule_runs", SimpleNamespace(
        fail_schedule_run=lambda *a: failed.append(a), disable_schedule=lambda *a: None,
        count_refusal=lambda *a, **k: counted.append(a), PAUSED_AFTER_PRERUN="", PRERUN_REFUSED=""))
    monkeypatch.setitem(sys.modules, f"{UTILS}.schedule_notifications", SimpleNamespace(
        notify_schedule_event=lambda *a: notices.append(a)))
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.enums.all", SimpleNamespace(
        NotificationEventTypes=SimpleNamespace(schedule_run_failed="f", schedule_disabled="d")))
    row = schedule()
    row.principal = {"kind": "delegation", "ref": OWNER}
    row.entity_type = "agent"

    result = prerun.run_prerun_check(34, row, 99)

    assert result.ok is False and failed == [(34, 99, lost)] and notices == [] and counted == []


# --- after a version save --------------------------------------------------------------------------------------------

class FakeQuery:
    def __init__(self, store, rows):
        self.store, self.rows = store, rows

    def filter(self, *conditions):
        return self

    def all(self):
        return self.rows

    def update(self, values):
        self.store.append(values)


@pytest.fixture
def recheck(env, monkeypatch):
    import contextlib
    env.updates, env.notices = [], []
    env.triggers = [SimpleNamespace(id=1, application_id=5, created_by=OWNER, trigger_kind="webhook", name="hook"),
                    SimpleNamespace(id=2, application_id=5, created_by=OWNER, trigger_kind="cron", name="nightly")]
    # FakeQuery ignores the filters: the columns only need to build an expression
    column = SimpleNamespace(is_=lambda value: True)
    model = type("EntitySchedule", (), {name: column for name in (
        "id", "application_id", "created_by", "trigger_kind", "name", "version_id", "enabled")})
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.entity_schedule", SimpleNamespace(EntitySchedule=model))
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.enums.all", SimpleNamespace(
        NotificationEventTypes=SimpleNamespace(schedule_disabled="schedule_disabled")))
    monkeypatch.setitem(sys.modules, f"{UTILS}.schedule_notifications", SimpleNamespace(
        notify_schedule_event=lambda *args, **kwargs: env.notices.append((args, kwargs))))

    @contextlib.contextmanager
    def session(_project_id):
        yield SimpleNamespace(query=lambda *a: FakeQuery(env.updates, env.triggers), commit=lambda: None)

    monkeypatch.setattr(sys.modules["tools"], "db", SimpleNamespace(get_session=session), raising=False)
    return env


def test_7401_an_edit_adding_a_browser_only_toolkit_switches_its_webhooks_off(recheck):
    recheck.expanded = {"tools": [{"type": "mcp", "name": "Remote", "settings": {"url": "https://x.example.com/sse"}}]}

    recheck.prerun.recheck_version_triggers(34, 7)

    webhook, cron = recheck.updates
    assert webhook["enabled"] is False and webhook["disabled_reason"] == "connections_changed"
    assert "enabled" not in cron  # a schedule may wait for a sign-in (#7304): refs only
    [(args, kwargs)] = recheck.notices
    assert args[2] == "schedule_disabled" and "the agent changed" in kwargs["extra"]["reason"]
    assert recheck.refreshes == []  # static: no token call on a version save


def test_7401_an_edit_that_keeps_the_saved_logins_only_refreshes_the_refs(recheck):
    recheck.expanded = {"tools": [outlook(uuid="uuid-new")]}
    recheck.save_login("uuid-new")

    recheck.prerun.recheck_version_triggers(34, 7)

    assert recheck.updates == [{"credential_refs": ["uuid-new"]}] * 2 and recheck.notices == []


def test_7401_a_version_that_did_not_expand_switches_nothing_off(recheck):
    # An outage while expanding is not a change of the connections
    recheck.expanded = {"error": "rpc timeout"}

    recheck.prerun.recheck_version_triggers(34, 7)

    assert recheck.updates == [] and recheck.notices == []


def test_7401_every_credential_of_a_toolkit_is_checked(env):
    tool = {"type": "outlook", "name": "Mail", "settings": {
        **outlook()["settings"], "teams_configuration": {**outlook(uuid="uuid-t")["settings"]["outlook_configuration"]}}}
    env.expanded = {"tools": [tool]}
    env.save_login("uuid-o")

    [row] = env.check()["rows"]

    assert (row["status"], row["credential_uuid"]) == ("sign_in_required", "uuid-t")


def test_7401_a_failing_recheck_never_breaks_the_save(recheck, monkeypatch):
    monkeypatch.setattr(recheck.prerun, "check_connections", lambda *a, **k: 1 / 0)

    recheck.prerun.recheck_version_triggers(34, 7)  # logs, does not raise


def test_7401_a_link_without_a_version_starts_no_recheck(env, monkeypatch):
    started = []
    monkeypatch.setattr(env.prerun, "recheck_version_triggers", lambda *a: started.append(a))

    env.prerun.recheck_version_triggers_later(34, None)

    assert started == []
