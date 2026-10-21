"""C-1009 performance PR 3: the counts of admission, validation, release, audit only, fan-out, probes, owner scan and lists.

Each test pins a number of sessions, statements, secrets calls, Redis round trips or bus calls, so a change that makes a
path more expensive again fails here. Real models and statements on SQLite files (see ``fixtures/admission_env.py``);
the secrets plugin, Redis, the task node and the identity service are fakes that record their calls.
"""
import copy
import json
import sys
import threading
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env, mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.dispatch_wrapper import Original, build_wrapper  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import (  # noqa: E402  pylint: disable=C0413
    CALLER, RUN, SECRET, SUBJECT, Credential, call_kwargs, dispatch, payload_of, run_to_running, sealed_tool_settings,
    seeded,
)

USER = 42


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    ns = mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("perf_admission_release"))

    class Application(ns.Base):   # the one table of the project schema the binding list reads names from
        __tablename__ = "applications"
        __table_args__ = {"schema": "tenant"}
        id = ns.sa.Column(ns.sa.Integer, primary_key=True)
        name = ns.sa.Column(ns.sa.String, nullable=False)

    ns.Application = Application
    sys.modules[f"{admission_env.PKG}.models.all"].Application = Application
    yield ns
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.permissions[USER] = {"credentials.probe", "credentials.use"}
    return ns.env


def statements(env, project_id=1):
    seen = []
    engine = env.engine(project_id)
    listener = lambda conn, cursor, statement, params, context, many: seen.append(statement)   # noqa: E731
    env.ns.sa.event.listen(engine, "before_cursor_execute", listener)
    return seen, lambda: env.ns.sa.event.remove(engine, "before_cursor_execute", listener)


def kinds(seen):
    out = {"select": 0, "write": 0}
    for text in seen:
        out["select" if text.lstrip().upper().startswith("SELECT") else "write"] += 1
    return out


def credentials(env, count):
    first = seeded(env)
    return [first] + [Credential(env, first.binding, tool_id=index + 1, config_id=5 + index) for index in range(1, count)]


# --- M4: the reads of an admission --------------------------------------------------------------------------------

@pytest.mark.parametrize("count", [1, 3, 6])
def test_an_admission_reads_the_agent_in_one_session_and_one_version_and_toolkit_read(ns, env, count):
    creds = credentials(env, count)
    seen, stop = statements(env)
    before = len(env.sessions)
    dispatch(env, ns, *creds)
    stop()
    opened = env.sessions[before:]
    assert len(opened) == 2                         # the reads of the agent, and the context row with its events
    # version, binding, toolkit rows | max(attempt), insert of the context: whatever the number of toolkits
    assert kinds(seen) == {"select": 4, "write": 1} and len(seen) == 5


def test_the_agent_session_is_closed_before_the_facts_and_schemas_are_asked(ns, env):
    creds = credentials(env, 2)
    open_at = []
    real = env.ns.tools.this.for_module

    def watched(name):
        open_at.append((name, env.open_sessions))
        return real(name)

    env.ns.tools.this.for_module = watched
    try:
        dispatch(env, ns, *creds)
    finally:
        env.ns.tools.this.for_module = real
    assert [n for n in open_at if n[0] in ("configurations", "secrets")] and all(
        count == 0 for name, count in open_at if name in ("configurations", "secrets"))


def test_the_facts_of_the_credentials_are_one_secrets_call_when_the_plugin_joins_the_grants(ns, env):
    creds = credentials(env, 3)
    env.facts_version = 3
    before = len(env.secrets_calls)
    dispatch(env, ns, *creds)
    assert len(env.secrets_calls) - before == 2     # references, then grants (facts v3)
    env.facts_version = 4
    before = len(env.secrets_calls)
    call, ref = dispatch(env, ns, *creds)
    assert ref and len(env.secrets_calls) - before == 1
    [asked] = env.grants_for_calls[-1:]
    assert asked == sorted((str(c.binding), str(c.tool_id), c.config_id, "*", SUBJECT) for c in creds)


def test_more_than_64_uses_fall_back_to_the_two_reads(ns, env):
    env.facts_version = 4
    creds = credentials(env, 1)
    ctx = ns.credential_decision.CredentialDecisionContext(
        binding_project_id=1, agent_binding_ref=str(creds[0].binding), subject_ref=SUBJECT, policy_revision="r",
        now=ns.adm._now(), expires_at=ns.adm._now(), can_use=lambda: True)  # pylint: disable=W0212
    uses = [ns.credential_decision.CredentialUse(str(index), "credentials", creds[0].uuid, "uuid", 1) for index in range(70)]
    before = len(env.secrets_calls)
    ns.credential_decision._read_decision_facts(ctx, uses, {creds[0].uuid: env.configurations[(1, creds[0].uuid)]})  # pylint: disable=W0212
    assert len(env.secrets_calls) - before == 2


# --- M5: the recheck is one secrets call ----------------------------------------------------------------------------

def test_a_recheck_is_one_secrets_call(ns, env):
    creds = credentials(env, 3)
    call, ref = dispatch(env, ns, *creds)
    before = len(env.secrets_calls)
    assert ns.adm.validate_context(env.module(), 1, ref, CALLER)["ok"] is True
    assert len(env.secrets_calls) - before == 1


# --- M6: validate_context holds no connection across the identity service and the facts -----------------------------

def test_validate_context_holds_no_session_while_it_asks_the_identity_service_or_the_facts(ns, env):
    creds = credentials(env, 2)
    call, ref = dispatch(env, ns, *creds)
    seen = {}
    real_epochs = env.ns.tools.auth.read_identity_epochs
    real_recheck = ns.adm.recheck_credentials

    def epochs(kind, auth_id):
        seen["epochs"] = env.open_sessions
        return real_epochs(kind, auth_id)

    def recheck(*args):
        seen["recheck"] = env.open_sessions
        return real_recheck(*args)

    env.ns.tools.auth.read_identity_epochs, ns.adm.recheck_credentials = epochs, recheck
    try:
        env.max_open_sessions = 0
        before = len(env.sessions)
        assert ns.adm.validate_context(env.module(), 1, ref, CALLER)["ok"] is True
    finally:
        env.ns.tools.auth.read_identity_epochs, ns.adm.recheck_credentials = real_epochs, real_recheck
    assert seen == {"epochs": 0, "recheck": 0}
    assert env.max_open_sessions == 1               # never two connections at once
    assert len(env.sessions) - before == 2          # the read (context and binding), and the state write


def test_validate_context_still_revokes_in_a_session_of_its_own(ns, env):
    creds = credentials(env, 1)
    call, ref = dispatch(env, ns, *creds)
    env.epochs[USER] = {"active": True, "user": 99, "token": 0}
    result = ns.adm.validate_context(env.module(), 1, ref, CALLER)
    assert (result["ok"], result["reason_code"]) == (False, "EPOCH_MISMATCH")
    [row] = env.query(1, ns.models.ExecutionSecurityContext)
    assert (row.state, row.revoked_reason) == ("revoked", "EPOCH_MISMATCH") and env.open_sessions == 0


# --- M3 + S12: audit only ------------------------------------------------------------------------------------------

def audit_dispatch(ns, env, payload, spy=None):
    module = env.module(governed=False, audit=True)
    log = []

    class Ordered(Original):
        def __call__(self, *args, **kwargs):
            log.append(("start_task", env.tracing_calls))
            return super().__call__(*args, **kwargs)

    original = Ordered()
    wrapper = build_wrapper(ns, module, original)
    call = {"args": ["s", "m"], "kwargs": {"project_id": 1, "message_id": 5, "_elitea_predict_run_id": RUN, **payload},
            "pool": "agents", "meta": {"project_id": 1}, "admission": env.request()}
    task_id = wrapper("indexer_agent", **call)
    return task_id, original.calls, log


def test_audit_only_starts_the_task_before_the_shadow_admission(ns, env):
    creds = credentials(env, 2)
    task_id, calls, log = audit_dispatch(ns, env, payload_of(*creds))
    assert task_id == "task-1" and log == [("start_task", 0)]      # no audit call had happened when the task started
    assert env.tracing_calls > 0 and len(env.verify_calls) == 1      # and the admission ran after it
    assert ns.adm.ADMISSION_DURATION.count("audit_only", "would_allow") >= 1   # the histogram still records it


def test_audit_only_copies_the_toolkit_lists_and_nothing_else(ns, env, monkeypatch):
    creds = credentials(env, 2)
    history = [{"role": "user", "content": f"message {index}"} for index in range(500)]
    payload = payload_of(*creds, chat_history=history, user_input="hello")
    payload["application"]["version_details"]["instructions"] = "x" * 10000
    payload["tools"] = payload["application"]["version_details"]["tools"]      # shared tool dicts, as predict builds them
    copied = []
    real = copy.deepcopy

    def spy(value, memo=None):
        copied.append(value)
        return real(value, memo)

    monkeypatch.setattr(ns.adm, "copy", types.SimpleNamespace(deepcopy=spy))
    audit_dispatch(ns, env, payload)
    assert copied and all(isinstance(item, list) for item in copied)
    assert all(isinstance(tool, dict) and "settings" in tool for item in copied for tool in item)
    assert not [item for item in copied if item is history or item is payload]
    assert len(copied) == 2                          # the request's tools and the version's tools: nothing else


def test_the_toolkit_snapshot_keeps_the_sharing_of_tool_dicts(ns, env):
    tool = {"id": 1, "settings": {"a": {"configuration_type": "x"}}}
    payload = {"project_id": 1, "tools": [tool], "chat_history": [1, 2, 3],
               "application": {"version_details": {"tools": [tool], "instructions": "i"},
                               "subagent_version_details": {"9": {"tools": [{"id": 2, "settings": {}}], "x": 1}}}}
    snapshot = ns.adm._toolkit_snapshot(payload)   # pylint: disable=W0212
    assert snapshot["tools"][0] is snapshot["application"]["version_details"]["tools"][0] and snapshot["tools"][0] is not tool
    assert snapshot["tools"][0] == tool and "chat_history" not in snapshot
    assert snapshot["application"]["subagent_version_details"] == {"9": {"tools": [{"id": 2, "settings": {}}]}}
    assert "instructions" not in snapshot["application"]["version_details"]


def test_audit_only_sends_the_task_exactly_what_off_sends(ns, env):
    creds = credentials(env, 2)
    payload = payload_of(*creds)
    _, audited, _ = audit_dispatch(ns, env, copy.deepcopy(payload))
    original = Original()
    wrapper = build_wrapper(ns, env.module(governed=False), original)
    wrapper("indexer_agent", args=["s", "m"], kwargs={"project_id": 1, "message_id": 5, "_elitea_predict_run_id": RUN,
                                                    **copy.deepcopy(payload)}, pool="agents", meta={"project_id": 1})
    assert audited == original.calls


# --- S1: the schemas of provider-hub and MCP toolkits ---------------------------------------------------------------

def test_the_secret_fields_of_a_dynamic_type_are_found_once_until_the_registry_changes(ns, env):
    env.seed_agent()
    for index, kind in enumerate(("mcp_a", "mcp_b", "mcp_c")):
        env.schemas[kind] = {"properties": {"api_key": {"secret": True}}}
        env.add_toolkit(1, 70, 100 + index, kind, {"url": "https://x"})
    ns.adm.admit_execution(env.module(), env.request(), run_id="run-a", payload={})
    assert sorted(call[0] for call in env.schema_calls) == ["mcp_a", "mcp_b", "mcp_c"]
    env.schema_calls.clear()
    ns.adm.admit_execution(env.module(), env.request(), run_id="run-b", payload={})
    assert env.schema_calls == []
    ns.schema_registry_version.bump()
    ns.adm.admit_execution(env.module(), env.request(), run_id="run-c", payload={})
    assert len(env.schema_calls) == 3


def test_a_type_without_a_schema_is_looked_for_every_time(ns, env):
    env.seed_agent()
    env.add_toolkit(1, 70, 100, "unknown", {"url": "https://x"})
    ns.adm.admit_execution(env.module(), env.request(), run_id="run-a", payload={})
    ns.adm.admit_execution(env.module(), env.request(), run_id="run-b", payload={})
    assert len(env.schema_calls) == 2


def test_the_registry_counter_moves_on_every_provider_and_server_change():
    source = {name: (PLUGIN_ROOT / name).read_text() for name in (
        "methods/providers.py", "utils/mcp_servers_storage.py", "methods/toolkits.py")}
    assert source["methods/providers.py"].count("schema_registry_version.bump()") == 2
    assert source["methods/toolkits.py"].count("schema_registry_version.bump()") == 2
    assert source["utils/mcp_servers_storage.py"].count("schema_registry_version.bump()") == 4
    assert "schema_registry_version.bump()" in (PLUGIN_ROOT / "module.py").read_text()


# --- S3: one walk of a toolkit's settings per dispatch --------------------------------------------------------------

@pytest.mark.parametrize("audit", [False, True])
def test_the_settings_of_a_toolkit_are_walked_once_for_the_decision_and_the_seal(ns, env, monkeypatch, audit):
    creds = credentials(env, 3)
    walked = []
    scanner = sys.modules[f"{admission_env.PKG}.utils.application_utils"]
    real = scanner._find_configurations_in_settings   # pylint: disable=W0212

    def spy(settings):
        walked.append(id(settings))
        return real(settings)

    monkeypatch.setattr(scanner, "_find_configurations_in_settings", spy)
    if audit:
        audit_dispatch(ns, env, payload_of(*creds))
    else:
        dispatch(env, ns, *creds)
    assert len(walked) == 3 and len(set(walked)) == 3


# --- S4: a fan-out --------------------------------------------------------------------------------------------------

def test_the_children_of_a_fan_out_read_the_parent_decisions_once(ns, env):
    creds = credentials(env, 2)
    call, ref = dispatch(env, ns, *creds)

    def child():
        child_call = call_kwargs(payload_of(*creds), ref)
        ns.adm.admit_dispatch(env.module(), "indexer_agent", child_call, None)

    before = len(env.sessions)
    with ns.adm.parent_entries_scope():
        for _ in range(8):
            child()
    assert len(env.sessions) - before == 1
    before = len(env.sessions)
    child()
    child()
    assert len(env.sessions) - before == 2          # outside a fan-out each dispatch reads, as before


def test_a_fan_out_scope_does_not_leak_into_the_next_dispatch(ns, env):
    creds = credentials(env, 1)
    call, ref = dispatch(env, ns, *creds)
    with ns.adm.parent_entries_scope():
        ns.adm._parent_entries(1, ref)   # pylint: disable=W0212
    before = len(env.sessions)
    ns.adm._parent_entries(1, ref)   # pylint: disable=W0212
    assert len(env.sessions) - before == 1


def test_reopening_the_contexts_of_a_fan_out_is_one_statement_and_one_redis_round_trip(ns, env):
    env.seed_agent()
    module = env.module()
    refs = [ns.adm.admit_execution(module, env.request(), run_id=f"run-{index}").context_ref for index in range(3)]
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.models.ExecutionSecurityContext).values(state="completed"))
        session.commit()
    seen, stop = statements(env)
    before, calls = len(env.sessions), len(env.redis.calls)
    assert ns.adm.reopen_contexts(module, 1, refs + [refs[0], None, "not-a-uuid"]) == 3
    stop()
    assert len(env.sessions) - before == 1
    assert [text for text in seen if text.lstrip().upper().startswith("UPDATE")] == seen and len(seen) == 1
    assert [call[0] for call in env.redis.calls[calls:]] == ["set"] * 3
    assert all(env.redis.ttls[ns.adm.INHERITED_KEY + ref] == 3600 for ref in refs)


class Run:
    """An admitted and validated run of ``count`` credentials, and the batch its worker would ask for."""

    def __init__(self, ns, env, count=3):
        self.ns, self.env = ns, env
        self.creds = credentials(env, count)
        self.call, self.ref = dispatch(env, ns, *self.creds)
        run_to_running(env, ns, self.ref)
        self.private, self.public = ns.keypair()
        self.slots = [sealed_tool_settings(self.call, index)["credential_slot"] for index in range(count)]
        env.denied.clear()
        env.recorded.clear()
        env.release_calls.clear()
        env.redis.calls.clear()
        env.log.lines.clear()
        env.secrets_calls.clear()
        env.sessions.clear()

    def request(self, **over):
        values = dict(project_id=1, context_ref=self.ref, probe_ref=None, caller=dict(CALLER), recipient_key=self.public,
                      slots=[{"toolkit_instance_id": str(cred.tool_id), "field": "credentials", "slot": dict(slot)}
                             for cred, slot in zip(self.creds, self.slots)])
        values.update(over)
        return values

    def release(self, **over):
        return self.ns.credential_release.release_credentials(self.env.module(), **self.request(**over))

    def open(self, release):
        decision = next(slot["decision_id"] for slot in self.slots if slot["credential_ref"] == self._ref_of(release))
        aad = "|".join(("v1", "1", self.ref, release["toolkit_instance_id"], release["field"], decision,
                        release["release_id"])).encode()
        return json.loads(self.ns.transport.open_sealed(release["sealed"], self.private, aad))

    def _ref_of(self, release):
        return next(c.ref_id for c in self.creds if str(c.tool_id) == release["toolkit_instance_id"])


@pytest.fixture
def sealed(ns):
    ns.need_transport()


def test_a_context_a_fan_out_reopened_has_the_higher_release_bound(ns, env, sealed):
    run = Run(ns, env, 1)
    limit, higher = ns.credential_release.MAX_RELEASES_PER_FIELD, ns.credential_release.MAX_RELEASES_PER_FIELD_INHERITED
    assert (limit, higher) == (16, 128)
    key = f"{ns.credential_release.COUNTER_KEY}{run.ref}|1|credentials"
    env.redis.values[key] = limit
    assert run.release()["reason_code"] == "RATE_LIMITED"                   # the 17th of a plain context
    env.redis.values[key] = limit
    ns.adm._mark_inherited(env.module(), [uuid.UUID(run.ref)])   # pylint: disable=W0212
    env.redis.values[key] = limit
    assert run.release()["ok"] is True
    env.redis.values[key] = higher
    reply = run.release()
    assert reply["ok"] is False and reply["reason_code"] == "RATE_LIMITED"
    assert (reply["toolkit_instance_id"], reply["field"]) == ("1", "credentials")


def test_the_inherited_mark_is_only_asked_for_once_the_lower_bound_is_passed(ns, env, sealed):
    run = Run(ns, env, 1)
    assert run.release()["ok"] is True
    assert "exists" not in [call[0] for call in env.redis.calls]


# --- M1: the release of a batch ---------------------------------------------------------------------------------

@pytest.mark.parametrize("count", [1, 3, 6])
@pytest.mark.parametrize("store", ["batch", "single"])
def test_a_batch_release_is_one_read_one_audit_transaction_one_counter_round_trip_and_one_recheck(ns, env, sealed, count, store):
    env.facts_version = 4
    env.batch_store = store == "batch"
    run = Run(ns, env, count)
    seen, stop = statements(env)
    sent = env.redis.executes
    reply = run.release()
    stop()
    assert env.redis.executes - sent == 1                           # every counter of the batch in one round trip
    assert reply["ok"] is True and len(reply["releases"]) == count
    opened = env.sessions
    assert len(opened) == 2                                         # the read, and the audit events: whatever the count
    assert sum(item[1].commits for item in opened) == 1
    assert kinds(seen) == {"select": 2, "write": 0}                 # the context row and its binding
    assert len(env.redis.calls) == 2 * count                        # INCR + EXPIRE per use, in one pipeline
    assert len(env.secrets_calls) == 1                              # the recheck of all the uses: references and grants
    if store == "batch":
        [(project_id, items, release_id, actor)] = env.batch_release_calls
        assert len(items) == count and release_id == reply["releases"][0]["release_id"] and env.release_calls == []
    else:
        assert len(env.release_calls) == count and env.batch_release_calls == []
    assert len(env.recorded) == count and {session for session, _ in env.recorded} == {opened[-1][1]}
    assert env.denied == []


def test_each_slot_of_a_batch_is_sealed_under_its_own_binding(ns, env, sealed):
    run = Run(ns, env, 3)
    reply = run.release()
    assert [(r["toolkit_instance_id"], r["field"]) for r in reply["releases"]] == [("1", "credentials"), ("2", "credentials"), ("3", "credentials")]
    assert len({r["release_id"] for r in reply["releases"]}) == 3
    for release in reply["releases"]:
        cred = next(c for c in run.creds if str(c.tool_id) == release["toolkit_instance_id"])
        body = run.open(release)
        assert body["fields"] == {"token": f"{SECRET}-token"} and body["generation"] == 1
        other = next(r for r in reply["releases"] if r is not release)
        wrong = "|".join(("v1", "1", run.ref, other["toolkit_instance_id"], "credentials", "x", other["release_id"]))
        with pytest.raises(run.ns.transport.TransportError):   # a slot's ciphertext opens under no other slot's binding
            run.ns.transport.open_sealed(release["sealed"], run.private, wrong.encode())
        assert cred.ref_id
    assert SECRET not in json.dumps({k: v for k, v in reply.items()}, default=str)


def test_the_events_of_a_batch_are_one_credential_acquired_per_slot_and_carry_no_value(ns, env, sealed):
    run = Run(ns, env, 3)
    reply = run.release()
    events = [event for _, event in env.recorded]
    assert [event["event_id"] for event in events] == [r["release_id"] for r in reply["releases"]]
    assert {(e["event_type"], e["decision"], e["reason_code"]) for e in events} == {
        ("credential_acquired", "allow", "CREDENTIAL_RELEASED")}
    assert [e["toolkit_instance_id"] for e in events] == ["1", "2", "3"]
    assert SECRET not in json.dumps(events, default=str) and SECRET not in "\n".join(env.log.lines)


def test_the_first_failure_refuses_the_whole_batch_names_the_slot_and_audits_every_slot(ns, env, sealed):
    env.batch_store = True
    run = Run(ns, env, 3)
    run.creds[1].reference["state"] = "suspended"
    reply = run.release()
    assert reply["ok"] is False and "releases" not in reply and "sealed" not in json.dumps(reply)
    assert reply["reason_code"] == "CREDENTIAL_REFERENCE_INACTIVE"
    assert (reply["toolkit_instance_id"], reply["field"]) == ("2", "credentials")
    events = [event for _, event in env.recorded]
    assert len(events) == 3 and {e["decision"] for e in events} == {"deny"}
    assert {e["reason_code"] for e in events} == {reply["reason_code"]} and len(env.sessions) >= 2
    assert env.denied == [] and SECRET not in json.dumps(reply)


def test_a_refusal_of_the_store_names_the_slot_of_its_reference(ns, env, sealed):
    env.batch_store = True
    run = Run(ns, env, 3)
    env.batch_reply = {"ok": False, "reason_code": "STORE_UNAVAILABLE", "ref_id": run.creds[2].ref_id}
    reply = run.release()
    assert (reply["reason_code"], reply["toolkit_instance_id"], reply["field"]) == ("STORE_UNAVAILABLE", "3", "credentials")
    assert [e["decision"] for _, e in env.recorded] == ["deny"] * 3 and (
        env.query(1, ns.models.ExecutionSecurityContext)[0].state == "running")


def test_a_slot_the_context_was_not_admitted_for_refuses_the_batch_before_anything_is_counted_or_read(ns, env, sealed):
    run = Run(ns, env, 3)
    request = run.request()
    request["slots"][2]["field"] = "other"
    reply = run.ns.credential_release.release_credentials(env.module(), **request)
    assert (reply["reason_code"], reply["toolkit_instance_id"], reply["field"]) == ("CREDENTIAL_REFERENCE_REQUIRED", "3", "other")
    assert env.redis.calls == [] and env.release_calls == [] and env.batch_release_calls == []


@pytest.mark.parametrize("name, change", [
    ("no slots", lambda r: r.update(slots=[])), ("not a list", lambda r: r.update(slots={})),
    ("duplicate use", lambda r: r["slots"].append(dict(r["slots"][0]))),
    ("too many", lambda r: r.update(slots=[r["slots"][0]] * 65)),
    ("extra key", lambda r: r["slots"][0].update(extra=1)),
    ("missing key", lambda r: r["slots"][0].pop("field")),
    ("a bad slot", lambda r: r["slots"][1]["slot"].update(v=2)),
    ("bad recipient", lambda r: r.update(recipient_key="***")),
    ("both refs", lambda r: r.update(probe_ref=str(uuid.uuid4()))),
])
def test_a_batch_that_is_not_well_formed_names_nothing_and_reads_nothing(ns, env, name, change):
    run = Run(ns, env, 2)
    request = run.request()
    change(request)
    reply = run.ns.credential_release.release_credentials(env.module(), **request)
    assert reply["ok"] is False and reply["reason_code"] == "STALE_CONTEXT", name
    assert env.redis.calls == [] and env.release_calls == [] and env.recorded == [] and env.denied == []


def test_one_slot_release_is_still_served_with_the_old_handler(ns, env, sealed):
    run = Run(ns, env, 1)
    reply = ns.credential_release.release_credential(
        env.module(), project_id=1, context_ref=run.ref, probe_ref=None, caller=dict(CALLER), toolkit_instance_id="1",
        field="credentials", slot=dict(run.slots[0]), recipient_key=run.public)
    assert reply["ok"] is True and set(reply) == {"ok", "release_id", "fields", "expires_at", "sealed"}


def test_a_probe_asks_for_exactly_one_slot_and_its_ticket_is_taken_once(ns, env, sealed):
    run = Run(ns, env, 1)
    probe_ref = str(uuid.uuid4())
    ticket = {"project_id": 1, "credential_ref": run.creds[0].ref_id, "credential_generation": 1,
              "task_name": ns.credential_release.PROBE_TASK, "user_id": USER}
    env.redis.values[ns.credential_release.PROBE_KEY + probe_ref] = json.dumps(ticket)
    slot = {"v": 1, "credential_ref": run.creds[0].ref_id, "credential_generation": 1, "fields": ["token"],
            "probe_ref": probe_ref}
    request = dict(project_id=1, context_ref=None, probe_ref=probe_ref,
                   caller={"kind": "indexer_worker", "task_name": ns.credential_release.PROBE_TASK, "run_id": None},
                   recipient_key=run.public, slots=[{"toolkit_instance_id": "probe", "field": "configuration", "slot": slot}])
    reply = ns.credential_release.release_credentials(env.module(), **request)
    assert reply["ok"] is True and [r["toolkit_instance_id"] for r in reply["releases"]] == ["probe"]
    assert ns.credential_release.release_credentials(env.module(), **request)["reason_code"] == "STALE_CONTEXT"
    two = dict(request, slots=request["slots"] * 2)
    assert ns.credential_release.release_credentials(env.module(), **two)["reason_code"] == "STALE_CONTEXT"


def test_flag_off_the_batch_handler_refuses_and_touches_nothing(ns, env):
    module = env.module(governed=False)
    env.poison()
    env.redis.raises = AssertionError("redis was used")
    try:
        reply = ns.credential_release.release_credentials(module, project_id=1)
    finally:
        env.unpoison()
    assert reply["ok"] is False and env.release_calls == [] and env.redis.calls == [] and env.batch_release_calls == []


def test_the_validate_reply_says_whether_the_batch_handler_is_registered(ns, env):
    run = Run(ns, env, 1)
    off = env.module()
    assert "batch_release" not in ns.adm.validate_context(off, 1, run.ref, CALLER)       # never enforced in this process
    registered = env.module()
    registered._governed_handlers = {"validate": True, "release": True, "release_batch": True}   # pylint: disable=W0212
    reply = ns.adm.validate_context(registered, 1, run.ref, CALLER)
    assert reply["batch_release"] is True and set(reply) == {"ok", "context", "batch_release"}   # top level, a real True
    partial = env.module()
    partial._governed_handlers = {"validate": True, "release": True, "release_batch": False}   # pylint: disable=W0212
    reply = ns.adm.validate_context(partial, 1, run.ref, CALLER)
    assert reply["ok"] is True and "batch_release" not in reply
    refused = ns.adm.validate_context(registered, 1, str(uuid.uuid4()), CALLER)
    assert refused["ok"] is False and "batch_release" not in refused


def test_the_batch_handler_is_registered_next_to_the_single_one_and_the_gate_only_prefers_it():
    module = (PLUGIN_ROOT / "module.py").read_text()
    assert '("release_batch", self.release_credentials, "restricted_release_credentials")' in module
    caps = (PLUGIN_ROOT / "utils" / "worker_capabilities.py").read_text()
    assert 'CAPABILITY_CREDENTIAL_SLOTS_BATCH_V1 = "credential_slots_batch.v1"' in caps
    gate = caps[caps.index("def apply_envelope_gate"):]
    assert gate.index("raise WorkerCapabilityUnavailable") < gate.index("GOVERNED_TASK_PREFERRED_CAPABILITIES")


def test_a_worker_without_the_batch_capability_is_counted_and_never_refused(ns, env):
    caps = ns.caps
    seen = []
    registry = types.SimpleNamespace(has_capable_worker=lambda required: seen.append(frozenset(required)) or (
        "credential_slots_batch.v1" not in required))
    before = caps.GOVERNED_DISPATCH_WITHOUT_BATCH.total()
    call = {"kwargs": {}}
    caps.apply_envelope_gate("indexer_agent", call, env.module().governance_profile, registry)
    assert caps.GOVERNED_DISPATCH_WITHOUT_BATCH.total() == before + 1 and len(seen) == 2
    assert call["kwargs"]["envelope"]["required_capabilities"] == sorted(
        {"task_envelope.v1", "credential_slots.v1"})   # the batch capability is not required


# --- S7: a batch of probes -----------------------------------------------------------------------------------------

def test_a_batch_of_probes_asks_once_and_runs_the_probes_at_the_same_time(ns, env):
    creds = credentials(env, 4)
    env.engine(1)

    def stored(project_id, configuration_uuid, user_id):
        return {"configuration_uuid": configuration_uuid, "configuration_project_id": 1, "configuration_type": "jira",
                "configuration_generation": 3, "base_url": "https://jira.example", "token": "{{secret.t}}"}

    ns.credential_probe._stored_configuration = stored   # pylint: disable=W0212
    module = env.module()
    started_all = threading.Event()
    joined_after_all_started = []
    real_start = module.task_node.start_task

    def start(*args, **kwargs):
        task = real_start(*args, **kwargs)
        if len(env.started) >= 4:
            started_all.set()
        return task

    def join(task_id, timeout=None):
        joined_after_all_started.append(started_all.wait(5))
        return None

    module.task_node.start_task, module.task_node.join_task = start, join
    items = [{"configuration_uuid": cred.uuid, "config_type": "jira"} for cred in creds]
    before_facts, before_perm, before_cfg = len(env.secrets_calls), len(env.permission_calls), len(env.configuration_calls)
    answers = ns.credential_probe.governed_check_connection_many(module, 1, items, USER)
    assert answers == [{"success": True}] * 4
    assert joined_after_all_started == [True] * 4                 # every probe was started before the first one returned
    assert len(env.secrets_calls) - before_facts == 1             # the references of all four
    assert len(env.permission_calls) - before_perm == 1
    assert len(env.configuration_calls) - before_cfg == 1
    assert len(env.started) == 4


def test_a_batch_of_probes_refuses_item_by_item_and_all_when_the_actor_may_not(ns, env):
    creds = credentials(env, 2)
    env.engine(1)
    ns.credential_probe._stored_configuration = lambda *a: {   # pylint: disable=W0212
        "configuration_generation": 3, "token": "{{secret.t}}"}
    items = [{"configuration_uuid": creds[0].uuid, "config_type": "jira"},
             {"configuration_uuid": "not-a-uuid", "config_type": "jira"},
             {"configuration_uuid": creds[1].uuid, "config_type": "other_type"}]
    answers = ns.credential_probe.governed_check_connection_many(env.module(), 1, items, USER)
    assert answers[0] == {"success": True}
    assert [a.get("code") for a in answers[1:]] == ["CREDENTIAL_REFERENCE_REQUIRED"] * 2
    env.permissions[USER] = set()
    answers = ns.credential_probe.governed_check_connection_many(env.module(), 1, items[:1] * 2, USER)
    assert [a["code"] for a in answers] == ["DENIED_RBAC"] * 2
    assert ns.credential_probe.governed_check_connection_many(env.module(), 1, [], USER) == []


# --- S8: the owner scan ---------------------------------------------------------------------------------------------

class PublicSession:
    """A PostgreSQL session of the public schema that answers the catalog and the mandate probes of the scan."""

    def __init__(self, schemas, owning):
        self.schemas, self.owning, self.executed = schemas, owning, []
        self.is_pg = types.SimpleNamespace(dialect=types.SimpleNamespace(name="postgresql"))

    def get_bind(self):
        return self.is_pg

    def execute(self, statement, params=None):
        text = str(statement)
        self.executed.append((text, params))
        rows = self.schemas if "information_schema" in text else [pid for pid in self.owning if f'"p_{pid}"' in text]
        return types.SimpleNamespace(scalars=lambda: iter(rows))


def test_the_owner_scan_looks_in_one_session_and_suspends_only_where_there_are_mandates(ns, env, monkeypatch):
    owner = mandate_env.OWNER
    env.epochs[owner] = {"active": False, "user": 1, "token": 0}
    public = PublicSession([f"p_{pid}" for pid in range(1, 51)], owning=[7])
    suspended = []
    opened = []
    real = env.ns.tools.db.get_session

    def get_session(project_id):
        opened.append(project_id)
        return _ctx(public) if project_id is None else real(project_id)

    monkeypatch.setattr(env.ns.tools.db, "get_session", get_session)
    monkeypatch.setattr(ns.mandate_owner, "suspend_owner_in_project",
                        lambda project_id, user_id: suspended.append(project_id) or [str(uuid.uuid4())])
    result = ns.mandate_owner.suspend_inactive_owner(owner, project_ids=list(range(1, 51)))
    assert opened == [None] and suspended == [7] and list(result) == [7]       # one session to look, one suspension
    catalog, probe = public.executed
    assert "information_schema" in catalog[0]
    assert probe[1] == {"owner": owner} and probe[0].count("UNION ALL") == 49   # one statement for the 50 schemas
    assert "owner_user_id = :owner" in probe[0] and f"owner_user_id = {owner}" not in probe[0]


def test_the_owner_scan_looks_in_chunks_and_never_quotes_what_is_not_a_project_schema(ns, env, monkeypatch):
    public = PublicSession(["p_1", "p_2", "evil\"; drop table x; --", "p_3x"], owning=[])
    monkeypatch.setattr(env.ns.tools.db, "get_session", lambda project_id: _ctx(public))
    assert ns.mandate_owner.projects_with_active_mandates(5, [1, 2, 3]) == []
    assert "drop table" not in public.executed[1][0] and public.executed[1][0].count("UNION ALL") == 1
    public.schemas = [f"p_{pid}" for pid in range(1, 451)]
    public.executed.clear()
    ns.mandate_owner.projects_with_active_mandates(5, range(1, 451))
    assert len(public.executed) == 1 + 3                                        # the catalog, then 200 + 200 + 50


class _ctx:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, *exc):
        return False


def test_where_the_schemas_cannot_be_asked_at_once_every_project_is_scanned_as_before(ns, env):
    assert ns.mandate_owner.projects_with_active_mandates(5, [1, 2]) == [1, 2]    # SQLite: no catalog to ask


# --- S11: the binding lists -----------------------------------------------------------------------------------------

def seed_bindings(env, count):
    for index in range(count):
        version_id, application_id = 5000 + index, 500 + index
        env.add_application(1, application_id, f"Agent {index:03d}")
        env.add_version(1, version_id, application_id, "v1", agent_type="openai", instructions=f"Be agent {index}.",
                        llm_settings={"model_name": "m"})
        env.add_toolkit(1, version_id, 90000 + index, "github", {"url": "x"}, selected=("run",))
    with env.ns.sa_orm.Session(env.engine(1)) as session:
        stored = env.ns.digest.digests_for_versions(session, range(5000, 5000 + count))
    for index in range(count):
        env.add_binding(1, 500 + index, 5000 + index, state="active", digest=stored[5000 + index], mode="elitea_native")
    return stored


def test_a_page_of_bindings_is_four_statements_and_asks_for_the_users_with_no_session_open(ns, env, monkeypatch):
    monkeypatch.setattr(ns.binding_queries, "digests_for_versions", ns.real_digests_for_versions)
    seed_bindings(env, 50)
    open_at = []
    real = env.ns.tools.auth.list_users

    def list_users(user_ids):
        open_at.append(env.open_sessions)
        return real(user_ids)

    monkeypatch.setattr(env.ns.tools.auth, "list_users", list_users)
    seen, stop = statements(env)
    page = ns.binding_queries.list_page(1, limit=50)
    stop()
    assert len(page["rows"]) == 50 and page["total"] == 50
    assert len(seen) == 4                       # count (no joins), page, versions, toolkits
    assert "JOIN" not in seen[0].upper()
    assert open_at in ([], [0])


def test_a_search_still_counts_over_the_joined_applications(ns, env, monkeypatch):
    monkeypatch.setattr(ns.binding_queries, "digests_for_versions", ns.real_digests_for_versions)
    seed_bindings(env, 3)
    seen, stop = statements(env)
    page = ns.binding_queries.list_page(1, search="Agent 001")
    stop()
    assert page["total"] == 1 and "JOIN" in seen[0].upper()


def test_one_binding_is_three_statements_and_reads_the_users_with_no_session_open(ns, env, monkeypatch):
    monkeypatch.setattr(ns.binding_queries, "digest_for_version", ns.digest.digest_for_version)
    monkeypatch.setattr(ns.binding_queries, "digest_inputs_for_version", ns.digest.digest_inputs_for_version)
    monkeypatch.setattr(ns.EliteATool, "name", "tool", raising=False)   # the stand-in table has no name column
    stored = seed_bindings(env, 1)
    [binding] = env.query(1, ns.models.AgentIdentityBinding)
    open_at = []
    real = env.ns.tools.auth.list_users
    monkeypatch.setattr(env.ns.tools.auth, "list_users", lambda ids: open_at.append(env.open_sessions) or real(ids))
    seen, stop = statements(env)
    item = ns.binding_queries.get_detail(1, binding.id)
    stop()
    assert len(seen) == 3                       # the binding with the names, the version, its toolkits
    assert item["digest_status"] == "match" and item["digest_now"] == stored[5000]
    assert item["digest_inputs"]["tools"][0]["type"] == "github" and item["grants_count"] is None
    assert open_at in ([], [0])
    assert ns.binding_queries.get_detail(1, uuid.uuid4()) is None


# --- review fixes ------------------------------------------------------------------------------------------------

def counters(run):
    return [int(run.env.redis.values.get(
        f"{run.ns.credential_release.COUNTER_KEY}{run.ref}|{cred.tool_id}|credentials", 0)) for cred in run.creds]


def test_a_refused_batch_gives_the_counters_back(ns, env, sealed):
    env.batch_store = True
    run = Run(ns, env, 3)
    key = lambda cred: f"{ns.credential_release.COUNTER_KEY}{run.ref}|{cred.tool_id}|credentials"   # noqa: E731
    env.redis.values[key(run.creds[0])] = 4
    env.batch_reply = {"ok": False, "reason_code": "STORE_UNAVAILABLE", "ref_id": run.creds[1].ref_id}
    sent = env.redis.executes
    assert run.release()["reason_code"] == "STORE_UNAVAILABLE"
    assert counters(run) == [4, 0, 0] and env.redis.executes - sent == 2   # the count, and one pipeline to give it back
    env.batch_reply = None
    assert run.release()["ok"] is True and counters(run) == [5, 1, 1]       # a release that happened is counted


def test_rate_limited_on_the_middle_slot_refuses_the_batch_and_burns_nothing(ns, env, sealed):
    run = Run(ns, env, 3)
    key = f"{ns.credential_release.COUNTER_KEY}{run.ref}|{run.creds[1].tool_id}|credentials"
    env.redis.values[key] = ns.credential_release.MAX_RELEASES_PER_FIELD
    reply = run.release()
    assert (reply["reason_code"], reply["toolkit_instance_id"]) == ("RATE_LIMITED", "2")
    assert counters(run) == [0, 16, 0] and env.release_calls == [] and env.batch_release_calls == []
    assert [e["decision"] for _, e in env.recorded] == ["deny"] * 3


def test_a_seal_that_fails_for_the_second_slot_releases_nothing_and_gives_the_counters_back(ns, env, sealed, monkeypatch):
    run = Run(ns, env, 3)
    real = ns.credential_release._transport()   # pylint: disable=W0212
    sealed_so_far = []

    def seal(*args):
        if sealed_so_far:
            raise RuntimeError("no entropy")
        sealed_so_far.append(1)
        return real.seal(*args)

    monkeypatch.setattr(ns.credential_release, "_transport", lambda: types.SimpleNamespace(seal=seal))
    reply = run.release()
    assert (reply["ok"], reply["reason_code"]) == (False, "DEPENDENCY_UNAVAILABLE") and "releases" not in reply
    assert len(sealed_so_far) == 1                                         # the first slot was sealed, and dropped
    assert [e["decision"] for _, e in env.recorded] == ["deny"] * 3         # no allow row for it
    assert counters(run) == [0, 0, 0]


def test_the_failing_slot_in_the_middle_is_named_on_the_per_reference_path(ns, env, sealed, monkeypatch):
    run = Run(ns, env, 3)
    assert env.batch_store is False
    real = env.ns.tools.this.for_module

    def for_module(name):
        found = real(name)
        if name != "secrets":
            return found
        single = found.module.release_credential_material

        def release(project_id, ref_id, generation, release_id, actor_ref, **flags):
            if ref_id == run.creds[1].ref_id:
                return {"ok": False, "reason_code": "STORE_UNAVAILABLE"}
            return single(project_id, ref_id, generation, release_id, actor_ref, **flags)

        return types.SimpleNamespace(module=types.SimpleNamespace(
            release_credential_material=release, credential_use_facts=found.module.credential_use_facts,
            credential_facts_version=found.module.credential_facts_version))

    monkeypatch.setattr(env.ns.tools.this, "for_module", for_module)
    reply = run.release()
    assert (reply["reason_code"], reply["toolkit_instance_id"], reply["field"]) == ("STORE_UNAVAILABLE", "2", "credentials")
    assert counters(run) == [0, 0, 0]


def test_deny_rows_that_cannot_be_written_are_logged_and_the_refusal_stands(ns, env, sealed):
    run = Run(ns, env, 2)
    env.record_raises = RuntimeError("outbox full")
    reply = run.release()
    assert (reply["ok"], reply["reason_code"]) == (False, "AUDIT_UNAVAILABLE") and env.recorded == []
    assert any("deny events AUDIT_UNAVAILABLE were not recorded (RuntimeError)" in line for line in env.log.lines)
    assert counters(run) == [0, 0] and "outbox full" not in "\n".join(env.log.lines)


def test_a_counter_that_expired_meanwhile_is_not_left_below_zero(ns, env, sealed):
    run = Run(ns, env, 1)
    request = ns.credential_release._parse_batch(   # pylint: disable=W0212
        1, run.ref, None, dict(CALLER), run.public, run.request()["slots"])
    request[0].counter_key = "c1009:rel:gone"
    ns.credential_release._refund(env.module(), request)   # pylint: disable=W0212
    assert env.redis.values["c1009:rel:gone"] == 0


def test_the_secret_fields_of_a_static_type_are_kept_for_everyone_and_a_degraded_lookup_is_not_kept(ns, env):
    env.seed_agent()
    env.schemas["static_kit"] = {"properties": {"api_key": {"secret": True}}}
    env.schemas["mcp_a"] = {"properties": {"api_key": {"secret": True}}}
    env.static_types = {"static_kit"}
    env.add_toolkit(1, 70, 100, "static_kit", {"url": "https://x"})
    env.add_toolkit(1, 70, 101, "mcp_a", {"url": "https://x"})
    ns.adm.admit_execution(env.module(), env.request(), run_id="run-a", payload={})
    assert sorted(call[0] for call in env.schema_calls) == ["mcp_a", "static_kit"]
    env.schema_calls.clear()
    env.verify_result = {"ok": True, "principal": admission_env.principal(43)}
    ns.adm.admit_execution(env.module(), env.request(caller_id=43), run_id="run-b", payload={})
    assert [call[0] for call in env.schema_calls] == ["mcp_a"]            # the static type is the same for another user
    env.schema_calls.clear()
    ns.credential_decision.forget_secret_fields()
    env.schema_degraded = True
    ns.adm.admit_execution(env.module(), env.request(caller_id=43), run_id="run-c", payload={})
    ns.adm.admit_execution(env.module(), env.request(caller_id=43), run_id="run-d", payload={})
    assert sorted(call[0] for call in env.schema_calls) == ["mcp_a", "mcp_a", "static_kit"]   # only the static one kept


def _toolkits_loader():
    sys.path.insert(0, str(Path(__file__).parent))
    try:
        import test_5695_mcp_configuration_reconciliation as loader   # noqa: PLC0415
    finally:
        sys.path.pop(0)
    return loader


def test_a_new_toolkit_snapshot_moves_the_schema_registry_counter(monkeypatch):
    loader = _toolkits_loader()
    module = loader._load_method(monkeypatch, loader._RpcManager([]))   # pylint: disable=W0212
    counter = sys.modules["elitea_core_el5695.utils.schema_registry_version"]
    instance = loader._method_instance(module)   # pylint: disable=W0212
    seen = counter.current()
    instance.toolkits_collected(None, [{"title": "github", "properties": {}}])
    assert counter.current() != seen
    seen = counter.current()
    instance.toolkit_configurations_collected(None, {})
    assert counter.current() != seen
    source = (PLUGIN_ROOT / "methods" / "toolkits.py").read_text()
    assert source.count("schema_registry_version.bump()") == 2


def test_a_timeout_in_the_deferred_evaluation_does_not_change_the_dispatch_result(ns, env, monkeypatch):
    creds = credentials(env, 1)

    class Timeout(BaseException):   # a gevent.Timeout is not an Exception either
        pass

    def timed_out(*args, **kwargs):
        raise Timeout()

    monkeypatch.setattr(ns.adm, "_admit", timed_out)
    task_id, calls, _ = audit_dispatch(ns, env, payload_of(*creds))
    assert task_id == "task-1" and len(calls) == 1
    assert any("interrupted (Timeout)" in line for line in env.log.lines)

    class GreenletExit(BaseException):
        pass

    monkeypatch.setattr(ns.adm, "_admit", lambda *a, **k: (_ for _ in ()).throw(GreenletExit()))
    with pytest.raises(GreenletExit):   # the greenlet being killed is let through
        audit_dispatch(ns, env, payload_of(*creds))


def _ensure_source():
    text = (PLUGIN_ROOT / "module.py").read_text()
    body = text[text.index("    def _ensure_governed_handlers_registered"):text.index("    def _unregister_governed_handlers")]
    import textwrap  # noqa: PLC0415
    return textwrap.dedent(body)


@pytest.mark.parametrize("failing, ok, batch", [
    ("restricted_release_credentials", True, False), ("restricted_release_credential", False, True),
    ("restricted_validate_execution_context", False, True), (None, True, True)])
def test_the_batch_handler_is_optional_when_the_enforce_switch_registers_handlers(failing, ok, batch):
    lines = []
    namespace = {"log": types.SimpleNamespace(
        exception=lambda message, *args: lines.append(("exception", message % args)),
        error=lambda message, *args: lines.append(("error", message % args)))}

    def register(handler, name):
        if name == failing:
            raise RuntimeError("bus down")

    namespace["this"] = types.SimpleNamespace(for_module=lambda name: types.SimpleNamespace(
        module=types.SimpleNamespace(rpc_node=types.SimpleNamespace(register=register))))
    exec(compile(_ensure_source(), "module.py", "exec"), namespace)   # pylint: disable=W0122
    module = types.SimpleNamespace(
        _governed_handlers={"validate": False, "release": False, "release_batch": False},
        _governed_handlers_lock=threading.Lock(), validate_execution_context=object(), release_credential=object(),
        release_credentials=object())
    assert namespace["_ensure_governed_handlers_registered"](module) is ok
    assert module._governed_handlers["release_batch"] is batch   # pylint: disable=W0212
    if failing == "restricted_release_credentials":
        assert lines == [("error", "Failed to register restricted_release_credentials (RuntimeError); batch release is not offered")]
