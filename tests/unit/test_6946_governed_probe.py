"""C-1009 (#6946): the governed test of a saved credential (``governed_check_connection``).

Real models and statements, the real release handler and sealed transport; the stored configuration, the secrets
plugin, Redis, the task node and the permission check are fakes that record their calls. See ``fixtures/admission_env.py``.
"""
import copy
import json
import sys
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import SECRET, dumped, seeded  # noqa: E402  pylint: disable=C0413

USER = 42
TASK = "indexer_governed_check_connection"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("governed_probe"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    env = ns.env
    env.permissions[USER] = {"credentials.probe", "credentials.use"}
    return env


class Probe:
    """A saved credential in a project and the user who tests it."""

    def __init__(self, ns, env, **over):
        self.ns, self.env = ns, env
        self.cred = seeded(env, **over)
        self.stored = {"configuration_uuid": self.cred.uuid, "configuration_project_id": 1, "configuration_type": "jira",
                       "configuration_generation": 3, "elitea_title": "jira-main-credential",
                       "base_url": "https://jira.example", "token": "{{secret.jira_token}}"}
        ns.credential_probe._stored_configuration = self.read_stored   # pylint: disable=W0212
        env.denied.clear()
        env.recorded.clear()

    def read_stored(self, project_id, configuration_uuid, user_id):
        self.env.stored_read = (project_id, configuration_uuid, user_id)
        return copy.deepcopy(self.stored)

    def run(self, **over):
        values = dict(project_id=1, configuration_uuid=self.cred.uuid, config_type="jira", user_id=USER)
        values.update(over)
        return self.ns.credential_probe.governed_check_connection(
            self.env.module(), values["project_id"], values["configuration_uuid"], values["config_type"],
            values["user_id"])


@pytest.fixture
def probe(ns, env):
    return Probe(ns, env)


def refused(probe, answer, code):
    env, ns = probe.env, probe.ns
    assert answer == {"success": False, "code": code, "message": ns.credential_probe.PROBE_MESSAGES.get(
        code, ns.credential_probe.GENERIC_PROBE_MESSAGE)}
    assert env.started == [] and SECRET not in dumped(answer)
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"]) == ("probe", "deny", code)
    return event


# --- a probe that runs --------------------------------------------------------------------------

def test_a_probe_sends_the_stored_configuration_without_secrets_and_a_slot_to_the_governed_ops_pool(ns, env, probe):
    assert probe.run() == {"success": True}
    [(name, kwargs, pool, meta)] = env.started
    assert (name, pool) == (TASK, "governed_ops")
    settings = kwargs["settings"]
    assert kwargs["configuration_type"] == "jira" and set(kwargs) == {"configuration_type", "settings"}
    assert "token" not in settings and settings["base_url"] == "https://jira.example"
    [ticket] = [key for key in env.at_start[0] if key.startswith(ns.credential_release.PROBE_KEY)]
    probe_ref = ticket[len(ns.credential_release.PROBE_KEY):]
    assert settings["credential_slot"] == {
        "v": 1, "credential_ref": probe.cred.ref_id, "credential_generation": 1, "fields": ["token"],
        "probe_ref": probe_ref}
    assert "{{secret." not in dumped(kwargs) and SECRET not in dumped(kwargs)
    assert meta["llm_free"] is True and meta["project_id"] == 1 and meta["task_name"] == TASK
    assert env.stored_read == (1, probe.cred.uuid, USER)


def test_the_ticket_is_metadata_only_lives_ninety_seconds_and_follows_the_committed_event(ns, env, probe):
    order = []
    real_set = env.redis.set

    def watched_set(key, value, ex=None):
        [(session, _)] = env.recorded
        [(_, writer)] = [item for item in env.sessions if item[1] is session]
        order.append(("ticket", writer.commits))
        return real_set(key, value, ex=ex)

    env.redis.set = watched_set
    probe.run()
    assert order == [("ticket", 1)]   # the probe event was committed when the ticket was stored
    values, ttls = env.at_start
    [(key, ttl)] = list(ttls.items())
    assert ttl == 90
    assert set(json.loads(values[key])) == {"project_id", "credential_ref", "credential_generation", "task_name", "user_id"}


def test_the_ticket_is_removed_when_the_task_could_not_run(ns, env, probe):
    env.start_raises = RuntimeError("maintenance")
    assert probe.run()["code"] == "DEPENDENCY_UNAVAILABLE"
    assert not [key for key in env.redis.values if key.startswith(ns.credential_release.PROBE_KEY)]
    env.start_raises, env.start_result = None, None   # saturated pool
    probe.run()
    env.start_result, env.task_result = "task-1", ...   # timeout
    probe.run()
    assert not [key for key in env.redis.values if key.startswith(ns.credential_release.PROBE_KEY)]
    assert env.stopped == ["task-1"]


def test_the_probe_event_is_recorded_before_the_task_and_the_outcome_after(ns, env, probe):
    probe.run()
    [(_, allow)] = env.recorded
    probe_ref = allow["event_id"]
    assert allow == {
        "event_type": "probe", "decision": "allow", "reason_code": "CREDENTIAL_PROBE_ALLOWED", "event_id": probe_ref,
        "project_id": 1, "caller_ref": "user:42", "user_ref": "user:42", "credential_ref": probe.cred.ref_id,
        "credential_generation": 1, "policy_revision": "rev-1"}
    [done] = env.denied   # the autocommit writer takes the outcome event
    assert (done["event_type"], done["decision"], done["reason_code"], done["outcome"]) == (
        "probe", "allow", "CREDENTIAL_PROBE_SUCCEEDED", "success")


def test_the_worker_asks_for_exactly_the_capabilities_of_a_governed_probe(ns, env, probe):
    probe.run()
    assert env.capability_asks == [frozenset({"task_envelope.v1", "credential_slots.v1", "governed_ops_pool.v1"})]


def test_a_validated_credential_may_be_probed_before_it_is_active(ns, env):
    probe = Probe(ns, env, ref_state="validated")
    assert probe.run() == {"success": True}


def test_the_custodian_may_probe_a_personal_credential(ns, env):
    probe = Probe(ns, env)
    probe.cred.reference["ownership_class"] = "personal"
    refused(probe, probe.run(), "CREDENTIAL_CLASS_NOT_ALLOWED")
    env.denied.clear()
    probe.cred.reference["custodian_user_id"] = USER
    assert probe.run() == {"success": True}


# --- what the route is told --------------------------------------------------------------------

@pytest.mark.parametrize("result, expected", [
    (None, {"success": True}),
    ({"tools": ["a"], "warning": "w"}, {"success": True, "tools": ["a"], "warning": "w"}),
    ("Invalid token for the provider", {"success": False, "message": "Invalid token for the provider"}),
    ({"success": False, "requires_authorization": True, "error": "log in", "auth_metadata": {"url": "https://a"}},
     {"success": False, "requires_authorization": True, "auth_metadata": {"url": "https://a"}, "message": "log in"}),
])
def test_the_workers_result_becomes_the_routes_answer(ns, env, probe, result, expected):
    env.task_result = result
    assert probe.run() == expected
    [outcome] = env.denied
    assert outcome["event_type"] == "probe" and outcome["reason_code"] in (
        "CREDENTIAL_PROBE_SUCCEEDED", "CREDENTIAL_PROBE_FAILED")


def test_a_refusal_of_the_worker_is_a_readable_refusal_with_its_code(ns, env, probe):
    env.task_result = {"success": False, "reason_code": "STORE_SEALED", "error": "ignored text of the worker"}
    answer = probe.run()
    assert answer == {"success": False, "code": "STORE_SEALED", "message": ns.credential_probe.PROBE_MESSAGES["STORE_SEALED"]}
    env.task_result = {"success": False, "reason_code": "CAPABILITY_MISSING", "error": "Task rejected by worker: CAPABILITY_MISSING"}
    assert probe.run()["code"] == "WORKER_CAPABILITY_UNAVAILABLE"
    env.task_result = ["nonsense"]
    assert probe.run()["code"] == "DEPENDENCY_UNAVAILABLE"


# --- refusals, each audited and none starting a task ---------------------------------------------

def test_without_credentials_probe_the_user_is_refused_with_denied_rbac(ns, env, probe):
    env.permissions[USER] = {"credentials.use", "configuration.secrets.secret.unsecret"}
    event = refused(probe, probe.run(), "DENIED_RBAC")
    assert event["caller_ref"] == "user:42" and event["project_id"] == 1
    assert env.permission_calls == [(USER, "default", 1)] and env.redis.calls == []
    assert env.release_calls == [] and env.capability_asks == []


def test_a_permission_that_cannot_be_read_is_a_dependency_refusal(ns, env, probe):
    env.permissions_raise = RuntimeError("auth down")
    refused(probe, probe.run(), "DEPENDENCY_UNAVAILABLE")


def test_the_audit_trail_being_unavailable_refuses_first(ns, env, probe):
    env.audit_allowed = False
    refused(probe, probe.run(), "AUDIT_UNAVAILABLE")
    assert env.permission_calls == []


@pytest.mark.parametrize("over", [
    {"project_id": "1"}, {"project_id": True}, {"user_id": None}, {"user_id": True},
    {"configuration_uuid": "not-a-uuid"}, {"configuration_uuid": None}, {"config_type": ""}, {"config_type": None},
    {"configuration_uuid": str(uuid.uuid4()).upper()},
])
def test_arguments_that_cannot_name_a_saved_credential_are_refused(ns, env, probe, over):
    assert probe.run(**over)["code"] == "CREDENTIAL_REFERENCE_REQUIRED"
    assert env.started == [] and env.permission_calls == []


@pytest.mark.parametrize("name, change", [
    ("no such configuration", lambda p: p.env.configurations.clear()),
    ("not a credential", lambda p: p.env.configurations[(1, p.cred.uuid)].update(section="vector_storage")),
    ("another type", lambda p: p.env.configurations[(1, p.cred.uuid)].update(type="github")),
    ("another project", lambda p: p.env.configurations[(1, p.cred.uuid)].update(project_id=9)),
    ("no reference", lambda p: p.env.references.clear()),
])
def test_a_configuration_that_is_not_a_saved_credential_of_this_project_and_type_is_refused(ns, env, probe, name, change):
    change(probe)
    assert probe.run()["code"] == "CREDENTIAL_REFERENCE_REQUIRED", name
    assert env.started == [] and env.redis.calls == []


@pytest.mark.parametrize("state, code", [
    ("suspended", "CREDENTIAL_REFERENCE_INACTIVE"), ("pending", "CREDENTIAL_REFERENCE_INACTIVE"),
    ("retired", "CREDENTIAL_REFERENCE_REQUIRED"),   # the facts method leaves a retired reference out
])
def test_an_inactive_reference_is_refused(ns, env, probe, state, code):
    probe.cred.reference["state"] = state
    refused(probe, probe.run(), code)
    assert env.redis.calls == []


def test_a_store_kind_the_platform_does_not_read_is_refused(ns, env, probe):
    probe.cred.reference["store_kind"] = "managed_vault"
    refused(probe, probe.run(), "CREDENTIAL_REFERENCE_INACTIVE")


def test_a_configuration_changed_after_the_reference_was_approved_is_refused(ns, env, probe):
    env.configurations[(1, probe.cred.uuid)]["generation"] = 4
    refused(probe, probe.run(), "CREDENTIAL_GENERATION_MISMATCH")
    assert env.redis.calls == []


def test_facts_that_cannot_be_read_are_a_dependency_refusal(ns, env, probe):
    env.configurations_raises = RuntimeError("down")
    refused(probe, probe.run(), "DEPENDENCY_UNAVAILABLE")


def test_no_worker_that_can_run_it_is_a_readable_refusal_before_anything_is_recorded(ns, env, probe):
    env.capable = False
    refused(probe, probe.run(), "WORKER_CAPABILITY_UNAVAILABLE")
    assert env.recorded == [] and env.redis.calls == []


def test_a_configuration_that_moved_between_the_facts_and_the_read_is_not_probed(ns, env, probe):
    probe.stored["configuration_generation"] = 4   # the saved configuration changed after the reference was approved
    refused(probe, probe.run(), "CREDENTIAL_GENERATION_MISMATCH")
    assert env.redis.calls == [] and env.release_calls == []


def test_a_configuration_with_no_secret_value_is_refused_not_probed_with_an_empty_slot(ns, env, probe):
    probe.stored["token"] = ""
    refused(probe, probe.run(), "CREDENTIAL_REFERENCE_REQUIRED")
    probe.stored["token"] = "{{secret.jira_token}}"
    env.secret_fields["jira"] = []
    env.denied.clear()
    refused(probe, probe.run(), "CREDENTIAL_REFERENCE_REQUIRED")


def test_a_stored_header_map_is_taken_out_and_asked_for_like_a_run(ns, env, probe):
    probe.stored["headers"] = {"X-Api-Key": "{{secret.h1}}", "X-Empty": ""}
    assert probe.run() == {"success": True}
    [(_, kwargs, _, _)] = env.started
    assert kwargs["settings"]["headers"] == {}
    assert kwargs["settings"]["credential_slot"]["fields"] == ["token", "headers.X-Api-Key"]


def test_a_stored_template_outside_the_secret_fields_is_refused(ns, env, probe):
    probe.stored["base_url"] = "{{secret.url}}"
    refused(probe, probe.run(), "CREDENTIAL_REFERENCE_REQUIRED")
    assert env.redis.calls == []


def test_a_type_without_known_secret_fields_and_an_unreadable_configuration_are_refused(ns, env, probe):
    env.unregistered_types.add("jira")
    refused(probe, probe.run(), "CREDENTIAL_REFERENCE_REQUIRED")
    env.unregistered_types.clear()
    env.denied.clear()
    ns.credential_probe._stored_configuration = lambda *a: (_ for _ in ()).throw(RuntimeError("down"))   # pylint: disable=W0212
    refused(probe, probe.run(), "DEPENDENCY_UNAVAILABLE")


def test_a_probe_event_that_cannot_be_committed_stores_no_ticket_and_starts_nothing(ns, env, probe):
    env.record_raises = RuntimeError("outbox full")
    refused(probe, probe.run(), "AUDIT_UNAVAILABLE")
    assert env.redis.calls == []


def test_a_ticket_that_cannot_be_stored_starts_nothing(ns, env, probe):
    env.redis.raises = RuntimeError("redis down")
    refused(probe, probe.run(), "DEPENDENCY_UNAVAILABLE")


def test_a_task_that_cannot_start_is_a_readable_refusal(ns, env, probe):
    env.start_raises = ns.exceptions.WorkerCapabilityUnavailable(TASK, ("governed_ops_pool.v1",))
    answer = probe.run()
    assert answer["code"] == "WORKER_CAPABILITY_UNAVAILABLE" and env.denied[-1]["event_type"] == "probe"
    env.start_raises = RuntimeError("maintenance")
    assert probe.run()["code"] == "DEPENDENCY_UNAVAILABLE"
    env.start_raises, env.start_result = None, None   # the pool is saturated
    assert probe.run()["code"] == "DEPENDENCY_UNAVAILABLE"


def test_a_probe_that_times_out_is_stopped(ns, env, probe):
    env.task_result = ...
    assert probe.run()["code"] == "DEPENDENCY_UNAVAILABLE" and env.stopped == ["task-1"]


def test_the_handler_never_raises(ns, env, probe):
    env.permissions_raise = BaseException.__new__(RuntimeError)   # a broken exception object
    assert probe.run()["success"] is False


def test_probe_messages_are_fixed_per_code_and_carry_no_ids(ns, env, probe):
    messages = ns.credential_probe.PROBE_MESSAGES
    assert messages["DENIED_RBAC"] == "You do not have permission to test saved credentials."
    assert all(not any(part in text for part in ("uuid", "{", "}", "SECRET")) for text in messages.values())
    assert set(messages) >= {"DENIED_RBAC", "CREDENTIAL_REFERENCE_REQUIRED", "WORKER_CAPABILITY_UNAVAILABLE",
                             "STORE_SEALED", "AUDIT_UNAVAILABLE"}


# --- the probe and the release agree ------------------------------------------------------------

def test_what_the_probe_sends_is_what_the_release_accepts(ns, env, probe):
    ns.need_transport()
    env.redis.set = lambda key, value, ex=None: env.redis.values.__setitem__(key, value) or True
    probe.run()
    values, _ = env.at_start
    env.redis.values.update(values)   # the task asks while the probe runs: the ticket is there then
    [(_, kwargs, _, _)] = env.started
    slot = kwargs["settings"]["credential_slot"]
    private, public = ns.keypair()
    request = dict(project_id=1, context_ref=None, probe_ref=slot["probe_ref"],
                   caller={"kind": "indexer_worker", "task_name": TASK, "run_id": None},
                   toolkit_instance_id="probe", field="configuration", slot=slot, recipient_key=public)
    env.recorded.clear()
    reply = ns.credential_release.release_credential(env.module(), **request)
    assert reply["ok"] is True
    aad = f"v1|1|{slot['probe_ref']}|probe|configuration||{reply['release_id']}".encode()
    assert json.loads(ns.transport.open_sealed(reply["sealed"], private, aad))["fields"] == {"token": f"{SECRET}-token"}
    # one probe, one release: the ticket is spent
    assert ns.credential_release.release_credential(env.module(), **request)["reason_code"] == "STALE_CONTEXT"


def test_the_governed_probe_is_a_module_method_over_the_same_function():
    text = (PLUGIN_ROOT / "methods" / "governed_credentials.py").read_text()
    assert "def governed_check_connection(self, project_id, configuration_uuid, config_type, user_id):" in text
    assert "return governed_check_connection(self, project_id, configuration_uuid, config_type, user_id)" in text
    assert "def release_credential(" in text and "return release_credential(" in text
