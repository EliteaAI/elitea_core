"""C-1009 (#6946): the release of credential material under an autonomous mandate.

A mandate run is sealed and validated like an interactive one; the release adds the mandate to the audit event and
re-checks it (the live checks of ``validate_context``) before any material is read. Real models and statements on SQLite
files (``fixtures/mandate_env.py``); the store read, the facts methods and Redis are fakes.
"""
import json
import sys
import types
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.mandate_env import OWNER, add_mandate  # noqa: E402  pylint: disable=C0413

RUN = "run-1"
CALLER = {"kind": "indexer_worker", "task_name": "indexer_agent", "run_id": RUN}
OWNER_REF = "idp:tid-1:oid-5"
SECRET = "mandate-secret-value-0123456789"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("mandate_release"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    return mandate_env.prepare(ns, ns.env)


@pytest.fixture
def run(ns, env):
    """A mandate run of one credential, sealed, validated, and the requests its worker would make."""
    binding = env.seed_agent()
    mandate = add_mandate(env, ns, state="active", binding_ref=binding, operations=("run:schedule",))
    cfg_uuid, ref_id = str(uuid.uuid4()), str(uuid.uuid4())
    env.add_toolkit(1, 70, 1, "github", {"url": "https://x", "credentials": {
        "elitea_title": "gh", "private": False, "configuration_uuid": cfg_uuid, "configuration_project_id": 1}})
    env.configurations[(1, cfg_uuid)] = {"id": 5, "project_id": 1, "type": "github", "section": "credentials", "generation": 3}
    env.references.append({
        "project_id": 1, "configuration_id": 5, "ref_id": ref_id, "state": "active", "generation": 1,
        "configuration_generation": 3, "store_kind": "database", "ownership_class": "project_shared",
        "revocation_epoch": 2, "custodian_user_id": None})
    env.grants[(1, "|".join((str(binding), "1", ref_id, "*", f"mandate:{mandate}")))] = {
        "grant_id": str(uuid.uuid4()), "state": "active", "revocation_epoch": 4, "valid_from": None, "valid_until": None,
        "operations": ["*"]}
    env.secret_fields["github"] = ["token"]
    env.material[ref_id] = {"token": SECRET}
    payload = {"project_id": 1, "message_id": 5, "_elitea_predict_run_id": RUN,
               "envelope": {"schema_version": 1, "security_profile": "governed",
                            "required_capabilities": ["task_envelope.v1"], "context_ref": None},
               "application": {"version_details": {"tools": [{"id": 1, "type": "github", "settings": {
                   "url": "https://x", "credentials": {
                       "elitea_title": "gh", "configuration_uuid": cfg_uuid, "configuration_project_id": 1,
                       "configuration_type": "github", "configuration_generation": 3, "token": SECRET}}}]}}}
    call = {"args": ["t", 5], "kwargs": payload, "pool": "agents", "meta": {"project_id": 1}}
    asked = ns.AdmissionRequest(
        project_id=1, binding_project_id=1, application_id=7, version_id=70, run_kind="mandate",
        entry_point="schedule", caller_type="trigger", mandate_ref=str(mandate), trigger_ref="schedule:version:70")
    ref = ns.adm.admit_dispatch(env.module(), "indexer_agent", call, asked)
    assert ns.adm.validate_context(env.module(), 1, ref, CALLER)["ok"] is True
    private, public = ns.transport.generate_recipient_keypair()
    env.denied.clear()
    env.recorded.clear()
    slot = dict(payload["application"]["version_details"]["tools"][0]["settings"]["credentials"]["credential_slot"])
    return types.SimpleNamespace(
        ref=ref, mandate=mandate, slot=slot, private=private, public=public, ref_id=ref_id, call=call,
        release=lambda: ns.credential_release.release_credential(
            env.module(), project_id=1, context_ref=ref, probe_ref=None, caller=dict(CALLER), toolkit_instance_id="1",
            field="credentials", slot=dict(slot), recipient_key=public))


def test_the_payload_of_a_mandate_run_is_sealed_like_any_other(ns, env, run):
    assert SECRET not in json.dumps(run.call, default=str) and run.slot["fields"] == ["token"]


def test_a_mandate_runs_release_names_the_mandate_and_its_owner(ns, env, run):
    reply = run.release()
    assert reply["ok"] is True
    aad = f"v1|1|{run.ref}|1|credentials|{run.slot['decision_id']}|{reply['release_id']}".encode()
    assert json.loads(ns.transport.open_sealed(reply["sealed"], run.private, aad))["fields"] == {"token": SECRET}
    [(_, event)] = env.recorded
    assert event["mandate_ref"] == str(run.mandate) and event["user_ref"] == OWNER_REF
    assert event["caller_ref"] == "indexer_worker:indexer_agent" and event["event_type"] == "credential_acquired"
    assert event["agent_binding_ref"] and event["run_id"] == RUN


@pytest.mark.parametrize("values, code", [
    ({"state": "suspended", "suspended_reason": "MANUAL", "revocation_epoch": 1}, "MANDATE_INACTIVE"),
    ({"state": "revoked", "revocation_epoch": 1}, "MANDATE_INACTIVE"),
    ({"state": "expired", "revocation_epoch": 1}, "MANDATE_EXPIRED"),
])
def test_a_mandate_withdrawn_after_the_run_started_stops_the_release_and_the_run(ns, env, run, values, code):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.Mandate).where(ns.Mandate.id == run.mandate).values(**values))
        session.commit()
    reply = run.release()
    assert reply["ok"] is False and reply["reason_code"] == code and "sealed" not in reply
    assert env.release_calls == []
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"]) == ("credential_acquired", "deny", code)
    assert event["mandate_ref"] == str(run.mandate)
    [row] = env.query(1, ns.models.ExecutionSecurityContext)
    assert (row.state, row.revoked_reason) == ("revoked", code)


def test_an_owner_who_is_no_longer_active_ends_the_release_and_suspends_the_mandate(ns, env, run):
    env.epochs[OWNER] = {"active": False, "user": 1, "token": 0}
    reply = run.release()
    assert reply["reason_code"] == "MANDATE_INACTIVE" and env.release_calls == []
    assert SECRET not in json.dumps(reply)
