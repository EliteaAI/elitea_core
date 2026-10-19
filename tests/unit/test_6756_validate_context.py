"""C-1009 (#6756): the worker proves a governed run still holds its admission - validate_context on Main.

Real models and statements on SQLite files; see ``fixtures/admission_env.py``.
"""
import json
import sys
from datetime import timedelta
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413

COOKIE = "cookie-secret-value"
RUN = "run-1"
CALLER = {"kind": "indexer_worker", "task_name": "indexer_agent", "run_id": RUN}


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("validate"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    return ns.env


@pytest.fixture
def admitted(ns, env):
    """An admitted run: one agent (project 1, app 7, version 70), user 42, run-1."""
    binding = env.seed_agent()
    view = ns.adm.admit_execution(env.module(), env.request(), run_id=RUN)
    env.denied.clear()
    env.log.lines.clear()
    return type("Admitted", (), {"view": view, "ref": view.context_ref, "binding": binding})


def validate(ns, env, ref, project_id=1, caller=CALLER):
    return ns.adm.validate_context(env.module(), project_id, ref, caller)


def row(env, ns, project_id=1):
    [context] = env.query(project_id, ns.models.ExecutionSecurityContext)
    return context


def binding_row(env, ns, project_id=1):
    [binding] = env.query(project_id, ns.models.AgentIdentityBinding)
    return binding


def update(env, ns, model, project_id=1, **values):
    with ns.sa_orm.Session(env.engine(project_id)) as session:
        session.execute(ns.sa.update(model).values(**values))
        session.commit()


def update_context(env, ns, **values):
    update(env, ns, ns.models.ExecutionSecurityContext, **values)


def update_binding(env, ns, project_id=1, **values):
    update(env, ns, ns.models.AgentIdentityBinding, project_id=project_id, **values)


# --- accepted ---------------------------------------------------------------------------------

def test_a_live_context_is_accepted_and_moves_to_running(ns, env, admitted):
    assert row(env, ns).state == "admitted"
    result = validate(ns, env, admitted.ref)
    assert result["ok"] is True
    assert result["context"] == {
        "context_ref": admitted.ref, "run_id": RUN, "attempt": 1, "project_id": 1, "application_id": 7,
        "version_id": 70, "agent_binding_ref": str(admitted.binding), "policy_revision": "rev-1",
        "expires_at": admitted.view.expires_at.isoformat(),
    }
    context = row(env, ns)
    assert context.state == "running" and context.started_at is not None
    assert env.denied == []


def test_the_answer_carries_no_secret_and_nothing_of_the_session(ns, env, admitted):
    text = json.dumps(validate(ns, env, admitted.ref)) + "\n".join(env.log.lines)
    assert COOKIE not in text and "eyJ" not in text and "delegation" not in text and "uti-" not in text


def test_a_restarted_worker_revalidates_a_running_context_without_moving_it(ns, env, admitted):
    first = validate(ns, env, admitted.ref)
    started = row(env, ns).started_at
    second = validate(ns, env, admitted.ref)
    assert first["ok"] and second == first
    context = row(env, ns)
    assert context.state == "running" and context.started_at == started


def test_validation_is_counted_and_a_success_is_not_audited(ns, env, admitted):
    before = ns.adm.CONTEXT_VALIDATION.total("allow", "VALIDATED")
    validate(ns, env, admitted.ref)
    assert ns.adm.CONTEXT_VALIDATION.total("allow", "VALIDATED") == before + 1
    assert env.denied == [] and env.recorded[-1][1]["event_type"] == "admission"  # only the admission itself


# --- malformed or unknown -----------------------------------------------------------------------

@pytest.mark.parametrize("ref", [None, "", "not-a-uuid", 42, "11111111-1111-1111-1111-11111111111"])
def test_a_context_ref_that_is_not_a_uuid_is_stale(ns, env, admitted, ref):
    assert validate(ns, env, ref)["reason_code"] == "STALE_CONTEXT"
    assert row(env, ns).state == "admitted"


@pytest.mark.parametrize("project_id", ["1", None, True, 1.0, 2])
def test_a_project_that_is_not_the_run_project_is_stale(ns, env, admitted, project_id):
    assert validate(ns, env, admitted.ref, project_id=project_id)["reason_code"] == "STALE_CONTEXT"
    assert row(env, ns).state == "admitted"


def test_an_unknown_context_is_stale(ns, env, admitted):
    assert validate(ns, env, "11111111-1111-1111-1111-111111111111")["reason_code"] == "STALE_CONTEXT"


@pytest.mark.parametrize("caller", [None, "indexer_worker", [], {}])
def test_a_caller_that_is_not_a_description_of_a_worker_is_stale(ns, env, admitted, caller):
    assert validate(ns, env, admitted.ref, caller=caller)["reason_code"] == "STALE_CONTEXT"


# --- schema, caller and run ownership ----------------------------------------------------------

@pytest.mark.parametrize("values", [{"schema_version": 2}, {"profile": "legacy"}])
def test_an_unknown_schema_or_profile_is_refused(ns, env, admitted, values):
    update_context(env, ns, **values)
    assert validate(ns, env, admitted.ref)["reason_code"] == "UNKNOWN_SCHEMA_VERSION"
    assert row(env, ns).state == "admitted"


@pytest.mark.parametrize("caller", [
    {**CALLER, "kind": "browser"}, {**CALLER, "task_name": "indexer_validator"}, {**CALLER, "task_name": None},
    {**CALLER, "run_id": "run-2"}, {**CALLER, "run_id": None}, {"kind": "indexer_worker", "task_name": "indexer_agent"},
])
def test_the_caller_must_be_the_worker_of_this_run(ns, env, admitted, caller):
    result = validate(ns, env, admitted.ref, caller=caller)
    assert result["reason_code"] == "STALE_CONTEXT"
    assert row(env, ns).state == "admitted"  # another run's guess does not revoke this run
    assert env.denied[0]["reason_code"] == "STALE_CONTEXT"


def test_both_admitted_tasks_are_accepted(ns, env, admitted):
    for task in ("indexer_agent", "indexer_predict_agent"):
        assert validate(ns, env, admitted.ref, caller={**CALLER, "task_name": task})["ok"] is True


# --- state and expiry --------------------------------------------------------------------------

@pytest.mark.parametrize("state, reason, code", [
    ("completed", None, "STALE_CONTEXT"),
    ("revoked", "NOT_DISPATCHED", "STALE_CONTEXT"),
    ("revoked", None, "STALE_CONTEXT"),
    ("revoked", "EPOCH_MISMATCH", "EPOCH_MISMATCH"),
    ("revoked", "BINDING_DIGEST_MISMATCH", "BINDING_DIGEST_MISMATCH"),
    ("revoked", "DENIED_BINDING", "DENIED_BINDING"),
])
def test_a_context_that_ended_is_refused_with_the_reason_it_ended(ns, env, admitted, state, reason, code):
    update_context(env, ns, state=state, revoked_reason=reason)
    assert validate(ns, env, admitted.ref)["reason_code"] == code
    assert row(env, ns).state == state


def test_expiry_is_exclusive_at_the_boundary(ns, env, admitted):
    expires = admitted.view.expires_at
    ns.adm._now = lambda: expires - timedelta(seconds=1)  # pylint: disable=W0212
    assert validate(ns, env, admitted.ref)["ok"] is True
    ns.adm._now = lambda: expires  # exactly now is already stale  # pylint: disable=W0212
    assert validate(ns, env, admitted.ref)["reason_code"] == "STALE_CONTEXT"
    ns.adm._now = lambda: expires + timedelta(seconds=1)  # pylint: disable=W0212
    assert validate(ns, env, admitted.ref)["reason_code"] == "STALE_CONTEXT"


# --- identity and binding ----------------------------------------------------------------------

def test_the_user_epoch_is_read_for_the_user_of_the_context(ns, env, admitted):
    validate(ns, env, admitted.ref)
    assert env.epoch_calls == [("user", 42)]


@pytest.mark.parametrize("epochs", [
    {"active": True, "user": 4, "token": 0}, {"active": False, "user": 3, "token": 0}, {"user": 3}, {},
])
def test_a_user_whose_epoch_moved_or_who_is_no_longer_active_revokes_the_run(ns, env, admitted, epochs):
    env.epochs[42] = epochs
    assert validate(ns, env, admitted.ref)["reason_code"] == "EPOCH_MISMATCH"
    context = row(env, ns)
    assert (context.state, context.revoked_reason) == ("revoked", "EPOCH_MISMATCH") and context.ended_at is not None
    # and it stays refused with the same reason when the worker asks again
    assert validate(ns, env, admitted.ref)["reason_code"] == "EPOCH_MISMATCH"


@pytest.mark.parametrize("make_unknown", [
    lambda env: env.epochs.pop(42), lambda env: setattr(env, "epochs_raise", True),
])
def test_an_identity_that_cannot_be_read_is_unknown_not_revoked(ns, env, admitted, make_unknown):
    make_unknown(env)
    assert validate(ns, env, admitted.ref)["reason_code"] == "DEPENDENCY_UNAVAILABLE"
    assert row(env, ns).state == "admitted"  # a retry may succeed


def test_a_user_id_missing_from_the_context_is_treated_as_revoked(ns, env, admitted):
    update_context(env, ns, caller={"kind": "user", "ref": "user:42"})
    assert validate(ns, env, admitted.ref)["reason_code"] == "EPOCH_MISMATCH"
    assert env.epoch_calls == []


@pytest.mark.parametrize("values, code", [
    ({"state": "suspended"}, "DENIED_BINDING"),
    ({"state": "retired"}, "DENIED_BINDING"),
    ({"revocation_epoch": 1}, "EPOCH_MISMATCH"),
    ({"definition_digest": "adv1:" + "e" * 64}, "BINDING_DIGEST_MISMATCH"),
])
def test_a_binding_that_changed_after_admission_revokes_the_run(ns, env, admitted, values, code):
    update_binding(env, ns, **values)
    assert validate(ns, env, admitted.ref)["reason_code"] == code
    context = row(env, ns)
    assert (context.state, context.revoked_reason) == ("revoked", code)


def test_a_binding_that_was_deleted_revokes_the_run(ns, env, admitted):
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.delete(ns.models.AgentIdentityBinding))
        session.commit()
    assert validate(ns, env, admitted.ref)["reason_code"] == "DENIED_BINDING"
    assert row(env, ns).state == "revoked"


def test_a_binding_in_another_project_is_read_from_that_project(ns, env):
    env.seed_agent(project_id=9)
    view = ns.adm.admit_execution(env.module(), env.request(project_id=3, binding_project_id=9), run_id=RUN)
    assert validate(ns, env, view.context_ref, project_id=3)["ok"] is True
    update_binding(env, ns, project_id=9, state="suspended")
    assert validate(ns, env, view.context_ref, project_id=3)["reason_code"] == "DENIED_BINDING"
    [context] = env.query(3, ns.models.ExecutionSecurityContext)
    assert context.state == "revoked"


def test_losing_the_race_to_a_revocation_is_stale_not_running(ns, env, admitted, monkeypatch):
    real = ns.adm._set_state  # pylint: disable=W0212

    def revoked_meanwhile(session, context_id, state, *args, **kwargs):
        if state == "running":
            update_context(env, ns, state="revoked", revoked_reason="EPOCH_MISMATCH")
            session.rollback()
        return real(session, context_id, state, *args, **kwargs)

    monkeypatch.setattr(ns.adm, "_set_state", revoked_meanwhile)
    assert validate(ns, env, admitted.ref)["reason_code"] == "STALE_CONTEXT"
    assert row(env, ns).state == "revoked"


# --- two users x two agents --------------------------------------------------------------------

def test_contexts_do_not_cross_between_users_agents_runs_or_projects(ns, env):
    env.add_version(1, 70, 7)
    env.add_version(1, 80, 8)
    a1, a2 = env.add_binding(1, 7, 70), env.add_binding(1, 8, 80)
    refs = {}
    for user in (42, 43):
        env.verify_result = {"ok": True, "principal": admission_env.principal(user, epochs=dict(env.epochs[user]))}
        for app, version in ((7, 70), (8, 80)):
            refs[(user, app)] = ns.adm.admit_execution(
                env.module(), env.request(caller_id=user, application_id=app, version_id=version),
                run_id=f"run-{user}-{app}").context_ref

    def caller(user, app):
        return {**CALLER, "run_id": f"run-{user}-{app}"}

    # a context validates for its own run only
    assert validate(ns, env, refs[(42, 7)], caller=caller(43, 7))["reason_code"] == "STALE_CONTEXT"
    assert validate(ns, env, refs[(42, 7)], caller=caller(42, 8))["reason_code"] == "STALE_CONTEXT"
    assert validate(ns, env, refs[(42, 7)], project_id=2)["reason_code"] == "STALE_CONTEXT"
    assert all(validate(ns, env, ref, caller=caller(*key))["ok"] for key, ref in refs.items())
    # suspending agent 1 (what the admin task does) takes down both of its contexts and nothing else
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.models.AgentIdentityBinding).where(
            ns.models.AgentIdentityBinding.id == a1).values(state="suspended", revocation_epoch=1))
        assert ns.adm.revoke_binding_contexts(session, a1) == 2
        session.commit()
    for user in (42, 43):
        assert validate(ns, env, refs[(user, 7)], caller=caller(user, 7))["reason_code"] == "EPOCH_MISMATCH"
        assert validate(ns, env, refs[(user, 8)], caller=caller(user, 8))["ok"] is True
    assert a2 is not None


def test_suspending_one_agent_leaves_the_other_agents_contexts_valid(ns, env):
    env.add_version(1, 70, 7)
    env.add_version(1, 80, 8)
    a1, _ = env.add_binding(1, 7, 70), env.add_binding(1, 8, 80)
    refs = {}
    for app, version in ((7, 70), (8, 80)):
        refs[app] = ns.adm.admit_execution(
            env.module(), env.request(application_id=app, version_id=version), run_id=f"run-{app}").context_ref
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.execute(ns.sa.update(ns.models.AgentIdentityBinding).where(
            ns.models.AgentIdentityBinding.id == a1).values(state="suspended", revocation_epoch=1))
        ns.adm.revoke_binding_contexts(session, a1)
        session.commit()
    assert validate(ns, env, refs[7], caller={**CALLER, "run_id": "run-7"})["reason_code"] == "EPOCH_MISMATCH"
    assert validate(ns, env, refs[8], caller={**CALLER, "run_id": "run-8"})["ok"] is True


# --- audit, counters, robustness ---------------------------------------------------------------

def test_every_refusal_is_audited_as_a_deny_decision_without_secrets(ns, env, admitted):
    env.epochs[42] = {"active": False, "user": 3, "token": 0}
    before = ns.adm.CONTEXT_VALIDATION.total("deny", "EPOCH_MISMATCH")
    result = validate(ns, env, admitted.ref)
    assert result["message"] == ns.adm.MESSAGES["EPOCH_MISMATCH"]
    [event] = env.denied
    assert event == {
        "event_type": "decision", "decision": "deny", "reason_code": "EPOCH_MISMATCH", "project_id": 1,
        "run_id": RUN, "application_id": 7, "agent_binding_ref": str(admitted.binding), "policy_revision": "rev-1",
    }
    assert ns.adm.CONTEXT_VALIDATION.total("deny", "EPOCH_MISMATCH") == before + 1
    assert "security_metric name=execution_context_validation_total result=deny reason_code=EPOCH_MISMATCH" in env.log.lines
    assert not [line for line in env.log.lines if COOKIE in line]


def test_a_refusal_whose_audit_cannot_be_written_is_still_a_refusal(ns, env, admitted):
    env.epochs[42] = {"active": False, "user": 3, "token": 0}
    env.autocommit_raises = RuntimeError("audit down")
    assert validate(ns, env, admitted.ref)["reason_code"] == "EPOCH_MISMATCH"


def test_the_handler_never_raises_and_an_unexpected_failure_is_a_dependency_failure(ns, env, admitted, monkeypatch):
    def broken(project_id):
        raise RuntimeError("database down")

    monkeypatch.setattr(ns.tools.db, "get_session", broken)
    result = validate(ns, env, admitted.ref)
    assert result["ok"] is False and result["reason_code"] == "DEPENDENCY_UNAVAILABLE"
    assert "database down" not in json.dumps(result) and "database down" not in "\n".join(env.log.lines)


def test_untrusted_caller_values_cannot_break_the_audit_event(ns, env, admitted):
    result = validate(ns, env, admitted.ref, caller={**CALLER, "run_id": "x" * 1000})
    assert result["reason_code"] == "STALE_CONTEXT"  # the oversized run id is refused; its audit failure is swallowed
    assert row(env, ns).state == "admitted"


# --- lifecycle helpers used by the wrapper ------------------------------------------------------

def test_not_dispatched_revokes_only_a_context_that_never_started(ns, env, admitted):
    ns.adm.track_context(env.module(), None, 1, admitted.ref)
    assert (row(env, ns).state, row(env, ns).revoked_reason) == ("revoked", "NOT_DISPATCHED")
    assert validate(ns, env, admitted.ref)["reason_code"] == "STALE_CONTEXT"


def test_not_dispatched_does_not_end_a_running_context(ns, env, admitted):
    validate(ns, env, admitted.ref)
    ns.adm.track_context(env.module(), None, 1, admitted.ref)
    assert row(env, ns).state == "running"


def test_a_context_completes_once_and_cannot_be_revalidated(ns, env, admitted):
    module = env.module()
    ns.adm.track_context(module, "task-1", 1, admitted.ref)
    validate(ns, env, admitted.ref)
    ns.adm.complete_context(module, "task-1")
    ns.adm.complete_context(module, "task-1")
    assert row(env, ns).state == "completed"
    assert validate(ns, env, admitted.ref)["reason_code"] == "STALE_CONTEXT"


def test_completion_does_not_resurrect_a_revoked_context(ns, env, admitted):
    module = env.module()
    ns.adm.track_context(module, "task-1", 1, admitted.ref)
    update_context(env, ns, state="revoked", revoked_reason="EPOCH_MISMATCH")
    ns.adm.complete_context(module, "task-1")
    assert row(env, ns).state == "revoked"


def test_a_failure_to_track_is_logged_not_raised(ns, env, admitted, monkeypatch):
    monkeypatch.setattr(ns.tools.db, "get_session", lambda project_id: (_ for _ in ()).throw(RuntimeError("down")))
    ns.adm.track_context(env.module(), None, 1, admitted.ref)
    assert any("could not track" in line for line in env.log.lines)
