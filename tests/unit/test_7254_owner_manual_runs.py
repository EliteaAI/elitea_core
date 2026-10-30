"""#7254 (Roman 2026-10-30): an owner's Run now / Test run of their own schedule is the owner's own interactive run.

Entry point ``schedule_test``, built by the run endpoint from the signed-in owner (``mint_owner_run_admission``) and
taken by ``predict_sio`` from its ticket. Admitted like a toolkit test (#7281): the owner's verified subject, decided as a
development run is, whatever the development-runs switch, needing only the right to run the agent (no admin role, no
edit right); no mandate is read. Real admission on SQLite (``fixtures/admission_env.py``).
"""
import sys
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import RUN, SUBJECT, call_kwargs  # noqa: E402  pylint: disable=C0413

PREDICT = "models.applications.predict.post"
EDITOR_ONLY = {"credentials.use", PREDICT}   # an editor who owns the schedule: no admin, no edit right on the agent
DIGEST = "adv1:" + "5" * 64
SCHEDULE = "schedule:entity:41"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("owner_manual_runs"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns, monkeypatch):
    ns.env.reset()
    ns.env.facts_version = 5
    ns.env.permissions[42] = set(EDITOR_ONLY)
    ns.env.add_version(1, 70, 7)
    ns.env.digests[70] = DIGEST

    def no_mandate(*_args, **_kwargs):
        raise AssertionError("a mandate was read for an owner's own run")

    monkeypatch.setattr(ns.mandate_admission, "latest_mandates", no_mandate)
    monkeypatch.setattr(ns.adm, "_admit_mandate", no_mandate)
    return ns.env


def owner_request(env, **over):
    over.setdefault("entry_point", "schedule_test")
    over.setdefault("trigger_ref", SCHEDULE)
    return env.request(**over)


def admit(ns, env, module=None, **over):
    return ns.adm.admit_execution(module or env.module(), owner_request(env, **over), run_id=RUN, payload=None)


def refusal(ns, env, module=None, **over):
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        admit(ns, env, module, **over)
    return err.value.reason_code


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


def test_an_editor_owner_is_admitted_as_their_own_run_with_their_subject_and_no_mandate(ns, env):
    view = admit(ns, env)   # development runs off (the env default): admitted anyway, like a toolkit test
    [row] = contexts(env, ns)
    assert (row.entry_point, row.profile, row.definition_digest) == ("schedule_test", "development", DIGEST)
    assert row.subject["ref"] == SUBJECT
    assert row.caller == {"kind": "user", "ref": "user:42", "user_id": 42, "trigger_ref": SCHEDULE}
    assert view.subject_ref == SUBJECT and view.subject_user_id == 42
    assert env.permission_calls == [(42, "default", 1)] and env.denied == []


@pytest.mark.parametrize("allowed", [True, False])
def test_the_development_runs_switch_does_not_matter(ns, env, allowed):
    admit(ns, env, env.module(allow_development_runs=allowed))
    assert [row.entry_point for row in contexts(env, ns)] == ["schedule_test"]


def test_a_bound_version_is_still_the_owners_run_not_the_bindings(ns, env):
    env.seed_agent(version_id=71)   # bound and active
    env.digests[71] = DIGEST
    admit(ns, env, version_id=71)
    [row] = contexts(env, ns)
    assert row.profile == "development" and row.agent_binding_ref is None


def test_an_api_token_is_refused_like_a_toolkit_run(ns, env):
    assert refusal(ns, env, caller_type="token", caller_reference=None) == "SCHEDULE_RUN_TOKEN_NOT_ADMITTED"
    assert "Run now and Test run" in ns.adm.MESSAGES["SCHEDULE_RUN_TOKEN_NOT_ADMITTED"]
    assert contexts(env, ns) == []


def test_without_the_right_to_run_the_agent_it_is_refused_fail_closed(ns, env):
    env.permissions[42] = {"credentials.use"}
    assert refusal(ns, env) == "DENIED_RBAC"
    env.permissions_raise = RuntimeError("auth down")
    assert refusal(ns, env) == "DEPENDENCY_UNAVAILABLE"
    assert contexts(env, ns) == []


def test_a_system_or_anonymous_caller_never_takes_this_entry(ns, env):
    assert refusal(ns, env, run_kind="system") == "MANDATE_REQUIRED"
    assert refusal(ns, env, caller_type="public", caller_id=None, caller_reference=None) == "IDENTITY_EVIDENCE_MISSING"
    assert contexts(env, ns) == []


def test_audit_only_evaluates_it_as_the_owners_run_with_no_mandate_row(ns, env):
    module = env.module(governed=False, audit=True)
    assert ns.adm.admit_dispatch(module, "indexer_agent", call_kwargs({}), owner_request(env)) is None
    assert contexts(env, ns) == [] and env.denied == []
    [(_, admission)] = env.recorded
    assert (admission["reason_code"], admission["would_deny"]) == ("ADMITTED_DEVELOPMENT", False)
    assert "MANDATE" not in str(env.recorded)


# --- the ticket of the run endpoint ------------------------------------------------------------------------------------

def holder(monkeypatch, ns, **over):
    values = dict(type="user", id=42, reference="cookie-secret-value", **over)
    monkeypatch.setattr(ns.pd_admission, "_holder", lambda sid: types.SimpleNamespace(**values))


@pytest.mark.parametrize("governed, audit", [(True, False), (False, True)])
def test_the_run_endpoint_mints_the_owners_request_and_predict_takes_it_once(ns, env, monkeypatch, governed, audit):
    holder(monkeypatch, ns)
    module = env.module(governed=governed, audit=audit)
    ticket = ns.mandate_admission.mint_owner_run_admission(
        module, project_id=1, application_id=7, version_id=70, schedule_id=41)
    request = ns.mandate_admission.take_trigger_admission(module, ticket, project_id=1, version_id=70)
    assert (request.entry_point, request.run_kind, request.caller_type, request.caller_id, request.trigger_ref) == (
        "schedule_test", "interactive", "user", 42, SCHEDULE)
    assert request.mandate_ref is None
    assert ns.mandate_admission.take_trigger_admission(module, ticket, project_id=1, version_id=70) is None   # once


def test_off_the_run_endpoint_mints_nothing_and_reads_no_holder(ns, env, monkeypatch):
    monkeypatch.setattr(ns.pd_admission, "_holder", lambda sid: (_ for _ in ()).throw(AssertionError("holder read")))
    module = env.module(governed=False)
    assert ns.mandate_admission.mint_owner_run_admission(
        module, project_id=1, application_id=7, version_id=70, schedule_id=41) is None
    assert len(module._mandate_tickets) == 0   # pylint: disable=W0212
