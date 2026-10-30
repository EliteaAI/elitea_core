"""C-1009 (#7290): the custodian of a personal credential may test it without ``credentials.probe``.

A personal reference belongs to its custodian. Testing their own (tester = actor = custodian, ``credentials.use`` in the
project) needs no ``credentials.probe``; everything else an actor without it asks for is ``DENIED_RBAC``, as before the
facts were read, with the same audit row (no reference named). Real models and statements, the real probe; the stored
configuration, the secrets plugin, Redis, the task node and the permission check are fakes that record their calls
(``fixtures/admission_env.py``).
"""
import copy
import sys
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import Credential, seeded  # noqa: E402  pylint: disable=C0413

OWNER, MEMBER = 42, 43
USE_ONLY = {"credentials.use", "configuration.secrets.secret.unsecret"}   # an editor/viewer: no admin role
OPERATOR = {"credentials.probe", "credentials.use"}
VOLATILE = ("event_id",)


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("custodian_probe"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    ns.env.permissions[OWNER] = set(USE_ONLY)
    ns.env.permissions[MEMBER] = set(USE_ONLY)
    ns.credential_probe.forget_outcomes()

    def stored(project_id, configuration_uuid, user_id):
        return {"configuration_uuid": configuration_uuid, "configuration_project_id": project_id,
                "configuration_type": "jira", "configuration_generation": 3, "base_url": "https://jira.example",
                "token": "{{secret.jira_token}}"}

    ns.credential_probe._stored_configuration = stored   # pylint: disable=W0212
    yield ns.env
    ns.credential_probe.forget_outcomes()


def personal(cred, custodian=OWNER):
    cred.reference.update(ownership_class="personal", custodian_user_id=custodian)
    return cred


def check(ns, env, cred, user_id=OWNER, actor_user_id=None, **kwargs):
    return ns.credential_probe.governed_check_connection(
        env.module(), 1, cred.uuid, "jira", user_id, actor_user_id, **kwargs)


def counts(env):
    """The reads and calls one probe makes."""
    return {"sessions": len(env.sessions), "permissions": len(env.permission_calls),
            "configuration_facts": len(env.configuration_calls), "reference_facts": len(env.secrets_calls),
            "capability_asks": len(env.capability_asks), "redis": len(env.redis.calls), "tracing": env.tracing_calls,
            "started": len(env.started), "audit_in_tx": len(env.recorded), "audit_own_tx": len(env.denied)}


def shape(event):
    return {key: value for key, value in event.items() if key not in VOLATILE}


def rbac_refusal(ns, env, answer, user_id=OWNER, actor_user_id=OWNER):
    """``DENIED_RBAC`` exactly as an actor without the permission was refused before #7290: no task, no ticket, one
    deny row naming the project and the users only."""
    assert answer == {"success": False, "code": "DENIED_RBAC",
                      "message": ns.credential_probe.PROBE_MESSAGES["DENIED_RBAC"]}
    assert env.started == [] and env.redis.calls == [] and env.capability_asks == [] and env.recorded == []
    [event] = env.denied
    assert shape(event) == {
        "event_type": "probe", "decision": "deny", "reason_code": "DENIED_RBAC", "project_id": 1,
        "caller_ref": f"user:{actor_user_id}", "user_ref": f"user:{user_id}", "policy_revision": "rev-1",
        **({"environment": event["environment"]} if "environment" in event else {})}
    return event


# --- the custodian tests their own ---------------------------------------------------------------------------------

def test_the_custodian_without_credentials_probe_tests_their_own_personal_credential(ns, env):
    cred = personal(seeded(env))
    assert check(ns, env, cred) == {"success": True}
    [(_, kwargs, pool, _)] = env.started
    assert pool == "governed_ops" and kwargs["settings"]["credential_slot"]["credential_ref"] == cred.ref_id
    [(_, allow)] = env.recorded
    assert shape(allow) == {
        "event_type": "probe", "decision": "allow", "reason_code": "CREDENTIAL_PROBE_ALLOWED", "project_id": 1,
        "caller_ref": "user:42", "user_ref": "user:42", "credential_ref": cred.ref_id, "credential_generation": 1,
        "policy_revision": "rev-1"}
    [done] = env.denied
    assert (done["decision"], done["reason_code"], done["outcome"], done["credential_ref"]) == (
        "allow", "CREDENTIAL_PROBE_SUCCEEDED", "success", cred.ref_id)


def test_the_custodians_probe_costs_and_records_what_a_permitted_actors_does(ns, env):
    """Same reads, same calls, same audit rows (bar the event id): the rule adds nothing for anyone."""
    cred = personal(seeded(env))
    env.permissions[OWNER] = set(OPERATOR)
    before = counts(env)
    assert check(ns, env, cred) == {"success": True}
    permitted = {key: value - before[key] for key, value in counts(env).items()}
    permitted_rows = [shape(event) for _, event in env.recorded] + [shape(event) for event in env.denied]
    env.recorded.clear()
    env.denied.clear()
    env.permissions[OWNER] = set(USE_ONLY)
    before = counts(env)
    assert check(ns, env, cred) == {"success": True}
    custodian = {key: value - before[key] for key, value in counts(env).items()}
    custodian_rows = [shape(event) for _, event in env.recorded] + [shape(event) for event in env.denied]
    assert custodian == permitted
    assert permitted["permissions"] == permitted["configuration_facts"] == permitted["reference_facts"] == 1
    assert custodian_rows == permitted_rows and len(custodian_rows) == 2


def test_a_failed_check_of_the_custodians_own_credential_is_the_providers_answer(ns, env):
    cred = personal(seeded(env))
    env.task_result = "Invalid token for the provider"
    assert check(ns, env, cred) == {"success": False, "message": "Invalid token for the provider"}
    [done] = env.denied
    assert (done["reason_code"], done["outcome"]) == ("CREDENTIAL_PROBE_FAILED", "failed")


def test_past_admission_the_custodian_sees_the_refusals_a_permitted_actor_sees(ns, env):
    cred = personal(seeded(env))
    env.capable = False
    answer = check(ns, env, cred)
    assert answer["code"] == "WORKER_CAPABILITY_UNAVAILABLE"
    [event] = env.denied
    assert event["credential_ref"] == cred.ref_id   # their own reference: named, as for a permitted actor


# --- everyone else without the permission: DENIED_RBAC, nothing said about the reference ----------------------------

def test_another_member_without_the_permission_is_refused_on_someone_elses_personal_credential(ns, env):
    cred = personal(seeded(env))
    rbac_refusal(ns, env, check(ns, env, cred, user_id=MEMBER), user_id=MEMBER, actor_user_id=MEMBER)
    env.denied.clear()   # a member holding the permission is told the class, as before
    env.permissions[MEMBER] = set(OPERATOR)
    assert check(ns, env, cred, user_id=MEMBER)["code"] == "CREDENTIAL_CLASS_NOT_ALLOWED"


def test_a_team_credential_needs_the_permission(ns, env):
    cred = seeded(env)   # project_shared
    rbac_refusal(ns, env, check(ns, env, cred))
    env.denied.clear()
    env.permissions[OWNER] = set(OPERATOR)
    assert check(ns, env, cred) == {"success": True}


def test_a_personal_credential_without_a_custodian_is_not_anyones_own(ns, env):
    cred = personal(seeded(env), custodian=None)
    rbac_refusal(ns, env, check(ns, env, cred))


@pytest.mark.parametrize("name, change", [
    ("no such configuration", lambda env, cred: env.configurations.clear()),
    ("no reference", lambda env, cred: env.references.clear()),
    ("another project", lambda env, cred: env.configurations[(1, cred.uuid)].update(project_id=9)),
    ("not a credential", lambda env, cred: env.configurations[(1, cred.uuid)].update(section="vector_storage")),
    ("another type", lambda env, cred: env.configurations[(1, cred.uuid)].update(type="github")),
    ("suspended", lambda env, cred: cred.reference.update(state="suspended")),
    ("pending", lambda env, cred: cred.reference.update(state="pending")),
    ("retired", lambda env, cred: cred.reference.update(state="retired")),
    ("a store the platform does not read", lambda env, cred: cred.reference.update(store_kind="managed_vault")),
    ("changed after approval", lambda env, cred: env.configurations[(1, cred.uuid)].update(generation=4)),
    ("configuration facts unreadable", lambda env, cred: setattr(env, "configurations_raises", RuntimeError("x"))),
    ("reference facts unreadable", lambda env, cred: setattr(env, "secrets_raises", RuntimeError("x"))),
])
def test_without_the_permission_any_credential_that_is_not_ones_own_probe_able_personal_one_is_denied_rbac(
        ns, env, name, change):
    """Missing, foreign, inactive, changed, unreadable: one answer, so the refusal tells nothing of the reference."""
    cred = personal(seeded(env))
    change(env, cred)
    rbac_refusal(ns, env, check(ns, env, cred))


def test_the_same_changes_are_told_by_name_to_a_permitted_actor(ns, env):
    env.permissions[OWNER] = set(OPERATOR)
    cred = personal(seeded(env))
    cred.reference.update(state="suspended")
    assert check(ns, env, cred)["code"] == "CREDENTIAL_REFERENCE_INACTIVE"


def test_an_operator_on_the_custodians_behalf_needs_the_permission(ns, env):
    cred = personal(seeded(env))
    rbac_refusal(ns, env, check(ns, env, cred, user_id=OWNER, actor_user_id=MEMBER),
                 user_id=OWNER, actor_user_id=MEMBER)
    env.denied.clear()
    env.permissions[MEMBER] = set(OPERATOR)
    assert check(ns, env, cred, user_id=OWNER, actor_user_id=MEMBER) == {"success": True}


def test_the_credential_onboardings_probe_needs_the_permission_even_for_the_custodian(ns, env):
    """The secrets plugin's probe action (``onboarding``) is an operator's: unchanged, refused before any facts read."""
    cred = personal(seeded(env))
    cred.reference.update(state="pending")
    rbac_refusal(ns, env, check(ns, env, cred, actor_user_id=OWNER, onboarding=True))
    assert env.configuration_calls == [] and env.secrets_calls == []


def test_neither_permission_is_refused_before_any_facts_are_read(ns, env):
    """Not a user of credentials in the project (not a member): refused as before, at the permission read."""
    cred = personal(seeded(env))
    env.permissions[OWNER] = set()
    rbac_refusal(ns, env, check(ns, env, cred))
    assert env.permission_calls == [(OWNER, "default", 1)]
    assert env.configuration_calls == [] and env.secrets_calls == []


def test_a_kept_outcome_is_not_served_once_the_permission_is_gone(ns, env):
    cred = seeded(env)   # a team credential, tested by an operator through a validator
    env.permissions[OWNER] = set(OPERATOR)
    assert check(ns, env, cred, reuse=True) == {"success": True}
    env.permissions[OWNER] = set(USE_ONLY)
    for recorded in (env.started, env.redis.calls, env.capability_asks, env.recorded, env.denied):
        recorded.clear()
    rbac_refusal(ns, env, check(ns, env, cred, reuse=True))   # the kept success is not the answer


def test_the_custodians_validator_check_is_kept_like_anyone_elses(ns, env):
    cred = personal(seeded(env))
    assert check(ns, env, cred, reuse=True) == {"success": True}
    assert check(ns, env, cred, reuse=True) == {"success": True}
    assert len(env.started) == 1


# --- the validators' batch (#7294 P-01) ---------------------------------------------------------------------------

def _mixed(env):
    own = personal(seeded(env))
    team = Credential(env, own.binding, tool_id=2, config_id=6)
    others = personal(Credential(env, own.binding, tool_id=3, config_id=7), custodian=MEMBER)
    return own, team, others


def test_a_batch_without_the_permission_probes_the_own_personal_and_refuses_the_rest(ns, env):
    own, team, others = _mixed(env)
    items = [{"configuration_uuid": cred.uuid, "config_type": "jira"} for cred in (own, team, others)]
    before = counts(env)
    answers = ns.credential_probe.governed_check_connection_many(env.module(), 1, items, OWNER, reuse=True)
    assert answers[0] == {"success": True}
    assert [answer["code"] for answer in answers[1:]] == ["DENIED_RBAC"] * 2
    used = {key: value - before[key] for key, value in counts(env).items()}
    assert (used["permissions"], used["configuration_facts"], used["reference_facts"]) == (1, 1, 1)   # once per batch
    [(_, kwargs, _, _)] = env.started
    assert kwargs["settings"]["credential_slot"]["credential_ref"] == own.ref_id
    denies = [event for event in env.denied if event["decision"] == "deny"]
    assert len(denies) == 2
    for event in denies:
        assert "credential_ref" not in event and event["reason_code"] == "DENIED_RBAC"
    [(_, allow)] = env.recorded
    assert allow["credential_ref"] == own.ref_id


def test_a_batch_with_the_permission_is_unchanged(ns, env):
    own, team, others = _mixed(env)
    env.permissions[OWNER] = set(OPERATOR)
    items = [{"configuration_uuid": cred.uuid, "config_type": "jira"} for cred in (own, team, others)]
    answers = ns.credential_probe.governed_check_connection_many(env.module(), 1, items, OWNER)
    assert answers[:2] == [{"success": True}] * 2 and answers[2]["code"] == "CREDENTIAL_CLASS_NOT_ALLOWED"


def test_a_batch_without_either_permission_is_refused_whole_before_any_facts(ns, env):
    own, team, _others = _mixed(env)
    env.permissions[OWNER] = set()
    items = [{"configuration_uuid": cred.uuid, "config_type": "jira"} for cred in (own, team)]
    answers = ns.credential_probe.governed_check_connection_many(env.module(), 1, items, OWNER)
    assert [answer["code"] for answer in answers] == ["DENIED_RBAC"] * 2
    assert env.configuration_calls == [] and env.secrets_calls == [] and env.started == []
    assert len(env.permission_calls) == 1


def test_a_batch_costs_the_custodian_what_it_costs_a_permitted_actor(ns, env):
    own = personal(seeded(env))
    second = personal(Credential(env, own.binding, tool_id=2, config_id=6))
    items = [{"configuration_uuid": cred.uuid, "config_type": "jira"} for cred in (own, second)]
    used = []
    for permissions in (OPERATOR, USE_ONLY):
        env.permissions[OWNER] = set(permissions)
        before = counts(env)
        assert ns.credential_probe.governed_check_connection_many(env.module(), 1, copy.deepcopy(items), OWNER) == [
            {"success": True}] * 2
        used.append({key: value - before[key] for key, value in counts(env).items()})
    assert used[0] == used[1]
