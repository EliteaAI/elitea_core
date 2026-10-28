"""C-1009 (#7120): audit only - every run is evaluated as enforce would evaluate it, inline, and nothing is refused.

The real wrapper of ``module.py``, the real admission code and statements run against SQLite files (see
``fixtures/admission_env.py``). A dispatch under audit only must always reach ``start_task``, with the legacy envelope
and no context; what enforce would have said is an audit event (``decision=allow``, ``would_deny=true``,
``reason_code=<the code enforce would refuse with>``) and a metric.
"""
import copy
import json
import os
import sys
import uuid
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import admission_env  # noqa: E402  pylint: disable=C0413
from fixtures.admission_env import principal  # noqa: E402  pylint: disable=C0413
from fixtures.dispatch_wrapper import Original, build_wrapper  # noqa: E402  pylint: disable=C0413
from fixtures.governed_credentials import SECRET, Credential, payload_of  # noqa: E402  pylint: disable=C0413

RUN = "run-1"


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield admission_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("audit_only"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    ns.env.reset()
    return ns.env


def contexts(env, ns):
    return env.query(1, ns.models.ExecutionSecurityContext)


def dispatch(ns, env, payload=None, module=None, admission="default", name="indexer_agent", **request):
    """One dispatch through the real wrapper: ``(task id, what start_task received, wrapper module)``."""
    module = module or env.module(governed=False, audit=True)
    original = Original()
    wrapper = build_wrapper(ns, module, original)
    kwargs = {"args": ["s", "m"], "kwargs": {"project_id": 1, "message_id": 5, **(payload or {})}, "pool": "agents",
              "meta": {"project_id": 1}}
    if admission == "default":
        admission = env.request(**request)
    if admission is not None:
        kwargs["admission"] = admission
    task_id = wrapper(name, **kwargs)
    return task_id, original.calls, module


def legacy_dispatch(ns, env, payload=None, name="indexer_agent"):
    """The same dispatch with governance off, for comparison."""
    return dispatch(ns, env, payload, module=env.module(governed=False), admission=None, name=name)[1]


def assert_dispatched_as_legacy(ns, env, calls, payload=None):
    [(args, sent)] = calls
    assert args == ("indexer_agent",)
    assert sent["kwargs"]["envelope"]["security_profile"] == "legacy" and sent["kwargs"]["envelope"]["context_ref"] is None
    assert "context_ref" not in sent["meta"] and "admission" not in sent
    assert contexts(env, ns) == []
    assert calls == legacy_dispatch(ns, env, payload)   # not one byte of the call differs from today's


def cred_case(**over):
    return lambda env: [Credential(env, env.seed_agent(), **over)]


def seeded_agent(env):
    env.seed_agent()
    return []


def verify_rejects(code):
    def setup(env):
        env.seed_agent()
        env.verify_result = {"ok": False, "reason_code": code, "message": "fixed text"}
        return []
    return setup


def mutate(env_change):
    def setup(env):
        creds = [Credential(env, env.seed_agent())]
        env_change(env, creds[0])
        return creds
    return setup


def env_set(name, value):
    return lambda env, _cred=None: setattr(env, name, value)


SYSTEM = dict(run_kind="system", entry_point="webhook", caller_type="trigger", caller_id=None, caller_reference=None,
              trigger_ref="webhook:version:70")
PUBLIC = dict(caller_type="public", caller_id=None, caller_reference=None)
TOKEN = dict(caller_type="token", caller_id=7, caller_reference=None)


def wrong_user(env):
    env.seed_agent()
    env.verify_result = {"ok": True, "principal": principal(43)}
    return []


def unverified(env):
    env.seed_agent()
    env.verify_result = {"ok": True, "principal": principal(42, verified=False)}
    return []


def no_tenant(env):
    env.seed_agent()
    env.verify_result = {"ok": True, "principal": principal(42, tid="")}
    return []


def verifier_down(env):
    env.seed_agent()
    env.verify_raises = RuntimeError("identity service: secret-detail")
    return []


def digest_changed(env):
    env.seed_agent()
    env.digests[70] = "adv1:" + "f" * 64
    return []


def personal(env, cred):
    cred.reference["ownership_class"] = "personal"


# Every refusal of steps 2 to 5b, with the request it needs and how to set it up
# (id, request override, setup(env) -> credentials, the code enforce would refuse with)
DENIES = [
    ("step 3: no verifiable caller", PUBLIC, seeded_agent, "IDENTITY_EVIDENCE_MISSING"),
    ("step 3: a token is not an admitted kind", TOKEN, verify_rejects("IDENTITY_KIND_NOT_ADMITTED"),
     "IDENTITY_KIND_NOT_ADMITTED"),
    ("step 3: revoked", {}, verify_rejects("IDENTITY_REVOKED"), "IDENTITY_REVOKED"),
    ("step 3: expired", {}, verify_rejects("IDENTITY_EXPIRED"), "IDENTITY_EXPIRED"),
    ("step 3: bad signature", {}, verify_rejects("IDENTITY_SIGNATURE_INVALID"), "IDENTITY_SIGNATURE_INVALID"),
    ("step 3: wrong audience", {}, verify_rejects("IDENTITY_AUDIENCE_MISMATCH"), "IDENTITY_AUDIENCE_MISMATCH"),
    ("step 3: wrong tenant", {}, verify_rejects("IDENTITY_TENANT_MISMATCH"), "IDENTITY_TENANT_MISMATCH"),
    ("step 3: a code that is not a code", {}, verify_rejects("not a code!"), "DEPENDENCY_UNAVAILABLE"),
    ("step 3: the verified user is not the caller", {}, wrong_user, "IDENTITY_SUBJECT_UNLINKED"),
    ("step 3: the verifier is down", {}, verifier_down, "DEPENDENCY_UNAVAILABLE"),
    ("step 4: no binding", {}, lambda env: [], "DENIED_BINDING"),
    ("step 4: the definition changed after approval", {}, digest_changed, "BINDING_DIGEST_MISMATCH"),
    ("step 5: an asserted user needs a mandate", {}, unverified, "MANDATE_REQUIRED"),
    ("step 5: verified user without tenant evidence", {}, no_tenant, "IDENTITY_EVIDENCE_MISSING"),
    ("step 5b: credential not registered", {}, cred_case(reference=False), "CREDENTIAL_REFERENCE_REQUIRED"),
    ("step 5b: credential suspended", {}, cred_case(ref_state="suspended"), "CREDENTIAL_REFERENCE_INACTIVE"),
    ("step 5b: credential changed after approval", {}, cred_case(config_generation=4),
     "CREDENTIAL_GENERATION_MISMATCH"),
    ("step 5b: a personal credential", {}, mutate(personal), "CREDENTIAL_CLASS_NOT_ALLOWED"),
    ("step 5b: no grant", {}, cred_case(grants=False), "DENIED_GRANT"),
    ("step 5b: the user may not use credentials",
     {}, mutate(lambda env, cred: env.permissions.__setitem__(42, {"projects.read"})), "DENIED_RBAC"),
    ("step 5b: the facts are unreadable",
     {}, mutate(lambda env, cred: setattr(env, "secrets_raises", RuntimeError("db down: secret-detail"))),
     "DEPENDENCY_UNAVAILABLE"),
]


@pytest.mark.parametrize("request_over, setup, code", [case[1:] for case in DENIES], ids=[case[0] for case in DENIES])
def test_each_refusal_of_steps_2_to_5b_is_recorded_as_a_would_deny_and_the_run_goes_on(ns, env, request_over, setup, code):
    creds = setup(env)
    payload = payload_of(*creds)
    before = (ns.adm.ADMISSION_DECISION.total("would_deny", code), ns.adm.ADMISSION_DURATION.count("audit_only", "would_deny"))
    task_id, calls, _ = dispatch(ns, env, payload, **request_over)
    assert task_id == "task-1"   # start_task was called and its answer is the caller's
    [event] = env.denied
    assert (event["event_type"], event["decision"], event["reason_code"], event["would_deny"]) == (
        "admission", "allow", code, True)
    assert (event["project_id"], event["policy_revision"]) == (1, "rev-1") and event["run_id"]
    assert env.recorded == []   # nothing was admitted, so no allow rows
    assert ns.adm.ADMISSION_DECISION.total("would_deny", code) == before[0] + 1
    assert ns.adm.ADMISSION_DURATION.count("audit_only", "would_deny") == before[1] + 1
    assert f"security_metric name=admission_decision_total decision=would_deny reason_code={code}" in env.log.lines
    assert_dispatched_as_legacy(ns, env, calls, payload)
    assert not [line for line in env.log.lines if "secret-detail" in line]


def test_step_1_an_audit_trail_that_cannot_take_a_decision_is_a_metric_and_a_log_line_only(ns, env):
    for break_it in (lambda: setattr(env, "audit_allowed", False), lambda: setattr(env, "audit_allowed_raises", True)):
        env.reset()
        env.seed_agent()
        break_it()
        before = ns.adm.ADMISSION_DECISION.total("would_deny", "AUDIT_UNAVAILABLE")
        task_id, calls, _ = dispatch(ns, env)
        assert task_id == "task-1" and env.denied == [] and env.recorded == []   # nowhere to write it
        assert ns.adm.ADMISSION_DECISION.total("would_deny", "AUDIT_UNAVAILABLE") == before + 1
        assert ("security_metric name=admission_decision_total decision=would_deny reason_code=AUDIT_UNAVAILABLE"
                in env.log.lines)
        assert_dispatched_as_legacy(ns, env, calls)


def test_a_run_with_no_run_id_cannot_be_evaluated_and_goes_on(ns, env):
    env.seed_agent()
    module = env.module(governed=False, audit=True)
    call = {"args": [], "kwargs": {"project_id": 1}, "pool": "agents", "meta": {"project_id": 1}}
    before = copy.deepcopy(call)
    assert ns.adm.admit_dispatch(module, "indexer_agent", call, env.request()) is None
    assert call == before
    assert [e["reason_code"] for e in env.denied] == ["DEPENDENCY_UNAVAILABLE"] and env.denied[0]["would_deny"] is True


def test_an_allowed_run_writes_the_allow_events_with_would_deny_false_and_no_context(ns, env):
    cred = Credential(env, env.seed_agent())
    payload = payload_of(cred)
    before = (ns.adm.ADMISSION_DECISION.total("would_allow", "ADMITTED"),
              ns.adm.CREDENTIAL_DECISION.total("would_allow", "CREDENTIAL_USE_ALLOWED"),
              ns.adm.ADMISSION_DURATION.count("audit_only", "would_allow"))
    task_id, calls, _ = dispatch(ns, env, payload)
    assert task_id == "task-1" and env.denied == []
    [(decision_session, decision), (admission_session, admission)] = env.recorded
    assert decision_session is admission_session   # one transaction, one commit
    [(_, write_session)] = [entry for entry in env.sessions if entry[1] is decision_session]
    assert write_session.commits == 1
    assert (decision["event_type"], decision["decision"], decision["reason_code"], decision["would_deny"]) == (
        "decision", "allow", "CREDENTIAL_USE_ALLOWED", False)
    assert decision["credential_ref"] == cred.ref_id and decision["policy_revision"] == "rev-1"
    assert (admission["event_type"], admission["decision"], admission["reason_code"], admission["would_deny"]) == (
        "admission", "allow", "ADMITTED", False)
    assert "attempt" not in admission and admission["run_id"] and admission["user_ref"] == "idp:tid-1:oid-42"
    assert ns.adm.ADMISSION_DECISION.total("would_allow", "ADMITTED") == before[0] + 1
    assert ns.adm.CREDENTIAL_DECISION.total("would_allow", "CREDENTIAL_USE_ALLOWED") == before[1] + 1
    assert ns.adm.ADMISSION_DURATION.count("audit_only", "would_allow") == before[2] + 1
    # the legacy material path is untouched: the secrets are still in the payload, nothing is sealed into a slot
    [(_, sent)] = calls
    assert sent["kwargs"]["application"] == payload["application"]
    assert_dispatched_as_legacy(ns, env, calls, payload)


def test_an_agent_without_credentials_is_allowed_with_a_single_admission_event(ns, env):
    env.seed_agent()
    dispatch(ns, env)
    [(_, admission)] = env.recorded
    assert admission["event_type"] == "admission" and admission["would_deny"] is False


def test_the_trace_reference_of_the_dispatch_is_carried_into_the_event(ns, env):
    env.seed_agent()
    module = env.module(governed=False, audit=True)
    call = {"args": [], "kwargs": {"project_id": 1, "_elitea_predict_run_id": RUN}, "pool": "agents",
            "meta": {"project_id": 1, "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"}}
    ns.adm.admit_dispatch(module, "indexer_agent", call, env.request())
    assert env.recorded[0][1]["traceparent"] == call["meta"]["traceparent"]


def test_every_event_of_audit_only_is_valid_against_the_audit_schema(ns, env):
    """The harness validates each event against the tracing plugin's model. It must be a model that has ``would_deny``:
    a run without a tracing checkout skips (``ELITEA_TEST_NO_TRACING=1``, or none next to this plugin)."""
    if ns.event_model is None:
        assert os.environ.get("ELITEA_TEST_NO_TRACING") or not any(
            (parent / "tracing" / "models" / "security_audit_event.py").is_file() for parent in PLUGIN_ROOT.parents), (
            "a tracing checkout is next to this plugin but its model was not used")
        pytest.skip("no tracing model in this run (ELITEA_TEST_NO_TRACING=1, or no checkout next to this plugin)")
    model = ns.event_model.SecurityAuditEventV1
    assert "would_deny" in model.model_fields, "the tracing model found has no would_deny: merge tracing #7120 first"
    cred = Credential(env, env.seed_agent())
    dispatch(ns, env, payload_of(cred))
    allowed = [event for _, event in env.recorded]
    env.reset()
    env.seed_agent()
    env.verify_result = {"ok": False, "reason_code": "IDENTITY_REVOKED", "message": "x"}
    dispatch(ns, env)
    assert allowed and env.denied
    for event in allowed + env.denied:
        validated = model.model_validate(event)   # the real model, not the harness's tolerant copy
        assert validated.would_deny is event["would_deny"]
    assert env.denied[0]["would_deny"] is True and all(event["would_deny"] is False for event in allowed)


# --- nothing in audit only can refuse or fail the run ----------------------------------------------

def test_an_unexpected_failure_in_the_evaluation_is_a_metric_and_a_log_and_the_dispatch_is_unaffected(ns, env, monkeypatch):
    env.seed_agent()

    def boom(*args, **kwargs):
        raise ValueError("secret-detail: cookie=abc")
    monkeypatch.setattr(ns.adm, "_admit", boom)
    before = (ns.adm.ADMISSION_DECISION.total("would_deny_error", "EVALUATION_FAILED"),
              ns.adm.ADMISSION_DURATION.count("audit_only", "would_deny_error"))
    task_id, calls, _ = dispatch(ns, env)
    assert task_id == "task-1" and env.denied == [] and env.recorded == []
    assert ns.adm.ADMISSION_DECISION.total("would_deny_error", "EVALUATION_FAILED") == before[0] + 1
    assert ns.adm.ADMISSION_DURATION.count("audit_only", "would_deny_error") == before[1] + 1
    assert any("audit-only evaluation failed (ValueError)" in line for line in env.log.lines)
    assert not any("secret-detail" in line or "abc" in line for line in env.log.lines)   # the type of the exception only
    monkeypatch.undo()
    assert_dispatched_as_legacy(ns, env, calls)


def test_an_audit_write_that_fails_on_the_allowed_path_is_a_would_deny_error_and_the_run_goes_on(ns, env):
    env.seed_agent()
    env.record_raises = RuntimeError("outbox full")
    before = ns.adm.ADMISSION_DECISION.total("would_deny_error", "EVALUATION_FAILED")
    task_id, calls, _ = dispatch(ns, env)
    assert task_id == "task-1" and env.recorded == []
    assert ns.adm.ADMISSION_DECISION.total("would_deny_error", "EVALUATION_FAILED") == before + 1
    env.record_raises = None
    assert_dispatched_as_legacy(ns, env, calls)


def test_a_would_deny_that_cannot_be_written_is_logged_and_the_run_goes_on(ns, env):
    env.seed_agent()
    env.verify_result = {"ok": False, "reason_code": "IDENTITY_REVOKED", "message": "x"}
    env.autocommit_raises = RuntimeError("outbox full")
    task_id, calls, _ = dispatch(ns, env)
    assert task_id == "task-1" and env.denied == []
    assert any("would-deny event IDENTITY_REVOKED was not recorded (RuntimeError)" in line for line in env.log.lines)
    assert ns.adm.ADMISSION_DECISION.total("would_deny", "IDENTITY_REVOKED") >= 1
    env.autocommit_raises = None
    assert_dispatched_as_legacy(ns, env, calls)


def test_a_failure_while_recording_a_would_deny_is_contained(ns, env, monkeypatch):
    env.seed_agent()
    env.verify_result = {"ok": False, "reason_code": "IDENTITY_REVOKED", "message": "x"}
    monkeypatch.setattr(ns.adm, "_record_deny", lambda *args, **kwargs: 1 / 0)
    assert dispatch(ns, env)[0] == "task-1"


def test_a_run_with_no_person_behind_it_is_not_evaluated_and_never_a_would_deny(ns, env):
    """A schedule, a webhook or a system user: enforce refuses these without a mandate, and a mandate cannot be rehearsed
    (its ticket is minted only for enforce), so audit only would record a refusal that enforce does not always give."""
    env.seed_agent()
    before = ns.adm.ADMISSION_DECISION.total("would_deny", "MANDATE_REQUIRED")
    for over in (SYSTEM, dict(SYSTEM, entry_point="schedule", trigger_ref="schedule:version:70"),
                 dict(run_kind="system", entry_point="chat_sio")):
        task_id, calls, _ = dispatch(ns, env, **over)
        assert task_id == "task-1"
        assert_dispatched_as_legacy(ns, env, calls)
    assert env.denied == [] and env.recorded == [] and env.verify_calls == []
    assert ns.adm.ADMISSION_DECISION.total("would_deny", "MANDATE_REQUIRED") == before
    text = (PLUGIN_ROOT / "utils" / "governance_settings.py").read_text()
    assert "Sub-agent runs and runs started by schedules or webhooks are not evaluated in this mode." in text


def test_a_mandate_request_or_a_dispatch_without_a_request_is_not_evaluated(ns, env):
    env.seed_agent()
    env.poison()
    mandate = env.request(run_kind="mandate", entry_point="schedule", caller_type="trigger", caller_id=None,
                          caller_reference=None, mandate_ref=str(uuid.uuid4()), trigger_ref="schedule:version:70")
    for admission in (mandate, None):
        task_id, calls, _ = dispatch(ns, env, admission=admission)
        assert task_id == "task-1" and len(calls) == 1
    env.unpoison()
    assert env.denied == [] and env.recorded == [] and env.verify_calls == []


def test_children_and_inherited_dispatches_are_not_evaluated(ns, env):
    """Child and parent-reconcile dispatches carry no request (and no parent context under audit only): nothing runs."""
    env.seed_agent()
    env.poison()
    module = env.module(governed=False, audit=True)
    for envelope in (None, {"schema_version": 1, "security_profile": "legacy", "required_capabilities": [],
                            "context_ref": None}):
        call = {"args": [], "kwargs": {"project_id": 1, "_elitea_predict_run_id": RUN, "envelope": envelope},
                "pool": "agents", "meta": {"project_id": 1}}
        before = copy.deepcopy(call)
        assert ns.adm.admit_dispatch(module, "indexer_agent", call, None) is None and call == before
    env.unpoison()
    assert env.denied == [] and env.recorded == []


def test_tasks_that_are_not_admitted_are_not_evaluated(ns, env):
    env.poison()
    call = {"args": [], "kwargs": {"project_id": 1, "_elitea_predict_run_id": RUN}, "pool": "agents", "meta": {}}
    assert ns.adm.admit_dispatch(env.module(governed=False, audit=True), "indexer_validator", call, env.request()) is None


# --- the two refusals that enforce makes when it seals the payload ---------------------------------------

def free_form_payload(cred):
    payload = payload_of(cred)
    payload["application"]["version_details"]["tools"].append(
        {"id": 9, "type": "x", "settings": {"headers": {"X-Key": "{{secret.api_key}}"}}})
    return payload


def test_a_secret_template_outside_a_credential_is_a_would_deny_found_before_the_unsecret(ns, env):
    cred = Credential(env, env.seed_agent())
    payload = free_form_payload(cred)
    module = env.module(governed=False, audit=True)
    found = ns.credential_slots.has_free_form_secret(module, payload)   # as the entry point does, before unsecret
    assert found is True
    ns.credential_slots.mark_free_form_secret(payload, found)
    before = (ns.adm.CREDENTIAL_DECISION.total("would_deny", "CREDENTIAL_REFERENCE_REQUIRED"),
              ns.adm.ADMISSION_DECISION.total("would_deny", "CREDENTIAL_REFERENCE_REQUIRED"))
    task_id, calls, _ = dispatch(ns, env, payload, module=module)
    assert task_id == "task-1"
    [event] = env.denied   # before any admission was made: nothing was evaluated, nothing allowed
    assert (event["decision"], event["reason_code"], event["would_deny"]) == (
        "allow", "CREDENTIAL_REFERENCE_REQUIRED", True)
    assert env.recorded == [] and env.verify_calls == []
    assert ns.adm.CREDENTIAL_DECISION.total("would_deny", "CREDENTIAL_REFERENCE_REQUIRED") == before[0] + 1
    assert ns.adm.ADMISSION_DECISION.total("would_deny", "CREDENTIAL_REFERENCE_REQUIRED") == before[1] + 1
    [(_, sent)] = calls
    assert ns.credential_slots.FREE_FORM_SECRET_MARK not in sent["kwargs"]   # the mark never travels
    assert_dispatched_as_legacy(ns, env, calls, {k: v for k, v in payload.items()
                                                 if k != ns.credential_slots.FREE_FORM_SECRET_MARK})


def test_the_free_form_scan_runs_for_audit_only_and_enforce_and_never_for_off(ns, env):
    payload = free_form_payload(Credential(env, env.seed_agent()))
    has = ns.credential_slots.has_free_form_secret
    assert has(env.module(governed=False, audit=True), payload) is True and has(env.module(), payload) is True
    env.poison()
    assert has(env.module(governed=False), payload) is False
    env.unpoison()


def test_a_marked_payload_loses_its_mark_even_when_nothing_is_evaluated(ns, env):
    env.seed_agent()
    module = env.module(governed=False, audit=True)
    for admission in (None, env.request(**SYSTEM)):
        payload = {}
        ns.credential_slots.mark_free_form_secret(payload, True)
        call = {"args": [], "kwargs": payload, "pool": "agents", "meta": {}}
        assert ns.adm.admit_dispatch(module, "indexer_agent", call, admission) is None
        assert call["kwargs"] == {}


def test_a_refusal_when_the_payload_is_sealed_is_a_would_deny_and_the_real_payload_is_not_sealed(ns, env):
    cred = Credential(env, env.seed_agent())
    env.secret_fields_raises = RuntimeError("configurations down: " + SECRET)   # admission passes, the seal cannot
    payload = payload_of(cred)
    before = (ns.adm.CREDENTIAL_DECISION.total("would_deny", "DEPENDENCY_UNAVAILABLE"),
              ns.adm.ADMISSION_DECISION.total("would_deny", "DEPENDENCY_UNAVAILABLE"),
              ns.adm.ADMISSION_DURATION.count("audit_only", "would_deny"))
    task_id, calls, _ = dispatch(ns, env, payload)
    assert task_id == "task-1"
    # as for enforce: the admission was allowed and written, then the seal refused
    assert [event["reason_code"] for _, event in env.recorded] == ["CREDENTIAL_USE_ALLOWED", "ADMITTED"]
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["would_deny"]) == (
        "allow", "DEPENDENCY_UNAVAILABLE", True)
    assert ns.adm.CREDENTIAL_DECISION.total("would_deny", "DEPENDENCY_UNAVAILABLE") == before[0] + 1
    assert ns.adm.ADMISSION_DECISION.total("would_deny", "DEPENDENCY_UNAVAILABLE") == before[1] + 1
    assert ns.adm.ADMISSION_DURATION.count("audit_only", "would_deny") == before[2] + 1
    [(_, sent)] = calls
    assert sent["kwargs"]["application"] == payload["application"] and SECRET in json.dumps(sent["kwargs"])
    assert contexts(env, ns) == [] and not [line for line in env.log.lines if SECRET in line]


def test_the_seal_check_is_run_on_a_copy_and_never_changes_the_payload_it_was_given(ns, env):
    cred = Credential(env, env.seed_agent())
    payload = payload_of(cred)
    snapshot = copy.deepcopy(payload)
    task_id, calls, _ = dispatch(ns, env, payload)   # the seal check passes: the copy would be sealed, the real one is not
    assert task_id == "task-1" and payload == snapshot
    [(_, sent)] = calls
    assert "credential_slot" not in json.dumps(sent["kwargs"]) and SECRET in json.dumps(sent["kwargs"])
    sealed = []
    original = ns.adm._seal_dispatch   # pylint: disable=W0212

    def spy(target, entries_of, decided=False, shadow=False, children=None):
        sealed.append((target is payload, decided, shadow))
        return original(target, entries_of, decided=decided, shadow=shadow, children=children)
    ns.adm._seal_dispatch = spy   # pylint: disable=W0212
    try:
        env.reset()
        cred = Credential(env, env.seed_agent())
        module = env.module(governed=False, audit=True)
        call = {"args": [], "kwargs": {"project_id": 1, "message_id": 5, "_elitea_predict_run_id": RUN,
                                        **payload_of(cred)}, "pool": "agents", "meta": {}}
        ns.adm.admit_dispatch(module, "indexer_agent", call, env.request())
    finally:
        ns.adm._seal_dispatch = original   # pylint: disable=W0212
    assert sealed == [(False, True, True)]   # a copy, after a decided admission, counted as a would-deny


def test_the_span_of_the_histogram_ends_before_the_seal_in_audit_only_as_in_enforce(ns, env, monkeypatch):
    cred = Credential(env, env.seed_agent())
    ticks = iter([0.0, 1.0, 10.0, 10.0, 10.0, 10.0])
    monkeypatch.setattr(ns.adm.time, "perf_counter", lambda: next(ticks, 99.0))
    seen = []
    monkeypatch.setattr(ns.adm.ADMISSION_DURATION, "observe", lambda seconds, *labels: seen.append((seconds, labels)))
    dispatch(ns, env, payload_of(cred))
    [(seconds, labels)] = seen
    assert labels == ("audit_only", "would_allow") and seconds == 1.0   # not the 10 s the seal check "took"


def test_a_deny_is_in_the_span_with_its_audit_write_in_both_modes(ns, env, monkeypatch):
    text = (PLUGIN_ROOT / "utils" / "admission.py").read_text()
    enforce = text[text.index("def admit_execution("):text.index("def _shadow_admit(")]
    assert enforce.index("_record_deny(") < enforce.index('ADMISSION_DURATION.observe(time.perf_counter() - started, "enforce", "deny")')
    shadow = text[text.index("def _shadow_admit("):text.index("def _carries_parent_context(")]
    assert shadow.index("_record_deny(") < shadow.index('"audit_only", decision)')


# --- the platform cannot decide ------------------------------------------------------------------

def test_an_unavailable_platform_makes_every_audited_run_a_would_deny_without_evaluating_it(ns, env):
    env.seed_agent()
    module = env.module(governed=False, audit=True, unavailable=True)
    task_id, calls, _ = dispatch(ns, env, module=module)
    assert task_id == "task-1"
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["would_deny"]) == ("allow", "DEPENDENCY_UNAVAILABLE", True)
    assert env.verify_calls == [] and env.recorded == []   # it was not evaluated: it could not be
    assert_dispatched_as_legacy(ns, env, calls)


def test_enforce_refuses_a_new_admission_while_the_platform_is_unavailable_and_leaves_running_runs_alone(ns, env):
    env.seed_agent()
    module = env.module(unavailable=True)
    original = Original()
    wrapper = build_wrapper(ns, module, original)
    call = {"args": ["s", "m"], "kwargs": {"project_id": 1, "message_id": 5}, "pool": "agents", "meta": {"project_id": 1}}
    with pytest.raises(ns.exceptions.AdmissionDenied) as err:
        wrapper("indexer_agent", admission=env.request(), **copy.deepcopy(call))
    assert err.value.reason_code == "DEPENDENCY_UNAVAILABLE"
    assert err.value.message == ns.adm.MESSAGES["DEPENDENCY_UNAVAILABLE"]
    [event] = env.denied
    assert (event["decision"], event["reason_code"], event["project_id"]) == ("deny", "DEPENDENCY_UNAVAILABLE", 1)
    assert "would_deny" not in event   # an enforce event is not a would-deny
    assert original.calls == [] and contexts(env, ns) == [] and env.verify_calls == []
    # a child of a run that was admitted before keeps its parent's admission
    inherited = copy.deepcopy(call)
    inherited["kwargs"]["envelope"] = {"schema_version": 1, "security_profile": "governed",
                                       "required_capabilities": ["task_envelope.v1"],
                                       "context_ref": "11111111-1111-1111-1111-111111111111"}
    inherited["kwargs"]["_elitea_predict_run_id"] = RUN
    env.poison()
    assert ns.adm.admit_dispatch(module, "indexer_agent", inherited, None) is None
    env.unpoison()


def test_enforce_admits_again_as_soon_as_the_platform_is_available(ns, env):
    env.seed_agent()
    original = Original()
    wrapper = build_wrapper(ns, env.module(), original)
    call = {"args": ["s", "m"], "kwargs": {"project_id": 1, "message_id": 5}, "pool": "agents", "meta": {"project_id": 1}}
    assert wrapper("indexer_agent", admission=env.request(), **call) == "task-1"
    [row] = contexts(env, ns)
    assert row.state == "admitted"


# --- the request, the histogram and the lifecycle ---------------------------------------------------

def test_the_request_is_built_for_audit_only_and_enforce_and_never_for_off(ns, env):
    def build(module):
        env.sio_users["sid-1"] = types_holder()
        return ns.pd_admission.admission_request_from(
            module, sid="sid-1", project_id=1, binding_project_id=1,
            version_row=type("V", (), {"id": 70, "application_id": 7})(), run_kind="interactive", entry_point="chat_sio")

    def types_holder():
        import types
        return types.SimpleNamespace(type="user", id="42", reference="cookie")

    assert build(env.module(governed=False)) is None
    assert build(env.module(governed=False, audit=True)).caller_id == 42
    assert build(env.module()).caller_id == 42


def test_enforce_and_audit_only_observe_the_same_histogram_with_the_mode_as_a_label(ns, env):
    env.seed_agent()
    before = (ns.adm.ADMISSION_DURATION.count("enforce", "allow"), ns.adm.ADMISSION_DURATION.count("audit_only", "would_allow"))
    ns.adm.admit_execution(env.module(), env.request(), run_id=RUN)
    dispatch(ns, env)
    assert ns.adm.ADMISSION_DURATION.count("enforce", "allow") == before[0] + 1
    assert ns.adm.ADMISSION_DURATION.count("audit_only", "would_allow") == before[1] + 1
    env.verify_result = {"ok": False, "reason_code": "IDENTITY_REVOKED", "message": "x"}
    deny_before = ns.adm.ADMISSION_DURATION.count("enforce", "deny")
    with pytest.raises(ns.exceptions.AdmissionDenied):
        ns.adm.admit_execution(env.module(), env.request(), run_id="run-2")
    assert ns.adm.ADMISSION_DURATION.count("enforce", "deny") == deny_before + 1
    assert ns.adm._LabeledHistogram.__name__ and "admission_duration_seconds" in (PLUGIN_ROOT / "utils" / "admission.py").read_text()


def test_audit_only_runs_the_same_admission_code_inline_on_the_dispatch_path():
    text = (PLUGIN_ROOT / "utils" / "admission.py").read_text()
    shadow = text[text.index("def _shadow_evaluate("):text.index("class _AsOf")]
    assert "_admit(module, admission, run_id, snapshot, shadow=True)" in shadow   # the same function enforce runs
    assert "Thread" not in text and "spawn" not in text and "apply_async" not in text   # the same greenlet, no pool
    dispatch_part = text[text.index("def admit_dispatch("):text.index("def _parent_entries(")]
    assert dispatch_part.index("task_name not in ADMITTED_TASKS") < dispatch_part.index(
        "_shadow_admit(module, call_kwargs, admission, unavailable, deferred, task_name)")


def test_runs_admitted_under_enforce_are_looked_after_after_a_switch_to_off(ns, env):
    env.seed_agent()
    enforce = env.module()
    ref = ns.adm.admit_execution(enforce, env.request(), run_id=RUN).context_ref
    ns.adm.track_context(enforce, "task-1", 1, ref)
    # enforce was left: governed is false, but the handlers stay registered in this process
    off = env.module(governed=False)
    off._context_by_task = enforce._context_by_task   # pylint: disable=W0212
    off.governed_handlers_registered = True
    ns.adm.complete_context(off, "task-1")
    assert contexts(env, ns)[0].state == "completed" and off._context_by_task == {}   # pylint: disable=W0212
    assert ns.adm.reopen_contexts(off, 1, [ref]) == 1
    assert contexts(env, ns)[0].state == "running"
    assert ns.adm.reopen_contexts(env.module(governed=False), 1, [ref]) == 0   # never governed here: nothing to do
    off.governed_handlers_registered = False
    off._context_by_task["task-2"] = (1, ref)   # pylint: disable=W0212
    assert ns.adm.enforce_run_deadlines(off) == 0 and "task-2" in off._context_by_task   # pylint: disable=W0212


def test_audit_only_never_tracks_a_task_or_registers_a_context(ns, env):
    env.seed_agent()
    _, _, module = dispatch(ns, env)
    assert module._context_by_task == {} and contexts(env, ns) == []   # pylint: disable=W0212
