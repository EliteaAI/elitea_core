"""#7011 owner schedules under C-1009 governance (bridge, story #6754): mandate admission and readable refusals.

The admission code, its SQL, the ticket store, the trigger principal, ``pipeline_execution`` and the starter are the
real files, run in the #6947 harness (``test_6947_mandate_triggers``). Only the starter's own row reads, the pre-run
check, the risk scan and the run writers are faked; ``predict_sio`` is the harness stand-in (it takes the ticket and
admits the dispatch as the real one does), and the real ``predict_sio`` is lifted for the request it builds.
"""
import sys
import types
from datetime import timedelta
from unittest import mock

import pytest

from unit.test_6947_mandate_triggers import (  # noqa: F401  pylint: disable=W0611 -- the shared harness fixtures
    CREATOR, FakeVersion, PKG, PLUGIN_ROOT, a_request, agent, arm, contexts, env, lift, new_self, ns, set_mandate,
    source_of, tick, wire,
)
from fixtures.helpers import load_module_with_stubs  # noqa: E402  pylint: disable=C0413
from fixtures.mandate_env import NOW, OWNER  # noqa: E402  pylint: disable=C0413

PROJECT_ID, SCHEDULE_ID, RUN_ID = 1, 41, 900
TRIGGER_REF = f"schedule:entity:{SCHEDULE_ID}"


@pytest.fixture(scope="module")
def starter(ns):
    """The real starter, its pipeline helper the harness's real ``pipeline_execution``. The stubs go straight into
    ``sys.modules``: ``ns`` saved it before and puts it back when this module's tests end."""
    writes = types.SimpleNamespace(calls=[], streak=[])
    stubs = {
        "models.entity_schedule": {"EntitySchedule": type("EntitySchedule", (), {}),
                                   "EntityScheduleRun": type("EntityScheduleRun", (), {})},
        "utils.budget_door": {"closed_budget_scope": lambda *args: None},
        "utils.schedule_prerun": {"run_prerun_check": lambda project_id, schedule, run_id: types.SimpleNamespace(
            ok=True, owner_id=schedule.created_by)},
        "utils.schedule_runs": {
            "fail_schedule_run": lambda *args: writes.calls.append(("fail", *args)),
            "block_schedule_run": lambda *args: writes.calls.append(("block", *args)),
            "finish_schedule_run": lambda *args, **kwargs: writes.calls.append(("finish", *args)),
            "run_failure_reason": lambda error: f"The run failed: {error}",
            "count_refusal": lambda *args: writes.streak.append(("refused", *args)),   # #7254: own tests
            "end_refusal_streak": lambda *args: writes.streak.append(("end", *args)),
            "record_run_events": lambda *args, **kwargs: None,
            "run_event": lambda *args, **kwargs: {}},
        "utils.schedule_risk": {
            "scan_schedule": lambda *args: {"blocking": [], "sensitive": [], "notices": [], "fingerprint": "fp"},
            "scan_complete": lambda review: True},
        "utils.authors": {"get_authors_data": lambda ids: [{"name": "Owner"}]},
    }
    for dotted, attrs in stubs.items():
        sys.modules[f"{PKG}.{dotted}"] = types.SimpleNamespace(**attrs)
    models_all = sys.modules[f"{PKG}.models.all"]
    if not hasattr(models_all, "ApplicationVersion"):
        models_all.ApplicationVersion = type("ApplicationVersion", (), {})
    module = load_module_with_stubs(PLUGIN_ROOT / "utils" / "schedule_run_starter.py",
                                    f"{PKG}.utils.schedule_run_starter")
    module.writes = writes
    return module


def predict_as_main(ns):
    """The harness stand-in of ``predict_sio`` with the run kind rule of the real one (pinned below)."""
    def predict_sio(**kwargs):
        module = ns.tools.this.module
        ns.state.predicts.append(kwargs)
        data = kwargs["data"]
        admission = ns.mandate_admission.take_trigger_admission(
            module, kwargs.get("mandate_ticket"), project_id=data["project_id"], binding_project_id=data["project_id"],
            version_id=data["version_id"],
        ) or ns.pd_admission.admission_request_from(
            module, sid=None, project_id=data["project_id"], binding_project_id=data["project_id"],
            version_row=types.SimpleNamespace(id=data["version_id"], application_id=data["application_id"]),
            run_kind="system" if kwargs["is_system_user"] or kwargs.get("unattended") else "interactive",
            entry_point="chat_sio")
        call = {"args": [data["stream_id"], data["message_id"]], "pool": "agents",
                "kwargs": {"message_id": data["message_id"],
                           "envelope": {"security_profile": "governed", "context_ref": None}},
                "meta": {"project_id": data["project_id"]}}
        ns.run_id.stamp_predict_run_id(call)
        try:
            context_ref = ns.adm.admit_dispatch(module, "indexer_agent", call, admission) if admission else None
        except ns.exceptions.AdmissionDenied as denied:
            return {"code": denied.reason_code, "error": denied.message}
        ns.state.contexts = getattr(ns.state, "contexts", []) + [context_ref]
        return {"task_id": f"task-{len(ns.state.predicts)}"}
    return predict_sio


@pytest.fixture
def run(ns, env, starter, monkeypatch):
    """Run one claimed run of a schedule of version 70 (``entity_type``, ``trigger``, ``owner``) through the starter."""
    monkeypatch.setattr(ns.execution, "rpc_tools", types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
        rpc=types.SimpleNamespace(timeout=lambda seconds: types.SimpleNamespace(
            applications_predict_sio=predict_as_main(ns))))))
    monkeypatch.setattr(starter, "_link_conversation", lambda *args: True)
    starter.writes.calls.clear()
    starter.writes.streak.clear()

    def go(entity_type="pipeline", trigger="cron", owner=OWNER, version=None):
        version = version or FakeVersion()
        loaded = types.SimpleNamespace(
            schedule=types.SimpleNamespace(
                id=SCHEDULE_ID, name="Daily", principal={"kind": "delegation", "ref": owner}, created_by=owner,
                entity_type=entity_type, run_mode=None, application_id=version.application_id,
                version_id=version.id, approval_rule="pause_and_ask", approval_fingerprint=None,
                approval_confirmed_by=None),
            version=types.SimpleNamespace(id=version.id, application_id=version.application_id, name="v1"),
            entity_name="Report", prompt="go", variables=[], trigger=trigger)
        monkeypatch.setattr(starter, "_load", lambda *args: loaded)
        starter.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)
        return starter.writes.calls
    return go


def counted(ns, monkeypatch):
    calls = []
    real = ns.mandate_admission.latest_mandates
    monkeypatch.setattr(ns.mandate_admission, "latest_mandates",
                        lambda *args, **kwargs: (calls.append(kwargs), real(*args, **kwargs))[1])
    return calls


# --- enforce --------------------------------------------------------------------------------------

@pytest.mark.parametrize("entity_type", ["pipeline", "agent"])
@pytest.mark.parametrize("trigger", ["cron", "manual", "test"])   # cron tick, Run now, Test run: one starter
def test_enforce_a_run_with_its_owners_mandate_is_admitted_on_the_mandate_with_entry_point_schedule(
        ns, env, agent, run, monkeypatch, entity_type, trigger):
    arm(ns, env)
    reads = counted(ns, monkeypatch)
    writes = run(entity_type=entity_type, trigger=trigger)
    [predict] = ns.state.predicts
    assert isinstance(predict["mandate_ticket"], str) and predict["unattended"] is True
    assert (predict["user_id"], predict["is_system_user"]) == (OWNER, False)   # the owner's run, never a system user
    [row] = contexts(env, ns)
    assert (row.entry_point, row.caller["ref"], row.subject["ref"]) == ("schedule", TRIGGER_REF,
                                                                         f"mandate:{agent.mandate}")
    assert writes == [] and ns.state.events == []   # nothing ended the run: it runs
    tickets = ns.tools.this.module._mandate_tickets   # pylint: disable=W0212
    assert len(reads) == 1 and len(tickets) == 0   # one read (as the legacy cron), and the ticket spent


def test_enforce_the_payload_carries_no_platform_token_from_the_starter(ns, env, agent, run):
    # the starter hands predict ids and the ticket only; the payload's own token is #7229's (follow-up, proof §0.3)
    arm(ns, env)
    run()
    [predict] = ns.state.predicts
    assert not {"api_key", "api_token", "token", "auth_session", "project_auth_token"} & set(predict)
    assert not {"api_key", "api_token", "token", "auth_session", "project_auth_token"} & set(predict["data"])


@pytest.mark.parametrize("how", ["no_mandate", "another_owner"])
def test_enforce_a_run_no_mandate_of_its_owner_covers_is_refused_mandate_required_and_ends_readably(
        ns, env, agent, run, how):
    arm(ns, env)
    if how == "no_mandate":
        with ns.sa_orm.Session(env.engine(1)) as session:
            session.execute(ns.sa.delete(ns.Mandate))
            session.commit()
    writes = run(owner=OWNER if how == "no_mandate" else CREATOR)
    [predict] = ns.state.predicts
    assert "mandate_ticket" not in predict and predict["unattended"] is True
    message = ns.adm.MESSAGES["MANDATE_REQUIRED"]
    # the run ends at once as failed with the fixed text (the writer sends the owner's notice), never left running
    assert writes == [("finish", PROJECT_ID, RUN_ID, "failed", f"The run failed: {message}")]
    [(name, payload)] = ns.state.events   # and the run's chat message ends with it
    assert name == "chat_message_stream_end" and payload["message_id"] == "response-1"
    assert payload["response_metadata"]["code"] == "MANDATE_REQUIRED" and message in payload["content"]
    assert contexts(env, ns) == []
    [denied] = env.denied   # decided and audited by admission as a system run, never as chat_sio / public identity
    assert denied["reason_code"] == "MANDATE_REQUIRED" and denied["decision"] == "deny"


@pytest.mark.parametrize("entity_type, label", [("agent", "Agent run"), ("pipeline", "Pipeline run")])
def test_a_refused_run_says_agent_or_pipeline_in_its_chat(ns, env, run, entity_type, label):
    arm(ns, env)
    run(entity_type=entity_type)
    [(_, payload)] = ns.state.events
    assert payload["content"] == f"{label} was not started: {ns.adm.MESSAGES['MANDATE_REQUIRED']}"


@pytest.mark.parametrize("change, code", [
    (lambda ns, env, agent: set_mandate(env, ns, agent.mandate, state="revoked"), "MANDATE_INACTIVE"),
    (lambda ns, env, agent: set_mandate(env, ns, agent.mandate, allowed_operations=["run:webhook"]),
     "DENIED_OPERATION"),
])
def test_enforce_a_refusal_on_the_mandate_ends_the_run_with_its_own_reason(ns, env, agent, run, change, code):
    arm(ns, env)
    change(ns, env, agent)
    writes = run()
    assert isinstance(ns.state.predicts[0]["mandate_ticket"], str)
    assert writes == [("finish", PROJECT_ID, RUN_ID, "failed", f"The run failed: {ns.adm.MESSAGES[code]}")]
    assert env.denied[-1]["reason_code"] == code and env.denied[-1]["caller_ref"] == TRIGGER_REF


def test_the_run_is_ended_before_its_chat_message_so_the_stream_end_hook_finds_it_ended(ns, env, run, starter):
    arm(ns, env)
    order = []
    starter.writes.calls = types.SimpleNamespace(append=lambda call: order.append(call[0]))
    ns.tools.this.module.context.event_manager.fire_event = lambda name, payload: order.append(name)
    try:
        run()
    finally:
        starter.writes.calls = []
    assert order == ["finish", "chat_message_stream_end"]


def test_a_failing_run_writer_still_ends_the_chat_message(ns, env, run, starter, monkeypatch):
    arm(ns, env)
    runs = sys.modules[f"{PKG}.utils.schedule_runs"]

    def down(*args, **kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(starter, "finish_schedule_run", down, raising=False)
    monkeypatch.setattr(runs, "finish_schedule_run", down)
    with pytest.raises(RuntimeError):   # the dispatch wrapper logs it and closes the run (schedule_dispatch)
        run()
    assert [name for name, _ in ns.state.events] == ["chat_message_stream_end"]


def test_enforce_a_failed_principal_read_fails_the_run_with_fixed_text_and_sends_nothing(ns, env, run, monkeypatch):
    arm(ns, env)
    monkeypatch.setattr(ns.mandate_admission, "latest_mandates",
                        mock.Mock(side_effect=RuntimeError("SELECT secret FROM mandates")))
    writes = run()
    assert writes == [("fail", PROJECT_ID, RUN_ID, "the run could not start")] and ns.state.predicts == []
    [(name, payload)] = ns.state.events   # the run's chat message ends too, without the error's own text
    assert name == "chat_message_stream_end" and "SELECT" not in str(payload)
    assert payload["content"] == "Pipeline run could not start: the run could not start"


def test_the_ticket_is_minted_after_the_conversation_right_before_predict(ns, env, agent, run, monkeypatch):
    arm(ns, env)
    order = []
    real = ns.mandate_admission.mint_trigger_admission
    monkeypatch.setattr(ns.mandate_admission, "mint_trigger_admission",
                        lambda *args: (order.append("mint"), real(*args))[1])
    class Recorded(list):
        def append(self, item):
            order.append("create")
            super().append(item)

    monkeypatch.setattr(ns.state, "conversations", Recorded())
    run()
    assert order == ["create", "mint"] and len(ns.state.predicts) == 1


# --- audit only -----------------------------------------------------------------------------------

def test_audit_only_an_uncovered_run_runs_and_leaves_exactly_one_would_deny_row(ns, env, run):
    arm(ns, env, governed=False, audit=True)
    writes = run()
    [predict] = ns.state.predicts
    assert "mandate_ticket" not in predict and predict["unattended"] is True
    assert writes == [] and ns.state.events == []   # nothing refused: it runs
    [row] = env.denied   # one row: the shadow evaluation does not add a chat_sio / public one
    assert (row["reason_code"], row["would_deny"], row["decision"], row["caller_ref"]) == (
        "MANDATE_REQUIRED", True, "allow", TRIGGER_REF)
    assert row["project_id"] == PROJECT_ID and row["application_id"] == 7


def test_audit_only_a_run_its_owners_mandate_covers_leaves_no_row(ns, env, agent, run):
    arm(ns, env, governed=False, audit=True)
    writes = run()
    assert writes == [] and env.denied == [] and len(ns.state.predicts) == 1
    assert "mandate_ticket" not in ns.state.predicts[0]   # no ticket outside enforce


@pytest.mark.parametrize("values, code", [
    ({"state": "revoked"}, "MANDATE_INACTIVE"),
    ({"state": "suspended", "suspended_reason": "MANUAL"}, "MANDATE_INACTIVE"),
    ({"state": "expired"}, "MANDATE_EXPIRED"),
])
def test_audit_only_a_mandate_enforce_would_refuse_by_its_state_leaves_its_would_deny_row(ns, env, agent, run, values,
                                                                                          code):
    arm(ns, env, governed=False, audit=True)
    set_mandate(env, ns, agent.mandate, **values)
    writes = run()
    assert writes == [] and len(ns.state.predicts) == 1
    assert [(row["reason_code"], row["would_deny"], row["caller_ref"]) for row in env.denied] == [
        (code, True, TRIGGER_REF)]


@pytest.mark.parametrize("values, code", [
    ({"valid_until": NOW - timedelta(seconds=1)}, "MANDATE_EXPIRED"),   # past its end date, still marked active
    ({"allowed_operations": ["run:webhook"]}, "DENIED_OPERATION"),       # not allowed for schedules
])
def test_audit_only_a_mandate_enforce_would_refuse_by_date_or_operation_leaves_its_row(ns, env, agent, run, values,
                                                                                         code):
    arm(ns, env, governed=False, audit=True)
    set_mandate(env, ns, agent.mandate, **values)
    run()
    assert [(row["reason_code"], row["would_deny"], row["caller_ref"]) for row in env.denied] == [
        (code, True, TRIGGER_REF)]


def test_audit_only_another_persons_mandate_does_not_cover_the_run(ns, env, agent, run):
    arm(ns, env, governed=False, audit=True)
    run(owner=CREATOR)
    assert [row["reason_code"] for row in env.denied] == ["MANDATE_REQUIRED"]


def test_audit_only_a_failure_of_the_evaluation_never_stops_the_run(ns, env, run, monkeypatch):
    arm(ns, env, governed=False, audit=True)
    monkeypatch.setattr(ns.mandate_admission, "latest_mandates", mock.Mock(side_effect=RuntimeError("db")))
    writes = run()
    assert writes == [] and len(ns.state.predicts) == 1 and env.denied == []


# --- off ------------------------------------------------------------------------------------------

def test_off_the_predict_call_is_exactly_mains_and_nothing_is_read_or_minted(ns, env, agent, run, starter,
                                                                              monkeypatch):
    module = arm(ns, env, governed=False)
    module._mandate_tickets = mock.Mock(spec=[])   # any use fails   # pylint: disable=W0212
    reads = counted(ns, monkeypatch)
    sessions = []
    real = ns.tools.db.get_session
    monkeypatch.setattr(ns.tools.db, "get_session",
                        lambda project_id: (sessions.append(project_id), real(project_id))[1])
    writes = run()
    [predict] = ns.state.predicts
    assert predict == {
        "sid": None, "sio_event": "chat_predict", "chat_project_id": PROJECT_ID,
        "data": {"project_id": PROJECT_ID, "application_id": 7, "version_id": 70, "user_input": "go",
                 "chat_history": [], "message_id": "response-1", "stream_id": "conv-uuid",
                 "conversation_id": "conv-uuid"},
        "await_task_timeout": -1, "user_id": OWNER, "is_system_user": False, "non_interactive": True,
        "trigger_source": "scheduled", "auto_approve_sensitive_actions": False}
    assert reads == [] and sessions == [] and writes == [] and env.denied == []
    assert starter.writes.streak == []   # #7254: off neither counts nor ends a refusal streak


# --- #7254: the refusal streak per mode, and the mandate notice of a saved schedule ---------------

def test_enforce_a_refused_run_counts_one_refusal_with_its_reason(ns, env, run, starter):
    arm(ns, env)
    run(trigger="manual")   # Run now counts like a cron firing
    [(kind, project_id, schedule, run_id, reason, code)] = starter.writes.streak
    assert (kind, project_id, schedule.id, run_id, code) == ("refused", PROJECT_ID, SCHEDULE_ID, RUN_ID,
                                                             "MANDATE_REQUIRED")   # the code decides an outage
    assert reason == f"The run failed: {ns.adm.MESSAGES['MANDATE_REQUIRED']}"


def test_enforce_an_admitted_run_ends_the_streak(ns, env, agent, run, starter):
    arm(ns, env)
    run()
    assert starter.writes.streak == [("end", PROJECT_ID, SCHEDULE_ID)]


def test_audit_only_a_run_is_never_counted_as_refused(ns, env, run, starter):
    arm(ns, env, governed=False, audit=True)
    run()   # uncovered: a would-deny row, and the run goes on
    assert starter.writes.streak == [("end", PROJECT_ID, SCHEDULE_ID)]


@pytest.fixture
def notice(ns):
    return load_module_with_stubs(PLUGIN_ROOT / "utils" / "trigger_governance.py", f"{PKG}.utils.trigger_governance")


def save_notice(ns, env, notice, owner=OWNER, saver=None):
    with ns.sa_orm.Session(env.engine(1)) as session:
        return notice.schedule_governance_notice(
            ns.tools.this.module, session, PROJECT_ID, 7, 70, owner, owner if saver is None else saver)


def no_mandate_text(notice, whom="you"):
    return notice.NO_MANDATE_NOTICE.format(whom=whom)


def test_save_notice_enforce_the_owners_active_mandate_needs_no_notice_in_one_read(ns, env, agent, notice, monkeypatch):
    arm(ns, env)
    reads = counted(ns, monkeypatch)
    assert save_notice(ns, env, notice) is None and len(reads) == 1


@pytest.mark.parametrize("how", ["no_mandate", "another_owner", "revoked", "not_for_schedules"])
def test_save_notice_enforce_says_the_owner_needs_a_mandate(ns, env, agent, notice, how):
    arm(ns, env)
    owner = CREATOR if how == "another_owner" else OWNER
    if how == "no_mandate":
        with ns.sa_orm.Session(env.engine(1)) as session:
            session.execute(ns.sa.delete(ns.Mandate))
            session.commit()
    elif how == "revoked":
        set_mandate(env, ns, agent.mandate, state="revoked")
    elif how == "not_for_schedules":
        set_mandate(env, ns, agent.mandate, allowed_operations=["run:webhook"])
    assert save_notice(ns, env, notice, owner=owner) == (
        "Scheduled runs need an approved mandate for you on this version; ask a project admin "
        "(Admin → Governance → Mandates).")


def test_save_notice_an_admin_saving_another_owners_schedule_reads_of_the_owner(ns, env, agent, notice):
    arm(ns, env)
    set_mandate(env, ns, agent.mandate, state="revoked")
    assert save_notice(ns, env, notice, saver=CREATOR) == no_mandate_text(notice, "the schedule owner")


def test_save_notice_audit_only_adds_the_would_deny_sentence(ns, env, agent, notice):
    arm(ns, env, governed=False, audit=True)
    set_mandate(env, ns, agent.mandate, state="revoked")
    assert save_notice(ns, env, notice) == no_mandate_text(notice) + " Until then each run is recorded as 'would deny'."
    set_mandate(env, ns, agent.mandate, state="active")
    assert save_notice(ns, env, notice) is None


def test_save_notice_an_unbound_version_keeps_the_unbound_notice_only(ns, env, notice, monkeypatch):
    arm(ns, env)
    reads = counted(ns, monkeypatch)
    assert save_notice(ns, env, notice) == notice.UNBOUND_NOTICE and reads == []


def test_save_notice_off_asks_nothing_of_the_database(ns, env, notice):
    arm(ns, env, governed=False)
    untouchable = mock.Mock(spec=[])   # any use of the session fails
    assert notice.schedule_governance_notice(
        ns.tools.this.module, untouchable, PROJECT_ID, 7, 70, OWNER, OWNER) is None


# --- predict_sio: the request an unattended run gets (the real function) --------------------------

@pytest.fixture
def predict_sio(ns, env):
    return lift("rpc/application.py", "predict_sio",
                take_trigger_admission=ns.mandate_admission.take_trigger_admission,
                admission_request_from=ns.pd_admission.admission_request_from,
                AdmissionDenied=ns.exceptions.AdmissionDenied, SioEvents=types.SimpleNamespace(
                    application_predict="application_predict",
                    chat_predict=types.SimpleNamespace(value="chat_predict")),
                log=ns.log)


def owner_run(function, module, **kwargs):
    data = {"project_id": 1, "application_id": 7, "version_id": 70, "message_id": "m-1", "stream_id": "s-1"}
    function(module, sid=None, data=data, user_id=OWNER, is_system_user=False, **kwargs)
    return module.task_node.start_task.call_args


def test_predict_sio_makes_an_unattended_ticketless_run_a_system_run_and_keeps_the_owners_payload(ns, env, predict_sio):
    function, glob = predict_sio
    wire(glob)
    module = new_self(ns, env, governed=True)
    admission = owner_run(function, module, unattended=True).kwargs["admission"]
    assert (admission.run_kind, admission.caller_type) == ("system", "public")
    assert glob["generate_predict_payload"].call_args.kwargs["is_system_user"] is False
    assert owner_run(function, module).kwargs["admission"].run_kind == "interactive"   # without the flag: as before


def test_predict_sio_takes_an_unattended_runs_ticket_first(ns, env, agent, predict_sio):
    function, glob = predict_sio
    wire(glob)
    module = new_self(ns, env, governed=True)
    request = a_request(ns, env, agent.mandate, trigger_ref=TRIGGER_REF)
    ticket = ns.mandate_admission.mint_trigger_admission(module, request)
    assert owner_run(function, module, unattended=True, mandate_ticket=ticket).kwargs["admission"] is request


def test_predict_sio_off_the_flag_changes_nothing(ns, env, predict_sio):
    function, glob = predict_sio
    wire(glob)
    module = new_self(ns, env, governed=False)
    assert owner_run(function, module) == owner_run(function, module, unattended=True)
    assert module.task_node.start_task.call_args.kwargs["admission"] is None


def test_the_flag_is_an_rpc_argument_of_predict_sio_only():
    text = source_of("rpc/application.py")
    chat = text[text.index("def predict_sio("):text.index("def predict_sio_llm(")]
    assert "unattended: bool = False," in chat
    assert 'run_kind="system" if is_system_user or unattended else "interactive"' in chat
    assert "unattended" not in text[text.index("def predict_sio_llm("):]


def test_the_pipeline_helper_sends_the_flag_only_when_set(ns, env):
    arm(ns, env, governed=False)
    send = ns.execution.execute_pipeline_via_predict_sio
    common = dict(project_id=1, version=FakeVersion(), user_id=OWNER, conversation_uuid="c", response_message_id="m")
    send(**common)
    send(**common, unattended=False)
    send(**common, unattended=True)
    plain, explicit_false, flagged = ns.state.predicts[-3:]
    assert plain == explicit_false and "unattended" not in plain
    assert flagged == {**plain, "unattended": True}


# --- legacy cron and webhook: unchanged -----------------------------------------------------------

def test_the_legacy_cron_step_still_prepares_its_trigger_run_and_runs_as_a_system_user(ns, env, agent):
    arm(ns, env)
    tick(ns)
    [predict] = ns.state.predicts
    assert isinstance(predict["mandate_ticket"], str) and predict["is_system_user"] is True
    assert "unattended" not in predict
    [row] = contexts(env, ns)
    assert (row.entry_point, row.caller["ref"]) == ("schedule", "schedule:version:70")
    source = source_of("rpc/pipeline_scheduling.py")
    assert 'prepare_trigger_run(\n        trigger_kind="pipeline_schedule"' in source and "unattended" not in source


@pytest.mark.parametrize("relative", ["api/v2/webhook.py", "methods/predict.py", "rpc/pipeline_webhook.py"])
def test_the_webhook_paths_do_not_use_the_unattended_flag(relative):
    assert "unattended" not in source_of(relative)
