"""A Test Run has no schedule: the run starter, the pre-run check and the run writers work from the run row and its form."""
import sys
import types

import pytest
from unit._tool_spend_loader import load
from unit.utils.test_7011_run_starter_pipeline import (
    OWNER,
    PKG,
    _module,
    clean_review,
    load_starter,
)
from unit.utils.test_7011_schedule_runs_fail import runs, session  # noqa: F401
from unit.utils.test_7011_schedule_runs_outcome import sent  # noqa: F401

# ruff: noqa: F811

OTHER = OWNER + 1
SENSITIVE = {"code": "sensitive_action", "toolkit": "Outlook", "tool": "send_email"}


def _test_run(runs, session, form=None, status="running", requested_by=OWNER):
    row = runs.EntityScheduleRun(schedule_id=None, trigger="test", status=status, requested_by=requested_by)
    session.add(row)
    session.flush()
    if form is not None:
        session.execute(runs.EntityScheduleRun.__table__.update()
                        .where(runs.EntityScheduleRun.id == row.id).values({runs.RUN_FORM: form}))
    return row


def test_run_owner_of_a_run_with_no_schedule_is_its_requester(runs, session):
    run = _test_run(runs, session)

    assert runs.run_owner_id(session, run) == OWNER


def test_run_owner_of_a_saved_run_is_the_schedule_owner_and_none_when_it_is_gone(runs, session):
    schedule = runs.EntitySchedule(
        entity_type="agent", application_id=1, name="S1", cron="0 * * * *", timezone="UTC", created_by=5,
        principal={"kind": "delegation", "ref": 5})
    session.add(schedule)
    session.flush()
    run = runs.EntityScheduleRun(schedule_id=schedule.id, trigger="cron", status="running")
    gone = runs.EntityScheduleRun(schedule_id=999, trigger="cron", status="running")

    assert (runs.run_owner_id(session, run), runs.run_owner_id(session, gone)) == (5, None)


def test_decision_check_accepts_the_requester_of_a_run_with_no_schedule_and_refuses_another_user(runs, session):
    run = _test_run(runs, session, status="waiting_for_approval")

    assert runs.check_schedule_decision(1, run.id, OWNER, ["approve"]) == "approve"
    with pytest.raises(runs.ScheduleDecisionForbidden):
        runs.check_schedule_decision(1, run.id, OTHER, ["approve"])


def test_announce_skips_a_run_with_no_schedule(runs, session, sent):
    run = _test_run(runs, session, status="failed")

    runs._announce(1, session, [run.id])

    assert (sent.notices, sent.emitted) == ([], [])


def test_ending_a_run_with_no_schedule_writes_its_status_and_log(runs, session, sent):
    run = _test_run(runs, session)

    assert runs.finish_schedule_run(1, run.id, "failed", "boom", author_id=OWNER) is True

    session.refresh(run)
    assert (run.status, run.reason) == ("failed", "boom")
    assert sent.notices == []


def test_a_chat_hook_of_another_user_cannot_end_a_run_with_no_schedule(runs, session, sent):
    run = _test_run(runs, session)

    assert runs.finish_schedule_run(1, run.id, "success", author_id=OTHER) is False


def test_approval_rule_of_a_run_with_no_schedule_is_read_from_its_form(runs, session):
    run = _test_run(runs, session, form={"approval_rule": "run_without_asking"})

    assert runs._approval_rule(1, run.id) == "run_without_asking"


def test_approval_rule_of_a_saved_run_is_the_schedule_rule(runs, session):
    schedule = runs.EntitySchedule(
        entity_type="agent", application_id=1, name="S1", cron="0 * * * *", timezone="UTC", created_by=5,
        principal={"kind": "delegation", "ref": 5}, approval_rule="pause_and_ask")
    session.add(schedule)
    session.flush()
    run = runs.EntityScheduleRun(schedule_id=schedule.id, trigger="cron", status="running")
    session.add(run)
    session.flush()

    assert runs._approval_rule(1, run.id) == "pause_and_ask"


def test_a_failed_prerun_of_a_run_with_no_schedule_fails_the_run_and_does_not_disable_or_notify(monkeypatch):
    prerun = load("utils/schedule_prerun.py", fresh=True)
    calls = []
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_runs", types.SimpleNamespace(
        fail_schedule_run=lambda *a: calls.append(("fail", *a)),
        disable_schedule=lambda *a: calls.append(("disable", *a)),
        park_for_credentials=lambda *a, **k: False))
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_notifications", types.SimpleNamespace(
        notify_schedule_event=lambda *a, **k: calls.append(("notify", *a))))
    monkeypatch.setitem(sys.modules, f"{PKG}.models.enums.all", types.SimpleNamespace(
        NotificationEventTypes=types.SimpleNamespace(schedule_disabled="d", schedule_run_failed="f")))
    monkeypatch.setattr(prerun, "check_principal", lambda s: None)
    monkeypatch.setattr(prerun, "check_owner_access", lambda *a: prerun.OWNER_LOST_ACCESS)
    form = types.SimpleNamespace(id=None, created_by=OWNER, entity_type="agent", application_id=3, version_id=4)

    result = prerun.run_prerun_check(34, form, 90)

    assert (result.ok, result.reason) == (False, prerun.OWNER_LOST_ACCESS)
    assert [call[0] for call in calls] == ["fail"]


@pytest.mark.parametrize("trigger_kind, reason_name, keeps_started_owner, written", [
    ("cron", "OWNER_LOST_ACCESS", True, ["fail"]),
    ("webhook", "OWNER_LOST_ACCESS", True, ["fail"]),
    ("cron", "NO_TOKEN", True, ["fail"]),
    ("webhook", "NO_TOKEN", True, ["fail"]),
    # controls: the owner's own run still switches the schedule off, tells the owner and counts the refusal
    ("cron", "OWNER_LOST_ACCESS", False, ["fail", "disable", "notify"]),
    ("webhook", "OWNER_LOST_ACCESS", False, ["fail", "disable", "notify"]),
    ("cron", "NO_TOKEN", False, ["fail", "notify"]),
    ("webhook", "NO_TOKEN", False, ["fail", "notify", "refused"]),
])
def test_a_failed_prerun_of_a_former_owners_run_leaves_the_schedule_of_the_current_owner_alone(
        monkeypatch, trigger_kind, reason_name, keeps_started_owner, written):
    prerun = load("utils/schedule_prerun.py", fresh=True)
    calls = []
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_runs", types.SimpleNamespace(
        fail_schedule_run=lambda *a: calls.append(("fail", *a)),
        disable_schedule=lambda *a: calls.append(("disable", *a)),
        count_refusal=lambda *a, **k: calls.append(("refused", *a)),
        park_for_credentials=lambda *a, **k: False,
        PAUSED_AFTER_PRERUN="paused", PRERUN_REFUSED="refused"))
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_notifications", types.SimpleNamespace(
        notify_schedule_event=lambda *a, **k: calls.append(("notify", *a))))
    monkeypatch.setitem(sys.modules, f"{PKG}.models.enums.all", types.SimpleNamespace(
        NotificationEventTypes=types.SimpleNamespace(schedule_disabled="d", schedule_run_failed="f")))
    monkeypatch.setattr(prerun, "check_principal", lambda s: None)
    reason = getattr(prerun, reason_name)
    monkeypatch.setattr(prerun, "check_owner_access", lambda *a: reason)
    schedule = types.SimpleNamespace(
        id=7, created_by=OWNER, entity_type="agent", application_id=3, version_id=4, trigger_kind=trigger_kind,
        run_as_caller=False, keeps_started_owner=keeps_started_owner)

    result = prerun.run_prerun_check(34, schedule, 90)

    assert (result.ok, result.reason) == (False, reason)
    assert [call[0] for call in calls] == written


FORM = {"name": "Try", "entity_type": "agent", "application_id": 3, "version_id": 4, "run_mode": "prompt",
        "prompt": "Go {{ x }}", "variables": [{"name": "x", "value": "1"}], "approval_rule": "run_without_asking"}


class FakeSession:
    """Answers the three reads of `_load_test_run`: the form row, the version and the agent."""

    def __init__(self, form, version, agent):
        self.form, self.version, self.agent = form, version, agent

    def execute(self, _query):
        return types.SimpleNamespace(one=lambda: (self.form, 3))

    def get(self, model, key):
        return self.version if model is self.models.ApplicationVersion else self.agent


@pytest.fixture
def starter(monkeypatch):
    module = load_starter(monkeypatch, create=lambda **kw: None, predict=lambda **kw: None,
                          prerun=lambda *a: None)
    module.ApplicationVersion, module.Application = object(), type("Application", (), {})
    monkeypatch.setattr(module, "select", lambda *a: types.SimpleNamespace(where=lambda *w: None))
    monkeypatch.setattr(module, "EntityScheduleRun", types.SimpleNamespace(id=None))
    return module


def _load_from_form(starter, form=FORM, version=types.SimpleNamespace(id=4, application_id=3, name="v4"),
                    agent=types.SimpleNamespace(name="Agent")):
    session = FakeSession(form, version, agent)
    session.models = starter
    run = types.SimpleNamespace(id=7, requested_by=OWNER, trigger="test", resumed_at=None)
    starter.credential_toolkit_refs = lambda version: []
    return starter._load_test_run(session, run)


def test_load_of_a_run_with_no_schedule_builds_the_values_from_its_form(starter):
    loaded = _load_from_form(starter)

    schedule = loaded.schedule
    assert (schedule.id, schedule.created_by, schedule.principal) == (None, OWNER, {"kind": "delegation", "ref": OWNER})
    assert (schedule.approval_fingerprint, schedule.approval_confirmed_by) == (None, None)
    assert (schedule.name, schedule.entity_type, schedule.run_mode, schedule.application_id, schedule.version_id) == (
        "Try", "agent", "prompt", 3, 4)
    assert (loaded.prompt, loaded.variables, loaded.trigger, loaded.resumed, loaded.entity_name) == (
        "Go {{ x }}", [{"name": "x", "value": "1"}], "test", False, "Agent")


@pytest.mark.parametrize("version", [None, types.SimpleNamespace(id=4, application_id=99, name="v4")])
def test_load_of_a_run_with_no_schedule_is_none_when_the_version_is_gone_or_belongs_to_another_agent(starter, version):
    assert _load_from_form(starter, version=version) is None


def test_run_without_asking_never_holds_for_a_form_even_with_the_project_setting_on_and_sensitive_tools(
        starter, monkeypatch):
    schedule = _load_from_form(starter).schedule
    monkeypatch.setattr(starter, "_setting_allows_auto_approve", lambda project_id: True)
    review = {**clean_review(), "sensitive": [SENSITIVE]}

    auto_approve = starter._may_auto_approve(34, schedule, review)

    assert auto_approve is False
    outcome, _ = starter._risk_check(review, schedule.approval_rule, auto_approve)
    assert outcome == "needs_approval"


@pytest.mark.parametrize("run_created_by, expected", [(OWNER, True), (OWNER + 1, False)])
def test_run_without_asking_holds_for_the_owner_but_never_for_an_admin_who_acts_as_themselves(
        starter, monkeypatch, run_created_by, expected):
    schedule = types.SimpleNamespace(approval_rule="run_without_asking", approval_fingerprint="fp-1",
                                     approval_confirmed_by=OWNER, created_by=run_created_by)
    monkeypatch.setattr(starter, "_setting_allows_auto_approve", lambda project_id: True)
    review = {**clean_review(), "sensitive": [SENSITIVE]}

    assert starter._may_auto_approve(34, schedule, review) is expected


SIGNIN = {"code": "personal_signin_required", "toolkit": "Dataverse", "message": "Toolkit needs a sign-in"}


@pytest.fixture
def logging_starter(starter, monkeypatch):
    """The starter with a run_event that keeps its text and level (the loader stubs it with an empty dict)."""
    def run_event(kind: str, text: str, level: str = "info") -> dict:
        return {"kind": kind, "text": text, "level": level}

    monkeypatch.setattr(starter, "run_event", run_event)
    return starter


class TestSignInPartOfTheRiskLine:
    def test_a_sign_in_notice_alone_gives_its_own_outcome_and_line(self, logging_starter):
        review = {**clean_review(), "notices": [SIGNIN]}

        outcome, event = logging_starter._risk_check(review, "pause_and_ask", False)

        assert outcome == "needs_signin"
        assert event["text"] == "Risk check: needs a personal sign-in (Dataverse)"
        assert event["level"] == "warning"

    def test_a_sign_in_notice_beside_a_sensitive_tool_ends_the_approval_line(self, logging_starter):
        review = {**clean_review(), "sensitive": [SENSITIVE], "notices": [SIGNIN]}

        outcome, event = logging_starter._risk_check(review, "pause_and_ask", False)

        assert outcome == "needs_approval"
        assert event["text"] == "Risk check: needs approval for Outlook · send_email; needs a personal sign-in (Dataverse)"

    def test_a_sign_in_notice_beside_an_incomplete_scan_keeps_the_unknown_outcome(self, logging_starter):
        review = {**clean_review(), "notices": [{"code": "scan_incomplete", "message": "x"}, SIGNIN]}

        outcome, event = logging_starter._risk_check(review, "pause_and_ask", False)

        assert outcome == "unknown"
        assert event["text"] == "Risk check: not complete; needs a personal sign-in (Dataverse)"

    def test_the_line_names_each_toolkit_once_in_order(self, logging_starter):
        other = {**SIGNIN, "toolkit": "Outlook"}
        review = {**clean_review(), "notices": [SIGNIN, other, SIGNIN]}

        _, event = logging_starter._risk_check(review, "pause_and_ask", False)

        assert event["text"] == "Risk check: needs a personal sign-in (Dataverse, Outlook)"

    def test_a_clean_review_still_says_ok(self, logging_starter):
        outcome, event = logging_starter._risk_check(clean_review(), "pause_and_ask", False)

        assert (outcome, event["text"]) == ("ok", "Risk check: OK")
