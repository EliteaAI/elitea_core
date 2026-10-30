"""EL-7011 US-09 R1.4, R1.5, R2.5, R3.1-R3.3: run_prerun_check applies the failure effects and notices."""
import sys
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

PRERUN = "utils/schedule_prerun.py"
PKG = "plugins.elitea_core"
OWNER = 12
RUN_ID = 90


class Recorder:
    """Stands in for the checks, the writers and the notice helper; records every effect in order."""

    def __init__(self, principal=None, access=None, rights=None, credentials=None, parks=False):
        self.parks = parks
        self.answers = {"principal": principal, "access": access, "rights": rights, "credentials": credentials}
        self.calls = []

    def step(self, name):
        def check(*args):
            self.calls.append((name, *args))
            return self.answers[name]
        return check

    def fail_schedule_run(self, project_id, run_id, reason):
        self.calls.append(("fail", project_id, run_id, reason))

    def park_for_credentials(self, project_id, run_id, reason, toolkit=None, credential=None):
        self.calls.append(("park", project_id, run_id, reason))
        self.named = (toolkit, credential)
        return self.parks

    def disable_schedule(self, project_id, schedule_id, reason):
        self.calls.append(("disable", project_id, schedule_id, reason))

    def notify(self, project_id, owner_id, event_type, schedule, run=None, extra=None):
        self.calls.append(("notify", owner_id, event_type, run.id, run.reason))

    def effects(self):
        return [call[0] for call in self.calls]


@pytest.fixture
def apply(monkeypatch):
    prerun = load(PRERUN, fresh=True)

    def run(recorder):
        monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_runs", SimpleNamespace(
            fail_schedule_run=recorder.fail_schedule_run, disable_schedule=recorder.disable_schedule,
            park_for_credentials=recorder.park_for_credentials))
        monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_notifications", SimpleNamespace(
            notify_schedule_event=recorder.notify))
        monkeypatch.setitem(sys.modules, f"{PKG}.models.enums.all", SimpleNamespace(
            NotificationEventTypes=SimpleNamespace(schedule_disabled="schedule_disabled",
                                                   schedule_run_failed="schedule_run_failed")))
        monkeypatch.setattr(prerun, "check_principal", recorder.step("principal"))
        monkeypatch.setattr(prerun, "check_owner_access", recorder.step("access"))
        monkeypatch.setattr(prerun, "check_owner_rights", recorder.step("rights"))
        monkeypatch.setattr(prerun, "check_owner_credentials", recorder.step("credentials"))
        schedule = SimpleNamespace(id=5, created_by=OWNER, entity_type="agent", application_id=3, version_id=4)
        return prerun.run_prerun_check(34, schedule, RUN_ID)
    run.prerun = prerun
    return run


def test_7011_r3_3_all_steps_pass_returns_the_owner_and_changes_nothing(apply):
    recorder = Recorder()
    result = apply(recorder)
    assert (result.ok, result.owner_id, result.reason) == (True, OWNER, None)
    assert recorder.effects() == ["principal", "access", "rights", "credentials"]


def test_7011_r1_4_r1_5_lost_access_fails_the_run_disables_the_schedule_and_tells_the_owner(apply):
    reason = "owner lost access"
    recorder = Recorder(access=reason)
    result = apply(recorder)
    assert (result.ok, result.owner_id, result.reason) == (False, None, reason)
    assert recorder.calls[2:] == [
        ("fail", 34, RUN_ID, reason),
        ("disable", 34, 5, "owner_lost_access"),
        ("notify", OWNER, "schedule_disabled", RUN_ID, reason),
    ]
    assert "credentials" not in recorder.effects()  # R3.2: no credential fetch after a failed step


@pytest.mark.parametrize("answers", [
    {"principal": "principal does not match the owner"},
    {"access": "access check unavailable"},
    {"rights": "access check unavailable"},
    {"credentials": "no token for the owner"},
])
def test_7011_r2_5_other_failures_fail_the_run_keep_the_schedule_and_send_run_failed(apply, answers):
    recorder = Recorder(**answers)
    reason = next(iter(answers.values()))
    result = apply(recorder)
    assert (result.ok, result.reason) == (False, reason)
    assert "disable" not in recorder.effects()
    assert recorder.calls[-2:] == [
        ("fail", 34, RUN_ID, reason),
        ("notify", OWNER, "schedule_run_failed", RUN_ID, reason),
    ]


def test_a_lost_schedule_right_fails_the_run_disables_the_schedule_and_skips_the_credentials(apply):
    recorder = Recorder(rights="owner lost access")
    result = apply(recorder)
    assert (result.ok, result.reason) == (False, "owner lost access")
    assert ("rights", 34, OWNER, "agent", 3) in recorder.calls
    assert ("disable", 34, 5, "owner_lost_access") in recorder.calls
    assert "credentials" not in recorder.effects()


def test_7011_r3_2_a_failed_step_stops_the_later_steps(apply):
    recorder = Recorder(principal="principal does not match the owner")
    apply(recorder)
    assert recorder.effects() == ["principal", "fail", "notify"]


def test_7011_r3_1_the_effects_touch_only_the_given_schedule_and_run(apply):
    recorder = Recorder(access="owner lost access")
    apply(recorder)
    assert [call for call in recorder.calls if call[0] == "disable"] == [("disable", 34, 5, "owner_lost_access")]
    assert [call[2] for call in recorder.calls if call[0] == "fail"] == [RUN_ID]


def test_7011_r2_5_an_unexpected_error_fails_the_run_and_keeps_the_schedule(apply):
    recorder = Recorder()

    def broken(*args):
        raise ConnectionError("expansion RPC down")

    recorder.step = lambda name: broken if name == "credentials" else Recorder.step(recorder, name)
    result = apply(recorder)
    assert (result.ok, result.reason) == (False, "the pre-run check failed")
    assert recorder.calls[-2:] == [
        ("fail", 34, RUN_ID, "the pre-run check failed"),
        ("notify", OWNER, "schedule_run_failed", RUN_ID, "the pre-run check failed"),
    ]


def test_a_missing_credential_that_cannot_wait_fails_the_run_and_tells_the_owner(apply):
    recorder = Recorder(credentials="credential 9 not found", parks=False)
    result = apply(recorder)
    assert (result.ok, result.reason) == (False, "credential 9 not found")
    assert recorder.calls[-3:] == [
        ("park", 34, RUN_ID, "credential 9 not found"),
        ("fail", 34, RUN_ID, "credential 9 not found"),
        ("notify", OWNER, "schedule_run_failed", RUN_ID, "credential 9 not found"),
    ]


def test_a_missing_credential_that_waits_for_the_owner_does_not_fail_the_run(apply):
    recorder = Recorder(credentials="credential 9 not found", parks=True)
    result = apply(recorder)
    assert (result.ok, result.owner_id, result.reason) == (False, None, "credential 9 not found")
    assert recorder.calls[-1] == ("park", 34, RUN_ID, "credential 9 not found")
    assert "fail" not in recorder.effects() and "notify" not in recorder.effects()


@pytest.mark.parametrize("answer", ["no token for the owner", "version could not be expanded for the owner"])
def test_a_lost_token_or_broken_version_never_waits(apply, answer):
    recorder = Recorder(credentials=answer, parks=True)
    apply(recorder)
    assert "park" not in recorder.effects() and "fail" in recorder.effects()


@pytest.mark.parametrize("answers", [{"access": "owner lost access"}, {"principal": "principal does not match the owner"}])
def test_an_access_failure_never_waits(apply, answers):
    recorder = Recorder(parks=True, **answers)
    apply(recorder)
    assert "park" not in recorder.effects()


def test_the_parked_run_gets_the_credential_title_of_the_reason(apply):
    reason = type("Reason", (str,), {"credential": "qa_missing", "toolkit": None})("credential qa_missing not found")
    recorder = Recorder(credentials=reason, parks=True)
    apply(recorder)
    assert recorder.named == (None, "qa_missing")


def test_a_run_whose_approval_just_passed_the_credential_check_does_not_repeat_it(apply):
    recorder = Recorder()
    apply.prerun.mark_credentials_checked(RUN_ID)

    result = apply(recorder)

    assert result.ok is True
    assert recorder.effects() == ["principal", "access", "rights"]
    assert apply(Recorder()).ok is True  # the mark is spent once


def test_a_still_missing_credential_on_the_restart_fails_the_run_with_the_reason_and_no_second_wait(apply):
    recorder = Recorder(credentials="credential 9 not found", parks=False)

    result = apply(recorder)

    assert (result.ok, result.reason) == (False, "credential 9 not found")
    assert "fail" in recorder.effects()


def test_a_mark_is_cleared_when_an_earlier_check_fails(apply):
    apply.prerun.mark_credentials_checked(RUN_ID)

    result = apply(Recorder(access="access check unavailable"))

    assert result.ok is False
    assert RUN_ID not in apply.prerun._CHECKED_RUNS


def test_a_mark_is_cleared_when_a_check_raises(apply):
    recorder = Recorder()

    def broken(*args):
        raise ConnectionError("rights RPC down")

    recorder.step = lambda name: broken if name == "rights" else Recorder.step(recorder, name)
    apply.prerun.mark_credentials_checked(RUN_ID)

    result = apply(recorder)

    assert result.reason == "the pre-run check failed"
    assert RUN_ID not in apply.prerun._CHECKED_RUNS
