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

    def __init__(self, principal=None, access=None, credentials=None):
        self.answers = {"principal": principal, "access": access, "credentials": credentials}
        self.calls = []

    def step(self, name):
        def check(*args):
            self.calls.append((name, *args))
            return self.answers[name]
        return check

    def fail_schedule_run(self, project_id, run_id, reason):
        self.calls.append(("fail", project_id, run_id, reason))

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
            fail_schedule_run=recorder.fail_schedule_run, disable_schedule=recorder.disable_schedule))
        monkeypatch.setitem(sys.modules, f"{PKG}.utils.schedule_notifications", SimpleNamespace(
            notify_schedule_event=recorder.notify))
        monkeypatch.setitem(sys.modules, f"{PKG}.models.enums.all", SimpleNamespace(
            NotificationEventTypes=SimpleNamespace(schedule_disabled="schedule_disabled",
                                                   schedule_run_failed="schedule_run_failed")))
        monkeypatch.setattr(prerun, "check_principal", recorder.step("principal"))
        monkeypatch.setattr(prerun, "check_owner_access", recorder.step("access"))
        monkeypatch.setattr(prerun, "check_owner_credentials", recorder.step("credentials"))
        schedule = SimpleNamespace(id=5, created_by=OWNER, application_id=3, version_id=4)
        return prerun.run_prerun_check(34, schedule, RUN_ID)
    return run


def test_7011_r3_3_all_steps_pass_returns_the_owner_and_changes_nothing(apply):
    recorder = Recorder()
    result = apply(recorder)
    assert (result.ok, result.owner_id, result.reason) == (True, OWNER, None)
    assert recorder.effects() == ["principal", "access", "credentials"]


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
    {"credentials": "no token for the owner"},
    {"credentials": "credential 9 not found"},
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


def test_7011_r3_2_a_failed_step_stops_the_later_steps(apply):
    recorder = Recorder(principal="principal does not match the owner")
    apply(recorder)
    assert recorder.effects() == ["principal", "fail", "notify"]


def test_7011_r3_1_the_effects_touch_only_the_given_schedule_and_run(apply):
    recorder = Recorder(access="owner lost access")
    apply(recorder)
    assert [call for call in recorder.calls if call[0] == "disable"] == [("disable", 34, 5, "owner_lost_access")]
    assert [call[2] for call in recorder.calls if call[0] == "fail"] == [RUN_ID]
