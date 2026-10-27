"""EL-7011 owner CR 2026-10-21 (test panel log): a run writes its log from start to end.

Real: `run_owner_schedule`, the pre-run check, the run writers of `utils/schedule_runs.py` (pause,
decision, finish) and the schedule models, over SQLite tables. The harness is the `world` fixture of the
US-11 integration test; the risk scan and the user name lookup are stand-ins.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_run_event_log.py
"""
import sys
import types

import pytest
from integration.test_7011_agent_schedule_run import (  # noqa: F401
    OWNER,
    PKG,
    PROJECT_ID,
    VERSION_ID,
    world,
)

# ruff: noqa: F811

SENSITIVE = {"hitl_interrupt": {"type": "hitl", "guardrail_type": "sensitive_tool", "toolkit_name": "github",
                                "tool_name": "create_issue", "tool_args": {"title": "x"}}}
BLOCKING_CODES = {"personal_signin_required", "human_input_step", "missing_credential"}
GITHUB_WARNING = {"code": "sensitive_action", "message": "needs approval", "toolkit": "github", "tool": "create_issue"}


@pytest.fixture
def log(world, monkeypatch):
    """`world` with a risk scan and a name lookup the test sets; the run log and the run writers."""
    state = types.SimpleNamespace(warnings=[], world=world, runs=sys.modules[f"{PKG}.utils.schedule_runs"])
    def scan_schedule(project_id, version_id, owner_id):  # the review of `state.warnings`, as the real scan sorts it
        found = list(state.warnings)
        return {"blocking": [w for w in found if w["code"] in BLOCKING_CODES],
                "sensitive": [w for w in found if w["code"] == "sensitive_action"],
                "notices": [w for w in found if w["code"] not in BLOCKING_CODES | {"sensitive_action"}],
                "fingerprint": state.fingerprint}

    state.fingerprint = "fp-1"
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_risk"], "scan_schedule", scan_schedule)
    authors = types.ModuleType(f"{PKG}.utils.authors")
    authors.get_authors_data = lambda ids: [{"id": OWNER, "name": "Ann Smith", "email": "ann@example.com"}]
    monkeypatch.setitem(sys.modules, authors.__name__, authors)
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_notifications"], "emit_schedule_run_finished",
                        lambda *args: None, raising=False)
    state.events = lambda: world.run_row().events or []
    state.kinds = lambda: [event["kind"] for event in state.events()]
    return state


def test_cr_el7011_a_test_run_logs_start_account_risk_agent_pause_decision_and_end(log):
    log.warnings = [GITHUB_WARNING]

    log.world.run(trigger="test")
    log.runs.pause_schedule_run(PROJECT_ID, 1, SENSITIVE)
    log.runs.apply_schedule_decision(PROJECT_ID, 1, log.runs.check_schedule_decision(PROJECT_ID, 1, OWNER, ["approve"]))
    log.runs.finish_schedule_run(PROJECT_ID, 1, "success", None, 42)

    assert log.kinds() == ["started", "account", "risk_check", "agent_started", "paused", "decision", "finished"]
    texts = [event["text"] for event in log.events()]
    assert texts == [
        "Test run started · trigger: test · version v3",
        "Account: Ann Smith (schedule owner)",
        "Risk check: needs approval for github · create_issue",
        "Agent started: Daily report",
        "Paused for approval: github · create_issue",
        "Approved by the schedule owner",
        "Run finished: success",
    ]
    assert [event["level"] for event in log.events()][2::2] == ["warning", "warning", "success"]
    run = log.world.run_row()
    assert (run.version_id, run.principal_label, run.risk_outcome) == (
        VERSION_ID, "Ann Smith (schedule owner)", "needs_approval")
    assert all(event["at"] for event in log.events())


def test_cr_el7011_a_cron_run_without_sensitive_tools_logs_risk_ok(log):
    log.world.run()

    assert log.events()[0]["text"] == "Run started · trigger: cron · version v3"
    assert (log.events()[2]["text"], log.events()[2]["level"]) == ("Risk check: OK", "info")
    assert log.world.run_row().risk_outcome == "ok"


def test_cr_el7011_run_without_asking_says_so_in_the_risk_line(log, monkeypatch):
    log.warnings = [GITHUB_WARNING]
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_run_starter"], "_setting_allows_auto_approve",
                        lambda project_id: True)

    log.world.run(approval_rule="run_without_asking", approval_fingerprint="fp-1")

    assert log.events()[2]["text"] == "Risk check: needs approval for github · create_issue; Run without asking is on"
    assert log.world.predictions[0]["auto_approve_sensitive_actions"] == [("github", "create_issue")]


# Owner CR 2026-10-21 (risk review): new sensitive tools since the confirmation turn the run back to asking
def test_el7011_cr_a_changed_fingerprint_logs_needs_approval_and_pauses(log, monkeypatch):
    log.warnings = [GITHUB_WARNING]
    log.fingerprint = "fp-2"
    monkeypatch.setattr(sys.modules[f"{PKG}.utils.schedule_run_starter"], "_setting_allows_auto_approve",
                        lambda project_id: True)

    log.world.run(approval_rule="run_without_asking", approval_fingerprint="fp-1")

    assert (log.events()[2]["level"], log.world.run_row().risk_outcome) == ("warning", "needs_approval")
    assert "pauses and asks" in log.events()[2]["text"]
    assert log.world.predictions[0]["auto_approve_sensitive_actions"] is False


# Owner CR 2026-10-21 (risk review): a blocking finding logs the risk line and ends the run as blocked
def test_el7011_cr_a_blocking_finding_logs_blocked_and_a_finished_error(log):
    log.warnings = [{"code": "personal_signin_required", "message": 'Toolkit "GitHub" needs a personal sign-in'}]

    log.world.run(trigger="test")

    assert log.kinds() == ["started", "account", "risk_check", "finished"]
    risk, end = log.events()[2], log.events()[3]
    assert (risk["text"], risk["level"]) == ('Risk check: blocked: Toolkit "GitHub" needs a personal sign-in', "error")
    assert (end["level"], end["status"]) == ("error", "blocked")
    assert (log.world.run_row().status, log.world.run_row().risk_outcome) == ("blocked", "blocked")
    assert log.world.predictions == []


def test_cr_el7011_an_incomplete_scan_is_not_reported_as_ok(log):
    log.warnings = [{"code": "scan_incomplete", "message": "The version could not be fully checked for risks"}]

    log.world.run()

    assert (log.events()[2]["text"], log.world.run_row().risk_outcome) == ("Risk check: not complete", "unknown")


def test_cr_el7011_a_failed_name_lookup_still_names_the_account(log, monkeypatch):
    def down(ids):
        raise RuntimeError("auth down")

    monkeypatch.setattr(sys.modules[f"{PKG}.utils.authors"], "get_authors_data", down)

    log.world.run()

    assert log.world.run_row().principal_label == f"user {OWNER} (schedule owner)"


def test_cr_el7011_a_rejected_run_logs_the_decision_and_the_failed_end(log):
    log.world.run(trigger="test")
    log.runs.pause_schedule_run(PROJECT_ID, 1, SENSITIVE)

    log.runs.apply_schedule_decision(PROJECT_ID, 1, log.runs.check_schedule_decision(PROJECT_ID, 1, OWNER, ["reject"]))

    assert log.kinds()[-2:] == ["decision", "finished"]
    assert log.events()[-1]["text"] == "Run finished: failed (rejected by the owner)"


def test_cr_el7011_a_failed_prerun_logs_the_start_and_the_reason(log):
    log.world.auth.members.clear()

    log.world.run()

    assert log.kinds() == ["started", "finished"]
    assert (log.events()[1]["text"], log.events()[1]["level"]) == ("Run finished: failed (owner lost access)", "error")


def test_cr_el7011_a_failed_predict_call_ends_the_log_after_the_agent_line(log):
    log.world.predict_errors.append(RuntimeError("rpc down"))

    log.world.run()

    assert log.kinds()[-2:] == ["agent_started", "finished"]
    assert log.events()[-1]["text"] == "Run finished: failed (the run call failed)"
