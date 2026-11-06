"""EL-7011 US-18 R1.1, R3.1, R3.2: the run starter auto-approves only with the rule and the project setting.

Owner CR 2026-10-21 (risk review): the starter scans the version again. A blocking finding ends the run
as blocked; sensitive tools whose fingerprint differs from the confirmed one run as "Pause and ask me".
The real starter runs on faked neighbours; the project setting and the risk review are faked.
"""
import sys
import types

import pytest
from unit.utils.test_7011_run_starter_pipeline import (
    OWNER,
    PKG,
    _module,
    clean_review,
    load_starter,
)

PROJECT_ID, SCHEDULE_ID, RUN_ID = 34, 1, 3
SENSITIVE = {"code": "sensitive_action", "toolkit": "Outlook", "tool": "send_email"}


@pytest.fixture
def run(monkeypatch):
    predicts, setting_reads, blocks, scans, finishes, outcomes, parks = [], [], [], [], [], [], []
    state = types.SimpleNamespace(review=clean_review())

    def allowed(project_id):
        setting_reads.append(project_id)
        return allowed.value

    monkeypatch.setitem(
        sys.modules, f"{PKG}.utils.schedule_settings", _module(f"{PKG}.utils.schedule_settings",
                                                           is_run_without_asking_allowed=allowed))
    def park(project_id, run_id, review):
        # the real writer parks when the review lists a tool with a toolkit name (tested with the writers)
        parks.append((project_id, run_id, review))
        return any(item.get("code") == "sensitive_action" and item.get("toolkit") for item in review["sensitive"])

    starter = load_starter(
        monkeypatch,
        park=park,
        create=lambda **kw: ("uuid-1", 7, "resp-1"),
        predict=lambda **kw: predicts.append(kw),
        prerun=lambda *a: types.SimpleNamespace(ok=True, owner_id=OWNER),
        block=lambda *args: blocks.append(args),
        finish=lambda *args, **kw: finishes.append(args),
        review=lambda *args: scans.append(args) or state.review,
    )

    monkeypatch.setattr(starter, "record_run_events",
                        lambda *args, **kw: outcomes.append(kw["risk_outcome"]) if "risk_outcome" in kw else None)

    def start(rule, setting, entity_type="pipeline", confirmed="fp-1", confirmed_by=OWNER, resumed=False,
              pre_start_pause=None):
        allowed.value = setting
        loaded = types.SimpleNamespace(
            schedule=types.SimpleNamespace(
                id=SCHEDULE_ID, entity_type=entity_type, run_mode="prompt", approval_rule=rule,
                approval_fingerprint=confirmed, created_by=OWNER, approval_confirmed_by=confirmed_by),
            version=types.SimpleNamespace(id=9, name="v3"), entity_name="Flow", prompt="Go", variables=[],
            resumed=resumed, pre_start_pause=pre_start_pause)
        monkeypatch.setattr(starter, "_load", lambda *a: loaded)
        starter.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)
        return predicts[-1] if predicts else None, setting_reads

    start.state, start.blocks, start.scans, start.predicts = state, blocks, scans, predicts
    start.starter = starter
    start.finishes = finishes
    start.parks = parks
    start.outcomes = outcomes
    return start


# R3.1 / R3.2
@pytest.mark.parametrize("entity_type", ["agent", "pipeline"])
def test_run_without_asking_with_an_empty_scan_approves_nothing(run, entity_type):
    kwargs, reads = run("run_without_asking", True, entity_type)

    assert kwargs["auto_approve_sensitive_actions"] is False
    assert reads == [PROJECT_ID]


# R3.2
def test_el_7011_r3_2_run_without_asking_with_the_setting_off_pauses(run):
    kwargs, _ = run("run_without_asking", False)

    assert kwargs["auto_approve_sensitive_actions"] is False


# R1.1
def test_el_7011_r1_1_pause_and_ask_pauses_and_never_reads_the_setting(run):
    kwargs, reads = run("pause_and_ask", True)

    assert kwargs["auto_approve_sensitive_actions"] is False
    assert reads == []


# Owner CR 2026-10-21: the run scans the version of the schedule as its owner
def test_el7011_cr_the_run_scans_its_version_as_the_owner(run):
    run("pause_and_ask", False)

    assert run.scans == [(PROJECT_ID, 9, OWNER, 'pause_and_ask')]


# Owner CR 2026-10-21: the confirmed fingerprint keeps "Run without asking"
def test_el7011_cr_confirmed_sensitive_tools_still_auto_approve(run):
    run.state.review = {**clean_review(), "sensitive": [SENSITIVE], "fingerprint": "fp-1"}

    kwargs, _ = run("run_without_asking", True, confirmed="fp-1")

    assert kwargs["auto_approve_sensitive_actions"] == [("Outlook", "send_email")]


def test_a_confirmed_run_with_a_personal_credential_item_auto_approves(run):
    personal = {"code": "personal_credential", "toolkit": "GitHub", "tool": None}
    run.state.review = {**clean_review(), "sensitive": [SENSITIVE, personal], "fingerprint": "fp-1"}

    kwargs, _ = run("run_without_asking", True)

    assert kwargs["auto_approve_sensitive_actions"] == [("Outlook", "send_email"), ("GitHub", "*")]


@pytest.mark.parametrize("confirmed_by", [None, OWNER + 1])
def test_a_confirmation_by_someone_else_parks_the_run_before_the_start(run, confirmed_by):
    run.state.review = {**clean_review(), "sensitive": [SENSITIVE], "fingerprint": "fp-1"}

    kwargs, _ = run("run_without_asking", True, confirmed_by=confirmed_by)

    assert kwargs is None and len(run.parks) == 1


# Owner CR 2026-10-21: new sensitive tools since the confirmation run as "Pause and ask me"
@pytest.mark.parametrize("confirmed", ["fp-1", None])
def test_a_changed_fingerprint_parks_the_run_before_the_start(run, confirmed):
    run.state.review = {**clean_review(), "sensitive": [SENSITIVE], "fingerprint": "fp-2"}

    kwargs, _ = run("run_without_asking", True, confirmed=confirmed)

    assert kwargs is None and len(run.parks) == 1
    assert run.blocks == []


# Owner CR 2026-10-21: a tool that could not be checked never auto-approves
def test_el7011_cr_an_incomplete_scan_dispatches_as_pause_and_ask(run):
    run.state.review = {**clean_review(), "notices": [{"code": "scan_incomplete", "message": "x"}]}

    kwargs, _ = run("run_without_asking", True)

    assert kwargs["auto_approve_sensitive_actions"] is False


# Owner CR 2026-10-21: a blocking finding ends the run as blocked, with the findings as the reason
@pytest.mark.parametrize("rule", ["pause_and_ask", "run_without_asking"])
def test_el7011_cr_a_blocking_finding_blocks_the_run_with_the_reason(run, rule):
    run.state.review = {**clean_review(), "blocking": [
        {"code": "human_input_step", "message": 'Node "Review" waits for a person'},
        {"code": "missing_credential", "message": 'Toolkit "Jira" has no usable credential for the owner'},
    ]}

    kwargs, _ = run(rule, True)

    assert kwargs is None and run.predicts == []
    reason = 'Node "Review" waits for a person; Toolkit "Jira" has no usable credential for the owner'
    assert run.blocks == [(PROJECT_ID, RUN_ID, reason, 'error')]


def test_a_sensitive_item_without_a_toolkit_name_is_left_out_of_the_pairs(run):
    nameless = {"code": "sensitive_action", "toolkit": None, "tool": "send_email"}
    run.state.review = {**clean_review(), "sensitive": [nameless, SENSITIVE], "fingerprint": "fp-1"}

    kwargs, _ = run("run_without_asking", True)

    assert kwargs["auto_approve_sensitive_actions"] == [("Outlook", "send_email")]


def test_a_listed_tool_with_a_different_spelling_is_still_approved_by_its_runtime_name(run):
    listed = {"code": "sensitive_action", "toolkit": "Outlook", "tool": "Outlook___Send-Email", "from_list": True}
    run.state.review = {**clean_review(), "sensitive": [listed], "fingerprint": "fp-1"}

    kwargs, _ = run("run_without_asking", True, confirmed="fp-1")

    assert ("Outlook", "send-email") in kwargs["auto_approve_sensitive_actions"]
    assert ("Outlook", "*") in kwargs["auto_approve_sensitive_actions"]


def test_a_may_apply_tool_is_not_approved_by_another_confirmed_tool_of_the_toolkit(run):
    listed = {"code": "sensitive_action", "toolkit": "Outlook", "tool": "send_email", "from_list": True}
    maybe = {"code": "sensitive_action", "toolkit": "Outlook", "tool": "delete_mail", "from_list": True,
             "may_apply": True}
    run.state.review = {**clean_review(), "sensitive": [listed], "notices": [maybe], "fingerprint": "fp-1"}

    kwargs, _ = run("run_without_asking", True, confirmed="fp-1")

    assert kwargs["auto_approve_sensitive_actions"] == [("Outlook", "send_email")]


def test_a_confirmed_run_with_no_usable_pair_sends_false(run):
    nameless = {"code": "sensitive_action", "toolkit": "", "tool": "send_email"}
    run.state.review = {**clean_review(), "sensitive": [nameless], "fingerprint": "fp-1"}
    kwargs, _ = run("run_without_asking", True)

    assert kwargs["auto_approve_sensitive_actions"] is False


# A missing credential or sign-in: "Pause and ask me" saves and starts with it; "Run without asking" never waits
def test_a_sign_in_finding_ends_the_run_as_failed_under_run_without_asking(run):
    reason = 'Toolkit "Docs" needs a personal sign-in; a scheduled run cannot sign in'
    run.state.review = {**clean_review(), "blocking": [{"code": "personal_signin_required", "message": reason}]}

    kwargs, _ = run("run_without_asking", True)

    assert kwargs is None and run.predicts == [] and run.blocks == []
    assert run.finishes == [(PROJECT_ID, RUN_ID, "failed", reason)]


def test_a_human_input_step_still_blocks_the_run(run):
    run.state.review = {**clean_review(), "blocking": [{"code": "human_input_step", "message": "x"}]}

    run("run_without_asking", True)

    assert run.finishes == [] and len(run.blocks) == 1


def test_a_run_under_pause_and_ask_starts_when_the_scan_only_notices_a_sign_in(run):
    run.state.review = {**clean_review(), "notices": [
        {"code": "personal_signin_required", "message": "Toolkit needs a sign-in", "hint": "h"}]}

    kwargs, _ = run("pause_and_ask", True)

    assert kwargs is not None and run.blocks == [] and run.finishes == []


def test_pause_and_ask_without_a_sensitive_tool_starts_without_a_stop_and_with_outcome_ok(run):
    kwargs, _ = run("pause_and_ask", True)

    assert kwargs["auto_approve_sensitive_actions"] is False
    assert run.outcomes == ["ok"]


def test_pause_and_ask_with_a_listed_tool_gives_needs_approval_and_starts_no_run(run):
    run.state.review = {**clean_review(), "sensitive": [SENSITIVE], "fingerprint": "fp-1"}

    kwargs, _ = run("pause_and_ask", True)

    assert kwargs is None
    assert run.outcomes == ["needs_approval"]


def test_a_personal_credential_alone_does_not_give_needs_approval(run):
    personal = {"code": "personal_credential", "toolkit": "GitHub", "tool": None}
    run.state.review = {**clean_review(), "sensitive": [personal], "fingerprint": "fp-1"}

    run("pause_and_ask", True)

    assert run.outcomes == ["ok"]


REVIEW_TWO_TOOLS = {**clean_review(), "fingerprint": "fp-1", "sensitive": [
    {"code": "sensitive_action", "toolkit": "GitHub", "tool": "create_pull_request"},
    {"code": "sensitive_action", "toolkit": "GitHub", "tool": "delete_file"}]}
PRE_START_PAUSE = {"kind": "paused", "need": "tools", "fingerprint": "fp-1"}


def test_pause_and_ask_parks_the_run_before_the_first_call_and_lists_every_sensitive_tool(run):
    run.state.review = REVIEW_TWO_TOOLS

    kwargs, _ = run("pause_and_ask", True)

    assert kwargs is None and run.predicts == []
    assert run.parks == [(PROJECT_ID, RUN_ID, REVIEW_TWO_TOOLS)]
    assert run.outcomes == ["needs_approval"]


def test_run_without_asking_that_does_not_hold_parks_before_start(run):
    run.state.review = REVIEW_TWO_TOOLS

    kwargs, _ = run("run_without_asking", False)

    assert kwargs is None and len(run.parks) == 1


def test_run_without_asking_that_holds_does_not_park(run):
    run.state.review = REVIEW_TWO_TOOLS

    kwargs, _ = run("run_without_asking", True, confirmed="fp-1")

    assert run.parks == []
    assert kwargs["auto_approve_sensitive_actions"] == [("GitHub", "create_pull_request"), ("GitHub", "delete_file")]


def test_a_review_without_a_listed_tool_does_not_park(run):
    personal = {"code": "personal_credential", "toolkit": "GitHub", "tool": None}
    run.state.review = {**clean_review(), "sensitive": [personal]}

    kwargs, _ = run("pause_and_ask", True)

    assert run.parks == [] and kwargs is not None


def test_a_failing_park_ends_the_run_as_failed_and_starts_nothing(run, monkeypatch):
    run.state.review = REVIEW_TWO_TOOLS
    failed = []

    def broken(*_args):
        raise RuntimeError("database down")

    monkeypatch.setattr(run.starter, "park_before_start", broken)
    monkeypatch.setattr(run.starter, "fail_schedule_run", lambda *a: failed.append(a))

    kwargs, _ = run("pause_and_ask", True)

    assert kwargs is None and failed == [(PROJECT_ID, RUN_ID, run.starter.START_FAILED)]


def test_a_resumed_run_with_the_same_fingerprint_starts_with_the_listed_pairs(run):
    run.state.review = REVIEW_TWO_TOOLS

    kwargs, _ = run("pause_and_ask", True, resumed=True, pre_start_pause=PRE_START_PAUSE)

    assert run.parks == []
    assert kwargs["auto_approve_sensitive_actions"] == [("GitHub", "create_pull_request"), ("GitHub", "delete_file")]
    assert run.outcomes == ["needs_approval"]


def test_a_resumed_run_never_sends_a_wildcard_for_a_listed_tool(run):
    listed = {"code": "sensitive_action", "toolkit": "Outlook", "tool": "Outlook___Send-Email", "from_list": True}
    personal = {"code": "personal_credential", "toolkit": "GitHub", "tool": None}
    run.state.review = {**clean_review(), "sensitive": [listed, personal], "fingerprint": "fp-1"}

    kwargs, _ = run("pause_and_ask", True, resumed=True, pre_start_pause=PRE_START_PAUSE)

    assert kwargs["auto_approve_sensitive_actions"] == [("Outlook", "send-email")]


def test_a_may_apply_tool_is_not_in_the_pairs_of_a_resumed_run(run):
    maybe = {"code": "sensitive_action", "toolkit": "Outlook", "tool": "delete_mail", "from_list": True,
             "may_apply": True}
    run.state.review = {**REVIEW_TWO_TOOLS, "notices": [maybe]}

    kwargs, _ = run("pause_and_ask", True, resumed=True, pre_start_pause=PRE_START_PAUSE)

    pairs = kwargs["auto_approve_sensitive_actions"]
    assert pairs is not True and all(toolkit != "Outlook" for toolkit, _ in pairs)


def test_a_resumed_run_with_a_changed_fingerprint_parks_again(run):
    run.state.review = {**REVIEW_TWO_TOOLS, "fingerprint": "fp-2"}

    kwargs, _ = run("pause_and_ask", True, resumed=True, pre_start_pause=PRE_START_PAUSE)

    assert kwargs is None and len(run.parks) == 1


def test_a_resumed_run_after_another_pause_keeps_the_pause_at_the_call(run):
    run.state.review = REVIEW_TWO_TOOLS

    kwargs, _ = run("pause_and_ask", True, resumed=True, pre_start_pause=None)

    assert run.parks == [] and kwargs["auto_approve_sensitive_actions"] is False


def test_a_resumed_run_with_a_blocking_finding_still_blocks(run):
    run.state.review = {**REVIEW_TWO_TOOLS, "blocking": [{"code": "human_input_step", "message": "x"}]}

    kwargs, _ = run("pause_and_ask", True, resumed=True, pre_start_pause=PRE_START_PAUSE)

    assert kwargs is None and run.parks == [] and len(run.blocks) == 1


def test_the_newest_pre_start_pause_of_a_run_is_the_one_the_restart_reads(run):
    older = {"kind": "paused", "need": "tools", "fingerprint": "fp-0"}
    other = {"kind": "paused", "need": "credential"}
    row = types.SimpleNamespace(events=[older, PRE_START_PAUSE, other, {"kind": "decision"}])

    assert run.starter._pre_start_pause(row) is PRE_START_PAUSE
    assert run.starter._pre_start_pause(types.SimpleNamespace(events=[other])) is None
    assert run.starter._pre_start_pause(types.SimpleNamespace(events=None)) is None
