"""A scheduled run with several sensitive tools: the `paused` event lists them all, and one answer decides them all.

Contract under test: utils/schedule_runs.py (`pause_schedule_run`, `check_schedule_decision`,
`apply_schedule_decision`) on the real models, in-memory SQLite. The fixtures are the ones of the run event tests.
"""
import sys
import types

import pytest

from unit.utils import test_7011_schedule_runs_fail as run_fixtures
from unit.utils import test_7011_schedule_runs_outcome as outcome_fixtures

# The fixtures of the run writer tests, bound as attributes so pytest injects them by name; listed in __all__
# because the tests take them as arguments, which a static check does not count as a use
runs, session, sent = run_fixtures.runs, run_fixtures.session, outcome_fixtures.sent
_run, _schedule = run_fixtures._run, run_fixtures._schedule  # pylint: disable=W0212
__all__ = ["runs", "session", "sent"]

CLAIM = "plugins.elitea_core.utils.schedule_claim"
RUNS = "plugins.elitea_core.utils.schedule_runs"
ARTIFACT_CREATE = {"guardrail_type": "sensitive_tool", "toolkit_name": "Artifact", "tool_name": "create_file",
                   "interrupt_id": "hitl_a", "tool_call_id": "c1", "tool_args": {"token": "secret"}}
ARTIFACT_DELETE = {**ARTIFACT_CREATE, "tool_name": "delete_file", "interrupt_id": "hitl_b", "tool_call_id": "c2"}
ADO_LIST = {**ARTIFACT_CREATE, "toolkit_name": "ADO", "tool_name": "list_indexes", "interrupt_id": "hitl_c",
            "tool_call_id": "c3"}
AGGREGATE = {"guardrail_type": "parallel_sensitive_tools", "batch": "sequential", "pending": [
    {"interrupt_id": "hitl_a", "tool_call_id": "c1", "toolkit_name": "Artifact", "tool_name": "create_file"},
    {"interrupt_id": "hitl_b", "tool_call_id": "c2", "toolkit_name": "Artifact", "tool_name": "delete_file"},
    {"interrupt_id": "hitl_c", "tool_call_id": "c3", "toolkit_name": "ADO", "tool_name": "list_indexes"}]}
PAIRS = [{"toolkit": "Artifact", "tool": "create_file"}, {"toolkit": "Artifact", "tool": "delete_file"},
         {"toolkit": "ADO", "tool": "list_indexes"}]
COUNTED = [{**pair, "count": 1} for pair in PAIRS]


def _events(session, run):
    session.refresh(run)
    return run.events or []


def _log_scan(runs, run, *pairs):
    """Write the risk check line of the run start, with the tools the scan found."""
    claim = sys.modules[CLAIM]
    runs.record_run_events(1, run.id, claim.run_event("risk_check", "Risk check: needs approval", "warning",
                                                      tools=list(pairs)))


def _paused(session, run):
    return next(event for event in _events(session, run) if event["kind"] == "paused")


def test_a_pause_lists_every_pending_tool_by_name_in_tools(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE, ADO_LIST]})

    event = _paused(session, run)
    assert event["tools"] == COUNTED
    assert (event["toolkit"], event["tool"]) == ("Artifact", "create_file")
    assert "secret" not in str(event)


def test_the_paused_event_has_exactly_these_keys(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE]})

    assert set(_paused(session, run)) == {"at", "kind", "text", "level", "toolkit", "tool", "tools", "hidden",
                                          "later_tools", "later_hidden", "interrupt_ids", "wait_hours"}


def test_a_single_tool_pause_keeps_its_text_and_lists_one_tool(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupt": ARTIFACT_CREATE})

    event = _paused(session, run)
    assert event["text"] == "Paused for approval: Artifact · create_file"
    assert (event["tools"], event["later_tools"]) == ([COUNTED[0]], [])


def test_later_tools_are_the_scanned_pairs_that_are_not_pending(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    _log_scan(runs, run, *PAIRS, {"toolkit": "GitHub", "tool": "create_pull_request"})

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE]})

    event = _paused(session, run)
    assert event["tools"] == COUNTED[:2]
    assert event["later_tools"] == [COUNTED[2], {"toolkit": "GitHub", "tool": "create_pull_request", "count": 1}]


def test_a_scanned_pair_that_is_pending_with_other_letter_case_is_not_later(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    _log_scan(runs, run, {"toolkit": "artifact", "tool": "Create_File"})

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE]})

    assert _paused(session, run)["later_tools"] == []


def test_a_run_with_no_logged_scan_has_no_later_tools(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE]})

    assert _paused(session, run)["later_tools"] == []


def test_later_tools_carry_names_only(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    _log_scan(runs, run, {"toolkit": "GitHub", "tool": "delete_file", "token": "secret"})

    runs.pause_schedule_run(1, run.id, {"hitl_interrupt": ARTIFACT_CREATE})

    assert _paused(session, run)["later_tools"] == [{"toolkit": "GitHub", "tool": "delete_file", "count": 1}]


def test_a_failed_scan_read_gives_no_later_tools_and_the_pause_goes_on(runs, session, sent, monkeypatch):
    run = _run(runs, session, _schedule(runs, session))
    real = sys.modules["tools"].db.get_session
    reads = []

    def broken_first_read(project_id):
        reads.append(project_id)
        if len(reads) == 1:
            raise RuntimeError("database down")
        return real(project_id)

    monkeypatch.setattr(sys.modules["tools"].db, "get_session", broken_first_read)
    logged = []
    monkeypatch.setattr(runs, "log", types.SimpleNamespace(exception=lambda *args: logged.append(args)))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupt": ARTIFACT_CREATE})

    assert _paused(session, run)["later_tools"] == []
    assert (run.status, len(logged)) == ("waiting_for_approval", 1)


def test_a_long_tool_list_keeps_the_text_trim_and_lists_every_tool(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    items = [{**ARTIFACT_CREATE, "tool_name": f"tool_number_{number:02d}"} for number in range(30)]

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": items})

    event = _paused(session, run)
    assert len(event["text"]) <= sys.modules[CLAIM].TEXT_LIMIT
    assert event["text"].endswith(" more)")
    assert [pair["tool"] for pair in event["tools"]] == [f"tool_number_{number:02d}" for number in range(30)]


def test_the_tool_lists_are_cut_at_the_pair_limit_and_count_what_is_cut(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    limit = sys.modules[RUNS].PAIR_LIMIT
    items = [{**ARTIFACT_CREATE, "tool_name": f"tool_{number}", "interrupt_id": f"hitl_{number}"}
             for number in range(limit + 10)]
    _log_scan(runs, run, *({"toolkit": "Other", "tool": f"scan_{number}"} for number in range(limit + 10)))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": items})

    event = _paused(session, run)
    assert (len(event["tools"]), len(event["later_tools"])) == (limit, limit)
    assert (event["hidden"], event["later_hidden"]) == (10, 10)
    assert len(event["interrupt_ids"]) == limit


@pytest.mark.parametrize("payload", [
    {"hitl_interrupt": AGGREGATE},
    {"hitl_interrupts": [AGGREGATE]},
    AGGREGATE,
], ids=["in hitl_interrupt", "in hitl_interrupts", "as the payload"])
def test_the_aggregate_pause_is_read_from_pending(runs, session, sent, payload):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, payload)

    event = _paused(session, run)
    assert event["text"] == "Paused for approval: Artifact · create_file, Artifact · delete_file, ADO · list_indexes"
    assert event["tools"] == COUNTED
    assert run.status == "waiting_for_approval"


def test_an_aggregate_pause_without_pending_tools_fails_the_run(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupt": {**AGGREGATE, "pending": []}})

    assert run.status == "failed"


def test_one_reject_among_the_decisions_of_a_pause_fails_the_run(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session, created_by=12))
    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE, ADO_LIST]})

    decision = runs.check_schedule_decision(1, run.id, 12, ["approve", "reject", "approve"])
    runs.apply_schedule_decision(1, run.id, decision)

    assert decision == "reject"
    assert run.status == "failed"


def test_one_approve_of_a_pause_with_three_tools_resumes_the_run_once(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session, created_by=12))
    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE, ADO_LIST]})

    decision = runs.check_schedule_decision(1, run.id, 12, ["approve", "approve", "approve"])
    runs.apply_schedule_decision(1, run.id, decision)

    assert [event["kind"] for event in _events(session, run)] == ["paused", "decision"]
    assert run.status == "running"


def test_calls_of_the_same_tool_are_one_entry_with_their_number(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    third = {**ARTIFACT_CREATE, "interrupt_id": "hitl_d", "tool_call_id": "c4"}

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE, third]})

    event = _paused(session, run)
    assert event["tools"] == [{**PAIRS[0], "count": 2}, {**PAIRS[1], "count": 1}]
    assert event["interrupt_ids"] == ["hitl_a", "hitl_b", "hitl_d"]


def test_a_short_list_has_nothing_hidden(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    _log_scan(runs, run, *PAIRS)

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, ARTIFACT_DELETE]})

    event = _paused(session, run)
    assert (event["hidden"], event["later_hidden"]) == (0, 0)


def test_the_event_names_the_pending_interrupts_for_the_banner_to_send_back(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))

    runs.pause_schedule_run(1, run.id, {"hitl_interrupt": AGGREGATE})

    assert _paused(session, run)["interrupt_ids"] == ["hitl_a", "hitl_b", "hitl_c"]


def test_a_pause_with_an_item_that_has_no_interrupt_id_lists_no_ids(runs, session, sent):
    run = _run(runs, session, _schedule(runs, session))
    without_id = {key: value for key, value in ARTIFACT_DELETE.items() if key != "interrupt_id"}

    runs.pause_schedule_run(1, run.id, {"hitl_interrupts": [ARTIFACT_CREATE, without_id]})

    assert "interrupt_ids" not in _paused(session, run)
