"""A scheduled run starts with the batch pause on, and its risk check line lists the sensitive tools by name.

Contract under test: utils/schedule_run_starter.py (`run_owner_schedule`, `_risk_check`, `_listed_pairs`) on
the fakes of the other starter tests; the predict kwargs are the contract with `execute_pipeline_via_predict_sio`.
"""
import pytest
from unit.utils import test_7011_run_starter_auto_approve as auto_approve
from unit.utils.test_7011_run_starter_pipeline import clean_review, load_starter

SENSITIVE = auto_approve.SENSITIVE
run = auto_approve.run  # the fixture of the auto-approve starter tests, bound as an attribute


def _starter(monkeypatch):
    return load_starter(monkeypatch, create=lambda **kw: None, predict=lambda **kw: None, prerun=lambda *a: None)


def _item(toolkit, tool, from_list=False, secret=None):
    item = {"code": "sensitive_action", "toolkit": toolkit, "tool": tool, "from_list": from_list}
    return {**item, "secret": secret} if secret else item


def _captured_events(starter, monkeypatch):
    events = []
    monkeypatch.setattr(starter, "run_event", lambda kind, text, level="info", **data: events.append(
        {"kind": kind, "text": text, "level": level, **data}) or events[-1])
    return events


@pytest.mark.parametrize("entity_type", ["agent", "pipeline"])
@pytest.mark.parametrize("rule", ["ask_every_time", "run_without_asking"])
def test_a_scheduled_run_starts_with_the_batch_pause_on(run, entity_type, rule):
    kwargs, _ = run(rule, True, entity_type)

    assert kwargs["batch_sensitive_approvals"] is True


def test_the_listed_pairs_are_distinct_names_in_the_order_of_the_scan(monkeypatch):
    review = {**clean_review(), "sensitive": [
        _item("Artifact", "create_file"), _item("Artifact", "delete_file"), _item("Artifact", "create_file"),
        _item("ADO", "list_indexes")]}

    pairs = _starter(monkeypatch)._listed_pairs(review)  # pylint: disable=W0212

    assert pairs == [{"toolkit": "Artifact", "tool": "create_file"}, {"toolkit": "Artifact", "tool": "delete_file"},
                     {"toolkit": "ADO", "tool": "list_indexes"}]


def test_a_tool_from_the_global_list_is_named_as_the_sdk_compares_it(monkeypatch):
    review = {**clean_review(), "sensitive": [_item("GitHub", "GitHub___Create_Issue", from_list=True)]}

    pairs = _starter(monkeypatch)._listed_pairs(review)  # pylint: disable=W0212

    assert pairs == [{"toolkit": "GitHub", "tool": "create_issue"}]


def test_a_finding_without_a_toolkit_or_tool_or_of_another_kind_is_left_out(monkeypatch):
    review = {**clean_review(), "sensitive": [
        _item("", "create_file"), _item("Artifact", ""), {"code": "personal_credential", "toolkit": "GitHub"},
        _item("Artifact", "delete_file")]}

    pairs = _starter(monkeypatch)._listed_pairs(review)  # pylint: disable=W0212

    assert pairs == [{"toolkit": "Artifact", "tool": "delete_file"}]


def test_the_listed_pairs_are_cut_at_the_limit(monkeypatch):
    starter = _starter(monkeypatch)
    over = starter.SCAN_PAIR_LIMIT + 5
    review = {**clean_review(), "sensitive": [_item("Artifact", f"tool_{n}") for n in range(over)]}

    assert len(starter._listed_pairs(review)) == starter.SCAN_PAIR_LIMIT  # pylint: disable=W0212


def test_the_risk_check_line_that_needs_approval_carries_the_tools_with_exact_keys(monkeypatch):
    starter = _starter(monkeypatch)
    events = _captured_events(starter, monkeypatch)
    review = {**clean_review(), "sensitive": [SENSITIVE, _item("Artifact", "delete_file", secret="x")]}

    outcome, _ = starter._risk_check(review, "ask_every_time", False)  # pylint: disable=W0212

    assert outcome == "needs_approval"
    (event,) = events
    assert set(event) == {"kind", "text", "level", "tools"}
    assert event["tools"] == [{"toolkit": "Outlook", "tool": "send_email"},
                              {"toolkit": "Artifact", "tool": "delete_file"}]
    assert "secret" not in str(event)


@pytest.mark.parametrize("review", [clean_review(), {**clean_review(), "blocking": [{"message": "no"}]}],
                         ids=["ok", "blocked"])
def test_the_other_risk_check_lines_carry_no_tools(monkeypatch, review):
    starter = _starter(monkeypatch)
    events = _captured_events(starter, monkeypatch)

    starter._risk_check(review, "ask_every_time", False)  # pylint: disable=W0212

    assert "tools" not in events[0]
