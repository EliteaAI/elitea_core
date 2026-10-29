"""Unit tests: a schedule run hands the predict call the target version only.

The run never copies sub-agents or skills into the call, and it never reads the version of an agent that
uses the target as its sub-agent. The predict path then loads the children of that exact version at run time.
utils/schedule_run_starter.py is loaded from source; only its leaves are faked.
"""
import sys
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

PKG = "plugins.elitea_core"
PROJECT_ID, SCHEDULE_ID, RUN_ID, OWNER = 34, 7, 31, 12
HELPER_ID, HELPER_VERSION_ID = 5, 9
PARENT_ID, PARENT_VERSION_ID = 6, 10
CHILD_KEYS = ("sub_agents", "skills", "tools", "toolkits", "children")


class FakeSession:
    def __init__(self, rows, reads):
        self.rows, self.reads = rows, reads

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, model, row_id):
        self.reads.append((model.__name__, row_id))
        return self.rows.get((model.__name__, row_id))


@pytest.fixture
def run(monkeypatch):
    calls, reads = {}, []
    schedule = SimpleNamespace(
        id=SCHEDULE_ID, name="S1", principal={}, application_id=HELPER_ID, version_id=HELPER_VERSION_ID,
        created_by=OWNER, prompt="go", variables=[], entity_type="agent", run_mode=None,
        approval_rule="pause_and_ask", approval_fingerprint=None, approval_confirmed_by=None)
    rows = {
        ("EntitySchedule", SCHEDULE_ID): schedule,
        ("ApplicationVersion", HELPER_VERSION_ID): SimpleNamespace(
            id=HELPER_VERSION_ID, application_id=HELPER_ID, name="1.0"),
        ("ApplicationVersion", PARENT_VERSION_ID): SimpleNamespace(
            id=PARENT_VERSION_ID, application_id=PARENT_ID, name="2.0"),
        ("Application", HELPER_ID): SimpleNamespace(name="Helper"),
        ("Application", PARENT_ID): SimpleNamespace(name="Parent"),
        ("EntityScheduleRun", RUN_ID): SimpleNamespace(
            id=RUN_ID, conversation_id=None, conversation_uuid=None, trigger="cron"),
    }

    class Query:
        def filter_by(self, **_):
            return self

        def one(self):
            return SimpleNamespace(id=1)

    session = FakeSession(rows, reads)
    session.query = lambda model: Query()
    session.commit = lambda: None

    def create(**kwargs):
        calls["create"] = kwargs
        return "chat-uuid", 3, "response-1"

    def predict(**kwargs):
        calls["predict"] = kwargs

    stubs = {
        "models.all": {"Application": type("Application", (), {}), "ApplicationVersion": type("ApplicationVersion", (), {})},
        "models.conversation": {"Conversation": type("Conversation", (), {})},
        "models.entity_schedule": {"EntitySchedule": type("EntitySchedule", (), {}),
                                   "EntityScheduleRun": type("EntityScheduleRun", (), {})},
        "utils.schedule_prerun": {"run_prerun_check": lambda *a: SimpleNamespace(ok=True, owner_id=OWNER)},
        "utils.schedule_runs": {
            "fail_schedule_run": lambda *a: None, "block_schedule_run": lambda *a: None,
            "record_run_events": lambda *a, **k: None, "run_event": lambda *a, **k: {}},
        "utils.budget_door": {"closed_budget_scope": lambda *a: None},
        "utils.schedule_risk": {
            "scan_schedule": lambda project_id, version_id, owner_id: calls.update(scanned=version_id) or {
                "blocking": [], "sensitive": [], "notices": [], "fingerprint": "fp"},
            "scan_complete": lambda review: True},
        "utils.exceptions": {"BudgetDoorClosedError": type("BudgetDoorClosedError", (Exception,), {})},
        "utils.pipeline_execution": {
            "TriggerType": SimpleNamespace(SCHEDULED="scheduled", SCHEDULE_MANUAL="schedule_manual"),
            "create_trigger_run_conversation": create,
            "execute_pipeline_via_predict_sio": predict,
            "finish_trigger_run_with_error": lambda *a, **k: None},
    }
    for dotted, attrs in stubs.items():
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", SimpleNamespace(**attrs))
    monkeypatch.setattr(sys.modules["tools"].db, "get_session", lambda _project_id: session, raising=False)
    load("utils/schedule_run_starter.py", fresh=True).run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)
    calls["reads"] = reads
    return calls


def test_run_predicts_the_scheduled_version_of_the_target(run):
    version = run["predict"]["version"]
    assert (version.id, version.application_id, version.name) == (HELPER_VERSION_ID, HELPER_ID, "1.0")
    assert run["create"]["version"] is not None and run["create"]["version"].id == HELPER_VERSION_ID
    assert run["scanned"] == HELPER_VERSION_ID


def test_run_passes_no_copied_sub_agents_or_skills_to_the_call(run):
    for call in (run["predict"], run["create"]):
        assert not set(CHILD_KEYS) & set(call)


def test_run_never_reads_the_version_of_an_agent_that_uses_the_target(run):
    assert ("ApplicationVersion", PARENT_VERSION_ID) not in run["reads"]
    assert ("Application", PARENT_ID) not in run["reads"]
