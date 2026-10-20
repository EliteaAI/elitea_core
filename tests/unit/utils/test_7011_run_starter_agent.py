"""EL-7011 US-11 R1.1, R1.2, R1.5, R1.6, R2.2, R2.3, R2.5, R2.6, R3.1: the owner run starter of an agent schedule.

utils/schedule_run_starter.py is loaded from source. Only the leaves are faked: the project session,
the models, the pre-run check, the run conversation helper and the predict helper.
Rows refuse reads once their session closes, the way a detached ORM object can.
"""
import sys
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

STARTER = "utils/schedule_run_starter.py"
PKG = "plugins.elitea_core"
PROJECT_ID, SCHEDULE_ID, RUN_ID, OWNER, AGENT_ID, VERSION_ID = 34, 7, 31, 12, 5, 9
CONVERSATION_ID = 77
PROMPT = "Summarize open bugs"


class Row(SimpleNamespace):
    """A model row; a read after its session closes raises, like a detached instance."""

    def __getattribute__(self, name):
        world = object.__getattribute__(self, "__dict__").get("_world")
        if world is not None and not name.startswith("_") and not world.session_open:
            raise RuntimeError(f"detached read of {name}")
        return object.__getattribute__(self, name)


class FakeSession:
    def __init__(self, world):
        self.world = world

    def __enter__(self):
        self.world.session_open = True
        return self

    def __exit__(self, *exc):
        self.world.session_open = False
        return False

    def get(self, model, row_id):
        return self.world.rows.get((model.__name__, row_id))

    def query(self, _model):
        if self.world.link_error:
            raise self.world.link_error
        return SimpleNamespace(filter_by=lambda **_: SimpleNamespace(
            one=lambda: SimpleNamespace(id=CONVERSATION_ID)))

    def commit(self):
        self.world.calls.append(("commit",))


class World:
    def __init__(self):
        self.calls = []
        self.session_open = False
        self.prerun_ok = True
        self.create_error = self.link_error = self.predict_error = None
        self.schedule = Row(
            _world=self, id=SCHEDULE_ID, name="S1", principal={}, application_id=AGENT_ID, version_id=VERSION_ID,
            created_by=OWNER, prompt=PROMPT, variables=[], entity_type="agent", run_mode=None)
        self.run = SimpleNamespace(id=RUN_ID, conversation_id=None)
        self.rows = {
            ("EntitySchedule", SCHEDULE_ID): self.schedule,
            ("ApplicationVersion", VERSION_ID): Row(_world=self, id=VERSION_ID, application_id=AGENT_ID),
            ("Application", AGENT_ID): Row(_world=self, name="Daily report"),
            ("EntityScheduleRun", RUN_ID): self.run,
        }

    def names(self):
        return [call[0] for call in self.calls]

    def call(self, name):
        return next(call for call in self.calls if call[0] == name)


@pytest.fixture
def world(monkeypatch):
    state = World()

    def prerun(project_id, schedule, run_id):
        state.calls.append(("prerun", project_id, schedule.id, schedule.created_by, run_id))
        return SimpleNamespace(ok=state.prerun_ok, owner_id=OWNER if state.prerun_ok else None)

    def create(**kwargs):
        state.calls.append(("create", kwargs, kwargs["version"].id, kwargs["version"].application_id))
        if state.create_error:
            raise state.create_error
        return "uuid-1", 3, "response-1"

    def predict(**kwargs):
        state.calls.append(("predict", kwargs, kwargs["version"].id))
        if state.predict_error:
            raise state.predict_error
        return {"task_id": "task-1"}

    stubs = {
        "models.all": {"Application": type("Application", (), {}), "ApplicationVersion": type("ApplicationVersion", (), {})},
        "models.conversation": {"Conversation": type("Conversation", (), {})},
        "models.entity_schedule": {"EntitySchedule": type("EntitySchedule", (), {}),
                                   "EntityScheduleRun": type("EntityScheduleRun", (), {})},
        "utils.schedule_prerun": {"run_prerun_check": prerun},
        "utils.schedule_runs": {"fail_schedule_run": lambda *args: state.calls.append(("fail", *args))},
        "utils.pipeline_execution": {
            "TriggerType": SimpleNamespace(SCHEDULED="scheduled"),
            "create_trigger_run_conversation": create,
            "execute_pipeline_via_predict_sio": predict,
            "finish_trigger_run_with_error": lambda *args: state.calls.append(("finish_error", *args)),
        },
    }
    for dotted, attrs in stubs.items():
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", SimpleNamespace(**attrs))
    monkeypatch.setattr(sys.modules["tools"].db, "get_session", lambda _project_id: FakeSession(state), raising=False)
    state.module = load(STARTER, fresh=True)
    return state


def _run(world):
    world.module.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)


def test_7011_r1_1_prerun_runs_before_any_other_step(world):
    _run(world)
    assert world.names()[:2] == ["prerun", "create"]
    assert world.call("prerun") == ("prerun", PROJECT_ID, SCHEDULE_ID, OWNER, RUN_ID)


def test_7011_r1_2_failed_prerun_creates_no_conversation_and_calls_no_predict(world):
    world.prerun_ok = False
    _run(world)
    assert world.names() == ["prerun"]


def test_7011_r1_5_r2_2_conversation_is_named_for_the_agent_and_holds_the_prompt(world):
    _run(world)
    created = world.call("create")[1]
    assert created["conversation_name"] == "Scheduled run: Daily report"
    assert created["trigger_message"] == PROMPT
    assert (created["source"], created["user_id"], created["trigger_type"]) == ("agent", OWNER, "scheduled")
    assert created["extra_meta"] == {
        "schedule_id": SCHEDULE_ID, "schedule_run_id": RUN_ID, "execution_type": "scheduled"}
    assert world.call("predict")[1]["user_input"] == PROMPT


@pytest.mark.parametrize("missing", ["EntitySchedule", "ApplicationVersion"])
def test_7011_r1_5_missing_schedule_or_version_fails_the_run(world, missing):
    world.rows.pop((missing, SCHEDULE_ID if missing == "EntitySchedule" else VERSION_ID))
    _run(world)
    assert world.names() == ["fail"]
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "schedule or version not found")


def test_7011_r1_6_run_row_gets_the_conversation_id_before_predict(world):
    _run(world)
    assert world.run.conversation_id == CONVERSATION_ID
    assert world.names() == ["prerun", "create", "commit", "predict"]


def test_7011_r2_3_variables_fill_the_prompt_once_with_spaces_and_repeats(world):
    world.schedule.prompt = "Bugs in {{ region }} and {{region}} for {{team}}"
    world.schedule.variables = [{"name": "region", "value": "US"}, {"name": "team", "value": "core"}]
    _run(world)
    expected = "Bugs in US and US for core"
    assert world.call("create")[1]["trigger_message"] == expected
    assert world.call("predict")[1]["user_input"] == expected


def test_7011_r2_6_predict_gets_the_owner_and_no_schedule_variables(world):
    world.schedule.variables = [{"name": "region", "value": "US"}]
    _run(world)
    predicted = world.call("predict")[1]
    assert predicted["is_system_user"] is False
    assert predicted["user_id"] == OWNER
    assert predicted["conversation_uuid"] == "uuid-1"
    assert predicted["response_message_id"] == "response-1"
    assert "variables" not in predicted


def test_7011_r3_1_cron_run_sends_trigger_source_scheduled(world):
    _run(world)
    assert world.call("predict")[1]["trigger_source"] == "scheduled"


def test_7011_r2_5_predict_error_ends_the_run_message_with_the_error(world):
    error = RuntimeError("rpc down")
    world.predict_error = error
    _run(world)
    assert world.call("finish_error") == ("finish_error", PROJECT_ID, "response-1", error)


def test_7011_r2_5_predict_error_closes_the_run_as_failed(world):
    world.predict_error = RuntimeError("rpc down")
    _run(world)
    assert world.names()[-2:] == ["finish_error", "fail"]
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "the agent call failed")


@pytest.mark.parametrize("step", ["create_error", "link_error"])
def test_7011_r1_6_conversation_or_link_error_closes_the_run_and_calls_no_predict(world, step):
    setattr(world, step, RuntimeError("db down"))
    _run(world)
    assert "predict" not in world.names()
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "the run could not start")


def test_7011_r1_6_missing_run_row_closes_the_run_and_calls_no_predict(world):
    world.rows.pop(("EntityScheduleRun", RUN_ID))
    _run(world)
    assert world.names() == ["prerun", "create", "fail"]
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "run not found")


def test_7011_r1_5_agent_is_loaded_by_the_version_application(world):
    world.schedule.application_id = 99
    _run(world)
    assert world.call("create")[1]["conversation_name"] == "Scheduled run: Daily report"
    assert world.call("create")[2:] == (VERSION_ID, AGENT_ID)
    assert world.call("predict")[2] == VERSION_ID


@pytest.mark.parametrize(("prompt", "expected"), [
    ("Report for {{team}}", "Report for {{region}}"),
    ("Hi {{ who }} and {{team}}", "Hi {{ who }} and {{region}}"),
    ("{% if true %}yes{% endif %} {{ ''.__class__ }}", "{% if true %}yes{% endif %} {{ ''.__class__ }}"),
])
def test_7011_r2_3_fill_is_one_pass_keeps_unknown_names_and_runs_no_template_logic(world, prompt, expected):
    world.schedule.prompt = prompt
    world.schedule.variables = [{"name": "team", "value": "{{region}}"}, {"name": "region", "value": "US"}]
    _run(world)
    assert world.call("create")[1]["trigger_message"] == expected
