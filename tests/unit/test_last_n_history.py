"""The delivery branch of the owner run starter sends the last N chat messages to the predict call.

utils/schedule_run_starter.py is loaded from source. Only the leaves are faked: the project session,
the models, the chat history builder, the pre-run check, the run conversation helper and the predict helper.
"""
import sys
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

STARTER = "utils/schedule_run_starter.py"
PKG = "plugins.elitea_core"
PROJECT_ID, SCHEDULE_ID, RUN_ID, OWNER, AGENT_ID, VERSION_ID = 34, 7, 31, 12, 5, 9
TARGET_CHAT_ID, TARGET_CHAT_UUID = 55, "3f1c9a0e-5b7d-4c2a-8e6f-0b1d3c5a7e9f"
PARTICIPANT_ID, GROUP_ID, GROUP_UUID = 8, 901, "group-uuid-1"


class FakeQuery:
    """Answers the chat lookups and the newest-first message group query of the chat."""

    def __init__(self, world, model):
        self.world, self.model, self.max_rows = world, model.__name__, None

    def filter_by(self, **filters):
        return self

    def filter(self, *args):
        return self

    def order_by(self, *args):
        return self

    def limit(self, count):
        self.max_rows = count
        return self

    def all(self):
        self.world.calls.append(("read_groups", self.max_rows))
        return list(reversed(self.world.groups))[: self.max_rows]

    def one(self):
        return SimpleNamespace(id=TARGET_CHAT_ID if self.model == "Conversation" else GROUP_ID)

    def one_or_none(self):
        if self.model == "Conversation" and not self.world.chat_exists:
            return None
        return self.one()

    def first(self):
        return SimpleNamespace(id=1)


class FakeSession:
    def __init__(self, world):
        self.world = world

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, model, row_id):
        return self.world.rows.get((model.__name__, row_id))

    def query(self, model):
        return FakeQuery(self.world, model)

    def commit(self):
        pass


class World:
    def __init__(self, include_history):
        self.calls = []
        self.groups = []
        self.chat_exists = True
        self.owner_in_chat = True
        schedule = SimpleNamespace(
            id=SCHEDULE_ID, name="S1", principal={}, application_id=AGENT_ID, version_id=VERSION_ID,
            created_by=OWNER, prompt="Daily summary", variables=[], entity_type="agent", run_mode=None,
            approval_rule="pause_and_ask", approval_fingerprint=None, approval_confirmed_by=None,
            delivery_mode="conversation", delivery_conversation_uuid=TARGET_CHAT_UUID,
            delivery_participant_id=PARTICIPANT_ID)
        if include_history is not None:
            schedule.delivery_include_history = include_history
        self.rows = {
            ("EntitySchedule", SCHEDULE_ID): schedule,
            ("ApplicationVersion", VERSION_ID): SimpleNamespace(id=VERSION_ID, application_id=AGENT_ID, name="v3"),
            ("Application", AGENT_ID): SimpleNamespace(name="Daily report"),
            ("Participant", PARTICIPANT_ID): SimpleNamespace(
                entity_name="application", entity_meta={"id": AGENT_ID, "project_id": PROJECT_ID}),
            ("EntityScheduleRun", RUN_ID): SimpleNamespace(
                id=RUN_ID, conversation_id=None, conversation_uuid=None, message_group_id=None, trigger="cron"),
        }

    def predicted(self):
        return next(call[1] for call in self.calls if call[0] == "predict")


def _column():
    return SimpleNamespace(desc=lambda: None)


@pytest.fixture
def make_world(monkeypatch):
    def build(include_history):
        state = World(include_history)

        def create(**kwargs):
            state.calls.append(("create",))
            return TARGET_CHAT_UUID, PARTICIPANT_ID, GROUP_UUID

        def predict(**kwargs):
            state.calls.append(("predict", kwargs))

        def history(groups):
            state.calls.append(("build_history", list(groups)))
            return [{"role": "user", "content": [{"type": "text", "text": g}]} for g in groups]

        group_model = type("ConversationMessageGroup", (), {
            "conversation_id": _column(), "created_at": _column(), "id": _column()})
        stubs = {
            "models.all": {"Application": type("Application", (), {}),
                           "ApplicationVersion": type("ApplicationVersion", (), {})},
            "models.conversation": {"Conversation": type("Conversation", (), {})},
            "models.message_group": {"ConversationMessageGroup": group_model},
            "models.participants": {"Participant": type("Participant", (), {}),
                                    "ParticipantMapping": type("ParticipantMapping", (), {})},
            "models.enums.all": {"ParticipantTypes": SimpleNamespace(application=SimpleNamespace(value="application"))},
            "models.entity_schedule": {"EntitySchedule": type("EntitySchedule", (), {}),
                                       "EntityScheduleRun": type("EntityScheduleRun", (), {})},
            "utils.chat_history": {"generate_chat_history": history},
            "utils.conversation_access": {
                "is_conversation_participant": lambda session, chat_id, user_id: state.owner_in_chat},
            "utils.schedule_prerun": {"run_prerun_check": lambda *args: SimpleNamespace(ok=True, owner_id=OWNER)},
            "utils.schedule_runs": {
                "fail_schedule_run": lambda *args: None, "block_schedule_run": lambda *args: None,
                "record_run_events": lambda *args, **kwargs: None, "run_event": lambda *args, **kwargs: {}},
            "utils.budget_door": {"closed_budget_scope": lambda *args: None},
            "utils.schedule_risk": {
                "scan_schedule": lambda *args: {"blocking": [], "sensitive": [], "notices": [], "fingerprint": "fp"},
                "scan_complete": lambda review: True},
            "utils.exceptions": {"BudgetDoorClosedError": type("BudgetDoorClosedError", (Exception,), {})},
            "utils.pipeline_execution": {
                "TriggerType": SimpleNamespace(SCHEDULED="scheduled", SCHEDULE_MANUAL="schedule_manual"),
                "create_trigger_run_conversation": create,
                "execute_pipeline_via_predict_sio": predict,
                "finish_trigger_run_with_error": lambda *args, **kwargs: None},
        }
        for dotted, attrs in stubs.items():
            monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", SimpleNamespace(**attrs))
        monkeypatch.setattr(sys.modules["tools"].db, "get_session", lambda _pid: FakeSession(state), raising=False)
        state.module = load(STARTER, fresh=True)
        return state

    return build


def _run(world):
    world.module.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)


@pytest.mark.parametrize("value", [0, None])
def test_no_history_is_read_or_sent_for_zero_or_missing_count(make_world, value):
    world = make_world(value)
    world.groups = ["m1", "m2"]
    _run(world)
    assert [call[0] for call in world.calls] == ["create", "predict"]
    assert world.predicted()["chat_history"] == []


def test_the_last_n_messages_are_sent_oldest_first(make_world):
    world = make_world(2)
    world.groups = ["m1", "m2", "m3", "m4"]
    _run(world)
    assert [item["content"][0]["text"] for item in world.predicted()["chat_history"]] == ["m3", "m4"]
    assert ("read_groups", 2) in world.calls


def test_all_messages_are_sent_when_the_chat_holds_fewer_than_n(make_world):
    world = make_world(5)
    world.groups = ["m1", "m2"]
    _run(world)
    assert [item["content"][0]["text"] for item in world.predicted()["chat_history"]] == ["m1", "m2"]


def test_history_is_read_before_the_trigger_message_is_written(make_world):
    world = make_world(3)
    world.groups = ["m1"]
    _run(world)
    names = [call[0] for call in world.calls]
    assert names.index("read_groups") < names.index("create")


def _chat_is_gone(world):
    world.chat_exists = False


def _owner_left_the_chat(world):
    world.owner_in_chat = False


def _participant_is_another_agent(world):
    world.rows[("Participant", PARTICIPANT_ID)].entity_meta = {"id": AGENT_ID + 1, "project_id": PROJECT_ID}


@pytest.mark.parametrize("break_target", [_chat_is_gone, _owner_left_the_chat, _participant_is_another_agent])
def test_no_history_is_read_when_the_delivery_target_does_not_fit(make_world, break_target):
    world = make_world(3)
    world.groups = ["m1", "m2"]
    break_target(world)
    _run(world)
    assert "read_groups" not in [call[0] for call in world.calls]
    assert world.predicted()["chat_history"] == []
