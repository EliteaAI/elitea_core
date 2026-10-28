"""Unit tests for the delivery branch of the owner run starter.

utils/schedule_run_starter.py is loaded from source. Only the leaves are faked: the project session,
the models, the pre-run check, the run conversation helper and the predict helper.
A schedule that delivers to a chat writes its groups into that chat and links the run row to the answer group.
A run_history schedule keeps the run chat of the older path.
"""
import sys
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

STARTER = "utils/schedule_run_starter.py"
PKG = "plugins.elitea_core"
PROJECT_ID, SCHEDULE_ID, RUN_ID, OWNER, AGENT_ID, VERSION_ID = 34, 7, 31, 12, 5, 9
RUN_CHAT_ID, RUN_CHAT_UUID = 77, "uuid-1"
TARGET_CHAT_ID, TARGET_CHAT_UUID = 55, "3f1c9a0e-5b7d-4c2a-8e6f-0b1d3c5a7e9f"
PARTICIPANT_ID, GROUP_ID, GROUP_UUID = 8, 901, "group-uuid-1"
OTHER_AGENT_ID, OTHER_OWNER = 6, 99
PROMPT = "Daily summary"


class FakeQuery:
    """`session.query(Model).filter_by(...)` answered from the world's plain tables."""

    def __init__(self, world, model):
        self.world, self.model, self.found = world, model.__name__, None

    def filter_by(self, **filters):
        world = self.world
        if world.link_error and "create" in world.names():
            raise world.link_error
        if self.model == "Conversation":
            row_id = world.chats.get(str(filters["uuid"]))
            self.found = row_id and SimpleNamespace(id=row_id)
        elif self.model == "ConversationMessageGroup":
            self.found = SimpleNamespace(id=GROUP_ID)
        elif self.model == "ParticipantMapping":
            self.found = SimpleNamespace(id=1) if world.participant_in_chat else None
        return self

    def one(self):
        return self.found

    def one_or_none(self):
        return self.found

    def first(self):
        return self.found


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
        self.world.calls.append(("commit",))


class World:
    def __init__(self):
        self.calls = []
        self.prerun_ok = True
        self.budget_scope = None
        self.create_error = self.link_error = self.predict_error = None
        self.participant_in_chat = True
        self.owner_id = OWNER
        self.chat_members = {OWNER}
        self.chats = {RUN_CHAT_UUID: RUN_CHAT_ID, TARGET_CHAT_UUID: TARGET_CHAT_ID}
        self.schedule = SimpleNamespace(
            id=SCHEDULE_ID, name="S1", principal={}, application_id=AGENT_ID, version_id=VERSION_ID,
            created_by=OWNER, prompt=PROMPT, variables=[], entity_type="agent", run_mode=None,
            approval_rule="pause_and_ask", approval_fingerprint=None, approval_confirmed_by=None,
            delivery_mode="conversation", delivery_conversation_uuid=TARGET_CHAT_UUID,
            delivery_participant_id=PARTICIPANT_ID)
        self.run = SimpleNamespace(
            id=RUN_ID, conversation_id=None, conversation_uuid=None, message_group_id=None, trigger="cron")
        self.rows = {
            ("EntitySchedule", SCHEDULE_ID): self.schedule,
            ("ApplicationVersion", VERSION_ID): SimpleNamespace(id=VERSION_ID, application_id=AGENT_ID, name="v3"),
            ("Application", AGENT_ID): SimpleNamespace(name="Daily report"),
            ("Participant", PARTICIPANT_ID): SimpleNamespace(
                entity_name="application", entity_meta={"id": AGENT_ID, "project_id": PROJECT_ID}),
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
        state.calls.append(("prerun", run_id))
        return SimpleNamespace(ok=state.prerun_ok, owner_id=state.owner_id if state.prerun_ok else None)

    def create(**kwargs):
        state.calls.append(("create", kwargs))
        if state.create_error:
            raise state.create_error
        if kwargs.get("target_conversation") is not None:
            return TARGET_CHAT_UUID, PARTICIPANT_ID, GROUP_UUID
        return RUN_CHAT_UUID, 3, "response-1"

    def predict(**kwargs):
        state.calls.append(("predict", kwargs))
        if state.predict_error:
            raise state.predict_error
        return {"task_id": "task-1"}

    stubs = {
        "models.all": {"Application": type("Application", (), {}), "ApplicationVersion": type("ApplicationVersion", (), {})},
        "models.conversation": {"Conversation": type("Conversation", (), {})},
        "models.message_group": {"ConversationMessageGroup": type("ConversationMessageGroup", (), {})},
        "models.participants": {"Participant": type("Participant", (), {}),
                                "ParticipantMapping": type("ParticipantMapping", (), {})},
        "models.enums.all": {"ParticipantTypes": SimpleNamespace(application=SimpleNamespace(value="application"))},
        "models.entity_schedule": {"EntitySchedule": type("EntitySchedule", (), {}),
                                   "EntityScheduleRun": type("EntityScheduleRun", (), {})},
        "utils.conversation_access": {"is_conversation_participant": lambda session, chat_id, user_id: user_id in state.chat_members},
        "utils.schedule_prerun": {"run_prerun_check": prerun},
        "utils.schedule_runs": {
            "fail_schedule_run": lambda *args: state.calls.append(("fail", *args)),
            "block_schedule_run": lambda *args: state.calls.append(("block", *args)),
            "record_run_events": lambda *args, **kwargs: None,
            "run_event": lambda *args, **kwargs: {}},
        "utils.budget_door": {"closed_budget_scope": lambda *args: state.budget_scope},
        "utils.schedule_risk": {
            "scan_schedule": lambda *args: {"blocking": [], "sensitive": [], "notices": [], "fingerprint": "fp"},
            "scan_complete": lambda review: True},
        "utils.exceptions": {"BudgetDoorClosedError": type("BudgetDoorClosedError", (Exception,), {})},
        "utils.pipeline_execution": {
            "TriggerType": SimpleNamespace(SCHEDULED="scheduled", SCHEDULE_MANUAL="schedule_manual"),
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


def test_delivery_run_writes_the_groups_into_the_target_chat(world):
    _run(world)
    created = world.call("create")[1]
    assert created["target_conversation"] == TARGET_CHAT_ID
    assert created["delivery_participant_id"] == PARTICIPANT_ID
    assert (created["user_id"], created["trigger_message"]) == (OWNER, PROMPT)
    assert created["extra_meta"]["schedule_id"] == SCHEDULE_ID
    assert created["extra_meta"]["schedule_run_id"] == RUN_ID


def test_delivery_run_creates_no_other_run_chat(world):
    _run(world)
    assert world.names().count("create") == 1


def test_delivery_run_row_holds_the_chat_and_the_answer_group(world):
    _run(world)
    assert world.run.conversation_id == TARGET_CHAT_ID
    assert world.run.conversation_uuid == TARGET_CHAT_UUID
    assert world.run.message_group_id == GROUP_ID


def test_delivery_run_links_the_row_before_it_calls_predict(world):
    _run(world)
    assert world.names() == ["prerun", "create", "commit", "predict"]


def test_delivery_run_predicts_as_the_owner_and_the_participant_in_the_target_chat(world):
    _run(world)
    predicted = world.call("predict")[1]
    assert (predicted["user_id"], predicted["is_system_user"]) == (OWNER, False)
    assert predicted["trigger_source"] == "scheduled"
    assert predicted["participant_id"] == PARTICIPANT_ID
    assert predicted["conversation_uuid"] == TARGET_CHAT_UUID
    assert predicted["response_message_id"] == GROUP_UUID


def test_run_now_delivers_into_the_target_chat(world):
    world.run.trigger = "manual"
    _run(world)
    assert world.call("create")[1]["target_conversation"] == TARGET_CHAT_ID
    assert world.call("predict")[1]["trigger_source"] == "schedule_manual"
    assert world.run.message_group_id == GROUP_ID


def test_closed_budget_blocks_a_delivery_run_before_any_chat_write(world):
    world.budget_scope = "daily"
    _run(world)
    assert world.names() == ["prerun", "block"]
    assert world.run.message_group_id is None


def test_failed_prerun_writes_nothing_into_the_chat(world):
    world.prerun_ok = False
    _run(world)
    assert world.names() == ["prerun"]


def test_failed_predict_posts_a_fixed_message_and_closes_the_run(world):
    world.predict_error = RuntimeError("rpc down at 10.0.0.5")
    _run(world)
    assert world.names().count("create") == 1
    assert world.call("finish_error") == ("finish_error", PROJECT_ID, GROUP_UUID, "the run call failed")
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "the run call failed")
    assert (world.run.conversation_id, world.run.conversation_uuid) == (TARGET_CHAT_ID, TARGET_CHAT_UUID)


def test_closed_budget_door_after_the_write_posts_a_fixed_message(world):
    error = world.module.BudgetDoorClosedError("door closed for user 12")
    error.scope = "daily"
    world.predict_error = error
    _run(world)
    assert world.call("finish_error") == ("finish_error", PROJECT_ID, GROUP_UUID, "the daily budget is used up")
    assert world.call("block") == ("block", PROJECT_ID, RUN_ID, "the daily budget is used up")


def test_run_history_failed_predict_keeps_the_error_text_in_its_private_chat(world):
    world.schedule.delivery_mode = "run_history"
    error = RuntimeError("rpc down")
    world.predict_error = error
    _run(world)
    assert world.call("finish_error") == ("finish_error", PROJECT_ID, "response-1", error)


def test_failed_chat_write_leaves_no_group_to_end(world):
    world.create_error = RuntimeError("db down")
    _run(world)
    assert "finish_error" not in world.names()
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "the run could not start")


def test_failed_link_ends_the_streaming_group_before_the_run_closes(world):
    world.link_error = RuntimeError("db down")
    _run(world)
    assert "predict" not in world.names()
    ended = world.names().index("finish_error")
    assert world.names().index("fail") == ended + 1
    assert world.calls[ended] == ("finish_error", PROJECT_ID, GROUP_UUID, "the run could not start")
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "the run could not start")


def test_missing_run_row_ends_the_streaming_group_before_the_run_closes(world):
    world.rows.pop(("EntityScheduleRun", RUN_ID))
    _run(world)
    assert "predict" not in world.names()
    assert world.calls[world.names().index("finish_error")] == (
        "finish_error", PROJECT_ID, GROUP_UUID, "run not found")
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "run not found")


def test_run_history_failed_link_ends_no_group(world):
    world.schedule.delivery_mode = "run_history"
    world.link_error = RuntimeError("db down")
    _run(world)
    assert "finish_error" not in world.names()
    assert world.call("fail") == ("fail", PROJECT_ID, RUN_ID, "the run could not start")


@pytest.mark.parametrize("mode", ["run_history", None])
def test_run_history_schedule_keeps_its_own_run_chat(world, mode):
    world.schedule.delivery_mode = mode
    _run(world)
    created = world.call("create")[1]
    assert "target_conversation" not in created
    assert created["conversation_name"] == "Scheduled run: Daily report"
    assert "participant_id" not in world.call("predict")[1]
    assert world.run.conversation_id == RUN_CHAT_ID
    assert (world.run.conversation_uuid, world.run.message_group_id) == (None, None)


@pytest.mark.parametrize("lost", ["chat", "participant", "owner", "foreign_agent", "user_participant", "no_participant"])
def test_lost_delivery_target_runs_the_run_history_path(world, lost):
    participant = world.rows[("Participant", PARTICIPANT_ID)]
    if lost == "chat":
        world.chats.pop(TARGET_CHAT_UUID)
    elif lost == "participant":
        world.participant_in_chat = False
    elif lost == "owner":
        world.chat_members = set()
    elif lost == "foreign_agent":
        participant.entity_meta = {"id": OTHER_AGENT_ID, "project_id": PROJECT_ID}
    elif lost == "user_participant":
        participant.entity_name, participant.entity_meta = "user", {"id": AGENT_ID}
    else:
        world.rows.pop(("Participant", PARTICIPANT_ID))
    _run(world)
    assert "target_conversation" not in world.call("create")[1]
    assert "participant_id" not in world.call("predict")[1]
    assert world.run.conversation_id == RUN_CHAT_ID
    assert world.run.message_group_id is None


def test_participant_id_stored_as_text_still_matches_the_agent(world):
    world.rows[("Participant", PARTICIPANT_ID)].entity_meta = {"id": str(AGENT_ID), "project_id": PROJECT_ID}
    _run(world)
    assert world.call("create")[1]["target_conversation"] == TARGET_CHAT_ID


def test_new_owner_outside_the_chat_gets_no_delivery_rights(world):
    world.schedule.created_by = world.owner_id = OTHER_OWNER
    _run(world)
    created = world.call("create")[1]
    assert "target_conversation" not in created
    assert created["user_id"] == OTHER_OWNER
    assert world.run.conversation_id == RUN_CHAT_ID


def test_new_owner_inside_the_chat_delivers_as_that_owner(world):
    world.schedule.created_by = world.owner_id = OTHER_OWNER
    world.chat_members = {OTHER_OWNER}
    _run(world)
    created = world.call("create")[1]
    assert (created["target_conversation"], created["user_id"]) == (TARGET_CHAT_ID, OTHER_OWNER)
