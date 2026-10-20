"""EL-7011 US-11 R1.3, R1.4, R2.1, R2.4: the run conversation and the predict call of an owner run.

Contract under test: utils/pipeline_execution.py, loaded from source. Only the leaves (DB session,
models, participant lookup, predict RPC) are faked.
"""
import importlib.util
import pathlib
import sys
import types
from typing import ClassVar

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
MODULE = "plugins.elitea_core.utils.pipeline_execution"
OWNER_ID = 7
AGENT_ID = 15
SCHEDULE_META = {"schedule_id": 3, "schedule_run_id": 9, "execution_type": "scheduled"}


def _module(name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    return module


class Record:
    """A model row that keeps its constructor fields."""

    def __init__(self, **fields):
        vars(self).update(fields)
        self.id, self.uuid, self.participants = 1, "uuid-1", []


class TextItem(Record):
    __mapper_args__: ClassVar[dict] = {"polymorphic_identity": "text"}


class FakeSession:
    def __init__(self, world):
        self.world = world

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def query(self, _model):
        return types.SimpleNamespace(
            get=lambda _id: types.SimpleNamespace(name="Daily report"),
            filter=lambda *_: types.SimpleNamespace(update=lambda _values: None),
        )

    def add(self, row):
        self.world.added.append(row)

    def flush(self):
        """Nothing to flush: rows keep their ids from the constructor."""

    def commit(self):
        """Nothing to commit: the test reads the added rows."""


@pytest.fixture
def world(monkeypatch):
    state = types.SimpleNamespace(added=[], predict_calls=[])
    for name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.models",
                 "plugins.elitea_core.models.pd", "plugins.elitea_core.models.enums",
                 "plugins.elitea_core.models.message_items", "plugins.elitea_core.utils"):
        monkeypatch.setitem(sys.modules, name, _module(name, __path__=[]))
    log = types.SimpleNamespace(**{n: (lambda *a, **k: None) for n in ("debug", "info", "exception")})
    monkeypatch.setitem(sys.modules, "pylon", _module("pylon"))
    monkeypatch.setitem(sys.modules, "pylon.core", _module("pylon.core"))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", _module("pylon.core.tools", log=log))

    def predict_sio(**kwargs):
        state.predict_calls.append(kwargs)
        return {"task_id": "task-1"}

    rpc = types.SimpleNamespace(applications_predict_sio=predict_sio)
    monkeypatch.setitem(sys.modules, "tools", _module(
        "tools",
        db=types.SimpleNamespace(get_session=lambda _project_id: FakeSession(state)),
        rpc_tools=types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(timeout=lambda _seconds: rpc))),
        this=types.SimpleNamespace(),
    ))

    def get_or_create_one(session, entity_name, entity_meta):
        return types.SimpleNamespace(id=entity_meta.id, entity_name=entity_name), True

    entity = lambda **fields: types.SimpleNamespace(**fields)
    leaves = {
        "models.all": {"Application": object, "ApplicationVersion": object},
        "models.conversation": {"Conversation": Record},
        "models.enums.all": {"ParticipantTypes": types.SimpleNamespace(
            user="user", application=types.SimpleNamespace(value="application"))},
        "models.message_group": {"ConversationMessageGroup": Record},
        "models.message_items.text": {"TextMessageItem": TextItem},
        "models.participants": {"ParticipantMapping": types.SimpleNamespace(conversation_id=0, participant_id=0)},
        "models.pd.participant": {"ParticipantEntityApplication": entity, "ParticipantEntityUser": entity},
        "utils.participant_utils": {"get_or_create_one": get_or_create_one},
        "utils.sio_utils": {"SioEvents": types.SimpleNamespace(
            chat_predict=types.SimpleNamespace(value="chat_predict"))},
    }
    for dotted, attrs in leaves.items():
        name = "plugins.elitea_core." + dotted
        monkeypatch.setitem(sys.modules, name, _module(name, **attrs))

    spec = importlib.util.spec_from_file_location(MODULE, PLUGIN_ROOT / "utils" / "pipeline_execution.py")
    state.execution = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, MODULE, state.execution)
    spec.loader.exec_module(state.execution)
    state.version = types.SimpleNamespace(id=27, application_id=AGENT_ID)
    return state


def _create(world, trigger_type, **kwargs):
    world.execution.create_trigger_run_conversation(
        project_id=1, version=world.version, user_id=OWNER_ID, trigger_type=trigger_type,
        trigger_message="Report on sales", conversation_name="Scheduled run: Daily report", **kwargs,
    )
    return next(row for row in world.added if hasattr(row, "is_private"))


def _predict(world, **kwargs):
    world.execution.execute_pipeline_via_predict_sio(
        project_id=1, version=world.version, user_id=OWNER_ID,
        conversation_uuid="uuid-1", response_message_id="response-1", **kwargs,
    )
    return world.predict_calls[-1]


def test_7011_r1_3_agent_run_conversation_is_private_with_owner_and_agent(world):
    conversation = _create(world, world.execution.TriggerType.SCHEDULED, source="agent")
    assert conversation.source == "agent"
    assert conversation.is_private is True
    assert conversation.author_id == OWNER_ID
    types_ = world.execution.ParticipantTypes
    expected = {OWNER_ID: types_.user, AGENT_ID: types_.application}
    assert {p.id: p.entity_name for p in conversation.participants} == expected


def test_7011_r1_3_source_defaults_to_pipeline(world):
    conversation = _create(world, world.execution.TriggerType.SCHEDULED)
    assert conversation.source == "pipeline"


def test_7011_r1_4_scheduled_run_meta_has_schedule_fields(world):
    conversation = _create(world, world.execution.TriggerType.SCHEDULED, source="agent",
                           extra_meta=SCHEDULE_META)
    assert conversation.meta["scheduled_run"] is True
    assert {k: conversation.meta[k] for k in SCHEDULE_META} == SCHEDULE_META


def test_7011_r2_1_owner_run_predicts_as_the_owner(world):
    call = _predict(world, is_system_user=False, user_input="Report on sales", trigger_source="scheduled")
    assert call["is_system_user"] is False
    assert call["user_id"] == OWNER_ID
    assert call["data"]["user_input"] == "Report on sales"
    assert "variables" not in call["data"]


def test_7011_r2_4_caller_without_is_system_user_keeps_the_system_user(world):
    call = _predict(world)
    assert call["is_system_user"] is True
