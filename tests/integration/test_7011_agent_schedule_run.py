"""EL-7011 US-11 R1.3, R1.4, R1.5, R2.1, R2.3, R2.6, R3.1, R3.2: an agent schedule runs as its owner, end to end.

Real: `run_owner_schedule`, `run_prerun_check`, the run writers, `create_trigger_run_conversation`,
`execute_pipeline_via_predict_sio` and the schedule models, over SQLite tables.
Faked: the predict RPC, the auth RPC, the credential step and the notice helper; the chat tables are
small SQLite stand-ins. The prompt fill is the jinja2 one of `_resolve_variables` when jinja2 imports
(the integration environment may not have it); otherwise a one-pass regex fill with the same result.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_agent_schedule_run.py
"""
import re
import sys
import types
import uuid
from contextlib import contextmanager

import pytest
from integration.test_7011_entity_schedule_tables import REAL_SQLALCHEMY
from integration.test_7011_prerun_isolation import FakeAuth
from unit._tool_spend_loader import load

PKG = "plugins.elitea_core"
PROJECT_ID, OWNER, AGENT_ID, OTHER_AGENT_ID, VERSION_ID = 34, 12, 5, 6, 9
PROMPT = "Summarize open bugs"
VERSION_VARIABLES = [{"name": "region", "value": "EU"}]

_sa, _orm = REAL_SQLALCHEMY["sqlalchemy"], REAL_SQLALCHEMY["sqlalchemy.orm"]


def _fill(text, variables):
    """`_resolve_variables` of models/pd/chat.py: jinja2 with DebugUndefined, else the same one-pass fill."""
    values = {item.name: item.value for item in variables}
    try:
        from jinja2 import DebugUndefined, Environment
        environment = Environment(undefined=DebugUndefined)  # other suites leave a jinja2 stub that cannot build one
    except (ImportError, TypeError):
        return re.sub(r"\{\{\s*(\w+)\s*\}\}", lambda m: values.get(m.group(1), m.group(0)), text)
    return environment.from_string(text).render(**values)


def _chat_models(base):
    """Stand-ins for the chat tables `create_trigger_run_conversation` writes; same columns it reads."""
    def table(name):
        return {"__tablename__": name, "__table_args__": {"schema": "tenant"}}

    def column(*args, **kwargs):
        return _orm.mapped_column(*args, **kwargs)

    def foreign(target):
        return _sa.ForeignKey(f"tenant.{target}.id")

    class Application(base):
        locals().update(table("applications"))
        id = column(_sa.Integer, primary_key=True)
        name = column(_sa.String)

    class ApplicationVersion(base):
        locals().update(table("application_versions"))
        id = column(_sa.Integer, primary_key=True)
        application_id = column(_sa.Integer)
        variables = column(_sa.JSON, default=list)

    class Participant(base):
        locals().update(table("chat_participants"))
        id = column(_sa.Integer, primary_key=True)
        entity_name = column(_sa.String)
        entity_id = column(_sa.Integer)

    class ParticipantMapping(base):
        locals().update(table("chat_participant_mapping"))
        conversation_id = column(_sa.Integer, foreign("chat_conversations"), primary_key=True)
        participant_id = column(_sa.Integer, foreign("chat_participants"), primary_key=True)
        entity_settings = column(_sa.JSON, nullable=True)

    class Conversation(base):
        locals().update(table("chat_conversations"))
        id = column(_sa.Integer, primary_key=True)
        uuid = column(_sa.Uuid(as_uuid=False), default=lambda: str(uuid.uuid4()))
        name = column(_sa.String)
        source = column(_sa.String)
        author_id = column(_sa.Integer)
        is_private = column(_sa.Boolean)
        meta = column(_sa.JSON)
        participants = _orm.relationship(Participant, secondary=ParticipantMapping.__table__, lazy="selectin")

    class MessageGroup(base):
        locals().update(table("chat_message_groups"))
        id = column(_sa.Integer, primary_key=True)
        uuid = column(_sa.Uuid(as_uuid=False), default=lambda: str(uuid.uuid4()))
        conversation_id = column(_sa.Integer, foreign("chat_conversations"))
        conversation = _orm.relationship(Conversation)
        author_participant_id = column(_sa.Integer, foreign("chat_participants"))
        author_participant = _orm.relationship(Participant)
        sent_to_id = column(_sa.Integer, nullable=True)
        reply_to_id = column(_sa.Integer, nullable=True)
        is_streaming = column(_sa.Boolean, default=False)
        meta = column(_sa.JSON, nullable=True)

    class TextItem(base):
        locals().update(table("chat_message_items"))
        __mapper_args__ = {"polymorphic_identity": "text_message"}  # noqa: RUF012 -- SQLAlchemy reads it
        id = column(_sa.Integer, primary_key=True)
        message_group_id = column(_sa.Integer, foreign("chat_message_groups"))
        message_group = _orm.relationship(MessageGroup)
        item_type = column(_sa.String)
        content = column(_sa.String)
        order_index = column(_sa.Integer)

    return types.SimpleNamespace(
        Application=Application, ApplicationVersion=ApplicationVersion, Participant=Participant,
        ParticipantMapping=ParticipantMapping, Conversation=Conversation, MessageGroup=MessageGroup,
        TextItem=TextItem)


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    vars(module).update(attrs)
    return module


@pytest.fixture
def world(monkeypatch):
    """Agent 5 version 9 (variable region=EU) with schedule 1 of owner 12 and a running run; agent 6 beside it."""
    for name, module in REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)

    class Base(_orm.DeclarativeBase):
        pass

    chat = _chat_models(Base)
    models = _module(f"{PKG}.models", db=types.SimpleNamespace(Base=Base),
                     db_tools=types.SimpleNamespace(AbstractBaseMixin=type("AbstractBaseMixin", (), {})),
                     config=types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant"))
    monkeypatch.setitem(sys.modules, models.__name__, models)
    for name in ("db", "db_tools", "config"):
        monkeypatch.setitem(sys.modules, f"{models.__name__}.{name}", getattr(models, name))
    # Fresh loads of everything that holds a model or a session helper
    for dotted in ("models.entity_schedule", "utils.schedule_claim", "utils.schedule_runs", "utils.schedule_prerun",
                   "utils.pipeline_execution", "utils.schedule_run_starter"):
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", None)

    engine = _sa.create_engine("sqlite://", execution_options={"schema_translate_map": {"tenant": None}})
    predictions, notices = [], []
    tools = _module("tools", db=types.SimpleNamespace(get_session=None), this=types.SimpleNamespace(),
                    rpc_tools=types.SimpleNamespace())
    monkeypatch.setitem(sys.modules, "tools", tools)

    stubs = {
        "models.all": {"Application": chat.Application, "ApplicationVersion": chat.ApplicationVersion},
        "models.conversation": {"Conversation": chat.Conversation},
        "models.participants": {"ParticipantMapping": chat.ParticipantMapping},
        "models.enums.all": {
            "ParticipantTypes": types.SimpleNamespace(application=types.SimpleNamespace(value="application"),
                                                   user=types.SimpleNamespace(value="user")),
            "NotificationEventTypes": types.SimpleNamespace(schedule_disabled="schedule_disabled",
                                                         schedule_run_failed="schedule_run_failed")},
        "models.message_group": {"ConversationMessageGroup": chat.MessageGroup},
        "models.message_items.text": {"TextMessageItem": chat.TextItem},
        "models.pd.participant": {
            "ParticipantEntityApplication": lambda id, project_id: types.SimpleNamespace(id=id),
            "ParticipantEntityUser": lambda id: types.SimpleNamespace(id=id)},
        "models.pd.chat": {"_resolve_variables": _fill},
        "utils.participant_utils": {"get_or_create_one": lambda session, entity_name, entity_meta: _participant(
            session, chat, entity_name, entity_meta)},
        "utils.sio_utils": {"SioEvents": types.SimpleNamespace(chat_predict=types.SimpleNamespace(value="chat_predict"))},
        "utils.schedule_notifications": {"notify_schedule_event": lambda _p, owner_id, event, _s, run=None, extra=None:
                                             notices.append((owner_id, event, run.reason))},
    }
    for dotted, attrs in stubs.items():
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", _module(f"{PKG}.{dotted}", **attrs))

    schedules = load("models/entity_schedule.py", fresh=True)
    load("utils/schedule_claim.py", fresh=True)
    load("utils/schedule_runs.py", fresh=True)
    prerun = load("utils/schedule_prerun.py", fresh=True)
    load("utils/pipeline_execution.py", fresh=True)
    starter = load("utils/schedule_run_starter.py", fresh=True)
    schedules.EntitySchedule.__table__.c.variables.server_default = None  # '[]'::jsonb is Postgres only
    Base.metadata.create_all(engine)

    @contextmanager
    def get_session(_project_id):
        with _orm.Session(engine, expire_on_commit=False) as session:
            yield session

    monkeypatch.setattr(tools.db, "get_session", get_session)

    def predict(**kwargs):
        predictions.append(kwargs)
        return {"task_id": "task-1"}

    auth = FakeAuth(members={OWNER}, users={OWNER: {"id": OWNER, "suspended": False}})
    tools.rpc_tools.RpcMixin = lambda: types.SimpleNamespace(
        rpc=types.SimpleNamespace(timeout=lambda _s: types.SimpleNamespace(
            applications_predict_sio=predict, auth_check_user_in_project=auth.auth_check_user_in_project,
            auth_get_user=auth.auth_get_user)))
    monkeypatch.setattr(prerun, "check_owner_credentials", lambda _project, _schedule: None)

    with _orm.Session(engine) as session:
        session.add_all([chat.Application(id=AGENT_ID, name="Daily report"),
                         chat.Application(id=OTHER_AGENT_ID, name="Other"),
                         chat.ApplicationVersion(id=VERSION_ID, application_id=AGENT_ID, variables=VERSION_VARIABLES)])
        session.flush()
        schedule = schedules.EntitySchedule(
            entity_type="agent", application_id=AGENT_ID, version_id=VERSION_ID, name="S1", cron="0 * * * *",
            timezone="UTC", created_by=OWNER, principal={"kind": "delegation", "ref": OWNER}, prompt=PROMPT,
            variables=[])
        session.add(schedule)
        session.flush()
        session.add(schedules.EntityScheduleRun(schedule_id=schedule.id, trigger="cron", status="running"))
        session.commit()
        ids = types.SimpleNamespace(schedule=schedule.id, run=1)

    def run(trigger="cron", **changes):
        with _orm.Session(engine) as session:
            for field, value in changes.items():
                setattr(session.get(schedules.EntitySchedule, ids.schedule), field, value)
            session.commit()
        starter.run_owner_schedule(PROJECT_ID, ids.schedule, ids.run, trigger=trigger)

    def conversations(agent_id=AGENT_ID, source="agent"):
        """Conversations of one agent with `source`, the filter `list_conversations_rpc` applies for Agent Run History."""
        with _orm.Session(engine) as session:
            rows = session.query(chat.Conversation).filter_by(source=source).all()
            return [row for row in rows if row.meta["single_participant"]["entity_meta"]["id"] == agent_id]

    def first_message():
        with _orm.Session(engine) as session:
            return session.query(chat.TextItem).order_by(chat.TextItem.id).first().content

    def run_row():
        with _orm.Session(engine) as session:
            return session.get(schedules.EntityScheduleRun, ids.run)

    def agent_version():
        with _orm.Session(engine) as session:
            return session.get(chat.ApplicationVersion, VERSION_ID).variables

    return types.SimpleNamespace(run=run, predictions=predictions, conversations=conversations, auth=auth,
                                 first_message=first_message, run_row=run_row, notices=notices,
                                 agent_version=agent_version)


def _participant(session, chat, entity_name, entity_meta):
    row = session.query(chat.Participant).filter_by(entity_name=entity_name.value, entity_id=entity_meta.id).first()
    if not row:
        row = chat.Participant(entity_name=entity_name.value, entity_id=entity_meta.id)
        session.add(row)
        session.flush()
    return row, None


# TC-US11-01 / R1.3, R1.4, R1.6
def test_7011_r1_3_r1_4_cron_run_creates_a_private_agent_conversation_with_schedule_meta(world):
    world.run()

    (conversation,) = world.conversations()
    assert (conversation.source, conversation.author_id, conversation.is_private) == ("agent", OWNER, True)
    assert conversation.meta["scheduled_run"] is True
    assert (conversation.meta["schedule_id"], conversation.meta["schedule_run_id"]) == (1, 1)
    assert conversation.meta["execution_type"] == "scheduled"
    assert sorted(item.entity_id for item in conversation.participants) == [AGENT_ID, OWNER]
    assert world.run_row().conversation_id == conversation.id


# TC-US11-03 / R1.5
def test_7011_r1_5_conversation_is_named_for_the_agent_and_holds_the_prompt_as_first_message(world):
    world.run()

    assert world.conversations()[0].name == "Scheduled run: Daily report"
    assert world.first_message() == PROMPT


# TC-US11-02 / R1.3: a lost owner creates no conversation and starts no predict
def test_7011_r1_3_failed_prerun_creates_no_conversation_and_no_predict(world):
    world.auth.members.clear()

    world.run()

    assert world.conversations() == []
    assert world.predictions == []
    assert world.run_row().status == "failed"


# TC-US11-05, TC-US11-07 / R2.1: the predict call runs as the owner, never as the system user
def test_7011_r2_1_predict_runs_as_the_owner_not_the_system_user(world):
    world.run()

    (predicted,) = world.predictions
    assert (predicted["is_system_user"], predicted["user_id"], predicted["sid"]) == (False, OWNER, None)
    assert predicted["data"]["application_id"] == AGENT_ID and predicted["data"]["version_id"] == VERSION_ID
    assert predicted["non_interactive"] is True


# TC-US11-09 / R2.3, R2.6
def test_7011_r2_3_r2_6_variables_fill_the_prompt_and_stay_out_of_the_predict_data(world):
    world.run(prompt="Summarize open bugs for {{region}}", variables=[{"name": "region", "value": "US"}])

    (predicted,) = world.predictions
    assert world.first_message() == predicted["data"]["user_input"] == "Summarize open bugs for US"
    assert "variables" not in predicted["data"]
    assert world.agent_version() == VERSION_VARIABLES


# TC-US11-14 / R2.3: spaces and repeats
def test_7011_r2_3_placeholders_with_spaces_and_repeats_are_all_filled(world):
    world.run(prompt="Bugs in {{ region }} and {{region}} for {{team}}",
              variables=[{"name": "region", "value": "US"}, {"name": "team", "value": "core"}])

    assert world.first_message() == world.predictions[0]["data"]["user_input"] == "Bugs in US and US for core"


# TC-US11-15 / R2.3, R2.6: a value shaped like a placeholder is not filled a second time
def test_7011_r2_3_r2_6_a_value_that_looks_like_a_placeholder_is_filled_once(world):
    world.run(prompt="Report for {{team}}", variables=[{"name": "team", "value": "{{region}}"}])

    assert world.first_message() == world.predictions[0]["data"]["user_input"] == "Report for {{region}}"
    assert "variables" not in world.predictions[0]["data"]


# TC-US11-11 / R3.1, R3.2
def test_7011_r3_1_r3_2_cron_run_sends_trigger_source_scheduled_and_lists_in_agent_history(world):
    world.run()

    assert world.predictions[0]["trigger_source"] == "scheduled"
    assert [item.meta["schedule_run_id"] for item in world.conversations()] == [1]


# TC-US11-12 / R3.2
def test_7011_r3_2_run_conversation_is_not_listed_under_another_agent(world):
    world.run()

    assert world.conversations(agent_id=OTHER_AGENT_ID) == []


# R3.1: a manual run is not a scheduled one (ADR-0008)
def test_7011_r3_1_manual_run_sends_no_trigger_source_and_sets_no_scheduled_flag(world):
    world.run(trigger="manual")

    (conversation,) = world.conversations()
    assert world.predictions[0]["trigger_source"] is None
    assert "scheduled_run" not in conversation.meta and conversation.meta["execution_type"] == "manual"
