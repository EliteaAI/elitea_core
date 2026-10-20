"""EL-7011 US-16 R1.2, R2.1, R3.2: cron, manual and legacy runs in the two run history lists.

Real: the conversation list endpoint, `list_conversations_rpc`, `ConversationListExtended` and the run records
handler, over the SQLite tables of the agent schedule run world (real runs write the rows the lists read).
Faked: the predict RPC, the auth RPC, the credential step and the chat tables (SQLite stand-ins).

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_run_history_lists.py
"""
import sys
import types
from datetime import datetime

import pytest
from integration.test_7011_agent_schedule_run import OWNER, PKG, PROJECT_ID, _module
from integration.test_7011_agent_schedule_run import world as _agent_world
from integration.test_7011_entity_schedule_tables import REAL_SQLALCHEMY
from pydantic import BaseModel
from unit._tool_spend_loader import load

world = _agent_world  # the agent run fixture, shared

_sa, _orm = REAL_SQLALCHEMY["sqlalchemy"], REAL_SQLALCHEMY["sqlalchemy.orm"]
_pg = REAL_SQLALCHEMY["sqlalchemy.dialects.postgresql"]
STRANGER = 77
CREATED_AT = datetime(2030, 1, 1)


class Args(dict):
    """The part of the Werkzeug query-args API the list endpoint uses."""

    def get(self, key, default=None, type=None):
        value = super().get(key, default)
        return type(value) if type and value is not None else value


@pytest.fixture
def history(world, monkeypatch):
    """The conversation list endpoint wired to the real RPC over the world's tables, caller OWNER, source agent."""
    conversation = sys.modules[f"{PKG}.models.conversation"].Conversation
    participant = conversation.participants.property.mapper.class_
    mapping = sys.modules[f"{PKG}.models.participants"].ParticipantMapping
    tools = sys.modules["tools"]

    def db_session():
        return tools.db.get_session(PROJECT_ID)

    with db_session() as db:  # the PostgreSQL columns the list reads that the stand-ins lack
        db.execute(_sa.text("ALTER TABLE chat_participants ADD COLUMN entity_meta JSON"))
        db.execute(_sa.text("ALTER TABLE chat_conversations ADD COLUMN created_at DATETIME"))
        db.commit()
    participant.entity_meta = _orm.mapped_column(_pg.JSON)
    conversation.created_at = _orm.mapped_column(_sa.DateTime)
    conversation.__table__.c.meta.type.__class__ = _pg.JSON  # `.astext` is the PostgreSQL JSON operator
    participant.__table__.c.entity_meta.type.__class__ = _pg.JSON

    monkeypatch.setattr(tools, "serialize", lambda value: value if isinstance(value, dict) else {
        "id": value.id, "uuid": value.uuid, "name": value.name, "source": value.source, "meta": value.meta,
        "is_private": value.is_private, "author_id": value.author_id, "created_at": CREATED_AT}, raising=False)
    for name in ("config", "auth", "MinioClient"):
        monkeypatch.setattr(tools, name, types.SimpleNamespace(), raising=False)
    ParticipantTypes = types.SimpleNamespace(skill=types.SimpleNamespace(value="skill"), user=types.SimpleNamespace(
        value="user"))
    sibling_stubs = {
        "models.participants": {"Participant": participant, "ParticipantMapping": mapping},
        "models.enums.all": {"ParticipantTypes": ParticipantTypes, "SELF_MODELLED_PARTICIPANTS": ()},
        "models.pd.participant": {"ParticipantBase": BaseModel, "ParticipantCreate": BaseModel,
                                  "ParticipantEntityUser": BaseModel},
        "models.pd.message": {"MessageGroupDetail": BaseModel},
        "utils.chat_constants": {"CONVERSATION_NAME_MAX_LENGTH": 255},
        "utils.conversation_utils": {
            "get_conversation_details": None, "create_conversation": None, "resolve_persona_instructions": None,
            "reresolve_persona_instructions": None, "calculate_conversation_durations_batch": lambda ids, _s: {}},
        "utils.participant_utils": {"add_participant_to_conversation": None},
        "utils.chat_feature_flags": {"get_context_manager_feature_flag": None},
        "utils.context_analytics": {"set_context_strategy": None},
        "utils.exceptions": {"PoolSaturationError": Exception},
        "utils.model_defaults": {"creation_llm_settings": None, "chat_request_default": None},
        "utils.constants": {"PROMPT_LIB_MODE": "prompt_lib"},
        "utils.skill_participant_utils": {"SkillParticipantError": Exception, "validate_skill_participants": None},
    }
    for dotted, attrs in sibling_stubs.items():
        known = vars(sys.modules.get(f"{PKG}.{dotted}") or types.ModuleType(dotted))  # keep what the world stubbed
        merged = {**{k: v for k, v in known.items() if not k.startswith("__")}, **attrs}
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", _module(f"{PKG}.{dotted}", **merged))
    for dotted in ("models.pd.conversation", "rpc.chat_conversation", "api.v2.conversations"):
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", None)
    for dotted in ("models.pd", "api", "api.v2"):
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", _module(f"{PKG}.{dotted}"))
    web = types.SimpleNamespace(rpc=lambda *_a, **_k: (lambda func: func))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", types.SimpleNamespace(web=web, log=types.SimpleNamespace()))
    load("models/pd/conversation.py", fresh=True)
    rpc_module = load("rpc/chat_conversation.py", fresh=True)

    caller = types.SimpleNamespace(id=OWNER, admin=False)
    args = Args()
    request = types.SimpleNamespace(args=args, path="")
    rpc = types.SimpleNamespace(
        timeout=lambda _seconds: rpc,
        admin_check_user_is_admin=lambda _project, _user: caller.admin,
        chat_list_conversations_rpc=lambda **kwargs: rpc_module.RPC().list_conversations_rpc(**kwargs))
    monkeypatch.setitem(sys.modules, "flask", types.SimpleNamespace(request=request))
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=type("APIModeHandler", (), {}), APIBase=type("APIBase", (), {}),
        with_modes=lambda urls: urls, endpoint_metrics=lambda func: func)
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda _spec: (lambda func: func)),
        current_user=lambda: {"id": caller.id})
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE="administration", DEFAULT_MODE="default")
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc))
    tools.register_openapi = lambda **_meta: (lambda func: func)
    api_module = load("api/v2/conversations.py", fresh=True)

    def listing(**query):
        with db_session() as session:  # participants carry the entity_meta id the visibility filter reads
            for row in session.query(participant).filter(participant.entity_meta.is_(None)):
                row.entity_meta = {"id": row.entity_id}
            session.commit()
        args.clear()
        args.update({"source": "agent", **query})
        return api_module.PromptLibAPI().get(project_id=PROJECT_ID)

    def add(schedule_id, author=OWNER, **meta):
        """A conversation of `author` the way a run leaves it; `meta` adds or replaces keys of its meta."""
        with db_session() as session:
            owner = session.query(participant).filter_by(entity_name="user", entity_id=author).first()
            if not owner:
                owner = participant(entity_name="user", entity_id=author, entity_meta={"id": author})
            row = conversation(
                name="run", source="agent", author_id=author, is_private=True, created_at=CREATED_AT,
                meta={"schedule_id": schedule_id, **meta}, participants=[owner])
            session.add(row)
            session.commit()
            return row.id

    def edit_meta(conversation_id, drop=(), **changes):
        with db_session() as session:
            row = session.get(conversation, conversation_id)
            row.meta = {**{k: v for k, v in row.meta.items() if k not in drop}, **changes}
            session.commit()

    return types.SimpleNamespace(list=listing, add=add, edit_meta=edit_meta, caller=caller, request=request, args=args,
                                 db_session=db_session)


def _ids(body):
    return sorted(row["id"] for row in body["rows"])


def _types(body):
    return {row["id"]: row["execution_type"] for row in body["rows"]}


# TC-US16-03, TC-US16-04 / R1.2: the cron, manual and legacy rows of a schedule in the list
def test_7011_r1_2_cron_manual_and_legacy_rows_carry_their_execution_type(world, history):
    world.run(trigger="cron")
    (cron,) = world.conversations()
    world.set_run_status("success")
    world.run(trigger="manual")
    manual = next(row for row in world.conversations() if row.id != cron.id)
    legacy = history.add(1, scheduled_run=True)
    flag_off = history.add(1, scheduled_run=False)
    no_flag = history.add(1)

    body, status = history.list(schedule_id="1")

    assert status == 200
    assert _types(body) == {cron.id: "scheduled", manual.id: "manual", legacy: "scheduled",
                            flag_off: "manual", no_flag: "manual"}


# TC-US16-03 / R1.2: a cron run saved before execution_type existed still lists as scheduled
def test_7011_r1_2_a_cron_run_without_the_stored_type_lists_as_scheduled_by_its_flag(world, history):
    world.run(trigger="cron")
    (cron,) = world.conversations()
    history.edit_meta(cron.id, drop=("execution_type",))

    body, _ = history.list(schedule_id="1")

    assert _types(body) == {cron.id: "scheduled"}


# TC-US16-05 / R2.1: the schedule filter keeps only the runs of that schedule
def test_7011_r2_1_the_schedule_filter_returns_only_the_runs_of_that_schedule(world, history):
    world.run(trigger="cron")
    (cron,) = world.conversations()
    other = history.add(2, execution_type="scheduled")

    assert _ids(history.list(schedule_id="1")[0]) == [cron.id]
    assert _ids(history.list(schedule_id="2")[0]) == [other]
    assert _ids(history.list()[0]) == sorted([cron.id, other])
    assert history.list(schedule_id="3")[0] == {"total": 0, "rows": []}


# TC-US16-06 / R2.1: the filter never shows a conversation the caller cannot see
def test_7011_r2_1_the_schedule_filter_hides_another_users_private_run(world, history):
    own = history.add(1, execution_type="scheduled")
    foreign = history.add(1, author=STRANGER, execution_type="scheduled")

    assert _ids(history.list(schedule_id="1")[0]) == [own]
    history.caller.admin = True
    assert _ids(history.list(schedule_id="1")[0]) == sorted([own, foreign])


# TC-US16-07 / R2.1: a schedule_id that is not an integer is refused before the list runs
@pytest.mark.parametrize("value", ["abc", "", "1.5", " 5", "-1"])
def test_7011_r2_1_a_schedule_id_that_is_not_an_integer_is_400(world, history, value):
    history.add(1)

    body, status = history.list(schedule_id=value)

    assert (status, body["field"]) == (400, "schedule_id")



PERMISSIONS = {f"models.applications.schedules.{verb}" for verb in ("list", "details", "create", "update", "delete", "run")}


@pytest.fixture
def runs(world, history, monkeypatch):
    """The run records endpoint of schedule 1 over the world's tables; the caller is `history.caller`."""
    tools = sys.modules["tools"]
    tools.auth.decorators.check_api = lambda _spec: (lambda func: func)
    tools.auth.resolve_permissions = lambda **_kwargs: set(PERMISSIONS | ({"models.applications.schedules.admin"}
                                                                          if history.caller.admin else set()))
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE="administration", DEFAULT_MODE="default")
    monkeypatch.setitem(sys.modules, "opentelemetry", _module("opentelemetry", trace=types.SimpleNamespace(
        get_current_span=lambda: types.SimpleNamespace(set_attribute=lambda *_a: None))))
    stubs = {"utils.folder_access": {"entity_access_error": lambda *_a, **_k: None},
             "models.enums.all": {**vars(sys.modules[f"{PKG}.models.enums.all"]),
                                  "AgentTypes": types.SimpleNamespace(pipeline="pipeline")}}
    for dotted, attrs in stubs.items():
        merged = {k: v for k, v in attrs.items() if not k.startswith("__")}
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", _module(f"{PKG}.{dotted}", **merged))
    for dotted in ("utils.schedule_timing", "models.pd.index", "models.pd.entity_schedule", "models.pd.schedule_run",
                   "utils.schedule_access", "utils.schedule_dispatch", "utils.schedule_service", "api.v2.schedule"):
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", None)
        load(dotted.replace(".", "/") + ".py", fresh=True)
    handler = sys.modules[f"{PKG}.api.v2.schedule"].PromptLibAPI

    def get(schedule_id=1, **query):
        history.args.clear()
        history.args.update(query)
        history.request.path = f"/schedule/prompt_lib/{PROJECT_ID}/{schedule_id}/runs"
        return handler().get(project_id=PROJECT_ID, schedule_id=schedule_id)

    return get


# TC-US16-08, TC-US16-11 / R3.2: a run without a conversation lists with a null conversation_id
def test_7011_r3_2_a_run_without_a_conversation_lists_with_conversation_id_null(world, runs):
    world.run(trigger="cron")
    (cron,) = world.conversations()
    world.set_run_status("success")
    world.claim("manual")  # a manual run accepted but not started leaves a record with no conversation

    body, status = runs()

    assert status == 200
    manual, scheduled = body["rows"]
    assert (manual["trigger"], manual["conversation_id"]) == ("manual", None)
    assert (scheduled["trigger"], scheduled["conversation_id"]) == ("cron", cron.id)


# TC-US16-09, TC-US16-10, TC-US16-11 / R3.2: paging, the status filter and the refused status on real run records
def test_7011_r3_2_status_and_paging_apply_to_the_run_records(world, runs):
    world.run(trigger="cron")
    world.set_run_status("failed")
    world.claim("manual")

    assert [row["trigger"] for row in runs(limit="1", offset="1")[0]["rows"]] == ["cron"]
    assert [row["status"] for row in runs(status="failed")[0]["rows"]] == ["failed"]
    assert runs(status="skipped")[0] == {"total": 0, "rows": []}
    body, status = runs(status="done")
    assert (status, body["field"]) == (400, "status")


# TC-US16-12 / R3.2: a project admin reads the run records of another user's schedule
@pytest.mark.parametrize(("admin", "expected"), [(True, 200), (False, 404)])
def test_7011_r3_2_only_a_project_admin_reads_the_runs_of_another_users_schedule(world, history, runs, admin, expected):
    history.caller.id, history.caller.admin = STRANGER, admin

    assert runs()[1] == expected
