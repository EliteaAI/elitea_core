"""EL-7011 owner CR 2026-10-21 (test panel log): what the run detail reads besides the run row.

Real: `utils/schedule_run_detail.py` (version name, tool calls of the run conversation, usage summary),
the schedule models and the run event constants, over SQLite. The chat tables and the usage plugin's
`usage_event` table are small stand-ins with the columns the module reads.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_run_detail_read.py
"""
import json
import sys
import types
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from integration.test_7011_entity_schedule_tables import REAL_SQLALCHEMY
from unit._tool_spend_loader import load

PKG = "plugins.elitea_core"
PROJECT_ID, CONVERSATION_ID, OTHER_CONVERSATION_ID = 34, 901, 902
START = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
CONVERSATION_UUID = str(uuid.uuid4())
# Groups of one shared chat: the run's trigger (4) and answer (5), another member's message (3) and the
# answer to it (6)
TRIGGER_GROUP, ANSWER_GROUP, MEMBER_GROUP, MEMBER_ANSWER_GROUP = 4, 5, 3, 6
ANSWER_GROUP_UUID = str(uuid.uuid4())

_sa, _orm = REAL_SQLALCHEMY["sqlalchemy"], REAL_SQLALCHEMY["sqlalchemy.orm"]


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    vars(module).update(attrs)
    return module


@pytest.fixture
def detail(monkeypatch):
    """The real read module on stand-in tables; `put` adds rows, `run` builds a run row."""
    for name, module in REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)

    class Base(_orm.DeclarativeBase):
        pass

    def tenant(name):
        return {"__tablename__": name, "__table_args__": {"schema": "tenant"}}

    class ApplicationVersion(Base):
        locals().update(tenant("application_versions"))
        id = _sa.Column(_sa.Integer, primary_key=True)
        name = _sa.Column(_sa.String)

    class Conversation(Base):
        locals().update(tenant("chat_conversations"))
        id = _sa.Column(_sa.Integer, primary_key=True)
        uuid = _sa.Column(_sa.String)

    class MessageGroup(Base):
        locals().update(tenant("chat_message_groups"))
        id = _sa.Column(_sa.Integer, primary_key=True)
        conversation_id = _sa.Column(_sa.Integer)
        uuid = _sa.Column(_sa.String)
        meta = _sa.Column(_sa.JSON)
        reply_to_id = _sa.Column(_sa.Integer)

    class TraceStep(Base):
        locals().update(tenant("chat_message_trace_step"))
        id = _sa.Column(_sa.Integer, primary_key=True)
        message_group_id = _sa.Column(_sa.Integer)
        kind = _sa.Column(_sa.String)
        started_at = _sa.Column(_sa.DateTime(timezone=True))
        finished_at = _sa.Column(_sa.DateTime(timezone=True))
        is_error = _sa.Column(_sa.Boolean, default=False)
        tool_name = _sa.Column(_sa.String)
        tool_output = _sa.Column(_sa.Text)
        attrs = _sa.Column(_sa.JSON)

    usage_event = _sa.Table(
        "usage_event", Base.metadata, _sa.Column("id", _sa.Integer, primary_key=True),
        *(_sa.Column(name, kind) for name, kind in (
            ("project_id", _sa.Integer), ("conversation_id", _sa.String), ("run_id", _sa.String),
            ("ts", _sa.DateTime(timezone=True)),
            ("event_type", _sa.String), ("input_tokens", _sa.Integer), ("output_tokens", _sa.Integer),
            ("cost_nano_usd", _sa.BigInteger))),
    )

    models = _module(f"{PKG}.models", db=types.SimpleNamespace(Base=Base),
                     db_tools=types.SimpleNamespace(AbstractBaseMixin=type("AbstractBaseMixin", (), {})),
                     config=types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant"))
    monkeypatch.setitem(sys.modules, models.__name__, models)
    for dotted, attrs in {
        "models.all": {"ApplicationVersion": ApplicationVersion},
        "models.conversation": {"Conversation": Conversation},
        "models.message_group": {"ConversationMessageGroup": MessageGroup},
        "models.message_trace_step": {"MessageTraceStep": TraceStep},
    }.items():
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", _module(f"{PKG}.{dotted}", **attrs))
    for dotted in ("models.entity_schedule", "utils.schedule_claim", "utils.schedule_run_detail"):
        monkeypatch.setitem(sys.modules, f"{PKG}.{dotted}", None)
    _sa.Table("applications", Base.metadata, _sa.Column("id", _sa.Integer, primary_key=True), schema="tenant")
    load("models/entity_schedule.py", fresh=True).EntitySchedule.__table__.c.variables.server_default = None
    load("utils/schedule_claim.py", fresh=True)
    load("utils/run_id.py", fresh=True)
    module = load("utils/schedule_run_detail.py", fresh=True)

    engine = _sa.create_engine("sqlite://", execution_options={"schema_translate_map": {"tenant": None}})
    Base.metadata.create_all(engine)
    monkeypatch.setitem(sys.modules, "tools", _module("tools", db=types.SimpleNamespace(engine=engine)))
    session = _orm.Session(engine)
    session.add_all([
        ApplicationVersion(id=10, name="v3"),
        Conversation(id=CONVERSATION_ID, uuid=CONVERSATION_UUID),
        Conversation(id=OTHER_CONVERSATION_ID, uuid=str(uuid.uuid4())),
        MessageGroup(id=1, conversation_id=CONVERSATION_ID), MessageGroup(id=2, conversation_id=OTHER_CONVERSATION_ID),
        MessageGroup(id=MEMBER_GROUP, conversation_id=CONVERSATION_ID),
        MessageGroup(id=TRIGGER_GROUP, conversation_id=CONVERSATION_ID),
        MessageGroup(id=ANSWER_GROUP, conversation_id=CONVERSATION_ID, uuid=ANSWER_GROUP_UUID,
                     reply_to_id=TRIGGER_GROUP),
        MessageGroup(id=MEMBER_ANSWER_GROUP, conversation_id=CONVERSATION_ID, uuid=str(uuid.uuid4()),
                     reply_to_id=MEMBER_GROUP),
    ])
    session.commit()

    def tool(seconds, group=1, **columns):
        session.add(TraceStep(**{
            "message_group_id": group, "kind": "tool_call", "tool_name": "list_issues",
            "started_at": START + timedelta(seconds=seconds),
            "finished_at": START + timedelta(seconds=seconds, milliseconds=1200), **columns}))
        session.commit()

    def usage(seconds=10, conversation=CONVERSATION_UUID, project_id=PROJECT_ID, **columns):
        with engine.begin() as connection:
            connection.execute(usage_event.insert().values(**{
                "project_id": project_id, "conversation_id": conversation, "ts": START + timedelta(seconds=seconds),
                "event_type": "llm", "input_tokens": 0, "output_tokens": 0, "cost_nano_usd": 0, **columns}))

    def run(**columns):
        return types.SimpleNamespace(**{"id": 77, "conversation_id": CONVERSATION_ID, "message_group_id": None, "started_at": START,
                                        "finished_at": START + timedelta(seconds=38), **columns})

    yield types.SimpleNamespace(module=module, session=session, tool=tool, usage=usage, run=run, engine=engine)
    session.close()


def test_cr_el7011_tool_calls_of_the_run_conversation_become_log_events_in_time_order(detail):
    detail.tool(20, tool_name="create_issue", is_error=True, tool_output="403 Forbidden")
    detail.tool(5, attrs={"metadata": {"toolkit_name": "github"}},
                tool_output="12 open issues\n" + "issue " * 100)
    detail.tool(1, group=2)  # another conversation
    detail.tool(2, kind="thinking_step")  # not a tool call

    first, second = detail.module.tool_call_events(detail.session, CONVERSATION_ID)

    assert (first["at"], first["kind"], first["toolkit"], first["tool"], first["duration_ms"]) == (
        (START + timedelta(seconds=5)).isoformat(), "tool_call", "github", "list_issues", 1200)
    # The owner and a schedules admin read the whole result (owner decision 2026-10-21)
    assert first["summary"] == ("12 open issues\n" + "issue " * 100).strip()
    assert first["text"].startswith("Tool call: github · list_issues (1200 ms): 12 open issues")
    assert first["level"] == "info"
    assert (second["tool"], second["level"], second["summary"], "toolkit" in second) == (
        "create_issue", "error", "403 Forbidden", False)
    assert second["text"] == "Tool call: create_issue (1200 ms): 403 Forbidden"


def test_cr_el7011_a_huge_tool_result_is_cut_at_the_upper_bound_with_a_marker(detail):
    limit = detail.module.RESULT_LIMIT
    detail.tool(1, tool_output="x" * (limit + 500))
    detail.tool(2, tool_output="y" * limit)

    huge, exact = detail.module.tool_call_events(detail.session, CONVERSATION_ID)

    assert huge["summary"] == "x" * limit + " (truncated)"
    assert exact["summary"] == "y" * limit


def test_cr_el7011_tool_call_events_are_capped_and_need_a_conversation(detail):
    for second in range(205):
        detail.tool(second)

    assert len(detail.module.tool_call_events(detail.session, CONVERSATION_ID)) == 200
    assert detail.module.tool_call_events(detail.session, None) == []


def test_cr_el7011_usage_sums_the_tokens_and_the_cost_of_the_run_conversation(detail):
    detail.usage(input_tokens=1500, output_tokens=500, cost_nano_usd=25_000_000)
    detail.usage(20, input_tokens=100, output_tokens=40, cost_nano_usd=4_000_000)
    detail.usage(30, event_type="tool_cost", cost_nano_usd=1_000_000, input_tokens=999)  # cost only, no tokens
    detail.usage(conversation=str(uuid.uuid4()), input_tokens=7000)  # another conversation
    detail.usage(project_id=99, input_tokens=7000)  # another project
    detail.usage(3600, input_tokens=7000)  # an hour after the run

    assert detail.module.usage_summary(detail.session, PROJECT_ID, detail.run()) == (2140, 0.03)


def test_cr_el7011_a_running_run_counts_its_usage_so_far(detail):
    detail.usage(3600, input_tokens=10, output_tokens=5)

    assert detail.module.usage_summary(detail.session, PROJECT_ID, detail.run(finished_at=None)) == (15, 0.0)


def test_cr_el7011_no_usage_row_no_conversation_or_a_failed_read_gives_null(detail, monkeypatch):
    assert detail.module.usage_summary(detail.session, PROJECT_ID, detail.run()) == (None, None)
    assert detail.module.usage_summary(detail.session, PROJECT_ID, detail.run(conversation_id=None)) == (None, None)
    assert detail.module.usage_summary(detail.session, PROJECT_ID, detail.run(conversation_id=555)) == (None, None)

    detail.usage(input_tokens=10)
    monkeypatch.setattr(sys.modules["tools"].db, "engine", None)  # the usage store is not reachable
    assert detail.module.usage_summary(detail.session, PROJECT_ID, detail.run()) == (None, None)


def test_a_delivered_run_reads_the_tool_calls_of_its_own_groups_only(detail):
    detail.tool(1, group=TRIGGER_GROUP, tool_name="trigger_tool")
    detail.tool(2, group=ANSWER_GROUP, tool_name="answer_tool")
    detail.tool(3, group=MEMBER_GROUP, tool_name="member_tool", tool_output="secret of another member")
    detail.tool(4, group=MEMBER_ANSWER_GROUP, tool_name="member_answer_tool")
    detail.tool(5, group=1, tool_name="earlier_tool")

    events = detail.module.tool_call_events(detail.session, CONVERSATION_ID, ANSWER_GROUP)

    assert [event["tool"] for event in events] == ["trigger_tool", "answer_tool"]
    assert len(detail.module.tool_call_events(detail.session, CONVERSATION_ID)) == 5


def test_a_delivered_run_whose_answer_replies_to_nothing_reads_its_answer_group_only(detail):
    detail.tool(1, group=1, tool_name="own_tool")
    detail.tool(2, group=TRIGGER_GROUP, tool_name="other_tool")

    events = detail.module.tool_call_events(detail.session, CONVERSATION_ID, 1)

    assert [event["tool"] for event in events] == ["own_tool"]


def _answer_run_id(generation=0):
    return sys.modules[f"{PKG}.utils.run_id"].derived_run_id(
        {"message_id": ANSWER_GROUP_UUID, "execution_generation": generation})


def test_a_delivered_run_sums_the_usage_of_its_own_message_only(detail):
    detail.usage(input_tokens=1000, output_tokens=200, cost_nano_usd=10_000_000, run_id=_answer_run_id())
    detail.usage(20, input_tokens=300, output_tokens=0, cost_nano_usd=2_000_000, run_id=_answer_run_id())
    detail.usage(15, input_tokens=9000, output_tokens=9000, cost_nano_usd=900_000_000, run_id=str(uuid.uuid4()))
    detail.usage(16, input_tokens=5000, run_id=None)

    run = detail.run(message_group_id=ANSWER_GROUP)

    assert detail.module.usage_summary(detail.session, PROJECT_ID, run) == (1500, 0.012)


def test_a_delivered_run_with_no_usage_of_its_own_gives_null_not_the_chat_total(detail):
    detail.usage(input_tokens=9000, run_id=str(uuid.uuid4()))

    assert detail.module.usage_summary(
        detail.session, PROJECT_ID, detail.run(message_group_id=ANSWER_GROUP)) == (None, None)


def test_a_delivered_run_whose_group_is_gone_gives_null_usage(detail):
    detail.usage(input_tokens=9000)

    assert detail.module.usage_summary(
        detail.session, PROJECT_ID, detail.run(message_group_id=999)) == (None, None)


def test_cr_el7011_the_version_label_is_the_version_name(detail):
    assert detail.module.version_label(detail.session, 10) == "v3"
    assert detail.module.version_label(detail.session, 99) is None
    assert detail.module.version_label(detail.session, None) is None


def test_cr_el7011_postgres_numeric_sums_become_json_numbers(detail, monkeypatch):
    """Postgres SUM over BIGINT gives Decimal; the answer must still serialise as JSON."""
    from decimal import Decimal

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, statement):
            return types.SimpleNamespace(one=lambda: (5, Decimal(1119), Decimal(1581750)))

    monkeypatch.setattr(sys.modules["tools"].db, "engine", types.SimpleNamespace(connect=Connection))
    tokens, cost = detail.module.usage_summary(detail.session, PROJECT_ID, detail.run())

    assert (tokens, cost) == (1119, 0.001582)
    assert type(tokens) is int and type(cost) is float
    json.dumps({"tokens": tokens, "cost_usd": cost})
