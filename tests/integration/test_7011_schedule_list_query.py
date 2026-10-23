"""Schedule list query on SQLite (EL-7011 US-05).

Covers R1.1, R2.1, R3.1, R3.2, R3.3, R4.2, R4.3, R5.1. The query and schedule models are
real; the plugin package and the application tables around them are stubbed.

Run via:
    python tests/run_tests.py integration/test_7011_schedule_list_query.py -v
"""
import datetime
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs


def _import_real_sqlalchemy():
    """Real sqlalchemy modules by name, even when an earlier suite left stubs (no `__file__`) in sys.modules."""
    def current():
        return {n: m for n, m in sys.modules.items() if n == "sqlalchemy" or n.startswith("sqlalchemy.")}

    stubs = {n: m for n, m in current().items() if not hasattr(m, "__file__")}
    for n in stubs:
        del sys.modules[n]
    try:
        import sqlalchemy.dialects.postgresql
        import sqlalchemy.ext.compiler
        import sqlalchemy.orm  # noqa: F401
        return current()
    finally:
        sys.modules.update(stubs)


REAL_SQLALCHEMY = _import_real_sqlalchemy()
PRINCIPAL = {"kind": "mandate"}
JSONB = REAL_SQLALCHEMY["sqlalchemy.dialects.postgresql"].JSONB


@REAL_SQLALCHEMY["sqlalchemy.ext.compiler"].compiles(JSONB, "sqlite")
def _jsonb_as_json(type_, compiler, **kw):
    return "JSON"

_sa = REAL_SQLALCHEMY["sqlalchemy"]
_orm = REAL_SQLALCHEMY["sqlalchemy.orm"]
create_engine, Integer, String = _sa.create_engine, _sa.Integer, _sa.String
DeclarativeBase, Mapped, Session, mapped_column = _orm.DeclarativeBase, _orm.Mapped, _orm.Session, _orm.mapped_column

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = "elitea_core_7011_list"
ADMIN = types.SimpleNamespace(user_id=1, is_admin=True)
OWNER = types.SimpleNamespace(user_id=7, is_admin=False)
DAY = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)


def _filters(**overrides):
    values = {"entity_type": None, "status": None, "enabled": None, "owner_ids": None, "entity_id": None, "search": None,
              "sort_by": "next_run", "sort_order": "asc"}
    return types.SimpleNamespace(**{**values, **overrides})


def _package(name, monkeypatch, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    module.__dict__.update(attrs)
    monkeypatch.setitem(sys.modules, name, module)
    return module


@pytest.fixture
def env(monkeypatch):
    """(query module, schedule models, SQLite session) with two applications and one version each."""
    for name, module in REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)

    class Base(DeclarativeBase):
        pass

    class Application(Base):
        __tablename__ = "applications"
        __table_args__ = ({"schema": "tenant"},)
        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        name: Mapped[str] = mapped_column(String)

    class ApplicationVersion(Base):
        __tablename__ = "application_versions"
        __table_args__ = ({"schema": "tenant"},)
        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        name: Mapped[str] = mapped_column(String)

    _package(PKG, monkeypatch)
    _package(f"{PKG}.utils", monkeypatch)
    _package(
        f"{PKG}.models", monkeypatch, db=types.SimpleNamespace(Base=Base),
        db_tools=types.SimpleNamespace(AbstractBaseMixin=type("Mixin", (), {})),
        config=types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant"),
    )
    schedules = load_module_with_stubs(
        PLUGIN_ROOT / "models" / "entity_schedule.py", f"{PKG}.models.entity_schedule"
    )
    _package(f"{PKG}.models.all", monkeypatch, Application=Application, ApplicationVersion=ApplicationVersion)
    queries = load_module_with_stubs(PLUGIN_ROOT / "utils" / "schedule_queries.py", f"{PKG}.utils.schedule_queries")

    schedules.EntitySchedule.__table__.c.variables.server_default = None  # `::jsonb` is not SQLite
    engine = create_engine("sqlite://").execution_options(schema_translate_map={"tenant": None})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all([
            Application(id=1, name="Reporter"), Application(id=2, name="Digest"),
            ApplicationVersion(id=1, name="v1"), ApplicationVersion(id=2, name="v2"),
        ])
        session.commit()
        yield queries, schedules, session
    engine.dispose()


def _add(env, **overrides):
    _, schedules, session = env
    values = {
        "entity_type": "agent", "application_id": 1, "version_id": 1, "name": "nightly", "cron": "0 3 * * *",
        "timezone": "UTC", "created_by": 7, "principal": PRINCIPAL,
    }
    row = schedules.EntitySchedule(**{**values, **overrides})
    session.add(row)
    session.commit()
    return row


def _run(env, schedule, started_at, reason):
    _, schedules, session = env
    session.add(schedules.EntityScheduleRun(
        schedule_id=schedule.id, trigger="cron", status="failed", reason=reason, started_at=started_at
    ))
    session.commit()


def _names(result):
    return [r.name for r in result[1]]


def _list(env, scope=ADMIN, limit=20, offset=0, **filters):
    queries, _, session = env
    return queries.list_schedules(session, scope, _filters(**filters), limit, offset)


# R1.1: an admin sees the schedules of every owner
def test_7011_r1_1_admin_lists_every_owner(env):
    _add(env, name="a", created_by=7)
    _add(env, name="b", created_by=8)
    assert sorted(_names(_list(env))) == ["a", "b"]


# R2.1: a regular user sees only their own rows, even when owner_ids names another owner
def test_7011_r2_1_own_only_ignores_owner_ids(env):
    _add(env, name="mine", created_by=7)
    _add(env, name="other", created_by=8)
    assert _names(_list(env, OWNER)) == ["mine"]
    assert _names(_list(env, OWNER, owner_ids=[8])) == ["mine"]


# R3.1: each given filter narrows the rows
def test_7011_r3_1_filters_match_each_given_value(env):
    _add(env, name="agent-on", enabled=True, last_run_status="failed")
    _add(env, name="pipe-off", entity_type="pipeline", application_id=2, version_id=2, enabled=False,
         last_run_status="success")
    assert _names(_list(env, entity_type="pipeline")) == ["pipe-off"]
    assert _names(_list(env, status="failed")) == ["agent-on"]
    assert _names(_list(env, enabled=False)) == ["pipe-off"]
    assert _names(_list(env, entity_id=1)) == ["agent-on"]
    assert _names(_list(env, enabled=True, entity_id=2)) == []


# R3.2: an admin narrows to the given owners
def test_7011_r3_2_admin_owner_ids(env):
    _add(env, name="a", created_by=7)
    _add(env, name="b", created_by=8)
    _add(env, name="c", created_by=9)
    assert sorted(_names(_list(env, owner_ids=[7, 9]))) == ["a", "c"]


# R3.3: search matches the schedule name or the entity name, ignoring case, and treats % literally
def test_7011_r3_3_search_name_or_entity_name(env):
    _add(env, name="Nightly sync", application_id=1)
    _add(env, name="weekly", application_id=2, version_id=2)
    _add(env, name="100% done", application_id=1)
    assert _names(_list(env, search="NIGHTLY")) == ["Nightly sync"]
    assert _names(_list(env, search="digest")) == ["weekly"]
    assert _names(_list(env, search="%")) == ["100% done"]


# R4.2: the reason of the latest run is returned
def test_7011_r4_2_last_run_reason_is_latest(env):
    schedule = _add(env, name="a")
    _add(env, name="no-runs")
    _run(env, schedule, DAY, "old reason")
    _run(env, schedule, DAY + datetime.timedelta(hours=1), "new reason")
    rows = {r.name: r for r in _list(env)[1]}
    assert rows["a"].last_run_reason == "new reason"
    assert rows["no-runs"].last_run_reason is None


# R4.3: description and run_mode come back as stored, null when empty; names come from the joins
def test_7011_r4_3_description_run_mode_and_names(env):
    _add(env, name="a", description="Daily", run_mode="prompt")
    _add(env, name="b", version_id=None)
    rows = {r.name: r for r in _list(env)[1]}
    assert (rows["a"].description, rows["a"].run_mode) == ("Daily", "prompt")
    assert (rows["b"].description, rows["b"].run_mode) == (None, None)
    assert (rows["a"].entity_name, rows["a"].version_name) == ("Reporter", "v1")
    assert rows["b"].version_name is None


# R5.1: pages follow next_run order (nulls last, then id) and total counts all matching rows
def test_7011_r5_1_paging_order_and_total(env):
    _add(env, name="late", next_run=DAY + datetime.timedelta(days=2))
    _add(env, name="none")
    _add(env, name="early", next_run=DAY)
    _add(env, name="other-owner", created_by=8)
    total, first = _list(env, OWNER, limit=2, offset=0)
    assert (total, [r.name for r in first]) == (3, ["early", "late"])
    total, second = _list(env, OWNER, limit=2, offset=2)
    assert (total, [r.name for r in second]) == (3, ["none"])


# Missed requirement (user review 2026-10-20): the table headers sort the list on the server
@pytest.mark.parametrize("sort_by, sort_order, expected", [
    ("name", "asc", ["early", "late", "none"]),
    ("name", "desc", ["none", "late", "early"]),
    ("next_run", "desc", ["late", "early", "none"]),
])
def test_7011_r5_1_sort_by_a_column_in_both_directions(env, sort_by, sort_order, expected):
    _add(env, name="late", next_run=DAY + datetime.timedelta(days=2))
    _add(env, name="none")
    _add(env, name="early", next_run=DAY)
    assert _names(_list(env, OWNER, sort_by=sort_by, sort_order=sort_order)) == expected
