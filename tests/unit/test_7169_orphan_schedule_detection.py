"""Issue #7169 - which index schedules are orphaned, and what removing one touches.

Run via:
    python tests/run_tests.py unit/test_7169_orphan_schedule_detection.py -v
"""

import contextlib
import copy
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from fixtures import identity  # noqa: E402
from fixtures.helpers import load_module_with_stubs  # noqa: E402

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def mod():
    for name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.utils"):
        sys.modules.setdefault(name, types.ModuleType(name)).__path__ = []
    # Other suites leave sqlalchemy stand-ins in sys.modules; bind the real package, then put theirs back.
    saved = {n: m for n, m in sys.modules.items() if n == "sqlalchemy" or n.startswith("sqlalchemy.")}
    identity.fresh_sqlalchemy()
    try:
        return load_module_with_stubs(
            PLUGIN_ROOT / "utils" / "orphaned_index_schedules.py",
            "plugins.elitea_core.utils.orphaned_index_schedules",
        )
    finally:
        for n in [n for n in sys.modules if n == "sqlalchemy" or n.startswith("sqlalchemy.")]:
            del sys.modules[n]
        sys.modules.update(saved)


def _schedule(enabled=True):
    return {"cron": "0 3 * * *", "enabled": enabled, "created_by": 7, "last_run": None}


# --- find_orphans ---------------------------------------------------------------------------

def test_present_index_is_kept_whatever_its_state(mod):
    """AC-3: any index_meta row, including an in_progress one, means the index exists."""
    indexes_meta = {"Live": {"schedules": {"7": _schedule()}}, "Running": {"schedules": {"7": _schedule()}}}
    assert mod.find_orphans(indexes_meta, {"Live", "Running"}) == {}


def test_missing_index_is_orphaned(mod):
    indexes_meta = {"Live": {"schedules": {"7": _schedule()}}, "Ghost": {"schedules": {"7": _schedule()}}}
    assert mod.find_orphans(indexes_meta, {"Live"}) == {"Ghost": indexes_meta["Ghost"]}


def test_disabled_schedule_is_orphaned_too(mod):
    """AC-11: the scheduler never visits a disabled schedule, so only this task can clear it."""
    indexes_meta = {"Ghost": {"schedules": {"7": _schedule(enabled=False)}}}
    assert list(mod.find_orphans(indexes_meta, set())) == ["Ghost"]


def test_no_index_present_orphans_everything(mod):
    """AC-10: an empty store means every scheduled index is gone."""
    indexes_meta = {"A": {"schedules": {}}, "B": {"schedules": {"-1": _schedule()}}}
    assert set(mod.find_orphans(indexes_meta, set())) == {"A", "B"}


# --- present_index_names --------------------------------------------------------------------

class _Engine:
    """Records every statement; answers the index_meta query with ``names``."""

    def __init__(self, names=(), has_table=True):
        self.names, self.has_table, self.statements = list(names), has_table, []

    def execution_options(self, **_):
        return self

    def connect(self):
        engine = self

        class _Conn:
            def execute(self, statement, *a, **k):
                engine.statements.append(str(statement))
                return [types.SimpleNamespace(name=n) for n in engine.names]

        return contextlib.nullcontext(_Conn())


def _inspector(engine):
    return types.SimpleNamespace(has_table=lambda table, schema=None: engine.has_table)


def test_present_names_come_from_the_toolkit_schema(mod, monkeypatch):
    monkeypatch.setattr(mod, "inspect", _inspector)
    engine = _Engine(names=["IdxA", "IdxB"])
    assert mod.present_index_names(engine, "345") == {"IdxA", "IdxB"}
    assert len(engine.statements) == 1
    assert '"345".langchain_pg_embedding' in engine.statements[0]
    assert "index_meta" in engine.statements[0]


def test_missing_table_is_empty_and_creates_nothing(mod, monkeypatch):
    """AC-10: a reachable DB without the toolkit's table means no index, and no DDL is run."""
    monkeypatch.setattr(mod, "inspect", _inspector)
    engine = _Engine(names=["never-read"], has_table=False)
    assert mod.present_index_names(engine, "345") == set()
    assert engine.statements == []


def test_non_numeric_schema_is_refused(mod, monkeypatch):
    """The schema name is interpolated into SQL; toolkit ids are integers."""
    monkeypatch.setattr(mod, "inspect", _inspector)
    with pytest.raises(ValueError):
        mod.present_index_names(_Engine(), '345"; DROP TABLE x; --')


def test_lookup_error_propagates(mod, monkeypatch):
    """AC-4: the caller must be able to tell 'no index' from 'could not check'."""
    def _broken(engine):
        raise RuntimeError("connection refused")
    monkeypatch.setattr(mod, "inspect", _broken)
    with pytest.raises(RuntimeError):
        mod.present_index_names(_Engine(), "345")


# --- prune_toolkit_orphans ------------------------------------------------------------------

class _Session:
    """Project session over one toolkit; refresh() reloads the stored meta like SQLAlchemy."""

    def __init__(self, toolkit, *, on_refresh=None, fail_commit=False):
        self.toolkit, self.stored = toolkit, copy.deepcopy(toolkit.meta)
        self.on_refresh, self.fail_commit = on_refresh, fail_commit
        self.commits = self.rollbacks = 0

    def refresh(self, toolkit):
        if self.on_refresh:
            self.on_refresh(self.stored)
        toolkit.meta = copy.deepcopy(self.stored)

    def commit(self):
        if self.fail_commit:
            raise RuntimeError("could not serialize access")
        self.commits += 1
        self.stored = copy.deepcopy(self.toolkit.meta)

    def rollback(self):
        self.rollbacks += 1
        self.toolkit.meta = copy.deepcopy(self.stored)


def _toolkit(indexes_meta, **extra_meta):
    return types.SimpleNamespace(id=345, meta={"indexes_meta": copy.deepcopy(indexes_meta), **extra_meta})


@pytest.fixture
def no_flag(mod, monkeypatch):
    monkeypatch.setattr(mod, "flag_modified", lambda obj, key: None)


def test_removes_only_the_orphan_key(mod, no_flag):
    """AC-12: other indexes and every other meta key stay as they are."""
    live = {"schedules": {"7": _schedule()}}
    ghost = {"schedules": {"7": _schedule(), "-1": _schedule(enabled=False)}}
    toolkit = _toolkit({"Live": live, "Ghost": ghost}, other={"keep": 1})
    session = _Session(toolkit)

    removed, changed = mod.prune_toolkit_orphans(session, toolkit, {"Ghost": copy.deepcopy(ghost)})

    assert removed == {"Ghost": ghost}
    assert changed == []
    assert session.stored == {"indexes_meta": {"Live": live}, "other": {"keep": 1}}
    assert session.commits == 1


def test_entry_changed_since_the_scan_is_kept(mod, no_flag):
    """AC-7: someone re-scheduled the index between the scan and the removal."""
    ghost = {"schedules": {"7": _schedule()}}
    toolkit = _toolkit({"Ghost": ghost})

    def reschedule(stored):
        stored["indexes_meta"]["Ghost"]["schedules"]["7"]["cron"] = "0 4 * * *"

    session = _Session(toolkit, on_refresh=reschedule)
    removed, changed = mod.prune_toolkit_orphans(session, toolkit, {"Ghost": copy.deepcopy(ghost)})

    assert removed == {}
    assert changed == ["Ghost"]
    assert session.stored["indexes_meta"]["Ghost"]["schedules"]["7"]["cron"] == "0 4 * * *"
    assert session.commits == 0


def test_entry_already_gone_is_reported_changed(mod, no_flag):
    """The scheduler's own 24h prune (#7023) may win the race; nothing is removed twice."""
    ghost = {"schedules": {"7": _schedule()}}
    toolkit = _toolkit({"Ghost": ghost})
    session = _Session(toolkit, on_refresh=lambda stored: stored["indexes_meta"].pop("Ghost"))

    removed, changed = mod.prune_toolkit_orphans(session, toolkit, {"Ghost": copy.deepcopy(ghost)})

    assert (removed, changed) == ({}, ["Ghost"])
    assert session.commits == 0


def test_failed_commit_rolls_back_and_raises(mod, no_flag):
    ghost = {"schedules": {"7": _schedule()}}
    toolkit = _toolkit({"Ghost": ghost})
    session = _Session(toolkit, fail_commit=True)

    with pytest.raises(RuntimeError):
        mod.prune_toolkit_orphans(session, toolkit, {"Ghost": copy.deepcopy(ghost)})

    assert session.rollbacks == 1
    assert session.stored == {"indexes_meta": {"Ghost": ghost}}


# --- parse_prune_params ---------------------------------------------------------------------

@pytest.mark.parametrize("param, expected", [
    ("", (None, None, True)),
    (None, (None, None, True)),
    ("project_id=all", (None, None, True)),
    ("project_id=3", (3, None, True)),
    ("project_id=3;toolkit_id=12;dry_run=false", (3, 12, False)),
    ("dry_run=FALSE", (None, None, False)),
    ("dry_run=no", (None, None, True)),
])
def test_params(mod, param, expected):
    """AC-1, AC-6: dry run unless dry_run=false is spelled out."""
    assert mod.parse_prune_params(param) == expected


@pytest.mark.parametrize("param", [
    "project_id=abc",
    "project_id=3;toolkit_id=x",
    "toolkit_id=12",  # toolkit ids are per project
])
def test_bad_params_raise(mod, param):
    with pytest.raises(ValueError):
        mod.parse_prune_params(param)
