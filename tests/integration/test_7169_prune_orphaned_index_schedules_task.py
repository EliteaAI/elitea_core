"""Issue #7169 - the prune_orphaned_index_schedules admin task, run end to end against fakes.

Run via:
    python tests/run_tests.py integration/test_7169_prune_orphaned_index_schedules_task.py -v
"""

import contextlib
import copy
import json
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


class Log:
    def __init__(self):
        self.lines = []

    def _rec(self, level):
        def _log(msg, *args, **_):
            self.lines.append((level, msg % args if args else msg))
        return _log

    def __getattr__(self, level):
        return self._rec(level)

    def text(self, level=None):
        return "\n".join(m for lvl, m in self.lines if level in (None, lvl))


def _schedule(enabled=True, cron="0 3 * * *"):
    return {"cron": cron, "enabled": enabled, "created_by": 7, "last_run": None}


class Toolkit(types.SimpleNamespace):
    pass


class Project:
    """One project schema: its toolkits as stored, and a session over them."""

    def __init__(self, toolkits, *, broken=False, refuse_commit_for=()):
        self.stored = {t.id: copy.deepcopy(t.meta) for t in toolkits}
        self.templates = {t.id: t for t in toolkits}
        self.broken = broken
        self.refuse_commit_for = set(refuse_commit_for)
        self.commits = 0

    @contextlib.contextmanager
    def session(self):
        if self.broken:
            raise RuntimeError("schema p_x does not exist")
        project = self

        class _Session:
            def refresh(self, toolkit):
                toolkit.meta = copy.deepcopy(project.stored[toolkit.id])

            def commit(self):
                for toolkit in self.loaded:
                    if toolkit.id in project.refuse_commit_for and toolkit.meta != project.stored[toolkit.id]:
                        raise RuntimeError("could not serialize access")
                project.commits += 1
                for toolkit in self.loaded:
                    project.stored[toolkit.id] = copy.deepcopy(toolkit.meta)

            def rollback(self):
                for toolkit in self.loaded:
                    toolkit.meta = copy.deepcopy(project.stored[toolkit.id])

        session = _Session()
        session.loaded = [
            Toolkit(id=t.id, type=t.type, settings=t.settings, author_id=t.author_id,
                    meta=copy.deepcopy(self.stored[t.id]))
            for t in self.templates.values()
        ]
        yield session


def _toolkit(toolkit_id, indexes_meta, *, conn="pg://vectors"):
    return Toolkit(id=toolkit_id, type="github", author_id=7, meta={"indexes_meta": indexes_meta},
                   settings={"pgvector_configuration": {"connection_string": conn}})


class World:
    """Projects, the toolkit vector stores they point at, and the task wired to both."""

    def __init__(self, mod, monkeypatch, projects, present, *, resolve_error=None, lookup_error=None):
        self.mod, self.projects, self.present = mod, projects, present
        self.resolve_error, self.lookup_error = resolve_error or {}, lookup_error or {}
        self.log = Log()
        self.lookups = []
        monkeypatch.setattr(mod, "log", self.log)
        monkeypatch.setattr(mod, "flag_modified", lambda obj, key: None)

    def _list_toolkits(self, session, toolkit_id):
        return [t for t in session.loaded if toolkit_id is None or t.id == toolkit_id]

    def _resolve(self, project_id, toolkit):
        if toolkit.id in self.resolve_error:
            raise self.resolve_error[toolkit.id]
        return toolkit.settings["pgvector_configuration"].get("connection_string")

    def _present(self, connection_string, schema):
        self.lookups.append(schema)
        if int(schema) in self.lookup_error:
            raise self.lookup_error[int(schema)]
        return set(self.present.get(int(schema), ()))

    def run(self, project_id=None, toolkit_id=None, dry_run=True):
        return self.mod.prune_orphaned_index_schedules(
            project_ids=[project_id] if project_id else list(self.projects),
            open_project_session=lambda pid: self.projects[pid].session(),
            list_toolkits=self._list_toolkits,
            resolve_connection_string=self._resolve,
            present_index_names=self._present,
            toolkit_id=toolkit_id,
            dry_run=dry_run,
        )

    def stored(self, project_id, toolkit_id):
        return self.projects[project_id].stored[toolkit_id]


GHOST = {"schedules": {"7": _schedule(), "-1": _schedule(enabled=False)}}
LIVE = {"schedules": {"7": _schedule()}}


def _world(mod, monkeypatch, **kw):
    projects = {
        1: Project([_toolkit(10, {"Ghost": copy.deepcopy(GHOST), "Live": copy.deepcopy(LIVE)})]),
    }
    return World(mod, monkeypatch, projects, present={10: {"Live"}}, **kw)


def test_dry_run_reports_and_changes_nothing(mod, monkeypatch):
    """AC-1"""
    world = _world(mod, monkeypatch)
    stats = world.run()

    assert world.stored(1, 10)["indexes_meta"].keys() == {"Ghost", "Live"}
    assert world.projects[1].commits == 0
    assert stats["orphans"] == 1 and stats["removed"] == 0
    text = world.log.text()
    assert "project=1" in text and "toolkit=10" in text and "index=Ghost" in text
    assert "-1" in text and "7" in text  # schedule owners
    assert "DRY RUN" in text


def test_removal_drops_only_the_orphan_and_logs_it_in_full(mod, monkeypatch):
    """AC-2, AC-11, AC-12"""
    world = _world(mod, monkeypatch)
    stats = world.run(dry_run=False)

    assert world.stored(1, 10) == {"indexes_meta": {"Live": LIVE}}
    assert stats["removed"] == 1
    warnings = world.log.text("warning")
    assert json.dumps(GHOST, sort_keys=True) in warnings


def test_healthy_index_is_never_touched(mod, monkeypatch):
    """AC-3"""
    projects = {1: Project([_toolkit(10, {"Live": copy.deepcopy(LIVE)})])}
    world = World(mod, monkeypatch, projects, present={10: {"Live"}})
    stats = world.run(dry_run=False)

    assert world.stored(1, 10) == {"indexes_meta": {"Live": LIVE}}
    assert world.projects[1].commits == 0
    assert stats["orphans"] == 0


@pytest.mark.parametrize("case", ["resolve_raises", "no_connection_string", "lookup_raises"])
def test_unverifiable_toolkit_is_skipped(mod, monkeypatch, case):
    """AC-4: never remove what could not be checked."""
    conn = None if case == "no_connection_string" else "pg://vectors"
    projects = {1: Project([_toolkit(10, {"Ghost": copy.deepcopy(GHOST)}, conn=conn)])}
    world = World(
        mod, monkeypatch, projects, present={},
        resolve_error={10: RuntimeError("vault down")} if case == "resolve_raises" else None,
        lookup_error={10: RuntimeError("connection refused")} if case == "lookup_raises" else None,
    )
    stats = world.run(dry_run=False)

    assert world.stored(1, 10) == {"indexes_meta": {"Ghost": GHOST}}
    assert stats["skipped"] == 1 and stats["removed"] == 0
    assert "toolkit=10" in world.log.text("warning")
    assert "skipped" in world.log.text("warning")


def test_same_name_in_two_toolkits_is_judged_per_toolkit(mod, monkeypatch):
    """AC-5"""
    projects = {1: Project([
        _toolkit(10, {"Docs": copy.deepcopy(LIVE)}),
        _toolkit(11, {"Docs": copy.deepcopy(LIVE)}),
    ])}
    world = World(mod, monkeypatch, projects, present={10: {"Docs"}, 11: set()})
    world.run(dry_run=False)

    assert world.stored(1, 10) == {"indexes_meta": {"Docs": LIVE}}
    assert world.stored(1, 11) == {"indexes_meta": {}}


def test_project_and_toolkit_filter(mod, monkeypatch):
    """AC-6"""
    projects = {
        1: Project([_toolkit(10, {"Ghost": copy.deepcopy(GHOST)}), _toolkit(11, {"Ghost": copy.deepcopy(GHOST)})]),
        2: Project([_toolkit(20, {"Ghost": copy.deepcopy(GHOST)})]),
    }
    world = World(mod, monkeypatch, projects, present={})
    stats = world.run(project_id=1, toolkit_id=11, dry_run=False)

    assert world.lookups == ["11"]
    assert world.stored(1, 10) == {"indexes_meta": {"Ghost": GHOST}}
    assert world.stored(1, 11) == {"indexes_meta": {}}
    assert world.stored(2, 20) == {"indexes_meta": {"Ghost": GHOST}}
    assert stats["projects"] == 1


def test_failing_project_does_not_stop_the_scan(mod, monkeypatch):
    """AC-6"""
    projects = {
        1: Project([], broken=True),
        2: Project([_toolkit(20, {"Ghost": copy.deepcopy(GHOST)})]),
    }
    world = World(mod, monkeypatch, projects, present={})
    stats = world.run(dry_run=False)

    assert world.stored(2, 20) == {"indexes_meta": {}}
    assert stats["failed_projects"] == 1
    assert stats["removed"] == 1


def test_entry_changed_mid_run_is_kept(mod, monkeypatch):
    """AC-7: the lookup returns after someone re-scheduled the index."""
    world = _world(mod, monkeypatch)
    original = world._present

    def present_then_reschedule(conn, schema):
        result = original(conn, schema)
        world.projects[1].stored[10]["indexes_meta"]["Ghost"]["schedules"]["7"]["cron"] = "0 4 * * *"
        return result

    world._present = present_then_reschedule
    stats = world.run(dry_run=False)

    assert "Ghost" in world.stored(1, 10)["indexes_meta"]
    assert stats["changed"] == 1 and stats["removed"] == 0


def test_summary(mod, monkeypatch):
    """AC-8"""
    projects = {
        1: Project([_toolkit(10, {"Ghost": copy.deepcopy(GHOST), "Live": copy.deepcopy(LIVE)}),
                    _toolkit(11, {"Ghost": copy.deepcopy(GHOST)})]),
        2: Project([], broken=True),
    }
    world = World(mod, monkeypatch, projects, present={10: {"Live"}},
                  lookup_error={11: RuntimeError("timeout")})
    stats = world.run(dry_run=False)

    assert {k: stats[k] for k in ("projects", "toolkits", "orphans", "removed", "changed",
                                  "skipped", "failed_projects")} == {
        "projects": 2, "toolkits": 2, "orphans": 1, "removed": 1, "changed": 0,
        "skipped": 1, "failed_projects": 1,
    }
    summary = world.log.lines[-1][1]
    for key in ("projects=2", "toolkits=2", "orphans=1", "removed=1", "skipped=1"):
        assert key in summary


def test_second_run_is_a_no_op(mod, monkeypatch):
    """AC-9"""
    world = _world(mod, monkeypatch)
    world.run(dry_run=False)
    commits = world.projects[1].commits

    stats = world.run(dry_run=False)

    assert stats["orphans"] == 0
    assert world.projects[1].commits == commits


def test_missing_vector_schema_orphans_all(mod, monkeypatch):
    """AC-10: present_index_names returns nothing for a reachable DB without the schema."""
    projects = {1: Project([_toolkit(10, {"A": copy.deepcopy(LIVE), "B": copy.deepcopy(GHOST)})])}
    world = World(mod, monkeypatch, projects, present={})
    stats = world.run(dry_run=False)

    assert world.stored(1, 10) == {"indexes_meta": {}}
    assert stats["removed"] == 2


def test_toolkit_without_schedules_is_not_looked_up(mod, monkeypatch):
    projects = {1: Project([_toolkit(10, {})])}
    world = World(mod, monkeypatch, projects, present={})
    world.run(dry_run=False)

    assert world.lookups == []


def test_failed_removal_keeps_the_entry_and_moves_on(mod, monkeypatch):
    """A commit failure on one toolkit is rolled back and does not stop the next toolkit."""
    projects = {1: Project([_toolkit(10, {"Ghost": copy.deepcopy(GHOST)}),
                            _toolkit(11, {"Ghost": copy.deepcopy(GHOST)})],
                           refuse_commit_for={10})}
    world = World(mod, monkeypatch, projects, present={})
    stats = world.run(dry_run=False)

    assert world.stored(1, 10) == {"indexes_meta": {"Ghost": GHOST}}
    assert world.stored(1, 11) == {"indexes_meta": {}}
    assert stats["skipped"] == 1 and stats["removed"] == 1 and stats["failed_projects"] == 0
