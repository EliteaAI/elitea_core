"""EL-7011 US-24 R1.1, R1.2, R3.3, R3.4, R4.1, R4.2: one project moves under its advisory lock, with a report.

Real: `migrate_project` of utils/legacy_schedule_migration.py, its selector and the schedule model over
the SQLite tables of the US-01 integration test. SQLite has no advisory lock: the lock connection is
the fake of the tick test, so the test pins the key and the order of lock, session and unlock.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_legacy_migration_task.py
"""
import contextlib
import sys
import types

import pytest
from integration.test_7011_entity_schedule_tables import (  # noqa: F401
    REAL_SQLALCHEMY,
    parents,
    session,
    tables,
)
from integration.test_7011_legacy_migration_version import (
    JSON,
    MOVED,
    SETTINGS,
    Mapped,
    Session,
    mapped_column,
    select,
)
from unit._tool_spend_loader import load
from unit.rpc.test_7011_tick_legacy_step import LockConnection
from unit.test_7011_legacy_migration_rows import TRIGGER, migration  # noqa: F401 -- fixture reuse

PROJECT = 5
KEY = (7011 << 32) + PROJECT
String = REAL_SQLALCHEMY["sqlalchemy"].String

# version id: (agent_type, trigger); 6 and 7 are not legacy schedules, 8 has no parent row (insert fails)
VERSIONS = {
    1: ("pipeline", TRIGGER),
    2: ("pipeline", {**TRIGGER, "cron": "not a cron"}),
    3: ("pipeline", {**TRIGGER, "created_by": 8}),
    4: ("pipeline", {**TRIGGER, "cron": "* * * * *"}),
    5: ("pipeline", {**TRIGGER, "created_by": None}),
    6: ("pipeline", {"type": "chat_message"}),
    7: ("openai", TRIGGER),
    8: ("pipeline", TRIGGER),
}
FOUND = [1, 2, 3, 4, 5, 8]
AUTHOR = 9  # holds the row of a trigger without a usable creator (R3.1)


class BlockingLock(LockConnection):
    """The tick's lock fake, also answering the blocking pg_advisory_lock of the migration."""

    def execute(self, statement, params=None):
        if "lock_timeout" in str(statement):
            return types.SimpleNamespace(scalar=lambda: None)
        if "pg_advisory_lock(" in str(statement):
            self.world.locks.append(("block", params["key"]))
            return types.SimpleNamespace(scalar=lambda: None)
        return super().execute(statement, params)


@pytest.fixture
def project(tables, session, parents, migration, monkeypatch):  # noqa: F811 -- the fixtures are imported
    """(migration module, events, reader) for project 5 holding the versions of VERSIONS.

    `events` lists lock calls and session opens in order. The reader returns (schedule rows as
    (version id, enabled, disabled reason), pipeline_settings by version id) through a fresh session.
    """
    models, engine = tables[0], tables[1]
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.entity_schedule", models)

    class App(models.db.Base):
        __tablename__ = "legacy_task_apps_7011"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String)

    class Version(models.db.Base):
        __tablename__ = "legacy_task_versions_7011"
        id: Mapped[int] = mapped_column(primary_key=True)
        application_id: Mapped[int]
        name: Mapped[str] = mapped_column(String)
        agent_type: Mapped[str] = mapped_column(String)
        author_id: Mapped[int]
        pipeline_settings: Mapped[dict] = mapped_column(JSON)

    App.__table__.create(engine)
    Version.__table__.create(engine)
    session.execute(parents["application_versions"].insert(), [{"id": 3}, {"id": 4}, {"id": 5}])
    session.add(App(id=1, name="Nightly"))
    session.add_all([Version(id=v, application_id=1, name=f"v{v}", agent_type=kind, author_id=AUTHOR,
                             pipeline_settings={**SETTINGS, "trigger": trigger})
                     for v, (kind, trigger) in VERSIONS.items()])
    session.commit()

    for name, attrs in {"models.all": {"Application": App, "ApplicationVersion": Version},
                        "models.enums.all": {"AgentTypes": types.SimpleNamespace(
                            pipeline=types.SimpleNamespace(value="pipeline"))}}.items():
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + name, types.SimpleNamespace(**attrs))
    load("utils/schedule_claim.py", fresh=True)
    monkeypatch.setattr(migration, "check_owner_access",
                        lambda project_id, owner: "owner lost access" if owner == 8 else None)

    events = []

    @contextlib.contextmanager
    def get_session(project_id):
        events.append(("session", project_id))
        with Session(engine) as project_session:
            yield project_session

    lock_world = types.SimpleNamespace(locks=events, held=set())
    db = types.SimpleNamespace(engine=types.SimpleNamespace(connect=lambda: BlockingLock(lock_world)),
                               get_session=get_session)
    monkeypatch.setattr(sys.modules["tools"], "db", db, raising=False)

    def stored():
        with Session(engine) as fresh:
            rows = [(r.version_id, r.enabled, r.disabled_reason)
                    for r in fresh.scalars(select(models.EntitySchedule).order_by(models.EntitySchedule.version_id))]
            return rows, {v.id: v.pipeline_settings for v in fresh.scalars(select(Version))}

    return migration, events, stored


BEFORE = {v: {**SETTINGS, "trigger": trigger} for v, (_, trigger) in VERSIONS.items()}


def _expected(enabled, errors):
    return {
        "project_id": PROJECT, "found": len(FOUND), "migrated_enabled": enabled,
        "migrated_disabled": [{"version_id": 2, "created_by": 7, "reason": "invalid_cron"},
                              {"version_id": 3, "created_by": 8, "reason": "owner_lost_access"},
                              {"version_id": 5, "created_by": AUTHOR, "reason": "owner_lost_access"}],
        "below_minimum_interval": [{"version_id": 4, "cron": "* * * * *"}],
        "above_project_cap": False, "errors": errors,
        "original_triggers": [{"version_id": v, "trigger": VERSIONS[v][1]} for v in FOUND],
    }


# TC-US24-01 / R1.1, R1.2: a dry run reports what would move and writes no row and no trigger
def test_7011_r1_1_r1_2_dry_run_reports_and_writes_nothing(project):
    module, _events, stored = project
    report = module.migrate_project(PROJECT, dry_run=True)
    assert report == _expected(3, [])
    assert stored() == ([], BEFORE)


# TC-US24 / R3.3: a real run moves each version, reports disabled rows, the floor and the errors
def test_7011_r3_3_real_run_moves_and_reports(project):
    module, _events, stored = project
    report = module.migrate_project(PROJECT, dry_run=False)
    errors = report.pop("errors")
    assert [e["version_id"] for e in errors] == [8]
    assert {**report, "errors": []} == _expected(2, [])
    rows, settings = stored()
    assert rows == [(1, True, None), (2, False, "invalid_cron"), (3, False, "owner_lost_access"), (4, True, None),
                    (5, False, "owner_lost_access")]
    assert settings == {**BEFORE, **{v: MOVED for v in (1, 2, 3, 4, 5)}}


# TC-US24 / R3.3: the report flags a project whose enabled rows pass the cap; the rows still move
def test_7011_r3_3_report_flags_the_project_cap(project, monkeypatch):
    module, _events, stored = project
    # schedule_timing keeps the `tools` stub of the first suite that loaded it
    config = sys.modules["plugins.elitea_core.utils.schedule_timing"].this.module.descriptor.config
    monkeypatch.setitem(config, "scheduler", {"schedules": {"max_enabled_per_project": 1}})
    assert module.migrate_project(PROJECT, dry_run=False)["above_project_cap"] is True
    assert [r[0] for r in stored()[0]] == [1, 2, 3, 4, 5]


# TC-US24 / R3.4: a second run finds only the versions that failed and changes nothing
def test_7011_r3_4_second_run_finds_only_the_failed(project):
    module, _events, stored = project
    module.migrate_project(PROJECT, dry_run=False)
    after = stored()
    report = module.migrate_project(PROJECT, dry_run=False)
    assert (report["found"], report["migrated_enabled"], report["migrated_disabled"]) == (1, 0, [])
    assert [e["version_id"] for e in report["errors"]] == [8]
    assert stored() == after


# TC-US24 / R4.1, R4.2: the project lock of the tick is held around the session and released after
def test_7011_r4_1_r4_2_lock_wraps_the_project(project):
    module, events, _stored = project
    module.migrate_project(PROJECT, dry_run=True)
    assert events == [("block", KEY), ("session", PROJECT), ("unlock", KEY)]


# TC-US24 / R4.2: a project that fails midway releases its lock and raises for the task report
def test_7011_r4_2_failed_project_releases_the_lock(project, monkeypatch):
    module, events, _stored = project
    monkeypatch.setattr(sys.modules["tools"].db, "get_session", lambda project_id: 1 / 0)
    with pytest.raises(ZeroDivisionError):
        module.migrate_project(PROJECT, dry_run=False)
    assert events == [("block", KEY), ("unlock", KEY)]
