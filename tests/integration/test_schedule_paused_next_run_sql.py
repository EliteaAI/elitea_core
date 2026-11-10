"""The real statement of the paused next run admin task, run on SQLite against the real schedule table.

The project schema `p_7` is an attached database, so the schema-qualified `UPDATE p_7.entity_schedules` runs as
it is written. Only rows that are paused and still hold a next run change.

Run via:
    python tests/run_tests.py integration/test_schedule_paused_next_run_sql.py -v
"""
import pathlib
import sys
from datetime import UTC, datetime

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core'
PROJECT_ID = 7
STALE = datetime(2020, 1, 1, tzinfo=UTC)


class World:
    """The real statement, the real schedule table and one SQLite engine with the project schema attached."""

    def seed(self, enabled, next_run, name='Nightly'):
        return sqlite_db.seed_schedule(
            self.session_factory, self.EntitySchedule, 1, name=name, enabled=enabled, next_run=next_run)

    def row(self, schedule_id):
        with self.session_factory() as session:
            return session.get(self.EntitySchedule, schedule_id)

    def clear(self):
        statement = self.lifecycle.CLEAR_PAUSED_NEXT_RUN.format(pid=PROJECT_ID)
        with self.session_factory() as session:
            changed = session.execute(sqlite_db.sa.text(statement)).rowcount
            session.commit()
        return changed


@pytest.fixture
def world(monkeypatch):
    original = sys.modules.copy()
    for name, module in sqlite_db.REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    world = World()
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.models.pd', 'pylon', 'pylon.core'):
        sqlite_db.package(name)
    base, application, application_version = sqlite_db.install_parent_models(PACKAGE)

    engine = sqlite_db.sa.create_engine('sqlite://')
    sqlite_db.sa.event.listen(
        engine, 'connect', lambda connection, _: connection.execute(f"ATTACH DATABASE ':memory:' AS p_{PROJECT_ID}"))
    engine = engine.execution_options(schema_translate_map={'tenant': f'p_{PROJECT_ID}'})
    world.session_factory = lambda: sqlite_db.orm.Session(engine, expire_on_commit=False)

    def load(relative_path, name):
        return load_module_with_stubs(PLUGIN_ROOT / relative_path, f'{PACKAGE}.{name}')

    world.EntitySchedule = sqlite_db.create_schedule_table(load, base, engine)
    world.lifecycle = load('utils/schedule_lifecycle.py', 'utils.schedule_lifecycle')
    sqlite_db.add_agent_and_pipeline(world.session_factory, application, application_version)
    yield world
    for key in set(sys.modules) - set(original):
        sys.modules.pop(key, None)


def test_a_paused_schedule_with_a_stale_next_run_loses_it(world):
    paused = world.seed(enabled=False, next_run=STALE)

    assert world.clear() == 1
    assert world.row(paused).next_run is None
    assert world.row(paused).enabled is False


def test_an_enabled_schedule_keeps_its_next_run(world):
    enabled = world.seed(enabled=True, next_run=STALE)

    assert world.clear() == 0
    assert world.row(enabled).next_run.replace(tzinfo=UTC) == STALE
    assert world.row(enabled).enabled is True


def test_only_the_paused_rows_with_a_next_run_are_written_and_a_second_run_writes_none(world):
    stale = world.seed(name='stale', enabled=False, next_run=STALE)
    already_clear = world.seed(name='clear', enabled=False, next_run=None)
    enabled = world.seed(name='live', enabled=True, next_run=STALE)

    assert world.clear() == 1
    assert world.clear() == 0
    assert world.row(stale).next_run is None
    assert world.row(already_clear).next_run is None
    assert world.row(enabled).next_run is not None
