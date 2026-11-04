"""EL-7011 US-14 - R2.1 and R2.3: disable the schedules of deleted versions in the caller's transaction.

`utils/schedule_lifecycle.py` is loaded alone behind a stand-in for the schedule model. The session is a
recorder, so the test reads the SQL the helper sends (PostgreSQL dialect) and the calls it never makes.
"""
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs
from sqlalchemy import Boolean, Integer, String
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PKG = 'plugins.elitea_core'


class Base(DeclarativeBase):
    pass


class EntitySchedule(Base):
    """The columns of the real model that the helper touches, on a plain base."""
    __tablename__ = 'entity_schedules'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    created_by: Mapped[int] = mapped_column(Integer)
    version_id: Mapped[int | None] = mapped_column(Integer)
    enabled: Mapped[bool] = mapped_column(Boolean)
    disabled_reason: Mapped[str | None] = mapped_column(String)
    updated_at: Mapped[str | None] = mapped_column(String)


class RecordingSession:
    """Keeps every statement; `execute` returns the rows set in `rows`."""

    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements = []
        self.calls = []

    def execute(self, statement):
        self.statements.append(statement)
        return types.SimpleNamespace(mappings=lambda: types.SimpleNamespace(all=lambda: self.rows))

    def commit(self):
        self.calls.append('commit')

    def rollback(self):
        self.calls.append('rollback')

    def flush(self):
        self.calls.append('flush')


@pytest.fixture
def lifecycle(isolated_sys_modules):
    for name in (PKG, f'{PKG}.models', f'{PKG}.utils'):
        sys.modules[name] = types.ModuleType(name)
        sys.modules[name].__path__ = []
    schedule_models = types.ModuleType(f'{PKG}.models.entity_schedule')
    schedule_models.EntitySchedule = EntitySchedule
    sys.modules[schedule_models.__name__] = schedule_models
    return load_module_with_stubs(
        PLUGIN_ROOT / 'utils/schedule_lifecycle.py', f'{PKG}.utils.schedule_lifecycle'
    )


def _sql(statement):
    return str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={'literal_binds': True}))


def test_7011_r21_disable_sets_enabled_false_and_reason_for_every_schedule_of_the_versions(lifecycle):
    """R2.1 (TC-US14-04, TC-US14-05): one UPDATE, paused rows included, owners returned."""
    rows = [{'id': 1, 'name': 'A', 'created_by': 10}, {'id': 3, 'name': 'C', 'created_by': 11}]
    session = RecordingSession(rows)

    disabled = lifecycle.disable_version_schedules(session, [8, 9])

    assert disabled == rows
    assert len(session.statements) == 1
    sql = _sql(session.statements[0])
    assert 'UPDATE entity_schedules SET enabled=false' in sql
    assert "disabled_reason='version_deleted'" in sql
    assert 'updated_at=now()' in sql
    assert 'version_id IN (8, 9)' in sql
    assert 'enabled' not in sql.split('WHERE')[1]  # a paused schedule is disabled too
    assert 'RETURNING entity_schedules.id, entity_schedules.name, entity_schedules.created_by' in sql


def test_7011_r23_disable_leaves_the_commit_and_the_rollback_to_the_caller(lifecycle):
    """R2.3 (TC-US14-06): the helper never ends the transaction, so a refused delete keeps the schedules."""
    session = RecordingSession([{'id': 1, 'name': 'A', 'created_by': 10}])

    lifecycle.disable_version_schedules(session, [8])

    assert session.calls == []


@pytest.mark.parametrize('version_ids', [[], None])
def test_7011_r23_disable_without_versions_sends_no_statement(lifecycle, version_ids):
    """R2.3 (TC-US14-07 edge): nothing to disable means no UPDATE and an empty list."""
    session = RecordingSession()

    assert lifecycle.disable_version_schedules(session, version_ids) == []
    assert session.statements == []
