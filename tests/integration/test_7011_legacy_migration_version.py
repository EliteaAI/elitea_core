"""EL-7011 US-24 R2.4, R2.5: one legacy version moves in one transaction.

Real: `migrate_version` and `migrated_row` of utils/legacy_schedule_migration.py and the schedule model
over the SQLite tables of the US-01 integration test. The version is a small mapped class with a JSON
`pipeline_settings` column on the same Base.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_legacy_migration_version.py
"""
import sys
from datetime import UTC, datetime

import pytest
from integration.test_7011_entity_schedule_tables import (  # noqa: F401
    REAL_SQLALCHEMY,
    parents,
    session,
    tables,
)
from unit.test_7011_legacy_migration_rows import TRIGGER, migration  # noqa: F401 -- fixture reuse

_sa = REAL_SQLALCHEMY["sqlalchemy"]
JSON, select = _sa.JSON, _sa.select
IntegrityError = REAL_SQLALCHEMY["sqlalchemy.exc"].IntegrityError
_orm = REAL_SQLALCHEMY["sqlalchemy.orm"]
Mapped, Session, mapped_column = _orm.Mapped, _orm.Session, _orm.mapped_column

NOW = datetime(2026, 10, 20, 8, 30, tzinfo=UTC)
SETTINGS = {"trigger": TRIGGER, "entry_point": "start"}
MOVED = {**SETTINGS, "trigger": {"type": "chat_message"}}


@pytest.fixture
def world(tables, session, parents, migration, monkeypatch):  # noqa: F811 -- the fixtures are imported
    """(migration module, session, Version class, reader) with versions 1 and 2 holding a legacy trigger.

    The reader returns (schedule version ids, pipeline_settings by version id) through a fresh session.
    """
    models, engine = tables[0], tables[1]
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.entity_schedule", models)

    class Version(models.db.Base):
        __tablename__ = "legacy_versions_7011"
        id: Mapped[int] = mapped_column(primary_key=True)
        pipeline_settings: Mapped[dict] = mapped_column(JSON)

    def stored():
        with Session(engine) as fresh:
            ids = sorted(fresh.scalars(select(models.EntitySchedule.version_id)))
            return ids, {v.id: v.pipeline_settings for v in fresh.scalars(select(Version))}

    Version.__table__.create(engine)
    session.add_all([Version(id=1, pipeline_settings=SETTINGS), Version(id=2, pipeline_settings=SETTINGS)])
    session.commit()
    return migration, session, Version, stored


def _row(module, version_id, **overrides):
    return {**module.migrated_row(5, 1, version_id, "Nightly", "v1", TRIGGER, NOW), **overrides}


# TC-US24 / R2.4: the row and the chat_message trigger are committed together; other settings stay
def test_7011_r2_4_version_moves_in_one_transaction(world):
    module, db, Version, stored = world
    module.migrate_version(db, db.get(Version, 1), _row(module, 1))
    assert stored() == ([1], {1: MOVED, 2: SETTINGS})


# TC-US24 / R2.5: a failed insert rolls back the trigger too, raises for the report, the next version moves
def test_7011_r2_5_failed_version_rolls_back_and_next_goes_on(world):
    module, db, Version, stored = world
    with pytest.raises(IntegrityError):
        module.migrate_version(db, db.get(Version, 1), _row(module, 1, run_mode="cron"))
    assert stored() == ([], {1: SETTINGS, 2: SETTINGS})
    module.migrate_version(db, db.get(Version, 2), _row(module, 2))
    assert stored() == ([2], {1: SETTINGS, 2: MOVED})
