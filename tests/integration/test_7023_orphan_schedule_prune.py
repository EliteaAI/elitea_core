"""Issue #7023 - a schedule whose index row stays gone for the grace is removed, not fired forever.

Run via:
    python tests/run_tests.py integration/test_7023_orphan_schedule_prune.py -v
"""

import contextlib
import copy
import pathlib
import sys
import time
import types
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_6583c_tick_behaviour import Recorder, _due_schedule, tick  # noqa: E402,F401

STAMPED_AT = "2026-01-01T00:00:00+00:00"


class StoredToolkit:
    """Project session over one toolkit row; refresh() rebinds meta like SQLAlchemy does."""

    def __init__(self, toolkit, *, on_refresh=None, refuse_dropping_ghost=False):
        self.toolkit = toolkit
        self.stored_meta = copy.deepcopy(toolkit.meta)
        self.on_refresh = on_refresh
        self.refuse_dropping_ghost = refuse_dropping_ghost

    def query(self, *a, **k):
        rows = [self.toolkit]
        return types.SimpleNamespace(filter=lambda *a, **k: types.SimpleNamespace(all=lambda: rows))

    def refresh(self, toolkit):
        if self.on_refresh:
            self.on_refresh(self.stored_meta)
        toolkit.meta = copy.deepcopy(self.stored_meta)

    def commit(self):
        if self.refuse_dropping_ghost and "Ghost" not in self.toolkit.meta["indexes_meta"]:
            raise RuntimeError("could not serialize access")
        self.stored_meta = copy.deepcopy(self.toolkit.meta)

    def rollback(self):
        self.toolkit.meta = copy.deepcopy(self.stored_meta)


class PgVector:
    """Answers the index_meta lookup per index name; records every lookup."""

    def __init__(self, present, *, raises=False):
        self.present, self.raises, self.lookups = set(present), raises, []

    def session(self, *a, **k):
        store = self

        class _Query:
            def filter(self, *criteria):
                self.name = criteria[-1].right.value
                return self

            def first(self):
                store.lookups.append(self.name)
                if store.raises:
                    raise RuntimeError("relation does not exist")
                if self.name not in store.present:
                    return None
                return types.SimpleNamespace(id=1, cmetadata={
                    "state": "completed", "updated_on": time.time()})

        return contextlib.nullcontext(types.SimpleNamespace(query=lambda *a, **k: _Query()))


def _environment(tick, monkeypatch, *, pgvector, ghost_missing_since=None, **session_options):
    rec = Recorder(tick, monkeypatch, schedule=_due_schedule())
    ghost = {"schedules": {"7": _due_schedule(), "-1": _due_schedule()}}
    if ghost_missing_since:
        ghost["missing_since"] = ghost_missing_since
    rec.toolkit.meta = {"indexes_meta": {"Ghost": ghost, "IdxA": {"schedules": {"7": _due_schedule()}}}}
    stored = StoredToolkit(rec.toolkit, **session_options)
    monkeypatch.setattr(tick, "db", types.SimpleNamespace(
        get_session=lambda pid: contextlib.nullcontext(stored)))
    monkeypatch.setattr(tick, "get_session_for_schema", pgvector.session)
    return rec, stored


def _hours_ago(hours):
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def _ghost(stored):
    return stored.stored_meta["indexes_meta"].get("Ghost")


def test_the_first_miss_starts_the_grace_without_removing_anything(tick, monkeypatch):
    pgvector = PgVector(present={"IdxA"})
    rec, stored = _environment(tick, monkeypatch, pgvector=pgvector)

    rec.run(ticks=1)

    ghost = _ghost(stored)
    assert sorted(ghost["schedules"]) == ["-1", "7"]
    started = datetime.fromisoformat(ghost["missing_since"])
    assert datetime.now(timezone.utc) - started < timedelta(minutes=1)
    assert ghost["schedules"]["7"]["last_run"] != STAMPED_AT
    assert rec.dispatched == 1


def test_a_miss_inside_the_grace_keeps_the_schedules_and_the_start(tick, monkeypatch):
    missing_since = _hours_ago(23)
    pgvector = PgVector(present={"IdxA"})
    rec, stored = _environment(tick, monkeypatch, pgvector=pgvector,
                               ghost_missing_since=missing_since)

    rec.run(ticks=1)

    assert _ghost(stored)["missing_since"] == missing_since
    assert sorted(_ghost(stored)["schedules"]) == ["-1", "7"]


def test_a_miss_after_the_grace_removes_the_schedules_of_every_owner(tick, monkeypatch):
    pgvector = PgVector(present={"IdxA"})
    rec, stored = _environment(tick, monkeypatch, pgvector=pgvector,
                               ghost_missing_since=_hours_ago(25))

    rec.run(ticks=1)

    assert sorted(stored.stored_meta["indexes_meta"]) == ["IdxA"]
    assert rec.dispatched == 1
    assert pgvector.lookups.count("Ghost") == 1


def test_a_removed_index_stays_quiet_on_later_ticks(tick, monkeypatch):
    pgvector = PgVector(present={"IdxA"})
    rec, _ = _environment(tick, monkeypatch, pgvector=pgvector,
                          ghost_missing_since=_hours_ago(25))

    rec.run(ticks=3)

    assert pgvector.lookups.count("Ghost") == 1


def test_an_index_found_again_ends_the_grace(tick, monkeypatch):
    pgvector = PgVector(present={"IdxA", "Ghost"})
    rec, stored = _environment(tick, monkeypatch, pgvector=pgvector,
                               ghost_missing_since=_hours_ago(23))

    rec.run(ticks=1)

    assert "missing_since" not in _ghost(stored)
    assert sorted(_ghost(stored)["schedules"]) == ["-1", "7"]


def test_a_failed_lookup_never_starts_the_grace_or_removes_schedules(tick, monkeypatch):
    pgvector = PgVector(present={"IdxA"}, raises=True)
    rec, stored = _environment(tick, monkeypatch, pgvector=pgvector)

    rec.run(ticks=2)

    assert "missing_since" not in _ghost(stored)
    assert _ghost(stored)["schedules"]["7"]["last_run"] == STAMPED_AT


def test_a_failed_lookup_after_the_grace_never_removes_schedules(tick, monkeypatch):
    pgvector = PgVector(present={"IdxA"}, raises=True)
    rec, stored = _environment(tick, monkeypatch, pgvector=pgvector,
                               ghost_missing_since=_hours_ago(25))

    rec.run(ticks=2)

    assert sorted(_ghost(stored)["schedules"]) == ["-1", "7"]


def test_an_index_recreated_after_the_lookup_keeps_its_new_schedule(tick, monkeypatch):
    pgvector = PgVector(present={"IdxA"})

    def owner_recreates_the_index_and_reschedules(stored_meta):
        if "Ghost" in pgvector.lookups and "Ghost" not in pgvector.present:
            pgvector.present.add("Ghost")
            stored_meta["indexes_meta"]["Ghost"]["schedules"]["7"]["cron"] = "0 4 * * *"

    rec, stored = _environment(tick, monkeypatch, pgvector=pgvector,
                               ghost_missing_since=_hours_ago(25),
                               on_refresh=owner_recreates_the_index_and_reschedules)

    rec.run(ticks=1)

    assert _ghost(stored)["schedules"]["7"]["cron"] == "0 4 * * *"


def test_a_removal_that_cannot_commit_falls_back_to_moving_the_cursor(tick, monkeypatch):
    pgvector = PgVector(present={"IdxA"})
    rec, stored = _environment(tick, monkeypatch, pgvector=pgvector,
                               ghost_missing_since=_hours_ago(25), refuse_dropping_ghost=True)

    rec.run(ticks=1)

    assert sorted(_ghost(stored)["schedules"]) == ["-1", "7"]
    assert _ghost(stored)["schedules"]["7"]["last_run"] != STAMPED_AT
