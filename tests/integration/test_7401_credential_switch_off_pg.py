"""#7401: switch off the triggers of one owner that use a credential, on a real PostgreSQL schema.

`disable_by_credential` filters on `credential_refs @> ["<uuid>"]` (GIN index) for the owner, schedules and
webhooks alike, and sends ONE notice. Set ELITEA_7011_PG_DSN as for test_7398_webhook_rows_pg.py.
"""
import importlib.util
import json
import pathlib
import sys
import types

import pytest
from integration.test_7398_webhook_rows_pg import DSN, SCHEMA, _migrate, world  # noqa: F401

pytestmark = pytest.mark.skipif(not DSN, reason="ELITEA_7011_PG_DSN is not set")
OWNER, OTHER = 3, 6


@pytest.fixture
def runs(world, monkeypatch):  # noqa: F811
    _migrate(world)
    notices = []
    package = "plugins.elitea_core"
    monkeypatch.setitem(sys.modules, f"{package}.models.enums", types.ModuleType(f"{package}.models.enums"))
    monkeypatch.setitem(sys.modules, f"{package}.models.enums.all", types.SimpleNamespace(
        NotificationEventTypes=types.SimpleNamespace(schedule_credential_expired="schedule_credential_expired")))
    monkeypatch.setitem(sys.modules, f"{package}.utils.schedule_notifications", types.SimpleNamespace(
        notify_schedule_event=lambda *args, **kwargs: notices.append((args, kwargs))))
    sys.modules["tools"].db.get_session = lambda _pid: world.session()
    _load(monkeypatch, "utils/error_text.py")
    module = _load(monkeypatch, "utils/schedule_runs.py")
    module.notices = notices
    return module


def _load(monkeypatch, relative):
    name = "plugins.elitea_core." + relative[:-3].replace("/", ".")
    path = pathlib.Path(__file__).resolve().parents[2] / relative
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def _row(world, name, owner, refs, kind="cron", enabled=True):  # noqa: F811
    cron = "'0 9 * * *', 'UTC'" if kind == "cron" else "NULL, NULL"
    world.sql(f"INSERT INTO {SCHEMA}.entity_schedules (entity_type, application_id, name, cron, timezone, created_by, "
              f"principal, enabled, trigger_kind, webhook_type, credential_refs) VALUES ('agent', 1, :name, {cron}, "
              f":owner, '{{}}', :enabled, :kind, :wtype, CAST(:refs AS jsonb))",
              name=name, owner=owner, enabled=enabled, kind=kind, wtype="custom" if kind == "webhook" else None,
              refs=None if refs is None else json.dumps(refs))


def test_7401_only_the_owners_enabled_triggers_using_the_credential_switch_off(world, runs):  # noqa: F811
    _row(world, "hook", OWNER, ["uuid-a", "uuid-b"], kind="webhook")
    _row(world, "nightly", OWNER, ["uuid-a"])
    _row(world, "other credential", OWNER, ["uuid-b"])
    _row(world, "no refs", OWNER, None)
    _row(world, "another owner", OTHER, ["uuid-a"])
    _row(world, "already off", OWNER, ["uuid-a"], enabled=False)

    rows = runs.disable_by_credential(1, OWNER, "uuid-a", "credential_expired", "team_outlook", False)

    assert sorted(row.name for row in rows) == ["hook", "nightly"]
    state = dict((name, (enabled, reason)) for name, enabled, reason in world.sql(
        f"SELECT name, enabled, disabled_reason FROM {SCHEMA}.entity_schedules"))
    assert state["hook"] == state["nightly"] == (False, "credential_expired")
    assert state["other credential"] == state["no refs"] == state["another owner"] == (True, None)
    assert state["already off"] == (False, None)  # keeps its own reason
    [(args, kwargs)] = runs.notices  # one notice for both
    assert args[1] == OWNER and kwargs["extra"]["more"] == 1 and kwargs["extra"]["credential"] == "team_outlook"


def test_7401_no_trigger_uses_it_means_no_notice(world, runs):  # noqa: F811
    _row(world, "nightly", OWNER, ["uuid-b"])

    assert runs.disable_by_credential(1, OWNER, "uuid-a", "credential_expired") == [] and runs.notices == []


def test_7401_the_lookup_uses_the_gin_index(world, runs):  # noqa: F811
    indexes = [name for (name,) in world.sql(
        "SELECT indexname FROM pg_indexes WHERE schemaname = :s AND tablename = 'entity_schedules'", s=SCHEMA)]
    assert "ix_entity_schedules_credential_refs" in indexes


def test_7401_the_trigger_that_found_it_is_switched_off_even_without_refs(world, runs):  # noqa: F811
    # A trigger saved before #7401 has no refs yet; its own run still switches it off
    _row(world, "old hook", OWNER, None, kind="webhook")
    _row(world, "untouched", OWNER, None)
    old_id = world.sql(f"SELECT id FROM {SCHEMA}.entity_schedules WHERE name = 'old hook'")[0][0]

    rows = runs.disable_by_credential(1, OWNER, "uuid-a", "credential_expired", also_id=old_id)

    assert [row.name for row in rows] == ["old hook"]
    assert world.sql(f"SELECT enabled FROM {SCHEMA}.entity_schedules WHERE name = 'untouched'") == [(True,)]
