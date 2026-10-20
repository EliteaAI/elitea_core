"""EL-7011 US-03: cron, timezone, stop date and enabled cap of a schedule.

Contract under test: models/pd/entity_schedule.py (ScheduleTiming) and
utils/schedule_timing.py (compute_next_run, end_of_day, schedule_limits, check_enabled_cap).
"""
import sys
import types
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from croniter import croniter
from pydantic import ValidationError
from sqlalchemy import Column, Integer, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase
from unit._tool_spend_loader import load

TIMING = "utils/schedule_timing.py"
MODEL = "models/pd/entity_schedule.py"
DAILY = "0 9 * * *"
YEARLY = "0 0 1 1 *"
# 20:00 UTC is already the next day in Tokyo
NOW = datetime(2026, 6, 15, 20, 0, tzinfo=UTC)


class FakeSession:
    """Answers the cap's COUNT query with a fixed number and keeps the statement."""

    def __init__(self, enabled):
        self.enabled = enabled
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        return types.SimpleNamespace(scalar=lambda: self.enabled)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz)


@pytest.fixture
def config(monkeypatch):
    """The plugin descriptor config the limits are read from, changeable between calls."""
    settings = {}
    descriptor = types.SimpleNamespace(config=settings)
    stub = types.ModuleType("tools")
    stub.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=descriptor))
    monkeypatch.setitem(sys.modules, "tools", stub)
    return settings


def _set_limits(config, minimum=None, maximum=None):
    section = config.setdefault("scheduler", {}).setdefault("schedules", {})
    if minimum is not None:
        section["min_interval_minutes"] = minimum
    if maximum is not None:
        section["max_enabled_per_project"] = maximum


@pytest.fixture
def schedule_table(monkeypatch):
    """The real EntitySchedule model on a fresh base in the tenant schema."""
    class Base(DeclarativeBase):
        pass

    for parent in ("applications", "application_versions"):
        Table(parent, Base.metadata, Column("id", Integer, primary_key=True), schema="tenant")
    stubs = {
        "db": types.SimpleNamespace(Base=Base),
        "db_tools": types.SimpleNamespace(AbstractBaseMixin=type("AbstractBaseMixin", (), {})),
        "config": types.SimpleNamespace(POSTGRES_TENANT_SCHEMA="tenant"),
    }
    # A fresh package: another suite may leave its own `models` package under this key
    package = types.ModuleType("plugins.elitea_core.models")
    package.__path__ = []
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models", package)
    for name, stub in stubs.items():
        setattr(package, name, stub)
        monkeypatch.setitem(sys.modules, "plugins.elitea_core.models." + name, stub)
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.models.entity_schedule", None)
    return load("models/entity_schedule.py", fresh=True).EntitySchedule.__table__


@pytest.fixture
def timing(config, schedule_table, monkeypatch):
    for dotted in ("utils.schedule_timing", "models.pd.entity_schedule"):
        monkeypatch.setitem(sys.modules, "plugins.elitea_core." + dotted, None)
    module = load(TIMING, fresh=True)
    load("models/pd/index.py")  # the model reuses its cron checks
    module.model = load(MODEL, fresh=True)
    return module


def _error_fields(excinfo):
    return {error["loc"][0] for error in excinfo.value.errors() if error["loc"]}


@pytest.fixture
def frozen_now(timing, monkeypatch):
    monkeypatch.setattr(timing.model, "datetime", FrozenDatetime)


# R1.1 a cron needs exactly 5 parts
@pytest.mark.parametrize("cron", ["* * * *", "0 9 * * * *", "", "0 9 * * * * *"])
def test_el_7011_r1_1_cron_without_five_parts_is_rejected_on_cron(timing, cron):
    with pytest.raises(ValidationError) as excinfo:
        timing.model.ScheduleTiming(cron=cron, timezone="UTC")
    assert _error_fields(excinfo) == {"cron"}


def test_el_7011_r1_1_cron_with_five_parts_is_accepted(timing):
    assert timing.model.ScheduleTiming(cron=DAILY, timezone="UTC").cron == DAILY


# R1.2 croniter must parse the cron
@pytest.mark.parametrize("cron", ["61 * * * *", "a b c d e", "0 25 * * *"])
def test_el_7011_r1_2_unparseable_cron_is_rejected_on_cron(timing, cron):
    with pytest.raises(ValidationError) as excinfo:
        timing.model.ScheduleTiming(cron=cron, timezone="UTC")
    assert _error_fields(excinfo) == {"cron"}
    assert "invalid cron expression" in str(excinfo.value)


# R1.3 two firings at least the floor apart (default 5 minutes)
@pytest.mark.parametrize("cron", ["* * * * *", "*/4 * * * *", "0,3 * * * *"])
def test_el_7011_r1_3_cron_under_the_floor_is_rejected(timing, cron):
    with pytest.raises(ValidationError) as excinfo:
        timing.model.ScheduleTiming(cron=cron, timezone="UTC")
    assert _error_fields(excinfo) == {"cron"}
    assert "Runs must be at least 5 minutes apart" in str(excinfo.value)


@pytest.mark.parametrize("cron", ["*/5 * * * *", "*/10 * * * *", DAILY])
def test_el_7011_r1_3_cron_at_or_over_the_floor_is_accepted(timing, cron):
    assert timing.model.ScheduleTiming(cron=cron, timezone="UTC").cron == cron


def test_el_7011_r1_3_default_floor_is_five_minutes(timing):
    assert timing.schedule_limits().min_interval_minutes == 5


# R1.4 an admin change of the floor applies to the next validation
def test_el_7011_r1_4_raised_floor_applies_to_the_next_validation(timing, config):
    assert timing.model.ScheduleTiming(cron="*/5 * * * *", timezone="UTC")
    _set_limits(config, minimum=10)
    with pytest.raises(ValidationError) as excinfo:
        timing.model.ScheduleTiming(cron="*/5 * * * *", timezone="UTC")
    assert "Runs must be at least 10 minutes apart" in str(excinfo.value)


def test_el_7011_r1_4_lowered_floor_applies_to_the_next_validation(timing, config):
    _set_limits(config, minimum=1)
    assert timing.model.ScheduleTiming(cron="* * * * *", timezone="UTC")
    assert timing.schedule_limits().min_interval_minutes == 1


# R2.1 the cap of enabled schedules per project (default 50)
def test_el_7011_r2_1_default_cap_is_fifty(timing):
    assert timing.schedule_limits().max_enabled_per_project == 50


def test_el_7011_r2_1_enabling_past_the_cap_raises_limit_error(timing):
    with pytest.raises(timing.ScheduleLimitError) as excinfo:
        timing.check_enabled_cap(FakeSession(enabled=50))
    assert excinfo.value.limit == 50


def test_el_7011_r2_1_enabling_up_to_the_cap_is_allowed(timing):
    timing.check_enabled_cap(FakeSession(enabled=49))


# R2.2 a disabled save adds nothing to the count
def test_el_7011_r2_2_counts_only_enabled_rows_of_the_tenant_table(timing):
    session = FakeSession(enabled=3)
    timing.check_enabled_cap(session)
    sql = " ".join(str(session.statements[0].compile(dialect=postgresql.dialect())).split())
    assert sql.startswith("SELECT count(*)")
    assert "FROM tenant.entity_schedules WHERE tenant.entity_schedules.enabled IS true" in sql


def test_el_7011_r2_2_row_already_enabled_adds_nothing(timing):
    timing.check_enabled_cap(FakeSession(enabled=50), adding=0)


# R2.3 an admin change of the cap applies to the next check
def test_el_7011_r2_3_lowered_cap_applies_to_the_next_check(timing, config):
    timing.check_enabled_cap(FakeSession(enabled=2))
    _set_limits(config, maximum=2)
    with pytest.raises(timing.ScheduleLimitError) as excinfo:
        timing.check_enabled_cap(FakeSession(enabled=2))
    assert excinfo.value.limit == 2


def test_el_7011_r2_3_raised_cap_applies_to_the_next_check(timing, config):
    _set_limits(config, maximum=100)
    timing.check_enabled_cap(FakeSession(enabled=60))


# R3.1 the timezone is an IANA name
@pytest.mark.parametrize("name", ["Mars/Olympus", "", "EST5EDT5", "not a zone"])
def test_el_7011_r3_1_unknown_timezone_is_rejected_on_timezone(timing, name):
    with pytest.raises(ValidationError) as excinfo:
        timing.model.ScheduleTiming(cron=DAILY, timezone=name)
    assert "timezone" in _error_fields(excinfo)


@pytest.mark.parametrize("name", ["UTC", "Europe/Kyiv", "America/New_York"])
def test_el_7011_r3_1_iana_timezone_is_accepted(timing, name):
    assert timing.model.ScheduleTiming(cron=DAILY, timezone=name).timezone == name


# R3.2 next run: the next firing after now in the schedule timezone, in UTC
@pytest.mark.parametrize("after, expected", [
    (datetime(2026, 1, 10, 8, 0, tzinfo=UTC), datetime(2026, 1, 11, 7, 0, tzinfo=UTC)),
    (datetime(2026, 7, 10, 8, 0, tzinfo=UTC), datetime(2026, 7, 11, 6, 0, tzinfo=UTC)),
])
def test_el_7011_r3_2_next_run_is_local_firing_in_utc(timing, after, expected):
    result = timing.compute_next_run(DAILY, "Europe/Kyiv", after)
    assert result == expected
    assert result.utcoffset() == timedelta(0)


def test_el_7011_r3_2_next_run_is_strictly_after_the_firing_instant(timing):
    after = datetime(2026, 1, 10, 7, 0, tzinfo=UTC)
    assert timing.compute_next_run(DAILY, "Europe/Kyiv", after) == datetime(2026, 1, 11, 7, 0, tzinfo=UTC)


# R3.3 a clock change gives the firing croniter gives for that timezone
@pytest.mark.parametrize("cron, zone, after", [
    ("30 2 * * *", "America/New_York", datetime(2026, 3, 8, 5, 0, tzinfo=UTC)),
    ("30 1 * * *", "America/New_York", datetime(2026, 11, 1, 4, 0, tzinfo=UTC)),
])
def test_el_7011_r3_3_clock_change_follows_croniter(timing, cron, zone, after):
    tz = ZoneInfo(zone)
    expected = croniter(cron, after.astimezone(tz)).get_next(datetime).astimezone(UTC)
    result = timing.compute_next_run(cron, zone, after)
    assert result == expected
    assert result > after
    assert result.utcoffset() == timedelta(0)


# R4.1 the stop date is stored as the end of that day in the schedule timezone, in UTC
@pytest.mark.parametrize("zone", ["UTC", "Asia/Tokyo", "America/New_York"])
def test_el_7011_r4_1_end_of_day_is_local_midnight_minus_one_second_in_utc(timing, zone):
    day = date(2026, 6, 15)
    expected = datetime(2026, 6, 15, 23, 59, 59, tzinfo=ZoneInfo(zone)).astimezone(UTC)
    result = timing.end_of_day(day, zone)
    assert result == expected
    assert result.utcoffset() == timedelta(0)


def test_el_7011_r4_1_end_of_day_on_a_clock_change_day_uses_that_day_offset(timing):
    result = timing.end_of_day(date(2026, 3, 8), "America/New_York")
    assert result == datetime(2026, 3, 9, 3, 59, 59, tzinfo=UTC)


# R4.2 a stop date before today in the schedule timezone is rejected
def test_el_7011_r4_2_past_stop_date_is_rejected_on_expires_at(timing, frozen_now):
    # Today in UTC, yesterday in Tokyo
    with pytest.raises(ValidationError) as excinfo:
        timing.model.ScheduleTiming(cron=DAILY, timezone="Asia/Tokyo", expires_at=date(2026, 6, 15))
    assert _error_fields(excinfo) == {"expires_at"}
    assert "The stop date is in the past" in str(excinfo.value)


# R4.3 the next run must come before the end of the stop date
def test_el_7011_r4_3_stop_date_before_next_run_is_rejected_on_expires_at(timing, frozen_now):
    with pytest.raises(ValidationError) as excinfo:
        timing.model.ScheduleTiming(cron=YEARLY, timezone="UTC", expires_at=date(2026, 6, 15))
    assert _error_fields(excinfo) == {"expires_at"}
    assert "The schedule never runs before its stop date" in str(excinfo.value)


def test_el_7011_r4_3_stop_date_after_next_run_is_accepted(timing, frozen_now):
    # Today's 09:00 has passed; the next run is 09:00 on the stop date
    model = timing.model.ScheduleTiming(cron=DAILY, timezone="UTC", expires_at=date(2026, 6, 16))
    assert model.expires_at is not None


def test_el_7011_r4_3_no_stop_date_is_accepted(timing):
    assert timing.model.ScheduleTiming(cron=YEARLY, timezone="UTC").expires_at is None
