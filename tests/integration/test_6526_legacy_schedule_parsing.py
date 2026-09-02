"""Issue #6526 - schedules written before the alita->elitea rename must still parse.

The scheduler builds a `ToolkitIndexingSchedule` from the stored dict on every tick. Rows
written before the rename carry `alita_title` inside `credentials`, and older rows carry no
`created_by`/`timezone`/`last_run` at all, so `parse_obj` raised and the tick logged
"Invalid schedule configuration" and skipped the schedule forever — the schedule looked
enabled in the UI but could never run. `configurations.expand_configuration` already reads
either title key, so accepting the legacy shape here loses nothing.

Run via:
    python tests/run_tests.py integration/test_6526_legacy_schedule_parsing.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def index_pd():
    """Load models/pd/index.py standalone (it has no plugin-relative imports)."""
    spec = importlib.util.spec_from_file_location(
        "elitea_core_models_pd_index", PLUGIN_ROOT / "models" / "pd" / "index.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _schedule(**overrides):
    base = {
        "cron": "56 15 * * *",
        "enabled": True,
        "created_by": 59,
        "timezone": "Asia/Yerevan",
        "last_run": "2026-09-02T11:55:34.140927+00:00",
        "credentials": {"private": False, "elitea_title": "test_ado_sam"},
    }
    base.update(overrides)
    return base


class TestLegacyCredentialTitle:
    def test_alita_title_is_accepted_as_elitea_title(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            _schedule(credentials={"private": False, "alita_title": "ado_creds"})
        )
        assert model.credentials.elitea_title == "ado_creds"

    def test_elitea_title_wins_when_both_are_present(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            _schedule(credentials={"elitea_title": "new", "alita_title": "old"})
        )
        assert model.credentials.elitea_title == "new"

    def test_credentials_with_neither_title_are_still_rejected(self, index_pd):
        with pytest.raises(Exception):
            index_pd.ToolkitIndexingSchedule.parse_obj(_schedule(credentials={"private": True}))


class TestLegacyMissingFields:
    def test_a_schedule_without_created_by_parses(self, index_pd):
        # A missing author only blocks resolving a *private* credential, which
        # resolve_credentials rejects on its own; it must not kill the whole schedule.
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            {k: v for k, v in _schedule().items() if k != "created_by"}
        )
        assert model.created_by is None

    def test_a_schedule_without_a_timezone_defaults_to_utc(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            {k: v for k, v in _schedule().items() if k != "timezone"}
        )
        assert model.timezone == "UTC"

    def test_a_schedule_without_last_run_is_immediately_due(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            {k: v for k, v in _schedule().items() if k != "last_run"}
        )
        assert model.last_run.startswith("1970-01-01")

    def test_an_explicit_null_last_run_is_treated_as_never_run(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(_schedule(last_run=None))
        assert model.last_run.startswith("1970-01-01")

    def test_an_explicit_null_timezone_is_treated_as_utc(self, index_pd):
        assert index_pd.ToolkitIndexingSchedule.parse_obj(_schedule(timezone=None)).timezone == "UTC"


class TestCurrentShapeUnchanged:
    def test_a_current_schedule_parses_as_before(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(_schedule())
        assert (model.created_by, model.timezone) == (59, "Asia/Yerevan")
        assert model.credentials.elitea_title == "test_ado_sam"

    def test_last_run_is_still_normalised_to_utc(self, index_pd):
        model = index_pd.ToolkitIndexingSchedule.parse_obj(
            _schedule(last_run="2026-09-02T15:55:34+04:00")
        )
        assert model.last_run == "2026-09-02T11:55:34+00:00"

    def test_an_invalid_timezone_is_still_rejected(self, index_pd):
        with pytest.raises(Exception):
            index_pd.ToolkitIndexingSchedule.parse_obj(_schedule(timezone="Mars/Olympus"))

    def test_an_invalid_cron_is_still_rejected(self, index_pd):
        with pytest.raises(Exception):
            index_pd.ToolkitIndexingSchedule.parse_obj(_schedule(cron="not a cron"))

    def test_a_zero_created_by_is_still_rejected(self, index_pd):
        with pytest.raises(Exception):
            index_pd.ToolkitIndexingSchedule.parse_obj(_schedule(created_by=0))
