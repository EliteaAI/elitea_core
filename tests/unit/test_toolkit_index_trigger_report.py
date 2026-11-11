"""The trigger report lists toolkit index schedule rows and the legacy settings entries once (SQLite, real models)."""
import sys

import pytest
from fixtures.helpers import load_module_with_stubs
from sqlalchemy.exc import OperationalError
from unit import test_toolkit_index_schedule_service as service_env

OWNER_ID, PACKAGE, PLUGIN_ROOT, env = service_env.OWNER_ID, service_env.PACKAGE, service_env.PLUGIN_ROOT, service_env.env

BODY = {"cron": "0 3 * * *", "enabled": True, "created_by": 7}


@pytest.fixture
def report(env):  
    """`utils/trigger_report.py` loaded over the SQLite environment; `env.set_meta` writes the toolkit JSON."""
    sys.modules[f"{PACKAGE}.models.agent_identity"] = sys.modules["tools"]
    for name in ("trigger_principal", "trigger_report"):
        loaded = load_module_with_stubs(PLUGIN_ROOT / "utils" / f"{name}.py", f"{PACKAGE}.utils.{name}")
    env.report = loaded
    all_models = sys.modules[f"{PACKAGE}.models.all"]
    sys.modules[f"{PACKAGE}.models.elitea_tools"] = all_models

    def set_meta(indexes_meta):
        with env.session_factory() as session:
            session.get(all_models.EliteATool, 3).meta = {"indexes_meta": indexes_meta}
            session.commit()

    env.set_meta = set_meta
    return env


def index_schedules(env):
    with env.session_factory() as session:
        return env.report._index_schedules(session)  # noqa: SLF001


def test_a_row_is_listed_with_its_owner(report):
    report.seed_index("docs", enabled=False)
    assert index_schedules(report) == [(3, "docs", str(OWNER_ID), False, OWNER_ID)]


def test_a_legacy_entry_of_an_index_with_a_row_is_not_listed_again(report):
    report.seed_index("docs")
    report.set_meta({"docs": {"schedules": {"7": BODY}}})
    assert index_schedules(report) == [(3, "docs", str(OWNER_ID), True, OWNER_ID)]


def test_a_legacy_entry_without_a_row_is_still_listed(report):
    report.seed_index("docs")
    report.set_meta({"docs": {"schedules": {"7": BODY}}, "other": {"schedules": {"-1": {**BODY, "enabled": False}}}})
    assert index_schedules(report) == [(3, "docs", str(OWNER_ID), True, OWNER_ID), (3, "other", "-1", False, 7)]


def test_a_schema_without_the_index_columns_falls_back_to_the_legacy_entries(report):
    report.set_meta({"docs": {"schedules": {"7": BODY}}})

    class Session:
        """Raises on the row query, as PostgreSQL does for a missing column, then serves the legacy query."""

        def __init__(self, inner):
            self.inner = inner

        def begin_nested(self):
            raise OperationalError("select", {}, Exception("no such column: toolkit_id"))

        def execute(self, *args, **kwargs):
            return self.inner.execute(*args, **kwargs)

    with report.session_factory() as session:
        assert report.report._index_schedules(Session(session)) == [(3, "docs", "7", True, 7)]  # noqa: SLF001
