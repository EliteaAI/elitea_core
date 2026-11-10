"""Deleting an index or a toolkit removes the index schedule row and the Test runs of the index (SQLite, real models)."""
import sys
from unittest.mock import MagicMock

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs
from unit import test_toolkit_index_schedule_service as service_env

PACKAGE, PLUGIN_ROOT, env = service_env.PACKAGE, service_env.PLUGIN_ROOT, service_env.env

PROJECT_ID = 7


@pytest.fixture
def tools_env(env):
    return load_tools_env(env)


def load_tools_env(env):
    """The SQLite environment with `utils/application_tools.py` loaded over it."""
    sys.modules[f"{PACKAGE}.models.all"].EntityToolMapping = MagicMock()
    for name in ("models.indexer", "utils.exceptions", "utils.utils"):
        sys.modules[f"{PACKAGE}.{name}"] = sqlite_db.LenientModule(f"{PACKAGE}.{name}")
    env.module = load_module_with_stubs(PLUGIN_ROOT / "utils" / "application_tools.py", f"{PACKAGE}.utils.application_tools")
    env.runs = sys.modules[f"{PACKAGE}.models.entity_schedule"].EntityScheduleRun
    return env


def add_test_run(env, toolkit_id, index_name):
    with env.session_factory() as session:
        session.execute(sqlite_db.sa.insert(env.runs.__table__).values(
            schedule_id=None, trigger="test", status="completed", application_id=None,
            form={"entity_type": "toolkit_index", "toolkit_id": toolkit_id, "index_name": index_name}))
        session.commit()


def run_index_labels(env):
    with env.session_factory() as session:
        return sorted(row.form["index_name"] + str(row.form["toolkit_id"]) for row in session.execute(
            sqlite_db.sa.select(env.runs.__table__.c.form)))


def schedule_indexes(env):
    with env.session_factory() as session:
        return sorted(row.index_name for row in session.query(env.EntitySchedule).all())


def test_removing_an_index_deletes_its_row_and_test_runs_only(tools_env):
    tools_env.seed_index("docs")
    tools_env.seed_index("other")
    for toolkit_id, name in ((3, "docs"), (3, "other"), (4, "docs")):
        add_test_run(tools_env, toolkit_id, name)
    result = tools_env.module.clean_up_schedule_in_toolkit(PROJECT_ID, 3, "docs")
    assert result == ({"ok": True}, 200)
    assert schedule_indexes(tools_env) == ["other"]
    assert run_index_labels(tools_env) == ["docs4", "other3"]


def test_removing_an_index_without_a_settings_entry_still_deletes_its_row(tools_env):
    # After the migration the toolkit JSON has no entry for the index, but the row is there
    tools_env.seed_index("docs")
    assert tools_env.module.clean_up_schedule_in_toolkit(PROJECT_ID, 3, "docs") == ({"ok": True}, 200)
    assert schedule_indexes(tools_env) == []


def test_removing_an_index_of_a_missing_toolkit_changes_nothing(tools_env):
    tools_env.seed_index("docs")
    body, code = tools_env.module.clean_up_schedule_in_toolkit(PROJECT_ID, 99, "docs")
    assert code == 404 and body["ok"] is False
    assert schedule_indexes(tools_env) == ["docs"]


def test_removing_a_toolkit_deletes_the_test_runs_of_all_its_indexes(tools_env):
    for toolkit_id, name in ((3, "docs"), (3, "other"), (4, "docs")):
        add_test_run(tools_env, toolkit_id, name)
    with tools_env.session_factory() as session:
        tools_env.module._delete_index_test_runs(session, 3)  # noqa: SLF001
        session.commit()
    assert run_index_labels(tools_env) == ["docs4"]
