"""The old index schedule routes for one more release: PATCH writes the schedule row, DELETE removes the old settings key."""
import sys
import types
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs
from sqlalchemy.dialects.postgresql import JSONB
from unit import test_toolkit_index_schedule_service as service_env

PACKAGE, PLUGIN_ROOT, env = service_env.PACKAGE, service_env.PLUGIN_ROOT, service_env.env

DAILY = "0 9 * * *"
DEPRECATED = {"Deprecation": "true"}


class EmbeddingBase(sa.orm.DeclarativeBase):
    pass


class EmbeddingStore(EmbeddingBase):
    """The columns of the pgvector table that the picker query reads; only built into queries, never executed."""
    __tablename__ = "langchain_pg_embedding"
    id = sa.Column(sa.String, primary_key=True)
    cmetadata = sa.Column(JSONB)


def passthrough(*args, **kwargs):
    return lambda func: func


@pytest.fixture
def api(env):
    return load_api(env)


def load_api(env):
    """`api/v2/index_schedule.py`, `api/v2/index_meta.py` and the compat helper loaded over the SQLite env; `env.request` holds the query."""
    tools = sys.modules["tools"]
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, endpoint_metrics=lambda func: func, with_modes=lambda urls: urls)
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=passthrough),
                                       current_user=lambda: {"id": 42})
    tools.config.PROMPT_LIB_MODE = "prompt_lib"
    tools.serialize = lambda value: value
    tools.VaultClient = MagicMock()
    env.request = types.SimpleNamespace(args={}, json=None, get_json=lambda silent=False: env.request.json)
    flask = types.ModuleType("flask")
    flask.request = env.request
    sys.modules["flask"] = flask
    all_models = sys.modules[f"{PACKAGE}.models.all"]
    sys.modules[f"{PACKAGE}.models.elitea_tools"] = all_models
    indexer = types.ModuleType(f"{PACKAGE}.models.indexer")
    indexer.EmbeddingStore, indexer.IndexRun = EmbeddingStore, MagicMock()
    sys.modules[indexer.__name__] = indexer
    application_tools = sqlite_db.LenientModule(f"{PACKAGE}.utils.application_tools")
    sys.modules[application_tools.__name__] = application_tools
    for name in ("utils.predict_utils", "utils.constants"):
        sys.modules[f"{PACKAGE}.{name}"] = sqlite_db.LenientModule(f"{PACKAGE}.{name}")
    sys.modules[f"{PACKAGE}.utils.constants"].PROMPT_LIB_MODE = "prompt_lib"
    sys.modules[f"{PACKAGE}.utils.constants"].DEPRECATION_HEADERS = DEPRECATED
    folder_access = sys.modules[f"{PACKAGE}.utils.folder_access"]
    folder_access.require_folder_access = passthrough
    for name in ("api", "api.v2"):
        sqlite_db.package(f"{PACKAGE}.{name}")
    env.application_tools = application_tools
    env.compat = load_module_with_stubs(PLUGIN_ROOT / "utils" / "index_schedule_compat.py", f"{PACKAGE}.utils.index_schedule_compat")
    for name in ("index_schedule", "index_meta"):
        setattr(env, name, load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / f"{name}.py", f"{PACKAGE}.api.v2.{name}"))
    return env


def patch(api, payload, toolkit_id=3, index_name="docs"):
    api.request.json = payload
    return api.index_meta.PromptLibAPI().patch(7, toolkit_id, index_name)


def schedule_rows(api):
    with api.session_factory() as session:
        return session.query(api.EntitySchedule).all()


def set_legacy_schedules(api, schedules):
    with api.session_factory() as session:
        toolkit = session.get(sys.modules[f"{PACKAGE}.models.all"].EliteATool, 3)
        toolkit.meta = {"indexes_meta": {"docs": {"schedules": schedules}}}
        session.commit()


def legacy_schedules(api):
    with api.session_factory() as session:
        toolkit = session.get(sys.modules[f"{PACKAGE}.models.all"].EliteATool, 3)
        return toolkit.meta["indexes_meta"]["docs"]["schedules"]


BODY = {"cron": DAILY, "timezone": "Etc/UTC", "enabled": True}


def test_patching_an_unscheduled_index_creates_its_schedule_row(api):
    body, status, headers = patch(api, BODY)
    assert (status, body["ok"], headers) == (200, True, DEPRECATED)
    [row] = schedule_rows(api)
    assert (row.entity_type, row.toolkit_id, row.index_name, row.cron, row.enabled, row.created_by) == (
        "toolkit_index", 3, "docs", DAILY, True, 42)
    assert body["schedule_id"] == row.id and body["next_run"]


def test_patching_a_scheduled_index_changes_its_row_and_pauses_it(api):
    row_id = api.seed_index("docs")
    body, status, headers = patch(api, {**BODY, "cron": "0 4 * * *", "enabled": False})
    assert (status, headers) == (200, DEPRECATED)
    [row] = schedule_rows(api)
    assert (row.id, row.cron, row.enabled, row.next_run) == (row_id, "0 4 * * *", False, None)
    assert body["schedule_id"] == row_id


def test_patching_another_users_schedule_answers_not_found(api):
    sqlite_db.seed_schedule(
        api.session_factory, api.EntitySchedule, service_env.OTHER_ID, entity_type="toolkit_index", application_id=None,
        version_id=None, prompt=None, toolkit_id=3, index_name="docs")
    body, status, _ = patch(api, BODY)
    assert (status, body["code"]) == (404, "not_found")
    assert [row.cron for row in schedule_rows(api)] == [DAILY]


def test_patching_an_index_that_the_settings_still_schedule_answers_409(api):
    set_legacy_schedules(api, {"7": {"cron": DAILY, "enabled": True}})
    body, status, _ = patch(api, BODY)
    assert (status, body["code"]) == (409, "index_scheduled")
    assert schedule_rows(api) == []


def test_patching_without_the_index_edit_permission_changes_nothing(api):
    api.permissions = set()
    body, status, _ = patch(api, BODY)
    assert (status, body["code"]) == (403, "read_only")
    assert schedule_rows(api) == []


@pytest.mark.parametrize("payload, field", [({**BODY, "cron": "* * * * *"}, "cron"), ({**BODY, "timezone": "Mars/Base"}, "timezone"),
                                            ({"cron": DAILY}, "timezone")])
def test_patching_with_a_bad_body_answers_400_with_the_field(api, payload, field):
    body, status, _ = patch(api, payload)
    assert (status, body["code"], body["field"]) == (400, "invalid_input", field)
    assert schedule_rows(api) == []


def test_patching_an_unknown_toolkit_answers_404(api):
    body, status, _ = patch(api, BODY, toolkit_id=99)
    assert (status, body["code"]) == (404, "not_found")


def test_deleting_a_settings_schedule_removes_the_key_of_the_caller(api):
    set_legacy_schedules(api, {"42": {"cron": DAILY}, "-1": {"cron": DAILY}})
    body, status, headers = api.index_schedule.PromptLibAPI().delete(7, 3, "docs")
    assert (status, headers) == (200, DEPRECATED)
    assert set(legacy_schedules(api)) == {"-1"}
    assert set(body["docs"]["schedules"]) == {"-1"}


def test_deleting_the_team_key_needs_the_user_id_argument(api):
    set_legacy_schedules(api, {"42": {"cron": DAILY}, "-1": {"cron": DAILY}})
    api.request.args = {"user_id": "-1"}
    assert api.index_schedule.PromptLibAPI().delete(7, 3, "docs")[1] == 200
    assert set(legacy_schedules(api)) == {"42"}


def test_deleting_an_index_without_a_settings_schedule_answers_404(api):
    set_legacy_schedules(api, {"-1": {"cron": DAILY}})
    body, status = api.index_schedule.PromptLibAPI().delete(7, 3, "docs")
    assert status == 404 and "42" in body["error"]


def test_deleting_leaves_the_schedule_rows_alone(api):
    api.seed_index("docs")
    set_legacy_schedules(api, {"42": {"cron": DAILY}})
    api.index_schedule.PromptLibAPI().delete(7, 3, "docs")
    assert [row.index_name for row in schedule_rows(api)] == ["docs"]
