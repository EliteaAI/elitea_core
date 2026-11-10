"""The old index schedule routes refuse writes: schedules are rows of the Schedules module (SQLite env, real models)."""
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

MESSAGE = "Index schedules are managed on the Schedules page"


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
    """`api/v2/index_schedule.py` and `api/v2/index_meta.py` loaded over the SQLite env; `env.request` holds the query."""
    tools = sys.modules["tools"]
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, endpoint_metrics=lambda func: func, with_modes=lambda urls: urls)
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=passthrough),
                                       current_user=lambda: {"id": 42})
    tools.config.PROMPT_LIB_MODE = "prompt_lib"
    tools.serialize = lambda value: value
    tools.VaultClient = MagicMock()
    env.request = types.SimpleNamespace(args={})
    flask = types.ModuleType("flask")
    flask.request = env.request
    sys.modules["flask"] = flask
    all_models = sys.modules[f"{PACKAGE}.models.all"]
    sys.modules[f"{PACKAGE}.models.elitea_tools"] = all_models
    indexer = types.ModuleType(f"{PACKAGE}.models.indexer")
    indexer.EmbeddingStore, indexer.IndexRun = EmbeddingStore, MagicMock()
    sys.modules[indexer.__name__] = indexer
    application_tools = sqlite_db.LenientModule(f"{PACKAGE}.utils.application_tools")
    application_tools.INDEX_SCHEDULE_MANAGED_MESSAGE = MESSAGE
    sys.modules[application_tools.__name__] = application_tools
    for name in ("utils.predict_utils", "utils.constants"):
        sys.modules[f"{PACKAGE}.{name}"] = sqlite_db.LenientModule(f"{PACKAGE}.{name}")
    sys.modules[f"{PACKAGE}.utils.constants"].PROMPT_LIB_MODE = "prompt_lib"
    folder_access = sys.modules[f"{PACKAGE}.utils.folder_access"]
    folder_access.require_folder_access = passthrough
    for name in ("api", "api.v2"):
        sqlite_db.package(f"{PACKAGE}.{name}")
    env.application_tools = application_tools
    for name in ("index_schedule", "index_meta"):
        setattr(env, name, load_module_with_stubs(PLUGIN_ROOT / "api" / "v2" / f"{name}.py", f"{PACKAGE}.api.v2.{name}"))
    return env


def test_deleting_an_index_schedule_through_the_old_route_is_refused(api):
    response = api.index_schedule.PromptLibAPI().delete(7, 3, "docs")
    assert response == ({"ok": False, "error": MESSAGE}, 400)


def test_patching_an_index_through_the_old_route_is_refused(api):
    response = api.index_meta.PromptLibAPI().patch(7, 3, "index-id")
    assert response == ({"ok": False, "error": MESSAGE}, 400)


def test_a_refused_write_changes_no_schedule_row(api):
    api.seed_index("docs")
    api.index_schedule.PromptLibAPI().delete(7, 3, "docs")
    api.index_meta.PromptLibAPI().patch(7, 3, "index-id")
    with api.session_factory() as session:
        assert [row.index_name for row in session.query(api.EntitySchedule).all()] == ["docs"]
