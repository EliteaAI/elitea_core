"""An index endpoint answers a readable 400, never a framework 500, when the toolkit or its store can't be used (#7450)."""
import ast
import sys
import types
from pathlib import Path
from typing import Optional

import pytest
from unit import test_toolkit_index_legacy_api_block as api_block
from unit import test_toolkit_index_schedule_service as service_env

PLUGIN_ROOT = Path(__file__).parents[2]
PACKAGE, env = service_env.PACKAGE, service_env.env
UNUSABLE = "This toolkit's credentials can't be used — check its configuration"


@pytest.fixture
def api(env):
    return api_block.load_api(env)


def broken(*args, **kwargs):
    raise RuntimeError("secret connection detail")


def reachable(api, monkeypatch):
    monkeypatch.setattr(api.index_meta, "load_toolkit_for_index", lambda *args: ("3", "conn", None))
    monkeypatch.setattr(api.index_meta, "inspect", lambda engine: types.SimpleNamespace(has_table=lambda *a, **k: True))
    api.request.args = {}


def test_an_unreachable_index_store_answers_400_without_the_error_text(api, monkeypatch):
    reachable(api, monkeypatch)
    monkeypatch.setattr(api.index_meta, "inspect", broken)
    body, status = api.index_meta.PromptLibAPI().get(7, 3)
    assert status == 400 and body["ok"] is False and "secret" not in repr(body)


def test_a_session_that_cannot_open_answers_the_handlers_own_400(api, monkeypatch):
    reachable(api, monkeypatch)
    monkeypatch.setattr(api.index_meta, "get_session_for_schema", broken)
    assert api.index_meta.PromptLibAPI().get(7, 3) == (
        {"ok": False, "error": "Error occurred while fetching index_meta"}, 400)
    assert api.index_meta.PromptLibAPI().delete(7, 3, "1") == (
        {"ok": False, "error": "Error occurred while deleting index_meta"}, 400)


class ConfigurationExpandError(Exception):
    def __init__(self, errors):
        super().__init__(str(errors))
        self.errors = errors


@pytest.fixture
def loader(monkeypatch):
    """The real `load_toolkit_for_index`, with the toolkit read and the validation it hands over to stubbed."""
    package = "plugins_7450_loader"
    for name in (package, f"{package}.utils"):
        module = types.ModuleType(name)
        module.__path__ = []
        monkeypatch.setitem(sys.modules, name, module)
    predict_utils = types.ModuleType(f"{package}.utils.predict_utils")
    monkeypatch.setitem(sys.modules, predict_utils.__name__, predict_utils)
    tree = ast.parse((PLUGIN_ROOT / "utils" / "application_tools.py").read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "load_toolkit_for_index")
    namespace = {
        "__name__": f"{package}.utils.application_tools", "__package__": f"{package}.utils",
        "Optional": Optional, "ConfigurationExpandError": ConfigurationExpandError,
        "log": types.SimpleNamespace(warning=lambda *a, **k: None),
        "load_and_validate_toolkit_for_index": lambda config: ("schema", config["conn"], None),
    }
    exec(compile(ast.Module([node], []), "application_tools.py", "exec"), namespace)  # pylint: disable=W0122
    return predict_utils, namespace["load_toolkit_for_index"]


def test_a_credential_the_caller_cannot_use_is_a_readable_400(loader):
    predict_utils, load_toolkit_for_index = loader

    def unresolved(project_id, user_id, toolkit_id):
        raise ConfigurationExpandError([{"loc": ("pgvector_configuration",), "msg": "private_credential_not_found"}])

    predict_utils.get_toolkit_config = unresolved
    assert load_toolkit_for_index(2, 42, 56) == (None, None, ({"ok": False, "error": UNUSABLE}, 400))


def test_a_usable_toolkit_goes_on_to_the_index_validation(loader):
    predict_utils, load_toolkit_for_index = loader
    predict_utils.get_toolkit_config = lambda project_id, user_id, toolkit_id: {"conn": f"{project_id}/{toolkit_id}"}
    assert load_toolkit_for_index(2, 42, 56) == ("schema", "2/56", None)
