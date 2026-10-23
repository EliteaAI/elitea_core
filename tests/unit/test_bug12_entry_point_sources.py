"""EL-7011 BUG-12 R1.1: chat, REST predict and MCP runs carry the trigger sources chat, api and mcp.

Contract under test: rpc/chat_all.py `_chat_trigger_kwargs` and its place in both chat predict calls, and
methods/predict.py `Method.do_predict`. chat_all.py does not load in the unit env, so its helper is compiled
from the file's source; do_predict is loaded from source with its neighbours faked.
"""
import ast
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest
from fixtures.cut_source import load_cut
from unit.utils.test_7011_pipeline_execution_owner import world  # noqa: F401 -- the real TriggerType

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = "plugins.elitea_core"
CHAT_ALL = ast.parse((PLUGIN_ROOT / "rpc" / "chat_all.py").read_text())
HELPER = "_chat_trigger_kwargs"


def _chat_helper(trigger_type):
    nodes = [node for node in CHAT_ALL.body if getattr(node, "name", None) == HELPER]
    namespace = {"TriggerType": trigger_type}
    namespace = load_cut(nodes, namespace, "chat_all.py")  # the real helper cut out of rpc/chat_all.py
    return namespace[HELPER]


def _rpc_calls(method_name):
    """The `getattr(self.context.rpc_manager.call, rpc_func)(...)` calls inside one chat_all method."""
    method = next(node for node in ast.walk(CHAT_ALL)
                  if isinstance(node, ast.FunctionDef) and node.name == method_name)
    return [node for node in ast.walk(method)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Call)
            and getattr(node.func.func, "id", None) == "getattr"
            and getattr(node.func.args[1], "id", None) == "rpc_func"]


def test_el_7011_bug12_r1_1_chat_sends_chat_to_the_agent_predict(world):  # noqa: F811
    helper = _chat_helper(world.execution.TriggerType)

    assert helper("applications_predict_sio") == {"trigger_source": "chat"}
    assert helper("applications_predict_sio_llm") == {}  # the LLM predict takes no trigger_source


@pytest.mark.parametrize("method_name", ["predict_sio", "continue_predict_sio"])
def test_el_7011_bug12_r1_1_both_chat_predict_calls_pass_the_chat_source(method_name):
    calls = _rpc_calls(method_name)

    assert calls
    for call in calls:
        spread = [kw.value for kw in call.keywords if kw.arg is None]
        assert any(getattr(value.func, "id", None) == HELPER for value in spread if isinstance(value, ast.Call))


class _Session:
    def __init__(self, version):
        self.version = version

    def query(self, _model):
        return types.SimpleNamespace(options=lambda *_: types.SimpleNamespace(get=lambda _id: self.version))


@pytest.fixture
def do_predict(monkeypatch, world):  # noqa: F811 -- the fixture imported above
    """do_predict(**kwargs) runs the real method and returns the kwargs it starts the agent task with."""
    version = types.SimpleNamespace(
        application=types.SimpleNamespace(name="Agent", webhook_secret=None), application_id=5,
        llm_settings={"model": "m"}, agent_type="openai", instructions="")
    request = types.SimpleNamespace(project_id=1, context_settings=True, application_name=None)
    chat_request = types.SimpleNamespace(
        parse_obj=lambda _payload: request,
        from_orm=lambda _row: types.SimpleNamespace(merge_update=lambda parsed: parsed))

    @contextmanager
    def get_session(_project_id):
        yield _Session(version)

    def module(name, **attrs):
        stub = types.ModuleType(name)
        stub.__path__ = []
        vars(stub).update(attrs)
        monkeypatch.setitem(sys.modules, name, stub)

    log = types.SimpleNamespace(warning=lambda *a, **k: None)
    module("opentelemetry", trace=types.SimpleNamespace(get_current_span=lambda: None))
    module("pylon.core.tools", log=log, web=types.SimpleNamespace(method=lambda *a, **k: (lambda f: f)))
    module("sqlalchemy.orm", joinedload=lambda *_: None)
    module("tools", VaultClient=lambda _pid: types.SimpleNamespace(unsecret=lambda value: value),
           db=types.SimpleNamespace(get_session=get_session), serialize=lambda value: value)
    leaves = {
        "models.pd.chat": {"ApplicationChatRequest": chat_request, "ContextStrategyModel": dict},
        "utils.predict_utils": {"generate_predict_payload": lambda *a, **k: {"user_input": "hi"},
                                "load_context_settings_from_conversation": None,
                                "user_input_preview": lambda value: value},
        "models.all": {"ApplicationVersion": types.SimpleNamespace(application=None)},
        "utils.budget_door": {"schema_may_use_llm": lambda *_: True},
        "utils.utils": {"verify_signature": None},
        "utils.exceptions": {"VerifySignatureError": Exception, "PoolSaturationError": Exception},
        "utils.sio_utils": {"SioEvents": types.SimpleNamespace()},
        "utils.vectorstore": {"get_pgvector_connection_string": lambda _pid: "connstr"},
    }
    for name in (PKG, f"{PKG}.methods", f"{PKG}.models", f"{PKG}.models.pd", f"{PKG}.utils"):
        module(name)
    for dotted, attrs in leaves.items():
        module(f"{PKG}.{dotted}", **attrs)
    monkeypatch.setitem(sys.modules, f"{PKG}.utils.pipeline_execution", world.execution)
    dotted = f"{PKG}.methods.predict"
    spec = importlib.util.spec_from_file_location(dotted, PLUGIN_ROOT / "methods" / "predict.py")
    loaded = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, dotted, loaded)
    spec.loader.exec_module(loaded)

    started = []
    owner = types.SimpleNamespace(task_node=types.SimpleNamespace(
        start_task=lambda *a, **k: started.append(k["kwargs"]) or "task-1"))

    def run(**kwargs):
        loaded.Method.do_predict(owner, project_id=1, user_id=2, version_id=3, payload_in={}, raw=b"",
                                 predict_wait=False, **kwargs)
        return started[-1]

    return run


def test_el_7011_bug12_r1_1_rest_predict_sends_api(do_predict):
    assert do_predict()["trigger_source"] == "api"


def test_el_7011_bug12_r1_1_mcp_caller_passes_mcp(do_predict):
    assert do_predict(trigger_source="mcp")["trigger_source"] == "mcp"
