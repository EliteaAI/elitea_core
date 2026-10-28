"""The predict helper forwards the chat history it is given and sends none by default.

Contract under test: utils/pipeline_execution.py, loaded from source with its leaves faked.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PREFIX = "plugins.elitea_core"
MODULE = f"{PREFIX}.utils.pipeline_execution"
LEAVES = {
    "models.all": ("Application", "ApplicationVersion"),
    "models.conversation": ("Conversation",),
    "models.enums.all": ("ParticipantTypes",),
    "models.message_group": ("ConversationMessageGroup",),
    "models.message_items.text": ("TextMessageItem",),
    "models.pd.participant": ("ParticipantEntityApplication", "ParticipantEntityUser"),
    "utils.participant_utils": ("get_or_create_one",),
    "utils.sio_utils": ("SioEvents",),
}
HISTORY = [
    {"role": "user", "content": "first question"},
    {"role": "assistant", "content": "first answer"},
]


def _module(name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    return module


@pytest.fixture
def predict(monkeypatch):
    calls = []

    def predict_sio(**kwargs):
        calls.append(kwargs)
        return {"task_id": "task-1"}

    rpc = types.SimpleNamespace(applications_predict_sio=predict_sio)
    log = types.SimpleNamespace(debug=lambda *a, **k: None)
    packages = ("plugins", PREFIX, f"{PREFIX}.models", f"{PREFIX}.models.pd",
                f"{PREFIX}.models.enums", f"{PREFIX}.models.message_items", f"{PREFIX}.utils")
    for name in packages:
        monkeypatch.setitem(sys.modules, name, _module(name, __path__=[]))
    monkeypatch.setitem(sys.modules, "pylon", _module("pylon"))
    monkeypatch.setitem(sys.modules, "pylon.core", _module("pylon.core"))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", _module("pylon.core.tools", log=log))
    monkeypatch.setitem(sys.modules, "tools", _module(
        "tools",
        db=None,
        rpc_tools=types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(timeout=lambda _seconds: rpc))),
        this=None,
    ))
    for dotted, names in LEAVES.items():
        attrs = {n: types.SimpleNamespace(chat_predict=types.SimpleNamespace(value="chat_predict"))
                 if n == "SioEvents" else object for n in names}
        monkeypatch.setitem(sys.modules, f"{PREFIX}.{dotted}", _module(f"{PREFIX}.{dotted}", **attrs))
    spec = importlib.util.spec_from_file_location(MODULE, PLUGIN_ROOT / "utils" / "pipeline_execution.py")
    execution = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, MODULE, execution)
    spec.loader.exec_module(execution)
    return types.SimpleNamespace(execute=execution.execute_pipeline_via_predict_sio, calls=calls)


def _predict(predict, **kwargs):
    predict.execute(
        project_id=1, version=types.SimpleNamespace(id=27, application_id=15), user_id=7,
        conversation_uuid="uuid-1", response_message_id="response-1", **kwargs,
    )
    return predict.calls[-1]["data"]["chat_history"]


def test_no_history_argument_sends_an_empty_history(predict):
    assert _predict(predict) == []


def test_none_history_sends_an_empty_history(predict):
    assert _predict(predict, chat_history=None) == []


def test_empty_history_sends_an_empty_history(predict):
    assert _predict(predict, chat_history=[]) == []


def test_given_history_is_sent_in_order(predict):
    assert _predict(predict, chat_history=HISTORY) == HISTORY


def test_given_history_does_not_replace_the_user_input(predict):
    _predict(predict, chat_history=HISTORY, user_input="now")

    assert predict.calls[-1]["data"]["user_input"] == "now"
