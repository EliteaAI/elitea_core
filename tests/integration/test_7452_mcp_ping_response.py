"""#7452: a JSON-RPC ``ping`` must be answered with ``{"result": {}}``.

``SseSession.dispatch_empty_response`` passed an ``EmptyResult`` model as ``JSONRPCResponse.result``, which the
installed ``mcp`` types only accept as a dict, so every ping raised a ValidationError that the HTTP layer turned
into a 400 "Invalid request format". Both transports (``/mcp`` and SSE ``/messages``) share this method.

``mcp_session.py`` is loaded standalone with the real ``mcp`` types; only ``tools``/``pylon`` are stubbed.
"""
import importlib.util
import json
import pathlib
import sys
import types

import pytest
from mcp import types as mcp_types


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture()
def session_cls(monkeypatch):
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.log = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None,
                                            error=lambda *a, **k: None, debug=lambda *a, **k: None)
    monkeypatch.setitem(sys.modules, "pylon", sys.modules.get("pylon") or types.ModuleType("pylon"))
    monkeypatch.setitem(sys.modules, "pylon.core", sys.modules.get("pylon.core") or types.ModuleType("pylon.core"))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", pylon_tools)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.context = types.SimpleNamespace(url_prefix="")
    tools_pkg.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={}))
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)

    spec = importlib.util.spec_from_file_location("mcp_session_7452", PLUGIN_ROOT / "utils" / "mcp_session.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.SseSession


def _sent(session_cls, call):
    """Run `call` on a bare session and return the decoded messages it dispatched."""
    session = session_cls.__new__(session_cls)
    sent = []
    session.dispatch_message = sent.append
    call(session)
    return [json.loads(m) for m in sent]


@pytest.mark.parametrize("request_id", [1, 24599, "abc"])
def test_ping_answer_is_an_empty_result_with_the_request_id(session_cls, request_id):
    assert _sent(session_cls, lambda s: s.dispatch_empty_response(request_id)) == [
        {"jsonrpc": "2.0", "id": request_id, "result": {}},
    ]


def test_ping_answer_validates_as_a_client_would_read_it(session_cls):
    # The client parses the reply into JSONRPCMessage; an EmptyResult must round-trip.
    [message] = _sent(session_cls, lambda s: s.dispatch_empty_response(7))
    parsed = mcp_types.JSONRPCMessage.model_validate(message).root
    assert isinstance(parsed, mcp_types.JSONRPCResponse)
    mcp_types.EmptyResult.model_validate(parsed.result)


def test_ping_request_from_a_real_client_payload_is_answered(session_cls):
    # The payload shape observed in production: params carrying progress meta.
    raw = {"jsonrpc": "2.0", "id": 24599, "method": "ping", "params": {"_meta": {"progressToken": 3}}}
    request = mcp_types.ClientRequest.model_validate(raw)
    assert isinstance(request.root, mcp_types.PingRequest)
    assert _sent(session_cls, lambda s: s.dispatch_empty_response(raw["id"])) == [
        {"jsonrpc": "2.0", "id": 24599, "result": {}},
    ]
