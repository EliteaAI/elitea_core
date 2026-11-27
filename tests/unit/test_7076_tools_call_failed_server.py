"""#7076: a tool call to a server its local client reported as failed is answered with the client's reason.

``api/v2/tools_call.py`` is loaded by path with the real ``models/mcp.py`` and ``utils/sio_utils.py``; flask, pylon
and ``tools`` are stand-ins, so the test reaches the handler's own branches only.
"""
import importlib.util
import pathlib
import sys
import types
from unittest.mock import MagicMock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'tools_call_7076_pkg'
PROJECT_ID = 13
BODY = {'server': 'jira', 'tool_call_id': 'call-1', 'tool_timeout_sec': 30, 'params': {'name': 'echo', 'arguments': {}}}


def _install_package(monkeypatch, request, auth):
    for name in (PKG, f'{PKG}.models', f'{PKG}.utils', f'{PKG}.api', f'{PKG}.api.v2'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    monkeypatch.setitem(sys.modules, 'flask', types.SimpleNamespace(request=request))
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = MagicMock()
    for name, module in (('pylon', types.ModuleType('pylon')), ('pylon.core', types.ModuleType('pylon.core')),
                         ('pylon.core.tools', pylon_tools)):
        monkeypatch.setitem(sys.modules, name, module)
    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(APIModeHandler=object, APIBase=object, endpoint_metrics=lambda f: f,
                                            with_modes=lambda params: params)
    tools.config = types.SimpleNamespace(DEFAULT_MODE='default', ADMINISTRATION_MODE='administration')
    tools.auth = auth
    tools.context = MagicMock()
    monkeypatch.setitem(sys.modules, 'tools', tools)
    return tools


def _load(monkeypatch, name, relpath):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relpath)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def call(monkeypatch):
    auth = MagicMock()
    auth.current_user.return_value = {'id': 7}
    tools = _install_package(monkeypatch, types.SimpleNamespace(json=BODY), auth)
    models = _load(monkeypatch, f'{PKG}.models.mcp', 'models/mcp.py')
    _load(monkeypatch, f'{PKG}.utils.sio_utils', 'utils/sio_utils.py')
    api = _load(monkeypatch, f'{PKG}.api.v2.tools_call', 'api/v2/tools_call.py')
    handler = object.__new__(api.PromptLibAPI)
    handler.module = MagicMock()
    handler.module.get_registered_servers_private_and_current.return_value = []
    handler.module.get_failed_server_private_and_current.return_value = None
    return types.SimpleNamespace(handler=handler, models=models, sio=tools.context.sio)


def test_a_failed_server_is_answered_with_the_reason_its_client_reported(call):
    call.handler.module.get_failed_server_private_and_current.return_value = call.models.McpFailedServer(
        name='jira', kind='exited', message='Server "jira": exited during start.')

    result = call.handler.post(PROJECT_ID)

    assert result == ({'error': "Mcp Server 'jira' failed on the local MCP client: Server \"jira\": exited during start."}, 400)
    call.handler.module.get_failed_server_private_and_current.assert_called_once_with(PROJECT_ID, 'jira', 7)
    call.sio.call.assert_not_called()


def test_a_server_nobody_reported_keeps_the_not_connected_answer(call):
    result = call.handler.post(PROJECT_ID)

    assert result == ({'error': "Mcp Server 'jira' is currently not connected to either private or current project."}, 400)
