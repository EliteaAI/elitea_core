"""#6928: a run-scoped Health request narrows the conversation to one root entity.

A skill's Run History row is a conversation the skill may share with other participants, so the
Health tab forwards root_entity_* to usage and counts only that skill's rows.
"""

import importlib.util
import pathlib
import sys
import types
from unittest import mock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'plugins.elitea_core'
CONVERSATION = '11111111-2222-3333-4444-555555555555'


class AnyModule(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return mock.MagicMock(name=f'{self.__name__}.{name}')


class RecordingRpc:
    def __init__(self, result=None, error=None):
        self.calls = []
        self.result = result if result is not None else []
        self.error = error

    def timeout(self, _seconds):
        return self

    def usage_event_type_health(self, project_id, **kwargs):
        self.calls.append((project_id, kwargs))
        if self.error:
            raise self.error
        return self.result


@pytest.fixture
def analytics(monkeypatch):
    rpc = RecordingRpc()
    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, with_modes=lambda params: params,
        endpoint_metrics=lambda func: func,
    )
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda func: func)),
    )
    tools.config = types.SimpleNamespace(DEFAULT_MODE='default', ADMINISTRATION_MODE='administration')
    tools.register_openapi = lambda *a, **k: (lambda func: func)
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc))
    monkeypatch.setitem(sys.modules, 'tools', tools)
    for name in (
        'pylon', 'pylon.core', 'pylon.core.tools', 'flask', 'sqlalchemy',
        f'{PKG}.utils.constants', f'{PKG}.utils.date_range',
    ):
        monkeypatch.setitem(sys.modules, name, AnyModule(name))
    for name in ('plugins', PKG, f'{PKG}.api', f'{PKG}.api.v2', f'{PKG}.utils'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    spec = importlib.util.spec_from_file_location(
        f'{PKG}.api.v2.analytics', PLUGIN_ROOT / 'api' / 'v2' / 'analytics.py',
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    module.recording_rpc = rpc
    return module


SKILL_ARGS = {
    'run_id': CONVERSATION, 'root_entity_type': 'skill', 'root_entity_id': '174',
    'root_entity_project_id': '2',
}


class TestRunScopeArgs:
    def test_the_root_entity_rides_along_with_the_run(self, analytics):
        assert analytics._run_scope_args(SKILL_ARGS) == {
            'run_id': CONVERSATION, 'root_entity_type': 'skill', 'root_entity_id': 174,
            'root_entity_project_id': 2,
        }

    def test_an_agent_run_carries_no_root_entity(self, analytics):
        assert analytics._run_scope_args({'run_id': CONVERSATION}) == {'run_id': CONVERSATION}

    def test_no_run_is_no_scope(self, analytics):
        assert analytics._run_scope_args({}) is None

    @pytest.mark.parametrize('args', [
        {'root_entity_type': 'skill', 'root_entity_id': '174'},
        {'run_id': CONVERSATION, 'root_entity_type': 'skill', 'root_entity_id': 'abc'},
        {'run_id': CONVERSATION, 'root_entity_type': 'skill', 'root_entity_id': '1',
         'root_entity_project_id': 'x'},
    ])
    def test_a_root_entity_without_a_run_or_with_a_bad_id_is_rejected(self, analytics, args):
        with pytest.raises(ValueError):
            analytics._run_scope_args(args)


class TestRunScopedHealth:
    def test_health_is_read_for_the_root_entity_only(self, analytics):
        analytics.recording_rpc.result = [
            {'event_type': 'llm', 'total': 2, 'errors': 1, 'avg_duration_ms': 750.0},
        ]
        scope = analytics._run_scope_args(SKILL_ARGS)

        body, status = analytics._run_scoped_analytics(2, {}, scope)

        assert status == 200
        assert analytics.recording_rpc.calls == [(2, {
            'date_from': None, 'date_to': None, 'run_id': CONVERSATION,
            'root_entity_type': 'skill', 'root_entity_id': 174, 'root_entity_project_id': 2,
        })]
        assert body['health'] == [
            {'event_type': 'llm', 'total': 2, 'errors': 1, 'error_rate': 50.0, 'avg_duration_ms': 750.0},
        ]

    def test_a_root_entity_usage_rejects_is_a_400_not_a_500(self, analytics):
        analytics.recording_rpc.error = ValueError('root_entity_type must be one of application, skill')

        body, status = analytics._run_scoped_analytics(2, {}, {'run_id': CONVERSATION, 'root_entity_type': 'x'})

        assert status == 400
        assert 'root_entity_type' in body['error']
