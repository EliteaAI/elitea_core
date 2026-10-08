"""#6926: Health totals come from llm and tool usage rows only.

An agent's skill activations are written as zero-cost event_type='skill' rows. If the usage
plugin returned them, a run-scoped Health view would count them as events and dilute the
error rate, and the project Health tab would grow a 'skill' bucket.
"""

import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins.elitea_core.api.v2'

LLM_ROW = {'event_type': 'llm', 'total': 3, 'errors': 1, 'avg_duration_ms': 1000.0}
TOOL_ROW = {'event_type': 'tool', 'total': 1, 'errors': 0, 'avg_duration_ms': 200.0}
SKILL_ROW = {'event_type': 'skill', 'total': 4, 'errors': 0, 'avg_duration_ms': None}


@pytest.fixture
def analytics(monkeypatch):
    usage_rows = {'rows': []}
    rpc = types.SimpleNamespace(
        usage_event_type_health=lambda *a, **k: usage_rows['rows'],
    )
    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, with_modes=lambda params: params,
        endpoint_metrics=lambda func: func,
    )
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=lambda *a, **k: (lambda f: f)))
    tools.config = types.SimpleNamespace(DEFAULT_MODE='default', ADMINISTRATION_MODE='administration')
    tools.register_openapi = lambda *a, **k: (lambda f: f)
    tools.rpc_tools = types.SimpleNamespace(
        RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(timeout=lambda _t: rpc)),
    )
    flask = types.ModuleType('flask')
    flask.request = types.SimpleNamespace(args={})
    constants = types.ModuleType('plugins.elitea_core.utils.constants')
    constants.SYSTEM_USER_EMAILS = ()
    constants.SYSTEM_USER_EMAIL_PATTERN = ''
    date_range = types.ModuleType('plugins.elitea_core.utils.date_range')
    date_range.parse_date_range = lambda args: (None, None)
    # Only the audit-event half of the endpoint builds SQL; a sibling test may have stubbed
    # sqlalchemy, so the names it imports are stood in for
    sqlalchemy = types.ModuleType('sqlalchemy')
    for name in ('func', 'case', 'cast', 'Date', 'or_'):
        setattr(sqlalchemy, name, object())
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(warning=lambda *a, **k: None, error=lambda *a, **k: None)
    for name, module in {
        'tools': tools, 'flask': flask, 'pylon.core.tools': pylon_tools, 'sqlalchemy': sqlalchemy,
        'plugins.elitea_core.utils.constants': constants,
        'plugins.elitea_core.utils.date_range': date_range,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.utils',
                 'plugins.elitea_core.api', PACKAGE):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)

    spec = importlib.util.spec_from_file_location(f'{PACKAGE}.analytics', PLUGIN_ROOT / 'api' / 'v2' / 'analytics.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    module.usage_rows = usage_rows
    return module


class TestRunScopedHealth:
    def test_skill_rows_do_not_change_run_totals_or_error_rate(self, analytics):
        analytics.usage_rows['rows'] = [LLM_ROW, TOOL_ROW]
        before, _ = analytics._run_scoped_analytics(7, {}, {'run_id': 'r'})
        analytics.usage_rows['rows'] = [LLM_ROW, TOOL_ROW, SKILL_ROW]

        after, status = analytics._run_scoped_analytics(7, {}, {'run_id': 'r'})

        assert status == 200
        assert after == before
        assert after['kpis']['total_events'] == 4
        assert after['kpis']['error_rate'] == 25.0


class TestProjectHealth:
    def test_skill_bucket_is_dropped(self, analytics):
        analytics.usage_rows['rows'] = [LLM_ROW, TOOL_ROW, SKILL_ROW]

        rows = analytics._usage_health(7, None, None)

        assert [r['event_type'] for r in rows] == ['llm', 'tool']
