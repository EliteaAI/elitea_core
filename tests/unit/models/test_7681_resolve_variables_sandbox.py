"""Issue #7681 — models/pd/chat.py `_resolve_variables` must render in a Jinja2 sandbox.

An unsafe template (dunder traversal) falls back to the raw text instead of rendering
Python internals; legitimate substitution and DebugUndefined output are unchanged.

Run via:
    python tests/run_tests.py unit/models/test_7681_resolve_variables_sandbox.py -v
"""
import importlib
import pathlib
import sys
import types
from types import SimpleNamespace

import pytest
from pydantic import BaseModel, ConfigDict

from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


class _PermissiveModel(BaseModel):
    model_config = ConfigDict(extra='allow')


def _stub(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    return module


@pytest.fixture(scope='module')
def resolve_variables():
    package = _stub('test_7681_pd')
    package.__path__ = [str(PLUGIN_ROOT / 'models' / 'pd')]
    # Other suites replace `jinja2` in sys.modules at import time. Use the real package while
    # these tests run (jinja2 imports submodules lazily during render), then restore it.
    saved = {name: sys.modules.pop(name) for name in list(sys.modules)
             if name == 'jinja2' or name.startswith('jinja2.')}
    importlib.import_module('jinja2.sandbox')
    stubs = {
        'test_7681_pd': package,
        'test_7681_pd.tool': _stub('test_7681_pd.tool', ToolChatModel=_PermissiveModel),
        'test_7681_pd.utils': _stub('test_7681_pd.utils', MergeUpdateBase=_PermissiveModel),
        'test_7681_pd.llm': _stub('test_7681_pd.llm', merge_llm_selection_override=lambda *a, **k: None),
        'test_7681_pd.version': _stub(
            'test_7681_pd.version',
            ApplicationVariableModel=_PermissiveModel,
            LLMSettingsModel=_PermissiveModel,
        ),
    }
    try:
        module = load_module_with_stubs(PLUGIN_ROOT / 'models' / 'pd' / 'chat.py', 'test_7681_pd.chat', stubs)
        yield module._resolve_variables
    finally:
        for name in [n for n in sys.modules if n == 'jinja2' or n.startswith('jinja2.')]:
            del sys.modules[name]
        sys.modules.update(saved)


def _vars(**values):
    return [SimpleNamespace(name=k, value=v) for k, v in values.items()]


@pytest.mark.unit
class TestUnsafeTemplateFallsBack:

    @pytest.mark.parametrize('payload', [
        "{{ ''.__class__.__mro__ }}",
        "{{ ().__class__.__base__.__subclasses__() }}",
    ])
    def test_payload_returns_raw_text(self, resolve_variables, payload):
        resolved = resolve_variables(payload, [])
        assert resolved == payload
        assert '<class' not in resolved


@pytest.mark.unit
class TestLegitimateTemplateUnchanged:

    def test_substitution_and_debug_undefined(self, resolve_variables):
        assert resolve_variables('hi {{ name }} {{ missing }}', _vars(name='bob')) == 'hi bob {{ missing }}'

    def test_loop_and_condition(self, resolve_variables):
        text = '{% for i in items %}{% if i > 1 %}{{ i }}{% endif %}{% endfor %}'
        assert resolve_variables(text, _vars(items=[1, 2, 3])) == '23'
