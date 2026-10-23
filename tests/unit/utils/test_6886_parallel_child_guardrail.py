"""#6886: a parallel child whose task the input guardrail blocks settles as a terminal error.

The parent then reports the refusal (``parallel_terminal_errors``) instead of a dispatch
failure. The envelope carries the user-facing message only, never the child's task text.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture()
def dispatch(monkeypatch):
    # Other suites leave bare pylon stubs in sys.modules; bring our own
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(**{n: (lambda *a, **k: None) for n in ('debug', 'info', 'warning', 'error', 'exception')})
    pylon_tools.web = types.SimpleNamespace(method=lambda *a, **k: (lambda f: f))
    for name, module in (('pylon', types.ModuleType('pylon')), ('pylon.core', types.ModuleType('pylon.core')), ('pylon.core.tools', pylon_tools)):
        monkeypatch.setitem(sys.modules, name, module)
    path = PLUGIN_ROOT / 'methods' / 'parallel_dispatch.py'
    spec = importlib.util.spec_from_file_location('parallel_dispatch_6886_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_guardrail_refusal_becomes_a_terminal_error(dispatch):
    error = types.SimpleNamespace(type='input_guardrail_blocked', message='It was not sent to the model.')

    assert dispatch._input_guardrail_terminal_error(error) == {
        'code': 'input_guardrail_blocked',
        'user_message': 'It was not sent to the model.',
    }


def test_other_dispatch_errors_keep_the_failure_path(dispatch):
    assert dispatch._input_guardrail_terminal_error(RuntimeError('boom')) is None
    assert dispatch._input_guardrail_terminal_error(types.SimpleNamespace(type='budget_exceeded')) is None


def test_start_task_except_settles_the_child_before_the_generic_failure(dispatch):
    source = (PLUGIN_ROOT / 'methods' / 'parallel_dispatch.py').read_text()
    settle = source.index('terminal_error=guardrail_error')

    assert source.index('guardrail_error = _input_guardrail_terminal_error(e)') < settle
    assert settle < source.index('log.exception("[PARALLEL] start_task raised for child')
