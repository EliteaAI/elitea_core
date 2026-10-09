import ast
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins_6927_stop.elitea_core'


def _load(dotted):
    name = f'{PACKAGE}.{dotted}'
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / f"{dotted.replace('.', '/')}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def run_stop(isolated_sys_modules):
    for name in ('plugins_6927_stop', PACKAGE, f'{PACKAGE}.utils'):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    _load('utils.parallel_hitl')
    _load('utils.toolkit_authorization')
    return _load('utils.run_stop')


def _stop_handler():
    tree = ast.parse((PLUGIN_ROOT / 'rpc' / 'chat_all.py').read_text())
    return next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == 'chat_stop_task'
    )


class TestLiveRun:
    def test_a_streaming_reply_is_live(self, run_stop):
        assert run_stop.is_live_run(True, {}) is True

    def test_a_reply_paused_for_a_decision_is_live(self, run_stop):
        assert run_stop.is_live_run(False, {'hitl_interrupt': {'interrupt_id': 'i1'}}) is True

    def test_a_reply_paused_for_authorization_is_live(self, run_stop):
        assert run_stop.is_live_run(False, {'authorization_requests': [{'toolkit_id': 1}]}) is True

    def test_a_finished_reply_is_not_live(self, run_stop):
        assert run_stop.is_live_run(False, {'is_error': False, 'thread_id': 't'}) is False
        assert run_stop.is_live_run(False, None) is False


class TestStopHandler:
    def test_liveness_is_read_before_streaming_is_cleared(self):
        handler = _stop_handler()
        live_read = next(
            node.lineno for node in ast.walk(handler)
            if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'is_live_run'
        )
        streaming_cleared = next(
            node.lineno for node in ast.walk(handler)
            if isinstance(node, ast.Assign)
            and any(getattr(target, 'attr', None) == 'is_streaming' for target in node.targets)
        )

        assert live_read < streaming_cleared

    def test_only_a_live_run_is_marked_stopped(self):
        handler = _stop_handler()
        guarded = [
            node for node in ast.walk(handler)
            if isinstance(node, ast.If) and getattr(node.test, 'id', None) == 'was_live'
        ]
        stamps = [
            node for node in ast.walk(handler)
            if isinstance(node, ast.Name) and node.id == 'RUN_STOPPED_META_KEY'
        ]

        assert stamps
        assert all(
            any(stamp in ast.walk(branch) for branch in guarded)
            for stamp in stamps
        )
