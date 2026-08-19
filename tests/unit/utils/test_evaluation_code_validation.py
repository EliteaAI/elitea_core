"""Unit tests for code_validation.py — the pylon_main-side code-validation path (EVAL-H2, §19).

This module is the dependency-free half: prelude assembly, the bool/number result
contract, sandbox-result → verdict mapping, the task-node dispatch executor, and the
end-to-end ``run_code_validation`` orchestration (screen → prelude → execute → map).

``code_validation`` does ``from .evaluation_code_screen import screen_validation_code``,
so the sibling is pre-loaded into sys.modules under its package name before we load
``code_validation`` itself (otherwise the relative import can't resolve).
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def cv(utils_path):
    # Pre-load the sibling so `from .evaluation_code_screen import ...` resolves from cache.
    load_utils_module(utils_path, 'evaluation_code_screen')
    return load_utils_module(utils_path, 'code_validation')


# ---------------------------------------------------------------------------
# build_validation_prelude — evidence injected as plain literals (§19.4)
# ---------------------------------------------------------------------------

def test_prelude_always_injects_output(cv):
    prelude = cv.build_validation_prelude('result = True', output='hello')
    assert "output = 'hello'" in prelude
    assert 'result = True' in prelude
    # opt-in vars absent when not provided
    assert 'expected =' not in prelude
    assert 'input =' not in prelude
    assert 'structure =' not in prelude


def test_prelude_epilogue_surfaces_result_as_last_expression(cv):
    # The sandbox captures the value of the LAST expression, not a variable named
    # 'result'. The trusted epilogue must therefore make 'result' the final
    # expression so an assignment-only user script surfaces its assigned value.
    prelude = cv.build_validation_prelude('result = len(output) > 0', output='hi')
    assert prelude.rstrip().endswith("globals().get('result')")
    # The assembled program's last expression evaluates to the assigned result.
    ns = {}
    lines = prelude.splitlines()
    last_expr = lines[-1]
    exec('\n'.join(lines[:-1]), ns)
    assert eval(last_expr, ns) is True


def test_prelude_epilogue_yields_none_when_result_unassigned(cv):
    # A script that never assigns 'result' must surface None (the contract's
    # missing-result signal), never raise NameError.
    prelude = cv.build_validation_prelude('x = 1', output='hi')
    ns = {}
    lines = prelude.splitlines()
    exec('\n'.join(lines[:-1]), ns)
    assert eval(lines[-1], ns) is None


def test_prelude_injects_optional_evidence(cv):
    prelude = cv.build_validation_prelude(
        'result = output == expected',
        output='a', expected='b', input={'k': 1}, structure=[1, 2],
    )
    assert "output = 'a'" in prelude
    assert "expected = 'b'" in prelude
    assert 'input = {' in prelude
    assert 'structure = [1, 2]' in prelude


def test_prelude_explicit_none_is_injected(cv):
    # An explicit None is distinct from the sentinel — it must be injected.
    prelude = cv.build_validation_prelude('result = expected is None',
                                          output='x', expected=None)
    assert 'expected = None' in prelude


def test_prelude_repr_is_safe_literal(cv):
    # Values with quotes/newlines round-trip as safe Python source via repr.
    tricky = 'line1\nline2 "quoted" \'single\''
    prelude = cv.build_validation_prelude('result = True', output=tricky)
    ns = {}
    exec(prelude.split('# --- user script')[0], ns)  # run only the prelude
    assert ns['output'] == tricky


# ---------------------------------------------------------------------------
# _coerce_to_contract — the §19.4 bool/number result contract
# ---------------------------------------------------------------------------

def test_coerce_missing_result_is_error(cv):
    native, passed, err = cv._coerce_to_contract(None, 'bool')
    assert native is None and passed is None
    assert err and 'result' in err


def test_coerce_bool_contract_true(cv):
    native, passed, err = cv._coerce_to_contract(True, 'bool')
    assert err is None and passed is True and native == 1.0


def test_coerce_bool_contract_number_truthy(cv):
    native, passed, err = cv._coerce_to_contract(3, 'bool')
    assert err is None and passed is True and native == 1.0
    native, passed, err = cv._coerce_to_contract(0, 'bool')
    assert err is None and passed is False and native == 0.0


def test_coerce_bool_contract_rejects_string(cv):
    native, passed, err = cv._coerce_to_contract('yes', 'bool')
    assert native is None and passed is None and err


def test_coerce_number_contract_ok(cv):
    native, passed, err = cv._coerce_to_contract(4.2, 'number')
    assert err is None and native == 4.2 and passed is None


def test_coerce_number_contract_rejects_bool(cv):
    # bool is an int subclass but is NOT a valid number result.
    native, passed, err = cv._coerce_to_contract(True, 'number')
    assert native is None and err and 'number' in err


def test_coerce_number_contract_rejects_string(cv):
    native, passed, err = cv._coerce_to_contract('5', 'number')
    assert native is None and err


@pytest.mark.parametrize('value', [float('nan'), float('inf'), float('-inf')])
def test_coerce_number_contract_rejects_non_finite(cv, value):
    # a divide-by-zero in the script must not become a top score: the downstream clamp turns
    # NaN into 100.0, because min(100.0, float('nan')) is 100.0 in Python.
    native, passed, err = cv._coerce_to_contract(value, 'number')
    assert native is None and err and 'finite' in err


# ---------------------------------------------------------------------------
# map_execution_result — exec dict → verdict
# ---------------------------------------------------------------------------

def _ok(result, **extra):
    d = {'result': result, 'stdout': 'out', 'stderr': None,
         'status': 'success', 'execution_time': 0.5}
    d.update(extra)
    return d


def test_map_success_bool_scored(cv):
    v = cv.map_execution_result(_ok(True), code_validation_id=7, name='v1')
    assert v['status'] == cv.STATUS_SCORED
    assert v['passed'] is True and v['native_score'] == 1.0
    assert v['stdout'] == 'out' and v['execution_time'] == 0.5
    assert v['error'] is None


def test_map_success_number_scored(cv):
    v = cv.map_execution_result(_ok(0.8), code_validation_id=7, name='v1',
                                return_contract='number')
    assert v['status'] == cv.STATUS_SCORED
    assert v['native_score'] == 0.8 and v['passed'] is None


def test_map_missing_result_is_error(cv):
    v = cv.map_execution_result(_ok(None), code_validation_id=7, name='v1')
    assert v['status'] == cv.STATUS_ERROR
    assert 'result' in v['error']
    # stdout/exec_time still surfaced on the error verdict
    assert v['stdout'] == 'out' and v['execution_time'] == 0.5


def test_map_sandbox_timeout_is_error(cv):
    exec_result = {'result': None, 'stdout': None,
                   'stderr': 'Execution timed out after 55 seconds',
                   'status': 'error', 'execution_time': 55.0}
    v = cv.map_execution_result(exec_result, code_validation_id=7, name='v1')
    assert v['status'] == cv.STATUS_ERROR
    assert 'timed out' in v['error']
    assert v['execution_time'] == 55.0


def test_map_sandbox_error_without_stderr_has_fallback(cv):
    exec_result = {'result': None, 'stdout': None, 'stderr': None,
                   'status': 'oom', 'execution_time': None}
    v = cv.map_execution_result(exec_result, code_validation_id=7, name='v1')
    assert v['status'] == cv.STATUS_ERROR
    assert 'oom' in v['error']


# ---------------------------------------------------------------------------
# make_task_node_executor — synchronous dispatch (start/join/stop)
# ---------------------------------------------------------------------------

class _FakeTaskNode:
    def __init__(self, *, task_id='t1', join_result=None, join_raises_sentinel=False):
        self._task_id = task_id
        self._join_result = join_result
        self._join_sentinel = join_raises_sentinel
        self.started = []
        self.joined = []
        self.stopped = []

    def start_task(self, task_name, *, kwargs, pool, meta):
        self.started.append((task_name, kwargs, pool, meta))
        return self._task_id

    def join_task(self, task_id, *, timeout):
        self.joined.append((task_id, timeout))
        if self._join_sentinel:
            return ...
        return self._join_result

    def stop_task(self, task_id):
        self.stopped.append(task_id)


def test_executor_dispatches_and_returns_result(cv):
    node = _FakeTaskNode(join_result={'result': True, 'status': 'success'})
    execute = cv.make_task_node_executor(node)
    out = execute('code body')
    assert out == {'result': True, 'status': 'success'}
    task_name, kwargs, pool, meta = node.started[0]
    assert task_name == 'indexer_code_validation'
    assert kwargs == {'code': 'code body'}
    assert pool == 'indexer'


def test_executor_pool_saturation_is_error(cv):
    node = _FakeTaskNode(task_id=None)
    execute = cv.make_task_node_executor(node)
    out = execute('code')
    assert out['status'] == cv.STATUS_ERROR
    assert 'saturated' in out['stderr']
    assert node.joined == []  # never joined a None task


def test_executor_join_timeout_is_error_and_stops(cv):
    node = _FakeTaskNode(join_raises_sentinel=True)
    execute = cv.make_task_node_executor(node, timeout=12.0)
    out = execute('code')
    assert out['status'] == cv.STATUS_ERROR
    assert 'timed out' in out['stderr'] and '12.0' in out['stderr']
    assert node.stopped == ['t1']  # timed-out task was stopped


# ---------------------------------------------------------------------------
# run_code_validation — end-to-end orchestration with a stub executor
# ---------------------------------------------------------------------------

def test_run_screen_violation_is_error_verdict(cv):
    calls = []

    def executor(code):
        calls.append(code)
        return _ok(True)

    v = cv.run_code_validation(
        'import os\nresult = True',
        code_validation_id=1, name='v', output='x', executor=executor,
    )
    assert v['status'] == cv.STATUS_ERROR
    assert 'not allowed' in v['error']
    assert calls == []  # screen blocked before dispatch


def test_run_unavailable_is_unavailable_verdict(cv):
    def executor(code):
        return {'result': None, 'stdout': None,
                'stderr': 'Deno/Pyodide sandbox runtime is not available.',
                'status': cv.STATUS_UNAVAILABLE, 'execution_time': None}

    v = cv.run_code_validation(
        'result = output == "x"',
        code_validation_id=1, name='v', output='x', executor=executor,
    )
    assert v['status'] == cv.STATUS_UNAVAILABLE
    assert 'not available' in v['error']


def test_run_success_scored_verdict(cv):
    captured = {}

    def executor(code):
        captured['prelude'] = code
        return _ok(True)

    v = cv.run_code_validation(
        'result = output == expected',
        code_validation_id=42, name='exact-match',
        output='foo', expected='foo', executor=executor,
    )
    assert v['status'] == cv.STATUS_SCORED
    assert v['passed'] is True and v['native_score'] == 1.0
    assert v['code_validation_id'] == 42 and v['name'] == 'exact-match'
    # evidence made it into the prelude the executor received
    assert "output = 'foo'" in captured['prelude']
    assert "expected = 'foo'" in captured['prelude']


def test_run_number_contract_scored(cv):
    def executor(code):
        return _ok(0.75)

    v = cv.run_code_validation(
        'result = 0.75',
        code_validation_id=5, name='sim', output='x',
        return_contract='number', executor=executor,
    )
    assert v['status'] == cv.STATUS_SCORED
    assert v['native_score'] == 0.75 and v['passed'] is None


def test_run_missing_result_is_error_verdict(cv):
    def executor(code):
        return _ok(None)

    v = cv.run_code_validation(
        'x = 1',  # never assigns result (passes screen, fails contract)
        code_validation_id=5, name='v', output='x', executor=executor,
    )
    assert v['status'] == cv.STATUS_ERROR
    assert 'result' in v['error']
