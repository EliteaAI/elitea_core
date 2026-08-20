"""Code-validation prelude/verdict plumbing + dispatch — EVAL-H2 (design §19).

A code validation is a **pure verdict function**: given case evidence it assigns ``result``
(pass/fail bool, or a number on the dimension scale). Execution itself is a locked-down
Deno/Pyodide sandbox that lives on ``pylon_indexer`` (§19.7) — this module is the
**pylon_main-side, dependency-free** half that can run and be tested without the SDK/Deno:

  * Layer-1 AST pre-screen (§19.2) is NOT reimplemented here — it is the single
    fail-closed ``screen_validation_code`` from :mod:`evaluation_code_screen` (EVAL-P1-B1),
    imported and reused verbatim so the author-time editor and this run-time path share one
    block-list (that screen also catches dunder traversal, which a naive copy would miss).
  * ``build_validation_prelude`` — assembles the trusted prelude that injects evidence as
    **plain Python literals** (no client, no network — §19.4) ahead of the untrusted script.
  * ``map_execution_result`` / ``*_verdict`` — turn a sandbox ``CodeExecutionResult``-shaped
    dict into a ``ValidationResult``-shaped verdict and enforce the ``result`` bool/number
    contract (§19.4): a missing/uncoercible ``result`` is an **error verdict**, never a
    crash and never a silent pass.
  * ``make_task_node_executor`` / ``run_code_validation`` — dispatch the assembled script to
    the indexer's ``indexer_code_validation`` task (§19.7) and fold the result into a verdict.
"""
import math
from typing import Any, Optional

from .evaluation_code_screen import screen_validation_code

# Verdict status vocabulary (parallels the AI judge's 'scored'/'error', §18).
STATUS_SCORED = 'scored'
STATUS_ERROR = 'error'
STATUS_NA = 'na'                 # reference-based validation, case has no expected_output (§17.5)
STATUS_UNAVAILABLE = 'unavailable'  # sandbox runtime (Deno) absent (§19.7)

_RESULT_SENTINEL = object()  # "evidence field not provided" (distinct from an explicit None)


def build_validation_prelude(
    script: str,
    *,
    output: str,
    expected: Any = _RESULT_SENTINEL,
    input: Any = _RESULT_SENTINEL,  # noqa: A002 - matches the injected variable name
    structure: Any = _RESULT_SENTINEL,
) -> str:
    """Assemble ``prelude + user script`` (§19.4).

    Evidence is injected as **plain Python literals** via ``repr`` (no client, no network).
    ``output`` is always injected; ``expected`` / ``input`` / ``structure`` only when the
    binding's evidence scope provided them (Axis-C). The untrusted script follows and must
    assign ``result``. NOTE: injection is done by the trusted harness, so ``repr`` of the
    (JSON-safe) evidence values is a safe Python source literal here.

    A trusted epilogue makes ``result`` the final evaluated expression: the sandbox
    (``main.js`` -> ``pyodide.runPythonAsync``) captures the value of the LAST expression,
    not a variable by name, so a script that merely ``result = ...`` (an assignment, which
    evaluates to ``None``) would otherwise surface as ``result=None`` -> a false "script did
    not assign 'result'" error. ``globals().get('result')`` surfaces the assigned value while
    still yielding ``None`` (the contract's missing-result signal) when the script never
    assigned it — never a ``NameError``.
    """
    lines = ['# --- eval harness prelude (trusted, generated) ---']
    lines.append(f'output = {output!r}')
    if expected is not _RESULT_SENTINEL:
        lines.append(f'expected = {expected!r}')
    if input is not _RESULT_SENTINEL:
        lines.append(f'input = {input!r}')
    if structure is not _RESULT_SENTINEL:
        lines.append(f'structure = {structure!r}')
    lines.append('# --- user script (untrusted) ---')
    lines.append(script)
    lines.append('# --- eval harness epilogue (trusted, generated) ---')
    lines.append("globals().get('result')")
    return '\n'.join(lines)


def _coerce_to_contract(value: Any, return_contract: str):
    """Return ``(native_score, passed, error)`` enforcing the §19.4 bool/number contract.

    ``passed`` is only meaningful for the bool contract (None for number). A missing or
    uncoercible ``value`` yields an error string — the caller turns that into an error verdict.
    """
    if value is None:
        return None, None, "script did not assign 'result'"

    if return_contract == 'number':
        # bool is a subclass of int; a bool is not a valid number result.
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None, None, 'result is not a number'
        # NaN/inf (a divide-by-zero in the script) would clamp to a perfect 100 downstream,
        # because min(100.0, float('nan')) is 100.0 — reject it as an error verdict instead.
        if not math.isfinite(value):
            return None, None, 'result is not a finite number'
        return round(float(value), 2), None, None

    # bool contract (default): accept a real bool, or a number treated as truthy/falsy.
    if isinstance(value, bool):
        passed = value
    elif isinstance(value, (int, float)):
        passed = bool(value)
    else:
        return None, None, 'result is not coercible to bool'
    return (1.0 if passed else 0.0), passed, None


def _base_verdict(code_validation_id: Optional[int], name: str) -> dict:
    return {
        'code_validation_id': code_validation_id, 'name': name,
        'native_score': None, 'passed': None,
        'stdout': None, 'execution_time': None,
        'status': STATUS_ERROR, 'error': None,
    }


def na_verdict(code_validation_id: Optional[int], name: str) -> dict:
    """Reference-based validation skipped: case has no ``expected_output`` (§17.5).
    Excluded from the aggregate; never counts as pass or fail."""
    v = _base_verdict(code_validation_id, name)
    v.update(status=STATUS_NA, error='Skipped: validation needs expected_output, case has none.')
    return v


def unavailable_verdict(code_validation_id: Optional[int], name: str,
                        detail: str = 'Sandbox runtime unavailable.') -> dict:
    """Deno/Pyodide runtime absent — a clear 'unavailable' verdict, never an unsandboxed
    fallback (§19.7)."""
    v = _base_verdict(code_validation_id, name)
    v.update(status=STATUS_UNAVAILABLE, error=detail)
    return v


def error_verdict(code_validation_id: Optional[int], name: str, error: str) -> dict:
    v = _base_verdict(code_validation_id, name)
    v.update(status=STATUS_ERROR, error=error)
    return v


def map_execution_result(
    exec_result: dict,
    *,
    code_validation_id: Optional[int],
    name: str,
    return_contract: str = 'bool',
) -> dict:
    """Map a sandbox ``CodeExecutionResult``-shaped dict → a ValidationResult verdict.

    ``exec_result`` carries ``{result, stdout, stderr, status, execution_time}`` (§19.4).
    A sandbox ``status != 'success'`` (timeout/OOM/exception) becomes an **error verdict** so
    the run survives and sibling cases still complete; a successful run then goes through the
    bool/number contract check.
    """
    stdout = exec_result.get('stdout')
    exec_time = exec_result.get('execution_time')
    status = exec_result.get('status')

    if status != 'success':
        v = error_verdict(code_validation_id, name,
                          exec_result.get('stderr') or f'Sandbox execution {status}.')
        v.update(stdout=stdout, execution_time=exec_time)
        return v

    native, passed, err = _coerce_to_contract(exec_result.get('result'), return_contract)
    if err:
        v = error_verdict(code_validation_id, name, err)
        v.update(stdout=stdout, execution_time=exec_time)
        return v

    return {
        'code_validation_id': code_validation_id, 'name': name,
        'native_score': native, 'passed': passed,
        'stdout': stdout, 'execution_time': exec_time,
        'status': STATUS_SCORED, 'error': None,
    }


def make_task_node_executor(
    task_node,
    *,
    timeout: float = 60.0,
    pool: str = 'indexer',
    task_name: str = 'indexer_code_validation',
):
    """Build the default sandbox ``executor`` used by ``run_code_validation``.

    Closes over the module's arbiter ``task_node`` and dispatches the assembled
    ``prelude + script`` to the indexer's ``indexer_code_validation`` task (pool ``indexer`` —
    the same light-task pool as ``indexer_validator``), then blocks on the result (§19.7).
    Kept dependency-free (no SDK, no ``this``) so callers can swap in a stub in tests. Pool
    saturation and the ``join_task`` timeout sentinel (``...``) both become an **error**-shaped
    exec dict — never a raise, never a silent pass — so a stuck worker fails one case, not the run.
    """
    def _execute(code: str) -> dict:
        task_id = task_node.start_task(
            task_name,
            kwargs={'code': code},
            pool=pool,
            meta={'task_name': task_name, 'user_input_preview': 'code validation'},
        )
        if task_id is None:
            return {'result': None, 'stdout': None,
                    'stderr': 'Sandbox worker pool saturated.',
                    'status': STATUS_ERROR, 'execution_time': None}
        result = task_node.join_task(task_id, timeout=timeout)
        if result is ...:  # arbiter join timeout sentinel (matches predict/runtool)
            task_node.stop_task(task_id)
            return {'result': None, 'stdout': None,
                    'stderr': f'Sandbox dispatch timed out after {timeout}s.',
                    'status': STATUS_ERROR, 'execution_time': None}
        return result

    return _execute


def run_code_validation(
    script: str,
    *,
    code_validation_id: Optional[int],
    name: str,
    output: Any,
    expected: Any = _RESULT_SENTINEL,
    input: Any = _RESULT_SENTINEL,  # noqa: A002 - matches the injected variable name
    structure: Any = _RESULT_SENTINEL,
    return_contract: str = 'bool',
    executor,
) -> dict:
    """End-to-end pylon_main-side path for one code validation → a verdict (§19.4/§19.7).

    Screen (defensive re-check) → assemble prelude with the case's evidence → run the
    untrusted script in the sandbox via ``executor`` → map the result to a verdict. Every
    failure mode is a **verdict**, never a raise, so one bad validation fails only its own
    case and sibling cases still complete:

      * a failed Layer-1 screen (should not happen for stored code, but never trusted) →
        error verdict listing the violations;
      * ``executor`` reporting ``status='unavailable'`` (Deno absent) → unavailable verdict
        (excluded from the aggregate — NOT a fail);
      * anything else runs through ``map_execution_result`` (timeout/OOM/exception → error,
        success → bool/number contract check).

    ``executor`` is the sandbox dispatcher (``make_task_node_executor`` in production, a stub
    in tests) — a callable ``(prelude_code: str) -> exec_result dict``.
    """
    violations = screen_validation_code(script)
    if violations:
        return error_verdict(code_validation_id, name, '; '.join(violations))

    prelude = build_validation_prelude(
        script, output=output, expected=expected, input=input, structure=structure,
    )
    exec_result = executor(prelude)

    if exec_result.get('status') == STATUS_UNAVAILABLE:
        return unavailable_verdict(
            code_validation_id, name,
            exec_result.get('stderr') or 'Sandbox runtime unavailable.',
        )
    return map_execution_result(
        exec_result, code_validation_id=code_validation_id,
        name=name, return_contract=return_contract,
    )
