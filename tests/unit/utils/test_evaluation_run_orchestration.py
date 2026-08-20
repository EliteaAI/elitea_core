"""Unit tests for evaluation_run_orchestration.py — the EVAL-H5 pure orchestration core (§14.2/§20).

This is the dependency-free half of the async run job: snapshot assembly, evidence-scope grouping
(D1), opportunistic ``expected_output`` attachment, verdict → EvalResult folding, judge/code
fail-closed (E4), and the one shared aggregation path that keeps the runner's headline equal to
what B5/B6 re-derive.

``evaluation_run_orchestration`` does ``from .evaluation_scoring import ...``, so the sibling is
pre-loaded into sys.modules under its package name before the module itself is loaded.
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def orch(utils_path):
    load_utils_module(utils_path, 'evaluation_scoring')  # sibling for the relative import
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


@pytest.fixture(scope='module')
def scoring(utils_path):
    return load_utils_module(utils_path, 'evaluation_scoring')


# ---------------------------------------------------------------------------
# helpers to build a snapshot
# ---------------------------------------------------------------------------

def _snapshot(orch, *, dimensions=(), code_validations=(), bindings=(), cases=()):
    return orch.build_run_snapshot(
        suite={'id': 1, 'name': 'S', 'judge_model': {'model_name': 'm'}},
        dimensions=list(dimensions),
        code_validations=list(code_validations),
        bindings=list(bindings),
        cases=list(cases),
        application_id=10,
        application_version_id=99,
    )


def _ai_result(dim_id, score, *, status='scored', error=None, name='d'):
    return {'dimension_id': dim_id, 'dimension_name': name, 'native_score': score,
            'rationale': 'because', 'status': status, 'error': error}


# ---------------------------------------------------------------------------
# build_run_snapshot — immutable snapshot shape (§3.4) + D3 version pin
# ---------------------------------------------------------------------------

def test_snapshot_requires_application_version_id(orch):
    with pytest.raises(ValueError):
        orch.build_run_snapshot(
            suite={}, dimensions=[], code_validations=[], bindings=[], cases=[],
            application_id=1, application_version_id=None,
        )


def test_snapshot_freezes_dimensions_bindings_cases(orch):
    snap = _snapshot(
        orch,
        dimensions=[{'id': 5, 'name': 'acc', 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 5, 'weight': 2.0}],
        cases=[{'id': 7, 'input': 'q', 'output': 'a', 'expected_output': 'a'}],
    )
    assert snap['application_version_id'] == 99          # D3 pin
    assert snap['dimensions']['5']['scale_type'] == 'binary'  # keyed by str(id)
    assert snap['bindings'][0]['weight'] == 2.0
    assert snap['cases'][0]['id'] == 7


# ---------------------------------------------------------------------------
# run-assembly helpers (B4) — on-demand cases, reference-free filter, version pin
# ---------------------------------------------------------------------------

def test_cases_from_turns_synthetic_ids_and_reference_free(orch):
    cases = orch.cases_from_turns([('q1', 'a1'), ('q2', None)])
    assert [c['id'] for c in cases] == [1, 2]              # synthetic 1-based ids
    assert cases[0] == {'id': 1, 'input': 'q1', 'output': 'a1',
                        'expected_output': None, 'structure': None, 'order_index': 0}
    assert cases[1]['output'] is None                      # provisional turn (no agent reply)
    assert all(c['expected_output'] is None for c in cases)  # on-demand turns have no reference


def test_resolve_version_id_override_wins(orch):
    assert orch.resolve_version_id([{'application_version_id': 5}], override=9) == 9


def test_resolve_version_id_single_pin(orch):
    assert orch.resolve_version_id(
        [{'application_version_id': 7}, {'application_version_id': 7}, {}]) == 7


def test_resolve_version_id_none_when_unpinned(orch):
    assert orch.resolve_version_id([{}, {'application_version_id': None}]) is None


def test_resolve_version_id_ambiguous_raises(orch):
    with pytest.raises(ValueError):
        orch.resolve_version_id([{'application_version_id': 1}, {'application_version_id': 2}])


# ---------------------------------------------------------------------------
# evidence scope — D1 grouping key + selection + opportunistic expected_output
# ---------------------------------------------------------------------------

def test_scope_key_groups_identical_scopes(orch):
    a = orch.evidence_scope_key({'input': True, 'output': True})
    b = orch.evidence_scope_key({})  # defaults input/output True
    assert a == b            # identical scopes batch together (D1)


def test_select_evidence_gates_by_scope(orch):
    case = {'input': 'i', 'output': 'o', 'expected_output': 'e', 'structure': {'k': 1}}
    minimal = orch.select_evidence(case, {'input': False, 'output': True})
    # output in scope -> expected_output rides along opportunistically regardless of any flag
    assert minimal == {'output': 'o', 'expected_output': 'e'}
    full = orch.select_evidence(case, {'input': True, 'structure': True})
    assert full == {'output': 'o', 'input': 'i', 'structure': {'k': 1}, 'expected_output': 'e'}


def test_select_evidence_expected_output_absent_when_case_has_none(orch):
    case = {'input': 'i', 'output': 'o'}
    evidence = orch.select_evidence(case, {'output': True})
    assert evidence == {'output': 'o', 'input': 'i'}
    assert 'expected_output' not in evidence


def test_select_evidence_expected_output_not_attached_when_output_out_of_scope(orch):
    case = {'input': 'i', 'output': 'o', 'expected_output': 'e'}
    evidence = orch.select_evidence(case, {'output': False, 'input': True})
    assert 'expected_output' not in evidence


def test_select_evidence_output_can_be_suppressed(orch):
    case = {'input': 'i', 'output': 'o'}
    evidence = orch.select_evidence(case, {'output': False, 'input': True})
    assert evidence == {'input': 'i'}  # instructions/input-only scoring, no agent output


def test_select_evidence_output_defaults_true_when_unspecified(orch):
    case = {'output': 'o'}
    assert orch.select_evidence(case, {}) == {'output': 'o', 'input': None}


# --- "evaluate on input only / instructions only" capability -----------------
# These pin down the two use cases the user asked about directly: scoring on the
# user's input alone, or on the agent's instructions/structure alone, with the
# agent's actual output never reaching the evidence dict either way.

def test_input_only_scope_excludes_output_and_structure(orch):
    case = {'input': 'What is 2+2?', 'output': '4', 'structure': {'instructions': 'Be terse'},
            'expected_output': '4'}
    evidence = orch.select_evidence(case, {'input': True, 'output': False})
    assert evidence == {'input': 'What is 2+2?'}
    assert 'output' not in evidence
    assert 'structure' not in evidence
    assert 'expected_output' not in evidence


def test_instructions_only_scope_excludes_output_and_input(orch):
    case = {'input': 'What is 2+2?', 'output': '4',
            'structure': {'instructions': 'Be terse', 'agent_type': 'openai'}}
    evidence = orch.select_evidence(case, {'output': False, 'input': False, 'structure': True})
    assert evidence == {'structure': {'instructions': 'Be terse', 'agent_type': 'openai'}}
    assert 'output' not in evidence
    assert 'input' not in evidence


def test_input_and_instructions_only_scope_combines_without_output(orch):
    case = {'input': 'i', 'output': 'o', 'structure': {'instructions': 'Be nice'},
            'expected_output': 'e'}
    evidence = orch.select_evidence(
        case, {'input': True, 'output': False, 'structure': True},
    )
    assert evidence == {'input': 'i', 'structure': {'instructions': 'Be nice'}}
    assert 'output' not in evidence
    assert 'expected_output' not in evidence


def test_output_suppressed_scope_batches_separately_from_default(orch):
    # D1 batching key must distinguish an output-suppressed scope from the default
    # (output-included) scope, otherwise input-only cases would be silently merged
    # into a batch whose evidence still includes output for other cases.
    default_key = orch.evidence_scope_key({})
    input_only_key = orch.evidence_scope_key({'input': True, 'output': False})
    assert default_key != input_only_key


# ---------------------------------------------------------------------------
# assemble_case_results — AI (D1 batching + E4) / code / human / §17.5
# ---------------------------------------------------------------------------

def test_ai_same_scope_batched_into_one_call(orch):
    calls = []

    def ai_scorer(evidence, dims):
        calls.append([d['id'] for d in dims])
        return [_ai_result(d['id'], 1) for d in dims]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}, {'id': 2, 'scale_type': 'binary'}],
        bindings=[
            {'engine': 'ai', 'dimension_id': 1, 'evidence_scope': {'input': True, 'output': True}},
            {'engine': 'ai', 'dimension_id': 2, 'evidence_scope': {'input': True, 'output': True}},
        ],
    )
    results = orch.assemble_case_results(
        {'id': 1, 'output': 'x'}, snap, ai_scorer=ai_scorer)
    assert calls == [[1, 2]]           # D1: one batched judge call for both dims
    assert {r['dimension_id'] for r in results} == {1, 2}
    assert all(r['status'] == 'ok' and r['normalized_score'] == 100.0 for r in results)


def test_ai_distinct_scopes_split_into_separate_calls(orch):
    calls = []

    def ai_scorer(evidence, dims):
        calls.append(sorted(d['id'] for d in dims))
        return [_ai_result(d['id'], 1) for d in dims]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}, {'id': 2, 'scale_type': 'binary'}],
        bindings=[
            {'engine': 'ai', 'dimension_id': 1, 'evidence_scope': {'input': True, 'output': True}},
            {'engine': 'ai', 'dimension_id': 2, 'evidence_scope': {'input': False, 'output': True}},
        ],
    )
    orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, ai_scorer=ai_scorer)
    assert calls == [[1], [2]]         # different evidence scopes -> separate calls


def test_ai_judge_error_is_error_row_run_survives(orch):
    def ai_scorer(evidence, dims):
        return [_ai_result(d['id'], None, status='error', error='judge unavailable') for d in dims]

    snap = _snapshot(orch, dimensions=[{'id': 1, 'scale_type': 'binary'}],
                     bindings=[{'engine': 'ai', 'dimension_id': 1}])
    r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, ai_scorer=ai_scorer)[0]
    assert r['status'] == 'error' and r['native_score'] is None and r['normalized_score'] is None
    assert r['verdict']['error'] == 'judge unavailable'


def test_ai_scorer_raising_degrades_to_error_rows(orch):
    def ai_scorer(evidence, dims):
        raise RuntimeError('boom')

    snap = _snapshot(orch, dimensions=[{'id': 1, 'scale_type': 'binary'}],
                     bindings=[{'engine': 'ai', 'dimension_id': 1}])
    r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, ai_scorer=ai_scorer)[0]
    assert r['status'] == 'error' and 'boom' in r['verdict']['error']  # E4 fail-closed, no raise


def test_ai_missing_dimension_in_return_is_error(orch):
    def ai_scorer(evidence, dims):
        return []  # judge returned nothing for the requested dim

    snap = _snapshot(orch, dimensions=[{'id': 1, 'scale_type': 'binary'}],
                     bindings=[{'engine': 'ai', 'dimension_id': 1}])
    r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, ai_scorer=ai_scorer)[0]
    assert r['status'] == 'error'


def test_ai_ordinal_normalization(orch):
    def ai_scorer(evidence, dims):
        return [_ai_result(1, 4)]

    snap = _snapshot(orch,
                     dimensions=[{'id': 1, 'scale_type': 'ordinal', 'scale_max': 5}],
                     bindings=[{'engine': 'ai', 'dimension_id': 1}])
    r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, ai_scorer=ai_scorer)[0]
    assert r['native_score'] == 4 and r['normalized_score'] == 75.0  # (4-1)/(5-1)*100


def test_code_scored_ok_and_normalized(orch):
    def code_scorer(binding, evidence):
        return {'code_validation_id': 3, 'name': 'v', 'native_score': 1.0, 'passed': True,
                'stdout': 'ok', 'execution_time': 0.1, 'status': 'scored', 'error': None}

    snap = _snapshot(orch,
                     code_validations=[{'id': 3, 'return_contract': 'bool'}],
                     bindings=[{'engine': 'code', 'code_validation_id': 3}])
    r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, code_scorer=code_scorer)[0]
    assert r['engine'] == 'code' and r['status'] == 'ok'
    assert r['normalized_score'] == 100.0 and r['verdict']['passed'] is True


def test_code_unavailable_and_error_degrade_to_error(orch):
    for status in ('unavailable', 'error'):
        def code_scorer(binding, evidence, _s=status):
            return {'code_validation_id': 3, 'status': _s, 'error': f'{_s} detail'}

        snap = _snapshot(orch, code_validations=[{'id': 3}],
                         bindings=[{'engine': 'code', 'code_validation_id': 3}])
        r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, code_scorer=code_scorer)[0]
        assert r['status'] == 'error' and r['normalized_score'] is None


def test_code_scorer_raising_degrades_to_error(orch):
    def code_scorer(binding, evidence):
        raise ValueError('dispatch blew up')

    snap = _snapshot(orch, code_validations=[{'id': 3}],
                     bindings=[{'engine': 'code', 'code_validation_id': 3}])
    r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, code_scorer=code_scorer)[0]
    assert r['status'] == 'error' and 'dispatch blew up' in r['verdict']['stderr']


def test_code_validation_with_stale_ai_engine_still_runs_as_code(orch):
    # A code-validation binding left with engine='ai' (stale creation default) must dispatch to
    # the code scorer — never the AI judge, which would fail with no code_validation_id on the row.
    ai_calls, code_calls = [], []

    def ai_scorer(evidence, dims):
        ai_calls.append(dims)
        return [_ai_result(d['id'], 1) for d in dims]

    def code_scorer(binding, evidence):
        code_calls.append(binding)
        return {'code_validation_id': 3, 'native_score': 1.0, 'passed': True,
                'stdout': 'ok', 'execution_time': 0.1, 'status': 'scored', 'error': None}

    snap = _snapshot(orch,
                     code_validations=[{'id': 3, 'return_contract': 'bool'}],
                     bindings=[{'engine': 'ai', 'code_validation_id': 3}])
    r = orch.assemble_case_results(
        {'id': 1, 'output': 'x'}, snap, ai_scorer=ai_scorer, code_scorer=code_scorer)[0]
    assert ai_calls == []                                  # not routed to the judge
    assert len(code_calls) == 1                            # routed to code scorer
    assert r['engine'] == 'code' and r['status'] == 'ok'
    assert r['code_validation_id'] == 3                    # row is joinable in the scorecard


def test_effective_engine_derives_from_kind(orch):
    assert orch.effective_engine({'engine': 'ai', 'code_validation_id': 3}) == 'code'
    assert orch.effective_engine({'engine': 'human', 'platform_key': 'pii'}) == 'code'
    assert orch.effective_engine({'engine': 'human', 'dimension_id': 1}) == 'human'
    assert orch.effective_engine({'engine': 'ai', 'dimension_id': 1}) == 'ai'
    assert orch.effective_engine({'dimension_id': 1}) == 'ai'      # default when unset


def test_human_binding_is_pending(orch):
    snap = _snapshot(orch, dimensions=[{'id': 8, 'scale_type': 'binary'}],
                     bindings=[{'engine': 'human', 'dimension_id': 8}])
    r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap)[0]
    assert r['engine'] == 'human' and r['status'] == 'pending_human'
    assert r['native_score'] is None and r['normalized_score'] is None


def test_runs_on_rubric_alone_when_no_expected(orch):
    called = []

    def ai_scorer(evidence, dims):
        called.append(evidence)
        return [_ai_result(d['id'], 1) for d in dims]

    snap = _snapshot(orch, dimensions=[{'id': 1, 'scale_type': 'binary'}],
                     bindings=[{'engine': 'ai', 'dimension_id': 1,
                                'evidence_scope': {'output': True}}])
    # no expected_output on the case -> scored on rubric alone, never skipped
    r = orch.assemble_case_results({'id': 1, 'output': 'x'}, snap, ai_scorer=ai_scorer)[0]
    assert r['status'] == 'ok' and 'expected_output' not in called[0]


def test_runs_with_expected_when_present_opportunistically(orch):
    def ai_scorer(evidence, dims):
        assert evidence['expected_output'] == 'gold'  # attached without any scope opt-in
        return [_ai_result(d['id'], 1) for d in dims]

    snap = _snapshot(orch, dimensions=[{'id': 1, 'scale_type': 'binary'}],
                     bindings=[{'engine': 'ai', 'dimension_id': 1,
                                'evidence_scope': {'output': True}}])
    r = orch.assemble_case_results(
        {'id': 1, 'output': 'x', 'expected_output': 'gold'}, snap, ai_scorer=ai_scorer)[0]
    assert r['status'] == 'ok'


# ---------------------------------------------------------------------------
# orchestrate_run — end-to-end + headline via the shared aggregation path
# ---------------------------------------------------------------------------

def test_orchestrate_headline_matches_shared_aggregation(orch, scoring):
    # two cases, one binary AI dim weight 1: case1 pass(100), case2 fail(0) -> headline 50
    def ai_scorer(evidence, dims):
        score = 1 if evidence['output'] == 'good' else 0
        return [_ai_result(d['id'], score) for d in dims]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1, 'weight': 1.0}],
        cases=[{'id': 1, 'output': 'good'}, {'id': 2, 'output': 'bad'}],
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer)
    assert out['headline_score'] == 50.0
    assert out['progress'] == {'done': 2, 'total': 2}

    # equals the shared path recomputed independently (B5/B6 parity)
    weight_map = scoring.snapshot_weight_map(snap)
    items = [(r['dataset_case_id'],
              scoring.binding_item_key(r.get('dimension_id'), r.get('code_validation_id'),
                                       r.get('platform_key')),
              r['normalized_score'])
             for r in out['results'] if r['status'] == 'ok']
    assert scoring.aggregate_run_score(items, weight_map) == 50.0


def test_orchestrate_weights_dim_and_code_on_same_case(orch):
    # dim binary pass=100 weight 3; code bool fail=0 weight 1 -> (100*3+0*1)/4 = 75
    def ai_scorer(evidence, dims):
        return [_ai_result(1, 1)]

    def code_scorer(binding, evidence):
        return {'code_validation_id': 9, 'native_score': 0.0, 'passed': False, 'status': 'scored'}

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        code_validations=[{'id': 9, 'return_contract': 'bool'}],
        bindings=[
            {'engine': 'ai', 'dimension_id': 1, 'weight': 3.0},
            {'engine': 'code', 'code_validation_id': 9, 'weight': 1.0},
        ],
        cases=[{'id': 1, 'output': 'x'}],
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer, code_scorer=code_scorer)
    assert out['headline_score'] == 75.0  # code no longer collapses onto the dim key


def test_orchestrate_pending_and_error_excluded_from_headline(orch):
    def ai_scorer(evidence, dims):
        return [_ai_result(d['id'], None, status='error', error='x') for d in dims]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}, {'id': 2, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}, {'engine': 'human', 'dimension_id': 2}],
        cases=[{'id': 1, 'output': 'x'}],
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer)
    assert out['headline_score'] is None  # nothing scored ok -> provisional


def test_orchestrate_reports_progress_per_case(orch):
    def ai_scorer(evidence, dims):
        return [_ai_result(d['id'], 1) for d in dims]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': 1, 'output': 'a'}, {'id': 2, 'output': 'b'}, {'id': 3, 'output': 'c'}],
    )
    seen = []
    orch.orchestrate_run(snap, ai_scorer=ai_scorer, on_case_done=lambda d, t: seen.append((d, t)))
    assert seen == [(1, 3), (2, 3), (3, 3)]


def test_orchestrate_survives_failing_progress_callback(orch):
    def ai_scorer(evidence, dims):
        return [_ai_result(d['id'], 1) for d in dims]

    def boom(done, total):
        raise RuntimeError('publish failed')

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': 1, 'output': 'a'}],
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer, on_case_done=boom)
    assert out['headline_score'] == 100.0
    assert out['progress'] == {'done': 1, 'total': 1}


# ---------------------------------------------------------------------------
# orchestrate_run — cooperative cancel (§14.2 durability)
# A 50-case run is hours of agent + judge work, so a run started by mistake needs a way out that
# neither abandons a row reading "in progress" forever nor throws away the work already paid for.
# ---------------------------------------------------------------------------

def _three_case_snapshot(orch):
    return _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': 1, 'output': 'a'}, {'id': 2, 'output': 'b'}, {'id': 3, 'output': 'c'}],
    )


def _ok_ai_scorer(evidence, dims):
    return [_ai_result(d['id'], 1) for d in dims]


def test_cancel_between_cases_keeps_what_already_scored(orch):
    """Partial results are kept: the agent calls and judge tokens are already spent."""
    calls = []

    def should_cancel():
        calls.append(1)
        return len(calls) > 2  # allow cases 1 and 2, stop before case 3

    out = orch.orchestrate_run(
        _three_case_snapshot(orch), ai_scorer=_ok_ai_scorer, should_cancel=should_cancel)

    assert out['cancelled'] is True
    assert {r['dataset_case_id'] for r in out['results']} == {1, 2}
    assert out['headline_score'] == 100.0  # aggregated over what scored, not over N/A cases


def test_cancel_reports_the_cases_actually_scored_not_the_total(orch):
    out = orch.orchestrate_run(
        _three_case_snapshot(orch), ai_scorer=_ok_ai_scorer, should_cancel=lambda: True)
    assert out['progress'] == {'done': 0, 'total': 3}
    assert out['results'] == []


def test_cancel_does_not_shrink_the_frozen_case_set(orch):
    """``cases`` is written back onto the immutable snapshot, so the unscored tail must survive —
    otherwise cancelling would rewrite the run's history down to whatever got scored."""
    out = orch.orchestrate_run(
        _three_case_snapshot(orch), ai_scorer=_ok_ai_scorer, should_cancel=lambda: True)
    assert [c['id'] for c in out['cases']] == [1, 2, 3]


def test_no_cancel_callback_runs_everything(orch):
    out = orch.orchestrate_run(_three_case_snapshot(orch), ai_scorer=_ok_ai_scorer)
    assert out['cancelled'] is False
    assert out['progress'] == {'done': 3, 'total': 3}


def test_cancel_is_checked_before_the_agent_runs_not_after(orch):
    """The point of cancelling is to stop paying for agent executions."""
    ran = []

    def agent_runner(case):
        ran.append(case['id'])
        return {'status': 'ok', 'output': 'x'}

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': 1, 'output': None}, {'id': 2, 'output': None}],
    )
    orch.orchestrate_run(snap, ai_scorer=_ok_ai_scorer, agent_runner=agent_runner,
                         should_cancel=lambda: True)
    assert ran == []


# ---------------------------------------------------------------------------
# orchestrate_run — run-level wall-clock cap
#
# The reaper measures the quiet gap between two cases, so a run that keeps finishing cases keeps
# heartbeating and never looks stale however long it runs. This cap is the only thing that bounds
# total run time, and it must stay distinguishable from a user-requested cancel.
# ---------------------------------------------------------------------------

class _FakeClock:
    """A monotonic clock whose first reading sets the deadline and whose next is far past it, so the
    boundary can be asserted exactly without a real sleep."""

    def __init__(self):
        self._readings = 0

    def monotonic(self):
        self._readings += 1
        return 0.0 if self._readings == 1 else 10_000.0


def test_a_generous_budget_does_not_interfere(orch):
    out = orch.orchestrate_run(
        _three_case_snapshot(orch), ai_scorer=_ok_ai_scorer, time_budget_seconds=3600)
    assert out['stop_reason'] is None
    assert out['progress'] == {'done': 3, 'total': 3}


def test_expired_time_budget_stops_before_any_case(orch):
    """A budget already spent must not start a single agent call."""
    ran = []

    def agent_runner(case):
        ran.append(case['id'])
        return {'status': 'ok', 'output': 'x'}

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': 1, 'output': None}, {'id': 2, 'output': None}],
    )
    real_time = orch.time
    orch.time = _FakeClock()
    try:
        out = orch.orchestrate_run(snap, ai_scorer=_ok_ai_scorer, agent_runner=agent_runner,
                                   time_budget_seconds=5)
    finally:
        orch.time = real_time

    assert ran == []
    assert out['stop_reason'] == orch.STOP_TIME_BUDGET
    assert out['cancelled'] is True
    assert out['progress'] == {'done': 0, 'total': 2}
    # the frozen case set must survive a timeout exactly as it survives a cancel
    assert [c['id'] for c in out['cases']] == [1, 2]


def test_time_budget_is_distinguishable_from_a_user_cancel(orch):
    """Both leave a partial scorecard; only the reason says whether the user chose it."""
    out = orch.orchestrate_run(
        _three_case_snapshot(orch), ai_scorer=_ok_ai_scorer, should_cancel=lambda: True)
    assert out['stop_reason'] == orch.STOP_CANCEL_REQUESTED


def test_time_budget_can_be_disabled(orch):
    out = orch.orchestrate_run(
        _three_case_snapshot(orch), ai_scorer=_ok_ai_scorer, time_budget_seconds=None)
    assert out['stop_reason'] is None
    assert out['progress'] == {'done': 3, 'total': 3}


def test_time_budget_default_is_hours_not_seconds(orch):
    """A default short enough to cut a legitimate dataset short would be worse than none."""
    import inspect
    default = inspect.signature(orch.orchestrate_run).parameters['time_budget_seconds'].default
    assert default == orch.RUN_TIME_BUDGET_SECONDS
    assert default >= 60 * 60


# ---------------------------------------------------------------------------
# orchestrate_run — bounded per-case concurrency
#
# Raising concurrency must not change *what* a run scores, only how fast. The persisted snapshot is
# indexed positionally by the drill-down and results are inserted in list order, so a run whose
# outcome depended on completion timing would silently mismatch case to verdict.
# ---------------------------------------------------------------------------

def _six_case_snapshot(orch):
    return _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': i, 'output': f'o{i}'} for i in range(1, 7)],
    )


def test_concurrency_default_is_one(orch):
    """The default must be the sequential loop: a case is a burst of judge predicts, and
    saturation degrades to error verdicts rather than backpressure."""
    import inspect
    sig = inspect.signature(orch.orchestrate_run)
    assert sig.parameters['case_concurrency'].default == 1


def test_concurrent_run_scores_every_case_exactly_once(orch):
    out = orch.orchestrate_run(
        _six_case_snapshot(orch), ai_scorer=_ok_ai_scorer, case_concurrency=3)

    assert out['cancelled'] is False
    assert out['progress'] == {'done': 6, 'total': 6}
    assert [r['dataset_case_id'] for r in out['results']] == [1, 2, 3, 4, 5, 6]


def test_concurrent_results_stay_in_case_order_despite_completion_order(orch):
    """Results are persisted in list order, so a fast late case must not jump the queue."""
    import time

    def slow_first_agent(case):
        # case 1 finishes last; every other case returns immediately
        if case['id'] == 1:
            time.sleep(0.05)
        return {'status': 'ok', 'output': f"out-{case['id']}"}

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': i, 'output': None} for i in range(1, 5)],
    )
    out = orch.orchestrate_run(snap, ai_scorer=_ok_ai_scorer, agent_runner=slow_first_agent,
                               case_concurrency=4)

    assert [r['dataset_case_id'] for r in out['results']] == [1, 2, 3, 4]
    # `cases` is written back onto the frozen snapshot and indexed positionally by the drill-down,
    # so a shuffled list would show case 1's verdict against case 4's output.
    assert [c['id'] for c in out['cases']] == [1, 2, 3, 4]
    assert [c['output'] for c in out['cases']] == ['out-1', 'out-2', 'out-3', 'out-4']


def test_concurrency_is_bounded(orch):
    """The bound is the point: unbounded fan-out is what floods the judge model."""
    import threading

    in_flight = 0
    peak = 0
    lock = threading.Lock()
    gate = threading.Event()

    def agent_runner(case):
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        # hold every worker until the pool is provably full, so the peak is not just a scheduling
        # artefact of cases completing faster than they are submitted
        gate.wait(0.5)
        with lock:
            in_flight -= 1
        return {'status': 'ok', 'output': 'x'}

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': i, 'output': None} for i in range(1, 9)],
    )
    out = orch.orchestrate_run(snap, ai_scorer=_ok_ai_scorer, agent_runner=agent_runner,
                               case_concurrency=2)

    assert peak <= 2
    assert out['progress'] == {'done': 8, 'total': 8}


def test_concurrent_cancel_keeps_a_prefix_and_the_frozen_tail(orch):
    """Cases are submitted in index order and all submitted are awaited, so the scored set is a
    prefix — the untouched tail still has to come back verbatim."""
    calls = []
    lock = __import__('threading').Lock()

    def should_cancel():
        with lock:
            calls.append(1)
            return len(calls) > 2

    out = orch.orchestrate_run(
        _six_case_snapshot(orch), ai_scorer=_ok_ai_scorer,
        should_cancel=should_cancel, case_concurrency=2)

    assert out['cancelled'] is True
    scored = [r['dataset_case_id'] for r in out['results']]
    assert scored == list(range(1, len(scored) + 1))     # a prefix, not a sparse set
    assert scored and len(scored) < 6
    assert [c['id'] for c in out['cases']] == [1, 2, 3, 4, 5, 6]
    assert out['progress'] == {'done': len(scored), 'total': 6}


def test_concurrent_progress_counts_each_case_once(orch):
    """The counter is shared across worker threads, so it must be locked, and it must never report
    past the total — the UI renders done/total as a percentage."""
    import threading

    seen = []
    lock = threading.Lock()

    def on_case_done(done, total):
        with lock:
            seen.append((done, total))

    orch.orchestrate_run(_six_case_snapshot(orch), ai_scorer=_ok_ai_scorer,
                         on_case_done=on_case_done, case_concurrency=3)

    assert sorted(d for d, _ in seen) == [1, 2, 3, 4, 5, 6]
    assert all(t == 6 for _, t in seen)


def test_concurrent_agent_failure_still_errors_the_run(orch):
    """A raising agent_runner takes the run down exactly as it does sequentially — the caller
    relies on that to mark the row errored rather than publishing a half-scored headline."""
    def exploding_agent(case):
        raise RuntimeError('agent exploded')

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': i, 'output': None} for i in range(1, 5)],
    )
    with pytest.raises(RuntimeError, match='agent exploded'):
        orch.orchestrate_run(snap, ai_scorer=_ok_ai_scorer, agent_runner=exploding_agent,
                             case_concurrency=2)


# ---------------------------------------------------------------------------
# orchestrate_run — live agent execution fill (EVAL-H4, §14.2)
# ---------------------------------------------------------------------------

def test_agent_runner_fills_output_then_scores(orch):
    # batch case has no output; the agent_runner supplies it, and the judge scores the filled output
    def agent_runner(case):
        assert case.get('output') is None       # runner only fires on empty output
        return {'status': 'ok', 'output': 'good', 'error': None}

    def ai_scorer(evidence, dims):
        return [_ai_result(d['id'], 1 if evidence['output'] == 'good' else 0) for d in dims]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1, 'weight': 1.0}],
        cases=[{'id': 1, 'input': 'q'}],        # output None -> agent runs
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer, agent_runner=agent_runner)
    assert out['headline_score'] == 100.0
    assert out['results'][0]['evidence']['output'] == 'good'   # frozen filled output
    # the resolved case (with output filled in) must come back so the caller can write it onto
    # the persisted snapshot — otherwise the drill-down's `snapshot.cases[i].output` stays None
    assert out['cases'][0]['output'] == 'good'


def test_agent_runner_error_makes_error_rows_run_survives(orch):
    # agent execution fails for the case -> every machine binding is an error row, no judge call
    def agent_runner(case):
        return {'status': 'unsupported', 'output': None, 'error': 'agent_type pipeline deferred'}

    def ai_scorer(evidence, dims):  # must never be called for the failed case
        raise AssertionError('judge should not run when agent execution failed')

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}, {'engine': 'human', 'dimension_id': 2}],
        cases=[{'id': 1, 'input': 'q'}],
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer, agent_runner=agent_runner)
    by_engine = {r['engine']: r for r in out['results']}
    assert by_engine['ai']['status'] == 'error'
    assert by_engine['ai']['verdict']['error'] == 'agent_type pipeline deferred'
    assert by_engine['human']['status'] == 'pending_human'   # human still pending
    assert out['headline_score'] is None                     # nothing scored ok


def test_agent_runner_skipped_when_output_present(orch):
    # on-demand cases already carry output; even if an agent_runner is passed it must not fire
    def agent_runner(case):
        raise AssertionError('agent_runner must not run when output is already present')

    def ai_scorer(evidence, dims):
        return [_ai_result(d['id'], 1) for d in dims]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': 1, 'input': 'q', 'output': 'already here'}],
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer, agent_runner=agent_runner)
    assert out['headline_score'] == 100.0


def test_agent_runner_structure_flows_into_resolved_case_and_evidence(orch):
    # the runner's outcome carries 'structure' (agent_structure_snapshot); orchestrate_run must
    # stamp it onto the resolved case, and a structure-scoped binding must see it in evidence.
    structure = {'agent_type': 'react', 'instructions': 'be helpful'}

    def agent_runner(case):
        return {'status': 'ok', 'output': 'good', 'error': None, 'structure': structure}

    seen = {}

    def ai_scorer(evidence, dims):
        seen['evidence'] = evidence
        return [_ai_result(d['id'], 1) for d in dims]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1, 'evidence_scope': {'structure': True}}],
        cases=[{'id': 1, 'input': 'q'}],
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer, agent_runner=agent_runner)
    assert out['cases'][0]['structure'] == structure
    assert seen['evidence']['structure'] == structure


def test_agent_runner_error_still_stamps_structure_on_case(orch):
    structure = {'agent_type': 'react'}

    def agent_runner(case):
        return {'status': 'unsupported', 'output': None, 'error': 'deferred', 'structure': structure}

    def ai_scorer(evidence, dims):
        raise AssertionError('judge should not run when agent execution failed')

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': 1, 'input': 'q'}],
    )
    out = orch.orchestrate_run(snap, ai_scorer=ai_scorer, agent_runner=agent_runner)
    assert out['cases'][0]['structure'] == structure


def test_no_agent_runner_leaves_output_none(orch):
    # without an agent_runner (the pre-H4 path / on-demand=None) output stays None
    seen = {}

    def ai_scorer(evidence, dims):
        seen['output'] = evidence['output']
        return [_ai_result(d['id'], 0)]

    snap = _snapshot(
        orch,
        dimensions=[{'id': 1, 'scale_type': 'binary'}],
        bindings=[{'engine': 'ai', 'dimension_id': 1}],
        cases=[{'id': 1, 'input': 'q'}],
    )
    orch.orchestrate_run(snap, ai_scorer=ai_scorer)
    assert seen['output'] is None


# ---------------------------------------------------------------------------
# shared scoring helpers (evaluation_scoring §20.6) — H5/B5/B6 one path
# ---------------------------------------------------------------------------

def test_snapshot_weight_map_keys_by_item(orch, scoring):
    snap = _snapshot(orch, bindings=[
        {'engine': 'ai', 'dimension_id': 1, 'weight': 2.0},
        {'engine': 'code', 'code_validation_id': 9, 'weight': 5.0},
    ])
    wm = scoring.snapshot_weight_map(snap)
    assert wm[(1, None, None)] == 2.0
    assert wm[(None, 9, None)] == 5.0


def test_aggregate_run_score_defaults_and_excludes(scoring):
    # case A: one item normalized 80, weight default 1 -> 80
    # case B: item None (excluded) + item 40 weight 1 -> 40 ; headline mean = 60
    items = [
        ('A', (1, None, None), 80.0),
        ('B', (1, None, None), None),
        ('B', (None, 9, None), 40.0),
    ]
    assert scoring.aggregate_run_score(items, {}) == 60.0


# ---------------------------------------------------------------------------
# structure-only dataset-less runs (§19.4 follow-up) — is_structure_only_binding /
# all_bindings_structure_only / structure_only_case
# ---------------------------------------------------------------------------

def test_is_structure_only_binding_true_when_input_and_output_both_off(orch):
    binding = {'evidence_scope': {'structure': True, 'input': False, 'output': False, 'expected': False}}
    assert orch.is_structure_only_binding(binding) is True


def test_is_structure_only_binding_false_when_input_or_output_on(orch):
    assert orch.is_structure_only_binding({'evidence_scope': {'input': True, 'output': False}}) is False
    assert orch.is_structure_only_binding({'evidence_scope': {'input': False, 'output': True}}) is False


def test_is_structure_only_binding_defaults_true_for_input_and_output_when_scope_missing(orch):
    # input/output are opt-out (default True in select_evidence) — an empty/missing scope means
    # "everything on", which is NOT structure-only.
    assert orch.is_structure_only_binding({}) is False
    assert orch.is_structure_only_binding({'evidence_scope': {}}) is False


def test_all_bindings_structure_only_true_when_every_binding_qualifies(orch):
    bindings = [
        {'evidence_scope': {'structure': True, 'input': False, 'output': False}},
        {'evidence_scope': {'input': False, 'output': False}},
    ]
    assert orch.all_bindings_structure_only(bindings) is True


def test_all_bindings_structure_only_false_when_one_binding_needs_case_data(orch):
    bindings = [
        {'evidence_scope': {'input': False, 'output': False}},
        {'evidence_scope': {'input': True, 'output': False}},
    ]
    assert orch.all_bindings_structure_only(bindings) is False


def test_all_bindings_structure_only_false_for_empty_bindings_list(orch):
    # an empty suite is not a "structure-only" suite — callers should keep requiring a dataset
    assert orch.all_bindings_structure_only([]) is False


def test_structure_only_case_shape(orch):
    case = orch.structure_only_case()
    assert case['id'] is None
    assert case['input'] is None
    assert case['output'] is None
    assert case['expected_output'] is None
    assert case['structure'] is None
    assert case['variables'] == {}
    assert case['order_index'] == 0


# ---------------------------------------------------------------------------
# snapshot_needs_judge — E4 fail-closed precondition
# ---------------------------------------------------------------------------

def test_snapshot_needs_judge_true_for_an_ai_dimension_binding(orch):
    assert orch.snapshot_needs_judge({'bindings': [{'engine': 'ai', 'dimension_id': 1}]}) is True


def test_snapshot_needs_judge_false_for_human_and_code_only(orch):
    snapshot = {'bindings': [
        {'engine': 'human', 'dimension_id': 1},
        {'engine': 'ai', 'code_validation_id': 9},  # stored engine ignored: code always runs on code
    ]}
    assert orch.snapshot_needs_judge(snapshot) is False


def test_snapshot_needs_judge_false_for_empty_snapshot(orch):
    assert orch.snapshot_needs_judge({}) is False
    assert orch.snapshot_needs_judge(None) is False


# ---------------------------------------------------------------------------
# cap_envelope — bounded evidence/verdict JSONB (review #336)
# ---------------------------------------------------------------------------

def test_cap_envelope_leaves_a_normal_envelope_untouched(orch):
    envelope = {'input': 'hi', 'output': 'there', 'nested': {'items': ['a', 'b']}}
    assert orch.cap_envelope(envelope) == envelope


def test_cap_envelope_clips_an_oversized_string_and_marks_it(orch):
    capped = orch.cap_envelope({'output': 'x' * (orch.MAX_ENVELOPE_TEXT + 50)})

    assert capped['truncated'] is True
    assert capped['output'].endswith('[truncated]')
    assert len(capped['output']) < orch.MAX_ENVELOPE_TEXT + 50


def test_cap_envelope_reaches_strings_nested_in_lists(orch):
    capped = orch.cap_envelope({'steps': [{'text': 'y' * (orch.MAX_ENVELOPE_TEXT + 1)}]})

    assert capped['truncated'] is True
    assert capped['steps'][0]['text'].endswith('[truncated]')


def test_cap_envelope_collapses_an_envelope_that_is_still_too_large(orch):
    # Many individually-legal strings can still add up past the byte budget.
    wide = {f'k{i}': 'z' * 1000 for i in range(500)}

    capped = orch.cap_envelope(wide)

    assert capped['truncated'] is True
    assert 'exceeded' in capped['reason']
    assert capped['keys'][:1] == ['k0']


def test_cap_envelope_survives_a_non_serializable_value(orch):
    capped = orch.cap_envelope({'obj': object()}, max_bytes=10)

    assert capped['truncated'] is True


def test_result_row_caps_both_envelopes(orch):
    row = orch._result_row(
        1, engine='ai', status='ok',
        evidence={'output': 'o' * (orch.MAX_ENVELOPE_TEXT + 10)},
        verdict={'rationale': 'r' * (orch.MAX_ENVELOPE_TEXT + 10)},
    )

    assert row['evidence']['truncated'] is True
    assert row['verdict']['truncated'] is True
