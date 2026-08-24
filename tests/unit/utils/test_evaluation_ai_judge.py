"""Unit tests for evaluate_case — the EVAL-H1 AI case-scorer prototype.

Pure logic: prompt/payload builders + score parse/clamp/error-mapping. The judge is injected as
a stub so no live model / this.module is needed (evaluate_case's default-judge import is lazy).
"""
import json
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def aij(utils_path):
    load_utils_module(utils_path, 'evaluation_scoring')  # sibling of evaluation_run_orchestration
    return load_utils_module(utils_path, 'evaluation_ai_judge')


@pytest.fixture(scope='module')
def truncated_mark(utils_path):
    orch = load_utils_module(utils_path, 'evaluation_run_orchestration')
    return orch._TRUNCATED_MARK


DIMS = [
    {'id': 1, 'name': 'accuracy', 'definition': 'is it right', 'scale_type': 'continuous',
     'scale_min': 0, 'scale_max': 100},
    {'id': 2, 'name': 'helpfulness', 'definition': 'is it useful', 'scale_type': 'ordinal',
     'scale_max': 5},
    {'id': 3, 'name': 'safe', 'definition': 'is it safe', 'scale_type': 'binary'},
]

CASE = {'input': 'q', 'output': 'a', 'expected_output': 'a!'}


def _judge_returning(data, status='ok', error=None):
    def judge(project_id, settings, system_prompt, payload, timeout, *, stream_key=None):
        judge.seen = {'system_prompt': system_prompt, 'payload': payload, 'timeout': timeout}
        return {'status': status, 'data': data, 'error': error, 'raw': None}
    return judge


# --- builders -----------------------------------------------------------------

def test_system_prompt_lists_dims_and_contract(aij):
    p = aij.build_judge_system_prompt(DIMS)
    assert 'id=1' in p and 'id=2' in p and 'id=3' in p
    assert '"scores"' in p and 'dimension_id' in p


def test_case_payload_includes_expected_and_dim_ids(aij):
    payload = json.loads(aij.build_case_payload(CASE, DIMS))
    assert payload['input'] == 'q' and payload['expected_output'] == 'a!'
    assert payload['dimension_ids'] == [1, 2, 3]


def test_case_payload_omits_absent_expected(aij):
    payload = json.loads(aij.build_case_payload({'input': 'q', 'output': 'a'}, DIMS))
    assert 'expected_output' not in payload


def test_case_payload_omits_input_and_output_when_out_of_scope(aij):
    # a structure-only binding's evidence has neither key at all (not present-but-empty) — the
    # payload must not fabricate `input`/`output` as '' (that made the judge think output was
    # empty rather than simply out of scope, per the "Instructions structure" bug report).
    payload = json.loads(aij.build_case_payload({'structure': {'instructions': 'Be terse'}}, DIMS))
    assert 'input' not in payload
    assert 'output' not in payload
    assert payload['structure'] == {'instructions': 'Be terse'}


def test_case_payload_includes_structure_alongside_input(aij):
    payload = json.loads(aij.build_case_payload(
        {'input': 'q', 'structure': {'role': 'assistant'}}, DIMS))
    assert payload['input'] == 'q'
    assert payload['structure'] == {'role': 'assistant'}
    assert 'output' not in payload


def test_system_prompt_tells_judge_absent_fields_are_out_of_scope_not_empty(aij):
    p = aij.build_judge_system_prompt(DIMS)
    assert 'not' in p.lower() and 'scope' in p.lower()
    assert 'structure' in p.lower()


# --- scoring + clamping -------------------------------------------------------

def test_all_dimensions_scored(aij):
    judge = _judge_returning({'scores': [
        {'dimension_id': 1, 'score': 87, 'rationale': 'mostly right'},
        {'dimension_id': 2, 'score': 4, 'rationale': 'quite helpful'},
        {'dimension_id': 3, 'score': 1, 'rationale': 'safe'},
    ]})
    res = aij.evaluate_case(2, {'model_name': 'm'}, CASE, DIMS, judge=judge)
    assert [r['status'] for r in res] == ['scored', 'scored', 'scored']
    assert [r['native_score'] for r in res] == [87.0, 4.0, 1.0]
    assert all(r['rationale'] for r in res)


def test_continuous_clamped_to_max(aij):
    judge = _judge_returning({'scores': [{'dimension_id': 1, 'score': 250, 'rationale': 'x'}]})
    res = aij.evaluate_case(2, {}, CASE, [DIMS[0]], judge=judge)
    assert res[0]['native_score'] == 100.0


def test_ordinal_clamped_to_min_one(aij):
    judge = _judge_returning({'scores': [{'dimension_id': 2, 'score': -3, 'rationale': 'x'}]})
    res = aij.evaluate_case(2, {}, CASE, [DIMS[1]], judge=judge)
    assert res[0]['native_score'] == 1.0


def test_binary_truthy_and_falsy(aij):
    j_true = _judge_returning({'scores': [{'dimension_id': 3, 'score': 5, 'rationale': 'x'}]})
    j_false = _judge_returning({'scores': [{'dimension_id': 3, 'score': 0, 'rationale': 'x'}]})
    assert aij.evaluate_case(2, {}, CASE, [DIMS[2]], judge=j_true)[0]['native_score'] == 1.0
    assert aij.evaluate_case(2, {}, CASE, [DIMS[2]], judge=j_false)[0]['native_score'] == 0.0


def test_missing_dimension_is_error(aij):
    judge = _judge_returning({'scores': [{'dimension_id': 1, 'score': 50, 'rationale': 'x'}]})
    res = aij.evaluate_case(2, {}, CASE, DIMS, judge=judge)
    by_id = {r['dimension_id']: r for r in res}
    assert by_id[1]['status'] == 'scored'
    assert by_id[2]['status'] == 'error' and by_id[2]['error'] == 'missing'
    assert by_id[3]['native_score'] is None


def test_non_numeric_score_is_error(aij):
    judge = _judge_returning({'scores': [{'dimension_id': 1, 'score': 'high', 'rationale': 'x'}]})
    res = aij.evaluate_case(2, {}, CASE, [DIMS[0]], judge=judge)
    assert res[0]['status'] == 'error' and res[0]['error'] == 'non-numeric score'


def test_empty_rationale_falls_back_but_stays_scored(aij):
    judge = _judge_returning({'scores': [{'dimension_id': 1, 'score': 42, 'rationale': '  '}]})
    res = aij.evaluate_case(2, {}, CASE, [DIMS[0]], judge=judge)
    assert res[0]['status'] == 'scored' and res[0]['rationale'] == aij._NO_RATIONALE


def test_match_by_name_when_id_absent(aij):
    judge = _judge_returning({'scores': [{'dimension_name': 'Accuracy', 'score': 60,
                                          'rationale': 'ok'}]})
    res = aij.evaluate_case(2, {}, CASE, [DIMS[0]], judge=judge)
    assert res[0]['status'] == 'scored' and res[0]['native_score'] == 60.0


# --- judge failure → per-dimension error, never raises ------------------------

def test_judge_failure_yields_error_results(aij):
    judge = _judge_returning(None, status='timeout', error='judge timed out after 60s')
    res = aij.evaluate_case(2, {}, CASE, DIMS, judge=judge)
    assert len(res) == 3
    assert all(r['status'] == 'error' and r['native_score'] is None for r in res)
    assert all('timed out' in r['error'] for r in res)
    assert all(r['rationale'] for r in res)


def test_empty_dimensions_returns_empty(aij):
    assert aij.evaluate_case(2, {}, CASE, [], judge=_judge_returning({})) == []


# --- token-budget splitting ----------------------------------------------------

def test_split_dimensions_for_budget_stays_one_batch_when_under_budget(aij, monkeypatch):
    monkeypatch.setattr(aij, 'estimate_group_tokens', lambda evidence, dims, model=None: len(dims) * 10)
    batches = aij.split_dimensions_for_budget(CASE, DIMS, budget_tokens=1000, model='m')
    assert batches == [DIMS]


def test_split_dimensions_for_budget_splits_when_over_budget(aij, monkeypatch):
    # each additional dimension costs 10 tokens; a budget of 25 fits at most 2 per batch
    monkeypatch.setattr(aij, 'estimate_group_tokens', lambda evidence, dims, model=None: len(dims) * 10)
    batches = aij.split_dimensions_for_budget(CASE, DIMS, budget_tokens=25, model='m')
    assert [d['id'] for d in DIMS] == [d['id'] for batch in batches for d in batch]
    assert all(len(batch) <= 2 for batch in batches)
    assert len(batches) >= 2


def test_split_dimensions_for_budget_empty_dims_returns_empty(aij):
    assert aij.split_dimensions_for_budget(CASE, [], budget_tokens=100, model='m') == []


def test_split_dimensions_for_budget_single_oversized_dim_is_its_own_batch(aij, monkeypatch):
    # a dimension that overflows the budget on its own cannot be split further — it comes back
    # as a one-item batch for the caller to shrink via _truncate_evidence_for_budget.
    monkeypatch.setattr(aij, 'estimate_group_tokens', lambda evidence, dims, model=None: 999)
    batches = aij.split_dimensions_for_budget(CASE, [DIMS[0]], budget_tokens=10, model='m')
    assert batches == [[DIMS[0]]]


# --- evidence truncation fallback ----------------------------------------------

def test_truncate_evidence_for_budget_shrinks_largest_field_and_marks_it(aij, truncated_mark, monkeypatch):
    calls = {'n': 0}

    def fake_estimate(evidence, dims, model=None):
        calls['n'] += 1
        # first call (untruncated) is over budget, every call after shrinking is under
        return 999 if calls['n'] == 1 else 1

    monkeypatch.setattr(aij, 'estimate_group_tokens', fake_estimate)
    big_output = 'x' * 1000
    evidence = {'input': 'q', 'output': big_output}
    shrunk = aij._truncate_evidence_for_budget(evidence, [DIMS[0]], budget_tokens=10, model='m')

    assert shrunk['_truncated_for_budget'] is True
    assert len(shrunk['output']) < len(big_output)
    assert shrunk['output'].endswith(truncated_mark)
    assert shrunk['input'] == 'q'  # output is trimmed first, per _TRUNCATE_ORDER


def test_truncate_evidence_for_budget_gives_up_gracefully_when_nothing_left_to_trim(aij, monkeypatch):
    # every field is already short; the loop can't find anything > 200 chars to shrink and must
    # bail out instead of looping forever, but still marks the evidence as truncated-attempted.
    monkeypatch.setattr(aij, 'estimate_group_tokens', lambda evidence, dims, model=None: 999)
    evidence = {'input': 'short', 'output': 'also short'}
    shrunk = aij._truncate_evidence_for_budget(evidence, [DIMS[0]], budget_tokens=10, model='m')
    assert shrunk['_truncated_for_budget'] is True
    assert shrunk['input'] == 'short'
    assert shrunk['output'] == 'also short'


def test_truncate_evidence_for_budget_result_still_reaches_ai_scorer(aij, monkeypatch):
    monkeypatch.setattr(aij, 'estimate_group_tokens', lambda evidence, dims, model=None: 1)
    evidence = {'input': 'q', 'output': 'a'}
    shrunk = aij._truncate_evidence_for_budget(evidence, [DIMS[0]], budget_tokens=1000, model='m')

    judge = _judge_returning({'scores': [{'dimension_id': 1, 'score': 50, 'rationale': 'ok'}]})
    res = aij.evaluate_case(2, {}, shrunk, [DIMS[0]], judge=judge)
    assert res[0]['status'] == 'scored' and res[0]['native_score'] == 50.0
