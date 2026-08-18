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
    return load_utils_module(utils_path, 'evaluation_ai_judge')


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
