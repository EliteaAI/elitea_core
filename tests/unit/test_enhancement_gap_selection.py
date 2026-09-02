"""Unit tests for the "Enhance with AI" gap selection + ranking (ENH-1, §3).

The module is dependency-free by design, so it is imported by path and exercised directly. The
cases here are the ones that decide whether the LLM is asked a sensible question at all: which
rows count as failures, how badly they failed, and which gaps make the cut.
"""
import importlib.util
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def gaps():
    path = PLUGIN_ROOT / 'utils' / 'enhancement_gap_selection.py'
    spec = importlib.util.spec_from_file_location('enhancement_gap_selection_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# evaluate_target_met — must agree with scorecard.helpers.js
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('score, operator, target, expected', [
    (80, '>=', 70, True),
    (70, '>=', 70, True),
    (69, '>=', 70, False),
    (71, '>', 70, True),
    (70, '>', 70, False),
    (2, '<=', 3, True),
    (4, '<=', 3, False),
    (2, '<', 3, True),
    (3, '<', 3, False),
    (5, '==', 5, True),
    (5, '==', 4, False),
])
def test_target_met_operators(gaps, score, operator, target, expected):
    assert gaps.evaluate_target_met(score, operator, target) is expected


@pytest.mark.parametrize('score, operator, target', [
    (None, '>=', 70),      # unscored
    (80, None, 70),        # no operator configured
    (80, '>=', None),      # no target configured
    (80, '>=', ''),         # cleared target from the UI arrives as an empty string
    (80, '~=', 70),        # operator we do not implement
    ('abc', '>=', 70),     # non-numeric score
])
def test_target_not_applicable_returns_none(gaps, score, operator, target):
    """None means "no target to judge against" and must never collapse to True or False — either
    would invent a gap or hide one."""
    assert gaps.evaluate_target_met(score, operator, target) is None


# ---------------------------------------------------------------------------
# scale span + shortfall
# ---------------------------------------------------------------------------

def test_span_binary_and_bool_code_contract(gaps):
    assert gaps.native_scale_span({'scale_type': 'binary'}) == 1.0
    # A bool code dimension is binary regardless of its stored scale fields.
    assert gaps.native_scale_span(
        {'scale_type': 'continuous', 'scale_min': 0, 'scale_max': 100, 'return_contract': 'bool'},
        gaps.ENGINE_CODE,
    ) == 1.0


def test_span_ordinal_defaults_to_one_based(gaps):
    assert gaps.native_scale_span({'scale_type': 'ordinal', 'scale_max': 5}) == 4.0


def test_span_continuous_defaults_to_zero_hundred(gaps):
    assert gaps.native_scale_span({'scale_type': 'continuous'}) == 100.0


@pytest.mark.parametrize('spec', [
    {'scale_type': 'ordinal'},                                  # no max
    {'scale_type': 'continuous', 'scale_min': 5, 'scale_max': 5},  # degenerate
])
def test_span_degenerate_is_none(gaps, spec):
    assert gaps.native_scale_span(spec) is None


def test_shortfall_is_a_magnitude_not_a_direction(gaps):
    """An overshot ``<=`` target and an undershot ``>=`` target are equally bad. Deriving this
    from the 0-100 quality axis instead would report 0 for every overshoot."""
    assert gaps.target_shortfall(60, 70, 100.0) == pytest.approx(0.1)
    assert gaps.target_shortfall(80, 70, 100.0) == pytest.approx(0.1)


def test_shortfall_clamped_and_guarded(gaps):
    assert gaps.target_shortfall(0, 500, 100.0) == 1.0
    assert gaps.target_shortfall(60, 70, None) is None
    assert gaps.target_shortfall(None, 70, 100.0) is None


# ---------------------------------------------------------------------------
# collect_binding_gaps
# ---------------------------------------------------------------------------

def _snapshot(**overrides):
    snapshot = {
        'dimensions': {
            '1': {
                'name': 'Helpfulness',
                'description': 'Score 0-100 on how helpful the answer is.',
                'scale_type': 'continuous', 'scale_min': 0, 'scale_max': 100,
                'polarity': 'higher_better',
            },
        },
        'bindings': [
            {'engine': 'ai', 'dimension_id': 1, 'platform_key': None,
             'weight': 2.0, 'target': 70, 'target_operator': '>=', 'order_index': 0},
        ],
        'cases': [
            {'id': 10, 'input': 'q1', 'output': 'a1', 'expected_output': None, 'order_index': 0},
            {'id': 11, 'input': 'q2', 'output': 'a2', 'expected_output': 'ref', 'order_index': 1},
        ],
    }
    snapshot.update(overrides)
    return snapshot


def _result(case_id, native, **overrides):
    row = {
        'dataset_case_id': case_id, 'dimension_id': 1, 'platform_key': None,
        'engine': 'ai', 'status': 'ok', 'native_score': native,
        'verdict': {'rationale': f'because {case_id}'},
    }
    row.update(overrides)
    return row


def test_gap_reports_miss_statistics_and_quotes_rubric_verbatim(gaps):
    out = gaps.collect_binding_gaps(_snapshot(), [_result(10, 40), _result(11, 90)])
    gap = out['gaps'][0]

    assert gap['missed_count'] == 1
    assert gap['scored_count'] == 2
    assert gap['miss_rate'] == pytest.approx(0.5)
    assert gap['mean_shortfall'] == pytest.approx(0.3)
    assert gap['rubric'] == 'Score 0-100 on how helpful the answer is.'
    assert [case['case_id'] for case in gap['cases']] == [10]
    assert gap['cases'][0]['reasoning'] == 'because 10'
    assert gap['cases'][0]['expected_output'] is None


def test_binding_without_a_target_produces_no_gap(gaps):
    """Without a target there is no defined notion of failure. Guessing one from a low score
    would have the AI argue against a standard the author never set."""
    snapshot = _snapshot(bindings=[
        {'engine': 'ai', 'dimension_id': 1, 'weight': 1.0, 'target': None, 'target_operator': None},
    ])
    out = gaps.collect_binding_gaps(snapshot, [_result(10, 1), _result(11, 2)])

    assert out['gaps'] == []
    assert out['coverage']['targeted_bindings'] == 0


def test_error_rows_are_excluded_not_counted_as_misses(gaps):
    """An errored judge never measured the agent. Counting it as a miss would have the AI rewrite
    instructions to fix infrastructure."""
    out = gaps.collect_binding_gaps(
        _snapshot(),
        [_result(10, None, status='error', verdict={'error': 'judge timed out'}), _result(11, 90)],
    )

    assert out['gaps'] == []
    assert out['coverage']['excluded_error_results'] == 1


def test_pending_human_rows_are_excluded(gaps):
    out = gaps.collect_binding_gaps(_snapshot(), [_result(10, None, status='pending_human'), _result(11, 90)])

    assert out['gaps'] == []
    assert out['coverage']['excluded_pending_human'] == 1


def test_human_engine_reads_latest_annotation_and_skips_unscored(gaps):
    snapshot = _snapshot(bindings=[
        {'engine': 'human', 'dimension_id': 1, 'weight': 1.0, 'target': 70, 'target_operator': '>='},
    ])
    human_scores = [
        {'dataset_case_id': 10, 'dimension_id': 1, 'native_score': 20, 'note': 'stale', 'is_latest': False},
        {'dataset_case_id': 10, 'dimension_id': 1, 'native_score': 50, 'note': 'weak answer', 'is_latest': True},
        # case 11 has no annotation at all -> pending, excluded
    ]
    out = gaps.collect_binding_gaps(snapshot, [], human_scores)
    gap = out['gaps'][0]

    assert gap['missed_count'] == 1
    assert gap['scored_count'] == 1
    assert gap['cases'][0]['native_score'] == 50
    assert gap['cases'][0]['reasoning'] == 'weak answer'
    assert out['coverage']['excluded_pending_human'] == 1


def test_code_engine_prefers_stderr_traceback_as_evidence(gaps):
    """The traceback names the exact assertion that failed — the highest-value evidence in the
    whole payload."""
    snapshot = _snapshot(bindings=[
        {'engine': 'code', 'dimension_id': 1, 'weight': 1.0, 'target': 1, 'target_operator': '=='},
    ])
    row = _result(10, 0, engine='code', verdict={
        'passed': False, 'stdout': 'checking', 'stderr': 'AssertionError: missing citation',
    })
    out = gaps.collect_binding_gaps(snapshot, [row])

    assert 'AssertionError: missing citation' in out['gaps'][0]['cases'][0]['reasoning']


def test_lower_better_overshoot_is_a_miss_with_real_shortfall(gaps):
    """A ``<=`` target overshot must register as a miss with a non-zero shortfall. Deriving the
    shortfall from the quality axis would report 0 here and rank the gap last."""
    snapshot = _snapshot(bindings=[
        {'engine': 'ai', 'dimension_id': 1, 'weight': 1.0, 'target': 20, 'target_operator': '<='},
    ])
    out = gaps.collect_binding_gaps(snapshot, [_result(10, 60)])
    gap = out['gaps'][0]

    assert gap['missed_count'] == 1
    assert gap['cases'][0]['shortfall'] == pytest.approx(0.4)


def test_long_evidence_is_truncated(gaps):
    snapshot = _snapshot(cases=[{'id': 10, 'input': 'x' * 5000, 'output': 'y' * 9000, 'order_index': 0}])
    out = gaps.collect_binding_gaps(snapshot, [_result(10, 10)])
    case = out['gaps'][0]['cases'][0]

    assert len(case['input']) < 5000 and case['input'].endswith('[truncated]')
    assert len(case['output']) < 9000 and case['output'].endswith('[truncated]')


# ---------------------------------------------------------------------------
# ranking + caps
# ---------------------------------------------------------------------------

def _gap(name, weight, shortfall, miss_rate, missed=1, cases=None):
    return {
        'name': name, 'weight': weight, 'mean_shortfall': shortfall,
        'miss_rate': miss_rate, 'missed_count': missed, 'cases': cases or [],
    }


def test_impact_multiplies_all_three_factors(gaps):
    assert gaps.gap_impact(_gap('a', 2.0, 0.5, 0.5)) == pytest.approx(0.5)


def test_impact_falls_back_to_weight_and_frequency_on_degenerate_scale(gaps):
    """A gap whose scale cannot express a shortfall must still rank, not drop to zero and vanish."""
    assert gaps.gap_impact(_gap('a', 2.0, None, 0.5)) == pytest.approx(1.0)


def test_ranking_prefers_high_impact_and_caps_dimensions(gaps):
    ranked = gaps.rank_gaps(
        [_gap('low', 1.0, 0.1, 0.1), _gap('high', 3.0, 0.9, 1.0), _gap('mid', 1.0, 0.5, 1.0)],
        max_dimensions=2,
    )
    assert [gap['name'] for gap in ranked] == ['high', 'mid']


def test_ranking_is_total_so_the_prompt_is_stable(gaps):
    """Two gaps with identical impact must order deterministically — otherwise an unchanged run
    yields a different prompt on every click and a prompt regression is indistinguishable from
    ordinary variance."""
    tied = [_gap('bravo', 1.0, 0.5, 1.0), _gap('alpha', 1.0, 0.5, 1.0)]
    assert [gap['name'] for gap in gaps.rank_gaps(tied)] == ['alpha', 'bravo']
    assert [gap['name'] for gap in gaps.rank_gaps(list(reversed(tied)))] == ['alpha', 'bravo']


def test_ranking_keeps_the_worst_cases_within_the_cap(gaps):
    cases = [
        {'case_id': 1, 'shortfall': 0.1}, {'case_id': 2, 'shortfall': 0.9},
        {'case_id': 3, 'shortfall': 0.5}, {'case_id': 4, 'shortfall': 0.7},
    ]
    ranked = gaps.rank_gaps([_gap('a', 1.0, 0.5, 1.0, cases=cases)], max_cases=2)
    assert [case['case_id'] for case in ranked[0]['cases']] == [2, 4]


def test_ranking_does_not_mutate_its_input(gaps):
    source = [_gap('a', 1.0, 0.5, 1.0, cases=[{'case_id': 1, 'shortfall': 0.1},
                                              {'case_id': 2, 'shortfall': 0.9}])]
    gaps.rank_gaps(source, max_cases=1)
    assert len(source[0]['cases']) == 2


# ---------------------------------------------------------------------------
# select_gaps coverage reporting
# ---------------------------------------------------------------------------

def test_coverage_reports_what_was_left_out(gaps):
    """A truncated analysis presented as a complete one is what this field exists to prevent."""
    snapshot = _snapshot(cases=[
        {'id': i, 'input': f'q{i}', 'output': f'a{i}', 'order_index': i} for i in range(10, 20)
    ])
    results = [_result(i, 10) for i in range(10, 20)]
    out = gaps.select_gaps(snapshot, results, max_cases=3)

    assert out['coverage']['missed_cases_total'] == 10
    assert out['coverage']['missed_cases_returned'] == 3
    assert out['coverage']['gap_dimensions_total'] == 1
    assert out['coverage']['max_cases_per_dimension'] == 3


def test_run_with_no_misses_yields_no_gaps(gaps):
    out = gaps.select_gaps(_snapshot(), [_result(10, 95), _result(11, 90)])
    assert out['gaps'] == []
    assert out['coverage']['missed_bindings'] == 0


def test_empty_snapshot_is_handled(gaps):
    out = gaps.select_gaps({}, [])
    assert out['gaps'] == []
    assert out['coverage']['total_cases'] == 0
