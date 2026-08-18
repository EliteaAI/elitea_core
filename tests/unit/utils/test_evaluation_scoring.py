"""Unit tests for evaluation_scoring.py — normalization + weighted aggregation (EVAL-P1-B6, §20).

Pure functions (stdlib only), loaded directly from their path. These lock the §20.3 formulas,
polarity flip (§20.4), and the §20.6 worked example so the numbers stay reproducible.
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def scoring(utils_path):
    return load_utils_module(utils_path, 'evaluation_scoring')


# --- normalize_score: per-scale formulas -------------------------------------

def test_none_native_returns_none(scoring):
    assert scoring.normalize_score(None, 'continuous', 0, 100) is None


def test_binary_true_is_100(scoring):
    assert scoring.normalize_score(True, 'binary') == 100.0


def test_binary_false_is_0(scoring):
    assert scoring.normalize_score(False, 'binary') == 0.0


def test_ordinal_midpoint(scoring):
    # 4 on a 1..5 scale -> (4-1)/(5-1)*100 = 75
    assert scoring.normalize_score(4, 'ordinal', 1, 5) == 75.0


def test_ordinal_min_is_0(scoring):
    assert scoring.normalize_score(1, 'ordinal', 1, 5) == 0.0


def test_ordinal_max_is_100(scoring):
    assert scoring.normalize_score(5, 'ordinal', 1, 5) == 100.0


def test_continuous_passthrough_on_0_100(scoring):
    assert scoring.normalize_score(78, 'continuous', 0, 100) == 78.0


def test_continuous_custom_range(scoring):
    # 15 on a 10..20 scale -> (15-10)/(20-10)*100 = 50
    assert scoring.normalize_score(15, 'continuous', 10, 20) == 50.0


def test_continuous_clamps_above_max(scoring):
    assert scoring.normalize_score(120, 'continuous', 0, 100) == 100.0


def test_continuous_clamps_below_min(scoring):
    assert scoring.normalize_score(-5, 'continuous', 0, 100) == 0.0


def test_lower_better_flips(scoring):
    # toxicity 12 on 0..100 lower_better -> 100 - 12 = 88 (§20.6 worked example)
    assert scoring.normalize_score(12, 'continuous', 0, 100, 'lower_better') == 88.0


def test_degenerate_continuous_raises(scoring):
    with pytest.raises(ValueError):
        scoring.normalize_score(5, 'continuous', 10, 10)


def test_degenerate_ordinal_raises(scoring):
    with pytest.raises(ValueError):
        scoring.normalize_score(1, 'ordinal', 1, 1)


# --- case_weighted_score ------------------------------------------------------

def test_worked_example_case_score(scoring):
    # §20.6: (78*2 + 75*1 + 100*1 + 88*1 + 100*1) / (2+1+1+1+1) = 519/6 = 86.5
    scored = [(78, 2), (75, 1), (100, 1), (88, 1), (100, 1)]
    assert scoring.case_weighted_score(scored) == pytest.approx(86.5)


def test_pending_none_excluded(scoring):
    # a None (pending) score drops out of numerator AND denominator
    assert scoring.case_weighted_score([(100, 1), (None, 3)]) == 100.0


def test_weight_zero_excluded(scoring):
    # weight 0 is informational — excluded from the weighted math (§20.6)
    assert scoring.case_weighted_score([(100, 1), (0, 0)]) == 100.0


def test_all_pending_returns_none(scoring):
    assert scoring.case_weighted_score([(None, 1), (None, 2)]) is None


def test_empty_case_returns_none(scoring):
    assert scoring.case_weighted_score([]) is None


# --- run_headline -------------------------------------------------------------

def test_headline_mean_of_cases(scoring):
    assert scoring.run_headline([80.0, 90.0, 100.0]) == pytest.approx(90.0)


def test_headline_excludes_provisional_cases(scoring):
    assert scoring.run_headline([80.0, None, 100.0]) == pytest.approx(90.0)


def test_headline_no_scored_cases_returns_none(scoring):
    assert scoring.run_headline([None, None]) is None
    assert scoring.run_headline([]) is None


# --- fold_latest_normalized: shared H5/B5/B6 fold (EVAL-P1-B5) -----------------

def test_fold_machine_only(scoring):
    # two dimensions on one case -> two distinct keyed items, no overrides
    items = scoring.fold_latest_normalized(
        [(10, 1, None, None, 80.0), (10, 2, None, None, 60.0)],
        [],
    )
    assert set(items) == {(10, (1, None, None), 80.0), (10, (2, None, None), 60.0)}


def test_fold_human_overrides_machine_on_dimension_key(scoring):
    # human annotation on (case 10, dim 1) supersedes the machine result for that key (§15.3)
    items = scoring.fold_latest_normalized(
        [(10, 1, None, None, 80.0)],
        [(10, 1, 40.0)],
    )
    assert items == [(10, (1, None, None), 40.0)]


def test_fold_human_does_not_touch_code_or_platform_items(scoring):
    # a code validation and a platform key keep their own keys; the human dim override lands only
    # on the dimension key, so nothing collapses (§16.2 — exactly one identity per item)
    items = dict(((c, k), v) for (c, k, v) in scoring.fold_latest_normalized(
        [(10, 1, None, None, 80.0),
         (10, None, 7, None, 100.0),
         (10, None, None, 'schema_valid', 0.0)],
        [(10, 1, 40.0)],
    ))
    assert items[(10, (1, None, None))] == 40.0        # human override
    assert items[(10, (None, 7, None))] == 100.0       # code untouched
    assert items[(10, (None, None, 'schema_valid'))] == 0.0  # platform untouched


def test_fold_then_aggregate_matches_manual(scoring):
    # parity: headline from fold+aggregate equals the hand-computed §20.6-style mean.
    # case 10: dim1 w2 = 80, dim2 w1 = 60  -> (80*2+60*1)/3 = 73.333...
    # case 11: dim1 w2 = 100              -> 100
    # run headline = mean(73.333, 100) = 86.666...
    weight_map = {(1, None, None): 2, (2, None, None): 1}
    items = scoring.fold_latest_normalized(
        [(10, 1, None, None, 80.0), (10, 2, None, None, 60.0), (11, 1, None, None, 100.0)],
        [],
    )
    headline = scoring.aggregate_run_score(items, weight_map)
    # aggregate_run_score rounds to 2 decimals, matching what the Results view shows
    assert headline == pytest.approx(round((80 * 2 + 60) / 3 / 2 + 100 / 2, 2))


def test_fold_none_scores_survive_to_aggregate_as_provisional(scoring):
    # an errored machine item (normalized None) is folded but excluded by the aggregate (§20.6)
    items = scoring.fold_latest_normalized(
        [(10, 1, None, None, None), (10, 2, None, None, 90.0)],
        [],
    )
    assert scoring.aggregate_run_score(items, {}) == pytest.approx(90.0)
