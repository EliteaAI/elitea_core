"""Unit tests for usage_attribution.evaluation_attribution — #6677.

Pure builder: no I/O. Locks the leaf/root shape (leaf=evaluation marks the run, root keeps the
real application identity) and the None-guard for runs with no application to attribute to.
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def ua(utils_path):
    return load_utils_module(utils_path, 'usage_attribution')


def test_builds_leaf_evaluation_root_application(ua):
    out = ua.evaluation_attribution(eval_run_id=42, application_id=7, application_version_id=3)
    assert out == {
        'entity': {'type': 'evaluation', 'id': 42},
        'root': {'type': 'application', 'id': 7, 'version_id': 3},
    }


def test_no_application_id_returns_none(ua):
    assert ua.evaluation_attribution(eval_run_id=42, application_id=None,
                                     application_version_id=3) is None


def test_no_eval_run_id_returns_none(ua):
    assert ua.evaluation_attribution(eval_run_id=None, application_id=7,
                                     application_version_id=3) is None


def test_missing_version_id_is_allowed(ua):
    out = ua.evaluation_attribution(eval_run_id=42, application_id=7, application_version_id=None)
    assert out['root'] == {'type': 'application', 'id': 7, 'version_id': None}
