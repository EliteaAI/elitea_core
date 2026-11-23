"""dry_run_flag: a dry run by default, unless the task opts into running by default (#7383)."""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def params():
    return load_utils_module(TESTS_DIR.parent / 'utils', 'admin_task_params')


@pytest.mark.parametrize('param, expected', [
    (None, True), ('', True), ('project_id=all', True), ('project_id=all;dry_run=false', False),
    ('dry_run', True), ('dry_run=true', True), ('dry_run=0', False), ('dry_run=no', False),
])
def test_dry_run_by_default(params, param, expected):
    assert params.dry_run_flag(param) is expected


@pytest.mark.parametrize('param, expected', [
    (None, False), ('project_id=all', False), ('project_id=12', False),
    ('project_id=all;dry_run=true', True), ('dry_run', True), ('dry_run=false', False),
    ('my_dry_run_note=x', False),
])
def test_runs_by_default_when_asked(params, param, expected):
    assert params.dry_run_flag(param, default=False) is expected
