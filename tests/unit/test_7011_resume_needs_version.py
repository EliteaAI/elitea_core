"""EL-7011 US-14: R2.4, a schedule without a version cannot resume (TC-US14-13, TC-US14-14).

Reuses the handler harness of the US-07 test file.

Run via:
    python tests/run_tests.py unit/test_7011_resume_needs_version.py -v
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from test_7011_schedule_crud_api import harness  # noqa: F401  (pytest fixture)


def test_7011_r2_4_resume_without_a_version_is_400_on_version_id(harness):  # noqa: F811
    schedule_id = harness.seed(version_id=None, enabled=False, disabled_reason='version_deleted')
    body, status = harness.call('post', None, schedule_id, suffix='/resume')
    row = harness.row(schedule_id)
    assert (status, body['field']) == (400, 'version_id')
    assert (row.enabled, row.disabled_reason) == (False, 'version_deleted')


def test_7011_r2_4_resume_with_a_chosen_version_clears_the_disabled_reason(harness):  # noqa: F811
    schedule_id = harness.seed(version_id=None, enabled=False, disabled_reason='version_deleted')
    assert harness.call('put', {'version_id': 10}, schedule_id)[1] == 200
    _, status = harness.call('post', None, schedule_id, suffix='/resume')
    row = harness.row(schedule_id)
    assert status == 200
    assert (row.enabled, row.version_id, row.disabled_reason) == (True, 10, None)
