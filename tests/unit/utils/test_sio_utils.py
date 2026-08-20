"""Unit tests for sio_utils.py — room naming and event names.

Room names are the contract between three places that must agree exactly: the
`@web.sio` join handler, the event-node re-emitter, and the frontend hook. A rename
on one side alone silently delivers progress to a room nobody joined, which looks
like "the run just stopped updating" rather than a bug.
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def sio_utils(utils_path):
    return load_utils_module(utils_path, 'sio_utils')


def test_eval_run_room_is_keyed_by_run_id(sio_utils):
    assert sio_utils.get_eval_run_room(42) == 'room_eval_run_progress_42'


def test_eval_run_room_accepts_str_and_int_alike(sio_utils):
    assert sio_utils.get_eval_run_room('42') == sio_utils.get_eval_run_room(42)


def test_eval_run_room_differs_per_run(sio_utils):
    assert sio_utils.get_eval_run_room(1) != sio_utils.get_eval_run_room(2)


@pytest.mark.parametrize('name', [
    'eval_run_progress',
    'eval_run_enter_room',
    'eval_run_leave_room',
])
def test_eval_events_exist_with_matching_values(sio_utils, name):
    # The frontend sends these as literals (EVAL_SIO_EVENTS), so the value must equal the name.
    assert getattr(sio_utils.SioEvents, name).value == name
