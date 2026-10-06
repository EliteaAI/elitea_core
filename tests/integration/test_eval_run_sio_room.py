"""Integration tests for the eval run progress room handlers in ``sio/all.py`` (phase 2).

The progress room is keyed by run id alone, so the feed's authorization rests on *two* checks that
have to hold together: ``auth.is_sio_user_in_project`` for the claimed project, and
``run_in_project`` to establish that the claimed run actually lives there. Both ids come from the
client, so dropping either one lets an authenticated socket name someone else's run id and watch
their evaluation — including the exception text a failed run carries. Nothing else in the stack
would notice, so both are pinned here.

``sio/all.py`` is loaded through the shared ``sio_all`` fixture (``fixtures/sio_harness.py``).
"""
import sys
import types
from unittest.mock import MagicMock

import pytest
from fixtures.sio_harness import SIO_PKG


class _Handler:
    """Minimal stand-in for the SIO mixin's `self`: only `context.sio` is touched."""

    def __init__(self):
        self.context = types.SimpleNamespace(sio=MagicMock())


def _call(sio_all, name, data, *, allowed, run_in_project=True):
    # `tools.auth` comes from the runner's pylon stubs, which do not carry this method.
    sio_all.auth.is_sio_user_in_project = MagicMock(return_value=allowed)
    sys.modules[f'{SIO_PKG}.utils.evaluation_run_utils'].run_in_project = MagicMock(
        return_value=run_in_project)
    handler = _Handler()
    getattr(sio_all.SIO, name)(handler, 'sid-1', data)
    return handler.context.sio


def test_enter_room_joins_the_run_room_for_a_project_member(sio_all):
    sio = _call(sio_all, 'eval_run_enter_room', {'project_id': 1, 'run_id': 7}, allowed=True)
    sio.enter_room.assert_called_once_with('sid-1', 'room_eval_run_progress_7')


def test_enter_room_refuses_a_sid_that_fails_the_project_check(sio_all):
    sio = _call(sio_all, 'eval_run_enter_room', {'project_id': 1, 'run_id': 7}, allowed=False)
    sio.enter_room.assert_not_called()


def test_enter_room_checks_membership_of_the_claimed_project(sio_all):
    _call(sio_all, 'eval_run_enter_room', {'project_id': 42, 'run_id': 7}, allowed=True)
    sio_all.auth.is_sio_user_in_project.assert_called_once_with('sid-1', 42)


def test_enter_room_acks_the_join_so_the_client_can_trust_the_feed(sio_all):
    """The browser disables its fallback poll on this ack, so a silent join is a silent dialog."""
    sio = _call(sio_all, 'eval_run_enter_room', {'project_id': 1, 'run_id': 7}, allowed=True)
    sio.emit.assert_called_once_with(
        event='eval_run_room_joined', data={'run_id': 7}, to='sid-1')


def test_enter_room_sends_no_ack_when_the_project_check_fails(sio_all):
    """Otherwise a refused socket looks live and never polls, so the dialog just stops moving."""
    sio = _call(sio_all, 'eval_run_enter_room', {'project_id': 1, 'run_id': 7}, allowed=False)
    sio.emit.assert_not_called()


def test_enter_room_refuses_a_run_that_lives_in_another_project(sio_all):
    """Both ids are client-supplied, so membership of the claimed project is not proof of
    ownership: naming a foreign run id must not deliver that run's frames."""
    sio = _call(sio_all, 'eval_run_enter_room', {'project_id': 1, 'run_id': 7},
                allowed=True, run_in_project=False)
    sio.enter_room.assert_not_called()
    sio.emit.assert_not_called()


def test_enter_room_checks_the_run_against_the_claimed_project(sio_all):
    _call(sio_all, 'eval_run_enter_room', {'project_id': 42, 'run_id': 7}, allowed=True)
    sys.modules[f'{SIO_PKG}.utils.evaluation_run_utils'].run_in_project.assert_called_once_with(42, 7)


def test_enter_room_rejects_a_payload_without_a_run_id(sio_all):
    with pytest.raises(Exception):
        _call(sio_all, 'eval_run_enter_room', {'project_id': 1}, allowed=True)


def test_leave_room_leaves_the_matching_room(sio_all):
    sio = _call(sio_all, 'eval_run_leave_room', {'project_id': 1, 'run_id': 7}, allowed=True)
    sio.leave_room.assert_called_once_with('sid-1', 'room_eval_run_progress_7')
