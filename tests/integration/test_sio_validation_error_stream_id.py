"""Regression tests for issue #6386.

Six SIO handlers in ``sio/all.py`` used to construct ``SioValidationError`` without the
required ``stream_id`` argument, so a malformed client payload raised a ``TypeError`` from
inside the validation-error path itself instead of the intended ``SioValidationError`` —
the client got no error event at all, and the real pydantic error detail was discarded.

``sio/all.py`` is loaded through the shared ``sio_all`` fixture (``fixtures/sio_harness.py``).
"""
import types
from unittest.mock import MagicMock

import pytest


class _Handler:
    """Minimal stand-in for the SIO mixin's `self`: only `context.sio` is touched."""

    def __init__(self):
        self.context = types.SimpleNamespace(sio=MagicMock())


def _raise_validation_error(sio_all, name, data):
    """Invoke `name` with a payload that fails pydantic validation and return the raised error."""
    handler = _Handler()
    with pytest.raises(sio_all.SioValidationError) as exc_info:
        getattr(sio_all.SIO, name)(handler, 'sid-1', data)
    return exc_info.value


@pytest.mark.parametrize('handler_name, data, expected_stream_id', [
    # project_id omitted -> validation fails, but conversation_id is still there to report.
    ('enter_room', {'conversation_id': 'conv-123'}, 'conv-123'),
    # stream_id itself is present and valid; event_name is the field that fails validation.
    ('test_toolkit_enter_room', {'stream_id': 'ttk-1', 'event_name': {'bad': True}}, 'ttk-1'),
    ('join_canvas', {'canvas_uuid': 'canvas-1'}, 'canvas-1'),
    ('edit_canvas', {'canvas_uuid': 'canvas-2'}, 'canvas-2'),
])
def test_single_value_handler_reports_a_real_stream_id_on_validation_failure(
    sio_all, handler_name, data, expected_stream_id,
):
    error = _raise_validation_error(sio_all, handler_name, data)
    assert error.stream_id == expected_stream_id


@pytest.mark.parametrize('handler_name', ['leave_rooms', 'canvas_leave_room'])
def test_list_handler_falls_back_to_empty_stream_id_on_validation_failure(sio_all, handler_name):
    """These handlers validate a list of items pre-parse, so no single id is available; they
    rely on SioValidationError's default rather than passing stream_id explicitly."""
    error = _raise_validation_error(sio_all, handler_name, {})
    assert error.stream_id == ''
