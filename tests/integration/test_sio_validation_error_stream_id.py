"""Regression tests for issue #6386.

Six SIO handlers in ``sio/all.py`` used to construct ``SioValidationError`` without the
required ``stream_id`` argument, so a malformed client payload raised a ``TypeError`` from
inside the validation-error path itself instead of the intended ``SioValidationError`` —
the client got no error event at all, and the real pydantic error detail was discarded.

This reuses the module-loading harness from ``test_eval_run_sio_room.py``: ``sio/all.py``
pulls in the whole chat surface (redis, ORM, SDK utils), so everything except the two
dependency-free modules the handlers actually need — ``utils/sio_utils.py`` and the
pydantic payloads in ``models/pd/sio.py`` — is stubbed out with mocks.
"""
import importlib.abc
import importlib.util
import pathlib
import sys
import types
from unittest.mock import MagicMock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'sioerrpkg_stream_id_test'

_STUBBED = ('redis', 'tools', 'sqlalchemy', 'pylon', f'{PKG}.models.conversation',
            f'{PKG}.models.enums', f'{PKG}.models.message_items', f'{PKG}.models.pd.participant',
            f'{PKG}.models.pd.predict', f'{PKG}.utils.continue_message',
            f'{PKG}.utils.participant_utils', f'{PKG}.utils.canvas_utils',
            f'{PKG}.utils.chat_constants')


class _MockFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """Hand back a MagicMock for anything under `_STUBBED`, leaving real imports alone."""

    def find_spec(self, fullname, path=None, target=None):  # noqa: ARG002
        if any(fullname == root or fullname.startswith(root + '.') for root in _STUBBED):
            return importlib.util.spec_from_loader(fullname, self)
        return None

    def create_module(self, spec):
        mock = MagicMock()
        mock.__name__ = spec.name
        mock.__spec__ = spec
        mock.__path__ = []
        if spec.name == 'pylon.core.tools':
            mock.web.sio = lambda *a, **k: (lambda f: f)
        return mock

    def exec_module(self, module):
        pass


def _load_sio_module():
    finder = _MockFinder()
    sys.meta_path.insert(0, finder)

    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    for name in (f'{PKG}.models', f'{PKG}.models.pd', f'{PKG}.utils', f'{PKG}.sio'):
        mod = types.ModuleType(name)
        mod.__path__ = []
        sys.modules[name] = mod
    sys.modules[PKG] = pkg

    # Imported lazily inside eval_run_enter_room, after the finder is gone.
    run_utils = types.ModuleType(f'{PKG}.utils.evaluation_run_utils')
    run_utils.run_in_project = MagicMock(return_value=True)
    sys.modules[f'{PKG}.utils.evaluation_run_utils'] = run_utils

    try:
        for full, relpath in (
            (f'{PKG}.utils.sio_utils', 'utils/sio_utils.py'),
            (f'{PKG}.models.pd.sio', 'models/pd/sio.py'),
            (f'{PKG}.sio.all', 'sio/all.py'),
        ):
            spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / relpath)
            module = importlib.util.module_from_spec(spec)
            sys.modules[full] = module
            spec.loader.exec_module(module)
    finally:
        sys.meta_path.remove(finder)

    return sys.modules[f'{PKG}.sio.all']


@pytest.fixture
def sio_all():
    module = _load_sio_module()
    yield module
    if hasattr(module.auth, 'is_sio_user_in_project'):
        del module.auth.is_sio_user_in_project
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]


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
