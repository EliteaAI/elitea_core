"""Integration tests for the eval run progress room handlers in ``sio/all.py`` (phase 2).

The progress room is keyed by run id alone, so the feed's authorization rests on *two* checks that
have to hold together: ``auth.is_sio_user_in_project`` for the claimed project, and
``run_in_project`` to establish that the claimed run actually lives there. Both ids come from the
client, so dropping either one lets an authenticated socket name someone else's run id and watch
their evaluation — including the exception text a failed run carries. Nothing else in the stack
would notice, so both are pinned here.

``sio/all.py`` pulls in the whole chat surface (redis, ORM, SDK utils), so everything except the
two dependency-free modules the eval handlers actually need — ``utils/sio_utils.py`` and the
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

PKG = 'evalsiopkg_room_test'

# Import roots that must resolve to mocks for `sio/all.py` to load at all.
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
            # `web.sio(event)` is used as a decorator: a MagicMock would replace every handler
            # with a mock, so this one attribute has to behave.
            mock.web.sio = lambda *a, **k: (lambda f: f)
        return mock

    def exec_module(self, module):
        pass


def _load_sio_module():
    finder = _MockFinder()
    sys.meta_path.insert(0, finder)

    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    for name, path in {
        f'{PKG}.models': None,
        f'{PKG}.models.pd': None,
        f'{PKG}.utils': None,
        f'{PKG}.sio': None,
    }.items():
        mod = types.ModuleType(name)
        mod.__path__ = [] if path is None else [path]
        sys.modules[name] = mod
    sys.modules[PKG] = pkg

    # The handler imports this lazily at call time, after the finder is gone, so it has to be
    # sitting in sys.modules already. `_call` swaps in the verdict it wants per test.
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
    # `tools.auth` is a process-wide stub shared with every other test.
    if hasattr(module.auth, 'is_sio_user_in_project'):
        del module.auth.is_sio_user_in_project
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]


class _Handler:
    """Minimal stand-in for the SIO mixin's `self`: only `context.sio` is touched."""

    def __init__(self):
        self.context = types.SimpleNamespace(sio=MagicMock())


def _call(sio_all, name, data, *, allowed, run_in_project=True):
    # `tools.auth` comes from the runner's pylon stubs, which do not carry this method.
    sio_all.auth.is_sio_user_in_project = MagicMock(return_value=allowed)
    sys.modules[f'{PKG}.utils.evaluation_run_utils'].run_in_project = MagicMock(
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
    sys.modules[f'{PKG}.utils.evaluation_run_utils'].run_in_project.assert_called_once_with(42, 7)


def test_enter_room_rejects_a_payload_without_a_run_id(sio_all):
    with pytest.raises(Exception):
        _call(sio_all, 'eval_run_enter_room', {'project_id': 1}, allowed=True)


def test_leave_room_leaves_the_matching_room(sio_all):
    sio = _call(sio_all, 'eval_run_leave_room', {'project_id': 1, 'run_id': 7}, allowed=True)
    sio.leave_room.assert_called_once_with('sid-1', 'room_eval_run_progress_7')
