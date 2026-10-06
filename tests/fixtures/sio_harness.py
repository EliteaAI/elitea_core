"""Loader for ``sio/all.py`` with the chat surface (redis, ORM, SDK utils) stubbed out.

Only the dependency-free modules the handlers need (``utils/sio_utils.py``,
``utils/toolkit_test_rooms.py``, ``models/pd/sio.py``) are loaded for real. Add a new stub or a
new real module here and every sio test picks it up.
"""
import importlib.abc
import importlib.util
import pathlib
import sys
import types
from unittest.mock import MagicMock

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

SIO_PKG = 'sio_harness_pkg'

# Import roots that must resolve to mocks for `sio/all.py` to load at all.
_STUBBED = ('redis', 'tools', 'sqlalchemy', 'pylon', f'{SIO_PKG}.models.conversation',
            f'{SIO_PKG}.models.message_group', f'{SIO_PKG}.models.enums',
            f'{SIO_PKG}.models.message_items', f'{SIO_PKG}.models.pd.participant',
            f'{SIO_PKG}.models.pd.predict', f'{SIO_PKG}.utils.continue_message',
            f'{SIO_PKG}.utils.participant_utils', f'{SIO_PKG}.utils.canvas_utils',
            f'{SIO_PKG}.utils.chat_constants', f'{SIO_PKG}.utils.conversation_access')

# Loaded for real, in dependency order.
_REAL = (
    (f'{SIO_PKG}.utils.sio_utils', 'utils/sio_utils.py'),
    (f'{SIO_PKG}.utils.toolkit_test_rooms', 'utils/toolkit_test_rooms.py'),
    (f'{SIO_PKG}.models.pd.sio', 'models/pd/sio.py'),
    (f'{SIO_PKG}.sio.all', 'sio/all.py'),
)


def _is_stubbed(name: str) -> bool:
    return any(name == root or name.startswith(root + '.') for root in _STUBBED)


class _MockFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """Hand back a MagicMock for anything under `_STUBBED`, leaving real imports alone."""

    def find_spec(self, fullname, path=None, target=None):  # noqa: ARG002
        if _is_stubbed(fullname):
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


def load_sio_module():
    finder = _MockFinder()
    # Stubs left in sys.modules by earlier tests would bypass the finder in a full-suite run
    shadowed = {k: sys.modules.pop(k) for k in list(sys.modules) if _is_stubbed(k)}
    sys.meta_path.insert(0, finder)

    pkg = types.ModuleType(SIO_PKG)
    pkg.__path__ = []
    for name in (f'{SIO_PKG}.models', f'{SIO_PKG}.models.pd', f'{SIO_PKG}.utils', f'{SIO_PKG}.sio'):
        mod = types.ModuleType(name)
        mod.__path__ = []
        sys.modules[name] = mod
    sys.modules[SIO_PKG] = pkg

    # Imported lazily inside eval_run_enter_room, after the finder is gone, so it has to be
    # sitting in sys.modules already. Tests swap in the verdict they want.
    run_utils = types.ModuleType(f'{SIO_PKG}.utils.evaluation_run_utils')
    run_utils.run_in_project = MagicMock(return_value=True)
    sys.modules[f'{SIO_PKG}.utils.evaluation_run_utils'] = run_utils

    try:
        for full, relpath in _REAL:
            spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / relpath)
            module = importlib.util.module_from_spec(spec)
            sys.modules[full] = module
            spec.loader.exec_module(module)
    finally:
        sys.meta_path.remove(finder)
        sys.modules.update(shadowed)

    return sys.modules[f'{SIO_PKG}.sio.all']


def unload_sio_module():
    for name in list(sys.modules):
        if name == SIO_PKG or name.startswith(SIO_PKG + '.'):
            del sys.modules[name]
