"""Run one method of an RPC class without importing the plugin around it.

``rpc/application.py`` and ``rpc/chat_all.py`` pull in the ORM, pylon and the SDK, so loading them
for a test means stubbing dozens of modules. The methods under test only reach a handful of names
before the code that matters, so the method is cut out of the source with ``ast`` and compiled
against a namespace where every name nobody provided is a mock.

Provide the names the test cares about (the real ``SioValidationError``, a ``redis`` carrying a real
exception class, ...) through ``overrides``; everything else resolves to a fresh ``MagicMock``.
"""
import ast
import builtins
import importlib.util
import pathlib
import sys
from unittest.mock import MagicMock

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


class _MockNamespace(dict):
    """Globals that invent a MagicMock for any name that is neither provided nor a builtin."""

    def __missing__(self, name):
        if name.startswith('__') or hasattr(builtins, name):
            raise KeyError(name)
        value = self[name] = MagicMock(name=name)
        return value


def load_real_module(relative_path, module_name):
    """Load a dependency-free plugin module (``utils/sio_utils.py``, ``utils/exceptions.py``) for real."""
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_rpc_method(relative_path, class_name, method_name, overrides=None):
    """Return the plain function ``class_name.method_name`` from ``relative_path``.

    Decorators are dropped (``@web.rpc`` would only register the function), so the result is called as
    ``method(fake_self, *args)``.
    """
    source = (PLUGIN_ROOT / relative_path).read_text()
    tree = ast.parse(source)
    owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
    function = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == method_name)
    function.decorator_list = []
    module = ast.Module(body=[function], type_ignores=[])
    ast.fix_missing_locations(module)

    namespace = _MockNamespace(overrides or {})
    exec(compile(module, str(PLUGIN_ROOT / relative_path), 'exec'), namespace)  # pylint: disable=W0122
    return namespace[method_name]
