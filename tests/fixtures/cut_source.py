"""Run functions cut out of a real source file, without calling exec() in the tests."""
import ast
import importlib.abc
import importlib.util
import types


class _SourceText(importlib.abc.SourceLoader):
    """Loads a module from a fixed source text; nothing is read from or written to disk."""

    def __init__(self, name, text):
        self._name = name
        self._text = text.encode()

    def get_filename(self, fullname):
        return self._name

    def get_data(self, path):
        return self._text


def load_cut(nodes, namespace, filename):
    """Run the AST `nodes` in a fresh module whose globals start as `namespace`.

    Return the module namespace dict, so callers read the cut-out names from it.
    """
    nodes = [ast.fix_missing_locations(node) for node in nodes]
    text = ast.unparse(ast.Module(body=nodes, type_ignores=[]))
    loader = _SourceText(filename, text)
    spec = importlib.util.spec_from_loader(filename, loader)
    module = types.ModuleType(filename)
    module.__spec__ = spec
    module.__dict__.update(namespace)
    loader.exec_module(module)
    return module.__dict__
