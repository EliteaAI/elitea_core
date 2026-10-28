"""Pylon binds every RPC method with the plugin Module as `self`. The Module gets a sibling method only
under the proxy name of `@web.rpc(name, proxy_name)` or the name of `@web.method()`; a call through `self`
to a sibling registered by `@web.rpc(name)` alone fails at run time with an AttributeError.

Contract under test: every module under rpc/.
"""
import ast
from pathlib import Path

import pytest

RPC_DIR = Path(__file__).parents[3] / "rpc"


def _on_the_module(method):
    """True when pylon also sets the method on the plugin Module under the name `self.<name>` reads."""
    for dec in method.decorator_list:
        if not (isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute)):
            continue
        if dec.func.attr == "method":
            names = [a.value for a in dec.args if isinstance(a, ast.Constant)]
            if not names or names[0] == method.name:
                return True
        if dec.func.attr == "rpc":
            if not dec.args and not dec.keywords:  # auto names: the proxy name is the method name
                return True
            if len(dec.args) > 1 and getattr(dec.args[1], "value", None) == method.name:
                return True
    return False


def _sibling_calls(path):
    """(method, sibling) pairs where a method of class RPC reads `self.<sibling>` of the same class."""
    found = []
    for cls in ast.parse(path.read_text()).body:
        if not (isinstance(cls, ast.ClassDef) and cls.name == "RPC"):
            continue
        methods = {
            node.name for node in cls.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not _on_the_module(node)
        }
        for method in cls.body:
            if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(method):
                if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                        and node.value.id == "self" and node.attr in methods):
                    found.append((method.name, node.attr))
    return found


@pytest.mark.parametrize("path", sorted(RPC_DIR.glob("*.py")), ids=lambda p: p.name)
def test_no_rpc_method_calls_a_sibling_method_through_self(path):
    assert _sibling_calls(path) == []


def test_the_scan_finds_a_sibling_call(tmp_path):
    sample = tmp_path / "sample.py"
    sample.write_text(
        "class RPC:\n"
        "    @web.rpc('tick')\n"
        "    def tick(self):\n"
        "        pass\n"
        "    @web.rpc('proxied_name', 'proxied')\n"
        "    def proxied(self):\n"
        "        pass\n"
        "    @web.method()\n"
        "    def helper(self):\n"
        "        pass\n"
        "    def old_name(self):\n"
        "        self.proxied()\n"
        "        self.helper()\n"
        "        return self.tick()\n"
    )
    assert _sibling_calls(sample) == [("old_name", "tick")]
