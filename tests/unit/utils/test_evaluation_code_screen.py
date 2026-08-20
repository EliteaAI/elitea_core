"""Unit tests for evaluation_code_screen.py — the Layer-1 AST pre-screen (EVAL-P1-B1).

The screen is a pure function (stdlib only), so it loads directly from its path.
It must fail *closed*: anything statically detectable as an escape is a violation,
and a syntax error is itself a violation (the body cannot be stored).
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def screen(utils_path):
    module = load_utils_module(utils_path, 'evaluation_code_screen')
    return module.screen_validation_code


def test_clean_body_passes(screen):
    assert screen('result = score > 0.5') == []


def test_multiline_clean_body_passes(screen):
    body = 'def check(x):\n    return x >= 10\nresult = check(score)'
    assert screen(body) == []


def test_empty_body_is_violation(screen):
    assert screen('') == ['code body is empty']
    assert screen('   \n  ') == ['code body is empty']


def test_syntax_error_is_violation(screen):
    violations = screen('def f(:')
    assert len(violations) == 1
    assert 'syntax error' in violations[0]


@pytest.mark.parametrize('mod', ['os', 'sys', 'subprocess', 'socket', 'requests', 'urllib', 'pathlib'])
def test_blocked_import(screen, mod):
    violations = screen(f'import {mod}\nresult = True')
    assert any(mod in v and 'not allowed' in v for v in violations)


def test_blocked_import_from(screen):
    violations = screen('from os import path\nresult = True')
    assert any('os' in v and 'not allowed' in v for v in violations)


def test_submodule_import_blocked_by_root(screen):
    violations = screen('import urllib.request\nresult = True')
    assert any('not allowed' in v for v in violations)


@pytest.mark.parametrize('call', ['exec("x=1")', 'eval("1")', 'open("/etc/passwd")', '__import__("os")', 'compile("1","","eval")'])
def test_blocked_builtin_call(screen, call):
    assert any('not allowed' in v for v in screen(call))


def test_bare_builtin_reference_blocked(screen):
    # passing `exec` as a callback is an escape even without calling it
    assert any('exec' in v and 'not allowed' in v for v in screen('cb = exec'))


@pytest.mark.parametrize('attr', ['__globals__', '__class__', '__bases__', '__subclasses__', '__code__'])
def test_blocked_dunder_attr(screen, attr):
    assert any(attr in v and 'not allowed' in v for v in screen(f'x = ().{attr}'))


def test_allowed_builtins_pass(screen):
    # ordinary safe builtins are not blocked
    assert screen('result = len([1, 2, 3]) > abs(-1)') == []


# ---------------------------------------------------------------------------
# Reflection escapes (review on elitea_core#336)
#
# The screen used to inspect only bare-`Name` calls and dunder *attribute names*, which left
# the whole object graph reachable: `getattr` was not blocked, a dunder written as a string
# was never examined, and a subscript was not walked at all. The PoC below passed cleanly.
# ---------------------------------------------------------------------------

REVIEW_POC = """
g = getattr((lambda: 0), '__globals__')
imp = g['__builtins__']['__import__']
os_mod = imp('os')
result = True
"""


def test_review_poc_is_rejected(screen):
    assert screen(REVIEW_POC)


@pytest.mark.parametrize('name', ['getattr', 'setattr', 'delattr'])
def test_reflection_builtins_are_blocked(screen, name):
    # These reach every BLOCKED_ATTRS dunder through a string, which no attribute rule sees.
    assert any(name in v and 'not allowed' in v for v in screen(f'x = {name}(o, "a")'))


@pytest.mark.parametrize('literal', ['__globals__', '__builtins__', '__import__', '__class__'])
def test_dunder_as_a_string_is_blocked(screen, literal):
    assert any(literal in v for v in screen(f'x = d[{literal!r}]\nresult = True'))


def test_blocked_builtin_reached_as_an_attribute(screen):
    assert any('eval' in v and 'not allowed' in v for v in screen('result = m.eval("1")'))


def test_call_through_a_subscript_is_blocked(screen):
    assert any('subscripted' in v for v in screen('result = tbl["f"]()'))


def test_plain_subscripting_still_passes(screen):
    # Only *calling* a subscript is refused; reading case data by key is the normal case.
    assert screen('result = row["score"] > 0.5') == []


def test_ordinary_strings_are_not_mistaken_for_dunders(screen):
    assert screen('result = text == "__" or text == "a__b__c"') == []
