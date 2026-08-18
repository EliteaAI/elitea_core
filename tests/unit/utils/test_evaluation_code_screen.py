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
