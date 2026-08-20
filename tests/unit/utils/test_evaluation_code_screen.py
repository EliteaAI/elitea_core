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


# ---------------------------------------------------------------------------
# Import allow-list + the false positives the first widening introduced
# (second review on elitea_core#336)
#
# The deny-list was answered with seven stdlib modules nobody had enumerated — `pdb.run`,
# `cProfile.run`, `timeit.timeit` and `code.InteractiveInterpreter().runsource` all execute
# arbitrary strings, and `linecache`/`zipfile`/`tarfile` read arbitrary paths — so imports are
# now an allow-list. The same review showed the widened rules rejected two *legitimate*
# snippets; both are pinned below so a future tightening cannot bring them back.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('module', [
    'pdb', 'cProfile', 'timeit', 'code', 'linecache', 'zipfile', 'tarfile',
])
def test_modules_outside_the_allow_list_are_rejected(screen, module):
    assert any(module in v for v in screen(f'import {module}\nresult = True'))


@pytest.mark.parametrize('snippet', [
    'import math\nresult = math.isclose(len(input), 3)',
    'import re\nresult = bool(re.search(r"\\d+", input))',
    'import json\nresult = bool(json.dumps({"a": 1}))',
    'import statistics as st\nresult = st.mean([1, 2, 3]) > 1',
    'from decimal import Decimal\nresult = Decimal("1.5") > 1',
])
def test_allow_listed_modules_pass(screen, snippet):
    assert screen(snippet) == []


def test_a_module_method_is_not_mistaken_for_the_builtin_of_the_same_name(screen):
    """`re.compile` is not the `compile` builtin. The attribute rule matches on the name alone,
    so without the module exemption every regex-based validation was rejected."""
    assert screen("import re\nresult = bool(re.compile(r'\\d+').search(input))") == []


def test_a_harmless_dunder_mentioned_as_data_passes(screen):
    """Asserting on the *text* of an agent's output is the normal job of a scoring snippet;
    only the dunders that actually reach the object graph are refused."""
    assert screen("result = '__init__' not in input") == []
    assert screen("result = '__globals__' not in input")  # ...but a reachable one still is


def test_an_alias_for_a_rejected_module_is_still_screened(screen):
    """The exemption is keyed on allow-listed imports only, so aliasing a refused module does not
    launder its calls past the builtin rule."""
    violations = screen("import pdb as m\nresult = m.eval('1')")
    assert any('pdb' in v for v in violations)
    assert any('eval' in v for v in violations)
