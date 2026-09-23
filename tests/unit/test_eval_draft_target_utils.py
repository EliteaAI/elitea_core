"""Unit tests for ``sanitize_draft_target`` — the backstop on LLM-proposed eval targets.

The prompt asks the model for a target on the dimension's own scale with one of the three
operators the dimension form offers. This pins what happens when it doesn't comply: the target
is dropped (never the draft), and a single surviving pair is mirrored onto the other.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE_NAME = 'eval_draft_target_utils_test'


def _pylon_stubs():
    noop = lambda *a, **k: None  # noqa: E731
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        info=noop, warning=noop, error=noop, debug=noop, exception=noop)
    pylon_core = types.ModuleType('pylon.core')
    pylon_core.tools = pylon_tools
    pylon = types.ModuleType('pylon')
    pylon.core = pylon_core
    return {'pylon': pylon, 'pylon.core': pylon_core, 'pylon.core.tools': pylon_tools}


@pytest.fixture(scope='module')
def sanitize():
    with pytest.MonkeyPatch.context() as mp:
        for name, stub in _pylon_stubs().items():
            mp.setitem(sys.modules, name, stub)
        spec = importlib.util.spec_from_file_location(
            MODULE_NAME, PLUGIN_ROOT / 'utils' / 'eval_draft_target_utils.py')
        module = importlib.util.module_from_spec(spec)
        mp.setitem(sys.modules, MODULE_NAME, module)
        spec.loader.exec_module(module)
        yield module.sanitize_draft_target


def _run(sanitize, **item):
    sanitize(item)
    return item


def _targets(item):
    return (
        item['default_target'], item['default_target_operator'],
        item['target'], item['target_operator'],
    )


def test_consistent_pairs_are_kept(sanitize):
    item = _run(sanitize, default_target=80, default_target_operator='>=',
                target=80, target_operator='>=')

    assert _targets(item) == (80.0, '>=', 80.0, '>=')


def test_default_pair_is_mirrored_onto_the_binding(sanitize):
    item = _run(sanitize, default_target=20, default_target_operator='<=')

    assert _targets(item) == (20.0, '<=', 20.0, '<=')


def test_binding_pair_is_mirrored_onto_the_default(sanitize):
    item = _run(sanitize, target=85, target_operator='>=')

    assert _targets(item) == (85.0, '>=', 85.0, '>=')


def test_differing_valid_pairs_are_both_kept(sanitize):
    item = _run(sanitize, default_target=80, default_target_operator='>=',
                target=90, target_operator='>=')

    assert _targets(item) == (80.0, '>=', 90.0, '>=')


def test_no_target_stays_empty(sanitize):
    item = _run(sanitize, name='Tone')

    assert _targets(item) == (None, None, None, None)


def test_invalid_pair_is_replaced_by_the_valid_one(sanitize):
    item = _run(sanitize, default_target=80, default_target_operator='>=',
                target=180, target_operator='>=')

    assert _targets(item) == (80.0, '>=', 80.0, '>=')


@pytest.mark.parametrize('operator', ['>', '<', '!=', '≥', None, ''])
def test_operator_the_form_cannot_show_is_dropped(sanitize, operator):
    item = _run(sanitize, default_target=80, default_target_operator=operator)

    assert _targets(item) == (None, None, None, None)


@pytest.mark.parametrize('target', [None, 'high', True, [80], {'v': 80}])
def test_non_numeric_target_is_dropped(sanitize, target):
    item = _run(sanitize, default_target=target, default_target_operator='>=')

    assert _targets(item) == (None, None, None, None)


def test_numeric_string_target_is_accepted(sanitize):
    item = _run(sanitize, default_target='75', default_target_operator='>=')

    assert _targets(item) == (75.0, '>=', 75.0, '>=')


def test_percentage_on_a_rating_scale_is_dropped(sanitize):
    item = _run(sanitize, scale_type='ordinal', scale_min=1, scale_max=5,
                default_target=80, default_target_operator='>=')

    assert _targets(item) == (None, None, None, None)


@pytest.mark.parametrize('target', [1, 5, 4])
def test_rating_target_within_scale_is_kept(sanitize, target):
    item = _run(sanitize, scale_type='ordinal', scale_min=1, scale_max=5,
                default_target=target, default_target_operator='>=')

    assert item['default_target'] == float(target)


def test_missing_scale_falls_back_to_the_model_defaults(sanitize):
    # EvalDimensionBaseModel defaults to 0..100, so an omitted scale is judged against that
    assert _run(sanitize, default_target=100, default_target_operator='<=')['target'] == 100.0
    assert _run(sanitize, default_target=101, default_target_operator='<=')['target'] is None


def test_unparseable_scale_drops_the_target(sanitize):
    item = _run(sanitize, scale_min='low', scale_max=5,
                default_target=4, default_target_operator='>=')

    assert _targets(item) == (None, None, None, None)


@pytest.mark.parametrize('target', [0, 1])
def test_binary_equality_target_is_kept(sanitize, target):
    item = _run(sanitize, scale_type='binary', scale_min=0, scale_max=1,
                default_target=target, default_target_operator='==')

    assert _targets(item) == (float(target), '==', float(target), '==')


@pytest.mark.parametrize('target, operator', [
    (1, '>='),     # equivalent on 0/1, but the pass/fail form only represents '=='
    (0.5, '=='),
    (0.5, '>='),
])
def test_binary_target_the_pass_fail_form_cannot_show_is_dropped(sanitize, target, operator):
    item = _run(sanitize, scale_type='binary', scale_min=0, scale_max=1,
                default_target=target, default_target_operator=operator)

    assert _targets(item) == (None, None, None, None)


def test_other_fields_are_untouched(sanitize):
    item = _run(sanitize, name='Tone', scale_type='continuous', default_weight=2.0,
                default_target=80, default_target_operator='>=')

    assert item['name'] == 'Tone'
    assert item['scale_type'] == 'continuous'
    assert item['default_weight'] == 2.0
