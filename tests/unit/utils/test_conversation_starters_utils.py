"""Chat starters limit: the getter, the count, and the authoring rule count <= max(limit, previous).

Run via:
    python tests/run_tests.py unit/utils/test_conversation_starters_utils.py -v
"""
import importlib.util
import json
import pathlib
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture(scope='module')
def mod():
    path = PLUGIN_ROOT / 'utils' / 'conversation_starters_utils.py'
    spec = importlib.util.spec_from_file_location('conversation_starters_utils_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _config(value):
    return {'conversation_starters_guardrail': {'max_per_version': value}}


class TestCount:
    @pytest.mark.parametrize('starters, expected', [
        (None, 0),
        ([], 0),
        (['a', 'b'], 2),
        (['a', None, '  ', ''], 1),
        ([' a ', 3, 'b'], 2),
    ])
    def test_counts_only_non_empty_strings(self, mod, starters, expected):
        assert mod.count_conversation_starters(starters) == expected


class TestResolveLimit:
    def test_missing_falls_back_to_default(self, mod):
        assert mod.resolve_max_conversation_starters(None) == 4
        assert mod.resolve_max_conversation_starters({}) == 4
        assert mod.resolve_max_conversation_starters({'conversation_starters_guardrail': {}}) == 4

    @pytest.mark.parametrize('value', [1, 7, 10])
    def test_valid_value_is_used(self, mod, value):
        assert mod.resolve_max_conversation_starters(_config(value)) == value

    @pytest.mark.parametrize('value', ['abc', '7', True, 0, 11, -1, 4.5, None])
    def test_invalid_value_falls_back_with_warning(self, mod, monkeypatch, value):
        warnings = []
        monkeypatch.setattr(mod.log, 'warning', lambda *a, **k: warnings.append(a))
        assert mod.resolve_max_conversation_starters(_config(value)) == 4
        assert len(warnings) == 1

    def test_getter_reads_the_live_descriptor(self, mod, monkeypatch):
        monkeypatch.setattr(mod, 'this', types.SimpleNamespace(
            descriptor=types.SimpleNamespace(config=_config(6)),
        ))
        assert mod.get_max_conversation_starters() == 6

    def test_getter_survives_a_missing_descriptor(self, mod, monkeypatch):
        monkeypatch.setattr(mod, 'this', types.SimpleNamespace())
        assert mod.get_max_conversation_starters() == 4


class TestRule:
    @pytest.mark.parametrize('new, previous, limit', [
        (4, 0, 4),     # create at the limit
        (0, 0, 1),     # clearing is always allowed
        (8, 8, 5),     # above-limit version saved unchanged
        (7, 8, 5),     # ... or with one removed
        (6, 2, 6),     # raised limit lets a version grow
    ])
    def test_allowed(self, mod, new, previous, limit):
        mod.check_conversation_starters_limit(new, previous, limit)

    @pytest.mark.parametrize('new, previous, limit, allowed', [
        (5, 0, 4, 4),  # create above the limit
        (9, 8, 5, 8),  # above-limit version can't grow
        (4, 2, 3, 3),  # lowered limit blocks growth
    ])
    def test_rejected_with_the_allowed_count(self, mod, new, previous, limit, allowed):
        with pytest.raises(mod.ConversationStartersLimitError) as exc:
            mod.check_conversation_starters_limit(new, previous, limit)
        assert str(exc.value) == f'conversation_starters cannot exceed {allowed} items, got {new}'

    def test_enforce_uses_the_configured_limit(self, mod, monkeypatch):
        monkeypatch.setattr(mod, 'get_max_conversation_starters', lambda: 3)
        mod.enforce_conversation_starters_limit(['a', 'b', 'c', '  '])
        with pytest.raises(mod.ConversationStartersLimitError):
            mod.enforce_conversation_starters_limit(['a', 'b', 'c', 'd'])
        mod.enforce_conversation_starters_limit(['a', 'b', 'c', 'd'], previous_count=5)

    def test_error_response_matches_the_pydantic_shape(self, mod):
        error = mod.ConversationStartersLimitError('conversation_starters cannot exceed 4 items, got 5')
        assert mod.limit_error_response(error) == [{
            'type': 'value_error',
            'loc': ['conversation_starters'],
            'msg': 'conversation_starters cannot exceed 4 items, got 5',
        }]


class TestImportErrors:
    def test_names_the_agent_and_version(self, mod):
        versions = [
            {'name': 'base', 'conversation_starters': ['x'] * 10},
            {'name': 'v2', 'conversation_starters': ['x'] * 11},
        ]
        assert mod.import_starters_errors('Refund Desk', versions) == [
            'Refund Desk / v2: conversation_starters cannot exceed 10 items, got 11',
        ]

    def test_no_starters_no_errors(self, mod):
        assert mod.import_starters_errors('A', [{'name': 'base'}]) == []


class TestAdminField:
    def test_schema_field_matches_the_helper(self, mod):
        schema = json.loads((PLUGIN_ROOT / 'admin_schema.json').read_text())
        field = schema['properties']['max_conversation_starters']
        assert field['path'] == f'{mod.CONFIG_SECTION}.{mod.CONFIG_KEY}'
        assert field['section'] == 'guardrails'
        assert field['default'] == mod.DEFAULT_MAX_CONVERSATION_STARTERS
        assert field['requires_restart'] is False
        assert field['value_schema'] == {
            'type': 'integer', 'minimum': 1, 'maximum': mod.ABSOLUTE_MAX_CONVERSATION_STARTERS,
        }
