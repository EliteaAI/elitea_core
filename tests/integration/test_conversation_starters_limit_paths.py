"""Chat starters limit on the write paths.

applications_update_version (every update path) applies count <= max(limit, saved count), but only
when the body sends starters. The shared model validator caps every path, copies included, at 10.
Both functions are cut out of their source files, with the real limit helpers injected.

Run via:
    python tests/run_tests.py integration/test_conversation_starters_limit_paths.py -v
"""
import ast
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fixtures.cut_source import load_cut

PLUGIN_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def limits():
    path = PLUGIN_ROOT / 'utils' / 'conversation_starters_utils.py'
    spec = importlib.util.spec_from_file_location('conversation_starters_utils_paths', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _function(rel_path, name):
    tree = ast.parse((PLUGIN_ROOT / rel_path).read_text())
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)


def _update(limits, monkeypatch, *, limit, saved, body_fields):
    monkeypatch.setattr(limits, 'get_max_conversation_starters', lambda: limit)
    namespace = {
        'ApplicationVersion': MagicMock(), 'ApplicationVariable': MagicMock(), 'Tag': MagicMock(),
        'ApplicationVersionDetailModel': MagicMock(), 'VersionNotUpdatableError': RuntimeError,
        'validate_model_selection_surface': lambda *a, **k: None,
        'sanitize_trigger_for_write': lambda stored, body: body,
        'loads': lambda _: {}, 'IntegrityError': ValueError, 'log': MagicMock(),
        'count_conversation_starters': limits.count_conversation_starters,
        'enforce_conversation_starters_limit': limits.enforce_conversation_starters_limit,
    }
    namespace = load_cut(
        [_function('utils/application_utils.py', 'applications_update_version')],
        namespace, 'utils/application_utils.py',
    )
    version = SimpleNamespace(id=1, name='v1', status='draft', agent_type='classic', llm_settings={},
                              pipeline_settings={}, meta={}, conversation_starters=saved)
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = version
    body = SimpleNamespace(
        id=1, model_fields={'id': object()}, model_fields_set=set(body_fields), name=None,
        variables=None, tags=None, project_id=1,
        model_dump=lambda **kw: dict(body_fields),
        **body_fields,
    )
    namespace['applications_update_version'](body, session, commit=False)
    return session


def _starters(n, prefix='s'):
    return [f'{prefix}{i}' for i in range(n)]


@pytest.mark.integration
class TestUpdateVersion:
    @pytest.mark.parametrize('sent', [
        _starters(8),
        _starters(7),
        _starters(7) + ['replaced'],
    ])
    def test_above_limit_version_keeps_edits_and_removals(self, limits, monkeypatch, sent):
        _update(limits, monkeypatch, limit=5, saved=_starters(8),
                body_fields={'conversation_starters': sent})

    def test_above_limit_version_cannot_grow(self, limits, monkeypatch):
        with pytest.raises(limits.ConversationStartersLimitError, match='cannot exceed 8 items, got 9'):
            _update(limits, monkeypatch, limit=5, saved=_starters(8),
                    body_fields={'conversation_starters': _starters(9)})

    def test_raised_limit_allows_growth(self, limits, monkeypatch):
        _update(limits, monkeypatch, limit=6, saved=_starters(2),
                body_fields={'conversation_starters': _starters(6)})

    def test_lowered_limit_blocks_growth_below_saved(self, limits, monkeypatch):
        with pytest.raises(limits.ConversationStartersLimitError, match='cannot exceed 3 items, got 4'):
            _update(limits, monkeypatch, limit=3, saved=_starters(2),
                    body_fields={'conversation_starters': _starters(4)})

    def test_body_without_starters_is_not_checked(self, limits, monkeypatch):
        _update(limits, monkeypatch, limit=3, saved=_starters(8), body_fields={"welcome_message": "hi"})

    def test_new_version_with_no_saved_starters_uses_the_limit(self, limits, monkeypatch):
        with pytest.raises(limits.ConversationStartersLimitError, match='cannot exceed 3 items, got 4'):
            _update(limits, monkeypatch, limit=3, saved=None,
                    body_fields={'conversation_starters': _starters(4)})


@pytest.fixture(scope='module')
def validator(limits):
    namespace = load_cut(
        [_function('models/pd/version.py', 'conversation_starters_validator')],
        {'ABSOLUTE_MAX_CONVERSATION_STARTERS': limits.ABSOLUTE_MAX_CONVERSATION_STARTERS},
        'models/pd/version.py',
    )
    return namespace['conversation_starters_validator']


@pytest.mark.integration
class TestModelValidator:
    def test_accepts_ten_for_every_path(self, validator):
        assert validator(_starters(10)) == _starters(10)

    def test_rejects_eleven(self, validator):
        with pytest.raises(ValueError, match='cannot exceed 10 items, got 11'):
            validator(_starters(11))

    def test_structural_checks_unchanged(self, validator):
        assert validator(None) is None
        assert validator([None, ' a ']) == ['a']
        with pytest.raises(ValueError, match='must be a list'):
            validator('a')
        with pytest.raises(ValueError, match='cannot be empty'):
            validator(['a', '  '])


def _publish_prompt_parts():
    tree = ast.parse((PLUGIN_ROOT / 'utils' / 'publish_utils.py').read_text())
    wanted = {'_VALIDATION_PROMPT_HEAD', '_DEFAULT_VALIDATION_RULES', '_VALIDATION_PROMPT_TAIL',
              '_VALIDATION_PROMPT_TEMPLATE'}
    nodes = [n for n in tree.body if isinstance(n, ast.Assign)
             and isinstance(n.targets[0], ast.Name) and n.targets[0].id in wanted]
    nodes += [n for n in tree.body if isinstance(n, ast.FunctionDef)
              and n.name in ('get_default_agent_validation_rules', '_build_validation_prompt')]
    return nodes


@pytest.mark.integration
class TestPublishReviewText:
    def _load(self, limit, custom_rules=''):
        return load_cut(_publish_prompt_parts(), {
            'get_max_conversation_starters': lambda: limit,
            'this': SimpleNamespace(module=SimpleNamespace(publish_validation_rules=custom_rules)),
        }, 'utils/publish_utils.py')

    def test_default_rules_show_the_configured_limit(self):
        ns = self._load(7)
        rules = ns['get_default_agent_validation_rules']()
        assert 'more than\n     7 starters defined (the configured limit is 7)' in rules
        assert '{max_conversation_starters}' not in rules

    def test_prompt_formats_with_default_and_custom_rules(self):
        assert 'the configured limit is 6' in self._load(6)['_build_validation_prompt']()
        assert 'my rules' in self._load(6, 'my rules')['_build_validation_prompt']()
