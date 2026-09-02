"""A reserved word in a generated skill name must cost the word, not the whole draft (#6416).

``description`` and ``instructions`` have always truncated rather than rejected; ``name`` raised,
so a model that named its own skill after itself turned a perfectly usable draft into
"Generated draft failed validation".

Run via:
    python tests/run_tests.py unit/models/test_6416_skill_draft_name_salvage.py -v
"""
import importlib.util
import pathlib
import re
import sys
import types

import pytest
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]

# Copied from models/pd/skill.py - the real module drags the ORM vocabulary in, and the point of
# this file is that the coercion satisfies exactly this rule.
SKILL_NAME_RE = re.compile(r'^[a-z0-9]$|^[a-z0-9][a-z0-9-]*[a-z0-9]$')
RESERVED_NAME_WORDS = ('claude', 'anthropic')


def _validate_skill_name(value: str) -> str:
    if len(value) > 64 or not SKILL_NAME_RE.match(value):
        raise ValueError('name must be <=64 chars, lowercase letters/digits/hyphens only')
    if any(word in value for word in RESERVED_NAME_WORDS):
        raise ValueError(f'name cannot contain {" or ".join(RESERVED_NAME_WORDS)}')
    return value


def _load(rel_path: str, name: str):
    spec = importlib.util.spec_from_file_location(
        name, PLUGIN_ROOT / rel_path, submodule_search_locations=[]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def pd_module():
    pkg = 'elitea_core_gsd'
    sys.modules[f'{pkg}.predict_llm'] = _load(
        'models/pd/predict_llm.py', f'{pkg}_predict_llm'
    )
    skill_stub = types.ModuleType(f'{pkg}.skill')
    skill_stub.validate_skill_name = _validate_skill_name
    skill_stub.RESERVED_NAME_WORDS = RESERVED_NAME_WORDS
    sys.modules[f'{pkg}.skill'] = skill_stub

    return _load('models/pd/generate_skill_draft.py', pkg)


def _draft(pd_module, name):
    return pd_module.GenerateSkillDraftResponse.model_validate({
        'name': name,
        'description': 'Reviews pull requests.',
        'instructions': '# Review\nBe strict.',
    })


@pytest.mark.parametrize('generated, expected', [
    ('claude-code-reviewer', 'code-reviewer'),
    ('Anthropic API Helper', 'api-helper'),
    ('claude-anthropic-log-summarizer', 'log-summarizer'),
])
def test_a_reserved_word_costs_the_word_not_the_draft(pd_module, generated, expected):
    assert _draft(pd_module, generated).name == expected


@pytest.mark.parametrize('generated, expected', [
    ('philanthropic-donor-tracker', 'donor-tracker'),
    ('Anthropocene Claude Reader', 'anthropocene-reader'),
])
def test_an_unrelated_word_is_dropped_whole_not_cut_in_half(pd_module, generated, expected):
    """'philanthropic' trips a substring rule; excising the match would leave the fragment 'phil'."""
    assert _draft(pd_module, generated).name == expected


@pytest.mark.parametrize('generated, expected', [
    ('Review PRs With Claude', 'review-prs'),
    ('log summarizer powered by claude', 'log-summarizer'),
    ('notes-writer-using-anthropic', 'notes-writer'),
    ('claude-powered-pr-reviewer', 'pr-reviewer'),
    ('Claude for PR review', 'pr-review'),
    ('pr review with claude for teams', 'pr-review-teams'),
])
def test_a_connector_left_dangling_by_the_drop_goes_too(pd_module, generated, expected):
    """Orphaned by adjacency, not position - leading and mid-name connectors strand too."""
    assert _draft(pd_module, generated).name == expected


def test_the_excision_fallback_does_not_resurrect_a_swept_connector(pd_module):
    """The sweep is what emptied the name here; rebuilding from every token would bring it back."""
    assert _draft(pd_module, 'with claudebot').name == 'bot'


@pytest.mark.parametrize('generated, expected', [
    ('ClaudePRReviewer', 'prreviewer'),
    ('claudebot', 'bot'),
])
def test_a_token_with_no_boundary_to_cut_on_falls_back_to_excision(pd_module, generated, expected):
    """Dropping the only token would lose a salvageable draft, so the substring comes out instead."""
    assert _draft(pd_module, generated).name == expected


@pytest.mark.parametrize('generated, expected', [
    ('github-pr-reviewer', 'github-pr-reviewer'),
    ('  GitHub PR Reviewer  ', 'github-pr-reviewer'),
    ('Release_Notes!!Writer', 'release-notes-writer'),
    ('search-for', 'search-for'),
])
def test_names_without_reserved_words_slugify_as_before(pd_module, generated, expected):
    """'search-for' proves the connector rule only fires when a reserved word was actually dropped."""
    assert _draft(pd_module, generated).name == expected


@pytest.mark.parametrize('generated', ['claude', 'Anthropic', 'claude anthropic', 'claude with'])
def test_a_name_that_is_only_a_reserved_word_still_fails(pd_module, generated):
    """Nothing survives the strip, so this is a genuine generation failure the user retries."""
    with pytest.raises(ValidationError):
        _draft(pd_module, generated)


def test_an_over_long_name_is_truncated_without_a_trailing_hyphen(pd_module):
    name = _draft(pd_module, 'claude ' + 'a' * 60 + ' reviewer').name

    assert len(name) <= pd_module.NAME_MAX_LENGTH
    assert _validate_skill_name(name) == name


def test_the_other_fields_still_truncate(pd_module):
    draft = pd_module.GenerateSkillDraftResponse.model_validate({
        'name': 'claude-reviewer',
        'description': 'd' * (pd_module.DESCRIPTION_MAX_LENGTH + 500),
        'instructions': 'i' * (pd_module.INSTRUCTIONS_MAX_LENGTH + 500),
    })

    assert draft.name == 'reviewer'
    assert len(draft.description) == pd_module.DESCRIPTION_MAX_LENGTH
    assert len(draft.instructions) == pd_module.INSTRUCTIONS_MAX_LENGTH
