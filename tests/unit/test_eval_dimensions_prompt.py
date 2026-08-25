"""Unit tests for ``build_eval_dimensions_system_prompt``.

The builder is a pure function over a template string, so it is tested directly rather than
through the endpoint — the endpoint's integration tests stub it out, which previously left the
count/existing-names clauses and the malformed-template guard with no coverage at all.

The template used here mirrors the placeholder set of the seeded
``GENERATE_EVAL_DIMENSIONS_DEFAULT_PROMPT`` (configurations plugin), including a ``{{``-escaped
JSON block, so a placeholder rename on either side of that cross-repo contract shows up as a
failure here.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'evalpkg_prompt_builder_test'

# Same placeholders as the seeded template, plus an escaped-brace block to prove the JSON schema
# section of the real prompt survives .format().
TEMPLATE = """You are an evaluation design assistant.

An agent named "{application_name}" has the following instructions:
{instructions}

{existing_dimensions}

Propose only dimensions that fit THIS agent. {count_clause}

Return ONLY JSON matching:
{{
  "dimensions": [{{"name": "<string>", "default_weight": <float>}}]
}}
"""


def _load_builder():
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = [str(PLUGIN_ROOT / 'utils')]

    # ORM models are only touched by the DB-backed helpers in this module, not by the builder.
    all_models = types.ModuleType(f'{PKG}.models.all')
    all_models.Application = type('Application', (), {})
    all_models.ApplicationVersion = type('ApplicationVersion', (), {})
    elitea_tools = types.ModuleType(f'{PKG}.models.elitea_tools')
    elitea_tools.EliteATool = type('EliteATool', (), {})
    skill = types.ModuleType(f'{PKG}.models.skill')
    skill.Skill = type('Skill', (), {})

    for name, mod in {
        PKG: pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.all': all_models,
        f'{PKG}.models.elitea_tools': elitea_tools,
        f'{PKG}.models.skill': skill,
        f'{PKG}.utils': utils_pkg,
    }.items():
        sys.modules[name] = mod

    full = f'{PKG}.utils.generate_application_utils'
    spec = importlib.util.spec_from_file_location(
        full, PLUGIN_ROOT / 'utils' / 'generate_application_utils.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def utils():
    module = _load_builder()
    yield module
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]


def _build(utils, **kwargs):
    kwargs.setdefault('template', TEMPLATE)
    kwargs.setdefault('application_name', 'Support Bot')
    kwargs.setdefault('instructions', 'Answer support tickets politely.')
    return utils.build_eval_dimensions_system_prompt(**kwargs)


def test_renders_agent_context_and_keeps_escaped_json_block(utils):
    out = _build(utils)

    assert 'Support Bot' in out
    assert 'Answer support tickets politely.' in out
    # {{ }} collapsed to literal braces, so the schema block survives for the model to copy
    assert '"dimensions": [{"name": "<string>", "default_weight": <float>}]' in out
    assert '{{' not in out


def test_no_count_hint_renders_a_complete_sentence(utils):
    out = _build(utils, count_hint=None)

    assert 'Propose 3-6 dimensions, using your judgment.' in out
    # regression guard: the old {count_hint} placeholder rendered "more than  dimensions"
    assert 'more than  dimensions' not in out
    clause_line = next(line for line in out.splitlines() if 'Propose' in line)
    assert '  ' not in clause_line


def test_count_hint_becomes_an_explicit_cap(utils):
    out = _build(utils, count_hint=4)

    assert 'Propose at most 4 dimensions.' in out
    assert 'using your judgment' not in out


def test_empty_instructions_get_a_placeholder(utils):
    out = _build(utils, instructions='')

    assert '(no instructions set)' in out


def test_existing_names_are_listed_as_do_not_repropose(utils):
    out = _build(utils, existing_dimension_names=['Politeness', 'Groundedness'])

    assert 'do not re-propose' in out
    assert 'Politeness, Groundedness' in out
    assert 'currently empty' not in out


def test_empty_library_says_so(utils):
    out = _build(utils, existing_dimension_names=[])

    assert 'The project\'s dimension library is currently empty.' in out


def test_existing_names_are_capped(utils):
    names = [f'Dimension {i}' for i in range(utils._MAX_EXISTING_DIMENSIONS + 25)]

    out = _build(utils, existing_dimension_names=names)

    assert f'Dimension {utils._MAX_EXISTING_DIMENSIONS - 1}' in out
    assert f'Dimension {utils._MAX_EXISTING_DIMENSIONS}' not in out


def test_unknown_placeholder_raises_service_prompt_template_error(utils):
    with pytest.raises(utils.ServicePromptTemplateError):
        _build(utils, template='Hello {application_name}, see {not_a_real_placeholder}.')


def test_single_braced_json_raises_service_prompt_template_error(utils):
    """The most likely admin mis-edit: forgetting to double the braces in the schema block."""
    with pytest.raises(utils.ServicePromptTemplateError):
        _build(utils, template='{application_name} must return {"dimensions": []}.')
