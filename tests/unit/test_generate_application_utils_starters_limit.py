"""The application draft prompt builders fill the Chat Starters and skill limits into the seeded templates.

The templates mirror the placeholder set of the seeded ``generate_application_draft`` and
``edit_application_draft`` prompts (configurations plugin), so a placeholder rename on either side
of that cross-repo contract shows up as a failure here.
"""
import enum
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

PKG = 'draftpkg_prompt_builder_test'

RESOURCES = 'Toolkits:\n{toolkits}\nMCP:\n{mcp}\nAgents:\n{agents}\nPipelines:\n{pipelines}\nSkills:\n{skills}\n'
CREATE_TEMPLATE = (
    RESOURCES + '"conversation_starters": [<string>] (max {max_conversation_starters} items)\n'
    'Maximum {max_skills_per_agent} skills.\n{{"name": "x"}}'
)
EDIT_TEMPLATE = 'Current:\n{current_config}\n' + CREATE_TEMPLATE
# A prompt seeded before the placeholders existed
LEGACY_TEMPLATE = RESOURCES + '"conversation_starters": [<string>] (max 4 items)\nMaximum 5 skills.'


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
    skill.SkillVersion = type('SkillVersion', (), {})
    enums_pkg = types.ModuleType(f'{PKG}.models.enums')
    enums_pkg.__path__ = []
    enums_all = types.ModuleType(f'{PKG}.models.enums.all')
    enums_all.PublishStatus = enum.Enum('PublishStatus', {
        'draft': 'draft', 'on_moderation': 'on_moderation', 'published': 'published',
        'rejected': 'rejected', 'user_approval': 'user_approval',
        'unpublished': 'unpublished', 'embedded': 'embedded',
    })

    # Stub pylon and tools for when the test runs without a prior test having leaked them.
    pylon_pkg = sys.modules.setdefault('pylon', types.ModuleType('pylon'))
    pylon_core = sys.modules.setdefault('pylon.core', types.ModuleType('pylon.core'))
    if 'pylon.core.tools' not in sys.modules:
        pylon_tools = types.ModuleType('pylon.core.tools')
        pylon_tools.log = types.SimpleNamespace(
            warning=lambda *a, **k: None, exception=lambda *a, **k: None,
            debug=lambda *a, **k: None, info=lambda *a, **k: None,
        )
        sys.modules['pylon.core.tools'] = pylon_tools
    sys.modules.setdefault('tools', types.SimpleNamespace(db=types.SimpleNamespace()))

    for name, mod in {
        PKG: pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.all': all_models,
        f'{PKG}.models.elitea_tools': elitea_tools,
        f'{PKG}.models.skill': skill,
        f'{PKG}.models.enums': enums_pkg,
        f'{PKG}.models.enums.all': enums_all,
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


def test_create_prompt_gets_the_limits(utils):
    prompt = utils.build_system_prompt(
        CREATE_TEMPLATE, 1, [], [], [], [], [], max_conversation_starters=7, max_skills_per_agent=37,
    )
    assert '(max 7 items)' in prompt and 'Maximum 37 skills.' in prompt and '{"name": "x"}' in prompt


def test_edit_prompt_gets_the_limits_and_config(utils):
    prompt = utils.build_edit_system_prompt(
        EDIT_TEMPLATE, 1, {'name': 'agent'}, [], [], [], [], [],
        max_conversation_starters=9, max_skills_per_agent=41,
    )
    assert '(max 9 items)' in prompt and 'Maximum 41 skills.' in prompt and '"name": "agent"' in prompt


def test_legacy_prompt_without_placeholders_still_renders(utils):
    prompt = utils.build_system_prompt(
        LEGACY_TEMPLATE, 1, [], [], [], [], [], max_conversation_starters=7, max_skills_per_agent=37,
    )
    assert '(max 4 items)' in prompt and 'Maximum 5 skills.' in prompt
