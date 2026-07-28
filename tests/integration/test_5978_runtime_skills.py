"""Issue #5978 - shaping version_details for the runtime.

`apply_runtime_skills` is what lets a sub-agent run with the skills attached to
its own version. Every invariant below fails silently when broken: the child
simply runs without its skills, which is indistinguishable from the original bug.

Run via:
    python tests/run_tests.py integration/test_5978_runtime_skills.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _register(name, module):
    sys.modules[name] = module
    return module


@pytest.fixture(scope='module')
def skill_utils_module():
    """Load skill_utils.py standalone with minimal stubs."""
    for name in (
        "plugins",
        "plugins.elitea_core",
        "plugins.elitea_core.models",
        "plugins.elitea_core.models.pd",
        "plugins.elitea_core.utils",
    ):
        mod = sys.modules.setdefault(name, types.ModuleType(name))
        mod.__path__ = []

    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools_mod = types.ModuleType("pylon.core.tools")
    tools_mod.log = types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        debug=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules.setdefault("pylon.core.tools", tools_mod)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.auth = types.SimpleNamespace(decorators=types.SimpleNamespace())
    tools_pkg.db = types.SimpleNamespace(get_session=lambda pid: None)
    tools_pkg.this = types.SimpleNamespace()
    tools_pkg.serialize = types.SimpleNamespace()
    tools_pkg.rpc_tools = types.SimpleNamespace()
    sys.modules["tools"] = tools_pkg

    utils_mod = types.ModuleType("plugins.elitea_core.utils.utils")
    utils_mod.set_columns_as_attrs = lambda *a, **k: None
    utils_mod.get_public_project_id = lambda: 1
    _register("plugins.elitea_core.utils.utils", utils_mod)

    like_utils = types.ModuleType("plugins.elitea_core.utils.like_utils")
    like_utils.add_likes = lambda *a, **k: None
    like_utils.add_my_liked = lambda *a, **k: None
    like_utils.add_trending_likes = lambda *a, **k: None
    like_utils.get_like_model = lambda *a, **k: None
    _register("plugins.elitea_core.utils.like_utils", like_utils)

    models_skill = types.ModuleType("plugins.elitea_core.models.skill")
    models_skill.Skill = type("Skill", (), {"id": 1})
    models_skill.SkillVersion = type("SkillVersion", (), {"id": 1, "skill_id": 1})
    models_skill.EntitySkillMapping = type(
        "EntitySkillMapping",
        (),
        {
            "id": 1,
            "entity_version_id": 1,
            "entity_type": 1,
            "skill_id": 1,
            "skill_version_id": 1,
        },
    )
    _register("plugins.elitea_core.models.skill", models_skill)

    models_all = types.ModuleType("plugins.elitea_core.models.all")
    models_all.Tag = type("Tag", (), {})
    models_all.ApplicationVersion = type(
        "ApplicationVersion", (), {"id": 1, "application_id": 1, "status": "draft"}
    )
    models_all.Application = type("Application", (), {"id": 1})
    _register("plugins.elitea_core.models.all", models_all)

    enums = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums.SkillEntityTypes = type("SkillEntityTypes", (), {"agent": "agent"})

    class _PublishStatus:
        draft = "draft"
        on_moderation = "on_moderation"
        published = "published"
        rejected = "rejected"
        user_approval = "user_approval"
        unpublished = "unpublished"
        embedded = "embedded"

    enums.PublishStatus = _PublishStatus
    enums.AgentTypes = type(
        "AgentTypes", (), {"pipeline": types.SimpleNamespace(value="pipeline")}
    )
    _register("plugins.elitea_core.models.enums.all", enums)

    from pydantic import BaseModel, ConfigDict

    class _PdBase(BaseModel):
        model_config = ConfigDict(extra="allow")

    models_pd_skill = types.ModuleType("plugins.elitea_core.models.pd.skill")
    for cls_name in (
        "SkillCreateModel",
        "SkillDetailModel",
        "SkillUpdateModel",
        "SkillImportResultModel",
        "AgentsWithSkillItemModel",
    ):
        setattr(models_pd_skill, cls_name, type(cls_name, (_PdBase,), {}))
    _register("plugins.elitea_core.models.pd.skill", models_pd_skill)

    models_pd_skill_version = types.ModuleType("plugins.elitea_core.models.pd.skill_version")
    for cls_name in (
        "SkillVersionCreateModel",
        "SkillVersionUpdateModel",
        "SkillVersionDetailModel",
    ):
        setattr(models_pd_skill_version, cls_name, type(cls_name, (_PdBase,), {}))
    _register("plugins.elitea_core.models.pd.skill_version", models_pd_skill_version)

    # Earlier tests in the session may have replaced sqlalchemy with stubs missing
    # names skill_utils imports at load time; this fixture only needs importability.
    for modname, names in (
        ('sqlalchemy', ('func', 'or_', 'asc', 'desc')),
        ('sqlalchemy.orm', ('selectinload', 'joinedload')),
    ):
        stub = sys.modules.get(modname)
        if stub is not None:
            for attr in names:
                if not hasattr(stub, attr):
                    setattr(stub, attr, lambda *a, **k: None)

    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.skill_utils",
        PLUGIN_ROOT / "utils" / "skill_utils.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


UPPERCASE = {
    'skill_id': 1,
    'skill_version_id': 10,
    'name': 'shout',
    'description': 'Use when the reply should be loud',
    'instructions': 'Write every letter in upper case.',
}
TERSE = {
    'skill_id': 2,
    'skill_version_id': 20,
    'name': 'terse',
    'description': 'Use when the reply should be short',
    'instructions': 'Answer in one sentence.',
}


class TestEmptyPayloadUntouched:
    """The webhook/API/MCP predict shape sends no version_details.

    The SDK refetches only while the dict stays falsy, so inventing a key here
    strands it with a truthy payload that has no llm_settings.
    """

    def test_empty_dict_gains_no_keys(self, skill_utils_module):
        version_details = {}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details == {}

    def test_none_instructions_are_left_alone(self, skill_utils_module):
        version_details = {'instructions': None, 'skills': [UPPERCASE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] is None


class TestPipelineInstructionsPreserved:
    """A pipeline's instructions are its YAML graph, not a prompt."""

    def test_yaml_survives_byte_identical(self, skill_utils_module):
        yaml = 'nodes:\n  - id: start\n    type: llm\n'
        version_details = {
            'instructions': yaml,
            'agent_type': 'pipeline',
            'skills': [UPPERCASE],
        }

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == yaml
        assert version_details['attached_skills'] == []


class TestReferencedSkillsStayLoadable:
    """No body is ever written into instructions; every skill loads on demand."""

    def test_referenced_skill_is_desigiled_and_stays_loadable(self, skill_utils_module):
        version_details = {
            'instructions': 'Answer briefly. Follow ~shout in every reply.',
            'skills': [UPPERCASE, TERSE],
        }

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == 'Answer briefly. Follow shout in every reply.'
        assert [s['name'] for s in version_details['attached_skills']] == ['shout', 'terse']

    def test_referenced_skill_description_carries_the_hint(self, skill_utils_module):
        version_details = {
            'instructions': 'Follow ~shout in every reply.',
            'skills': [UPPERCASE, TERSE],
        }

        skill_utils_module.apply_runtime_skills(version_details)

        by_name = {s['name']: s for s in version_details['attached_skills']}
        hint = skill_utils_module.INSTRUCTION_REFERENCED_HINT
        assert by_name['shout']['description'] == f"{UPPERCASE['description']} {hint}"
        assert by_name['terse']['description'] == TERSE['description']

    def test_unreferenced_skills_stay_loadable(self, skill_utils_module):
        version_details = {'instructions': 'Answer briefly.', 'skills': [UPPERCASE, TERSE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == 'Answer briefly.'
        assert [s['name'] for s in version_details['attached_skills']] == ['shout', 'terse']

    def test_skill_only_agent_still_gets_load_skill(self, skill_utils_module):
        """Empty instructions must not suppress the disclosable channel."""
        version_details = {'instructions': '', 'skills': [UPPERCASE, TERSE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == ''
        assert len(version_details['attached_skills']) == 2

    def test_blank_bodied_skill_is_consumed_but_not_offered(self, skill_utils_module):
        blank = {**UPPERCASE, 'instructions': '   '}
        version_details = {'instructions': 'Do ~shout now.', 'skills': [blank]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == 'Do shout now.'
        assert version_details['attached_skills'] == []


class TestRawMappingsDropped:
    """`skills` carries every body a second time and no runtime reads it."""

    def test_skills_key_is_removed(self, skill_utils_module):
        version_details = {'instructions': 'Answer briefly.', 'skills': [UPPERCASE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert 'skills' not in version_details

    def test_disclosable_entries_are_fresh_objects(self, skill_utils_module):
        version_details = {'instructions': 'Answer briefly.', 'skills': [UPPERCASE]}

        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['attached_skills'][0] is not UPPERCASE


class TestRepeatedApplication:
    """Shaping an already-shaped payload must not strip what it produced."""

    def test_second_call_keeps_attached_skills(self, skill_utils_module):
        version_details = {'instructions': 'Answer briefly.', 'skills': [UPPERCASE]}

        skill_utils_module.apply_runtime_skills(version_details)
        first = list(version_details['attached_skills'])
        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['attached_skills'] == first

    def test_second_call_leaves_instructions_untouched(self, skill_utils_module):
        version_details = {
            'instructions': 'Follow ~shout in every reply.',
            'skills': [UPPERCASE],
        }

        skill_utils_module.apply_runtime_skills(version_details)
        desigiled = version_details['instructions']
        skill_utils_module.apply_runtime_skills(version_details)

        assert version_details['instructions'] == desigiled


class TestDeterminism:
    """The instructions and registry feed the cached prompt, so identical input
    must render identically."""

    def test_identical_inputs_render_identically(self, skill_utils_module):
        def shape():
            version_details = {
                'instructions': 'Use ~shout and ~terse.',
                'skills': [UPPERCASE, TERSE],
            }
            skill_utils_module.apply_runtime_skills(version_details)
            return version_details['instructions'], version_details['attached_skills']

        assert shape() == shape()
