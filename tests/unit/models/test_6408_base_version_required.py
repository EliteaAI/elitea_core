"""Issue #6408 - a skill's initial version must be named 'base'.

The create endpoint documented a "mandatory initial 'base' version" and enforced
nothing: check_single_version counted the versions without looking at the name.
A skill created as e.g. 'v1-custom' then has no 'base' at all, and can never
acquire one - the add-version endpoint reserves that name - so it sits outside
every fallback that keys on it, starting with Skill.get_default_version().

Run via:
    python tests/run_tests.py unit/models/test_6408_base_version_required.py -v
"""
import sys
import types
import pathlib
import importlib.util
from enum import StrEnum
import pytest
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent

PKG = 'elitea_core_6408'


def _package(name: str):
    pkg = types.ModuleType(name)
    pkg.__path__ = []
    sys.modules[name] = pkg
    return pkg


def _module(name: str, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


def _load(module_name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / rel_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def pd_skill():
    """Load models/pd/skill.py under a synthetic package tree.

    It reaches three levels up for utils and enums, so the tree has to be that
    deep before the relative imports resolve.
    """
    _package(PKG)
    _package(f'{PKG}.models')
    _package(f'{PKG}.models.pd')
    _package(f'{PKG}.models.enums')
    _package(f'{PKG}.utils')

    _module(f'{PKG}.utils.authors', get_authors_data=lambda *a, **k: [])
    _module(f'{PKG}.utils.constants', ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API=210)
    _module(
        f'{PKG}.models.enums.all',
        PublishStatus=StrEnum('PublishStatus', {'published': 'published'}),
        SkillEntityTypes=StrEnum('SkillEntityTypes', {'agent': 'agent'}),
    )
    sys.modules.setdefault('tools', types.ModuleType('tools'))
    sys.modules['tools'].rpc_tools = types.SimpleNamespace()

    _load(f'{PKG}.models.pd.collection_base', 'models/pd/collection_base.py')
    _load(f'{PKG}.models.pd.tag', 'models/pd/tag.py')
    _load(f'{PKG}.models.pd.skill_version', 'models/pd/skill_version.py')

    return _load(f'{PKG}.models.pd.skill', 'models/pd/skill.py')


def _payload(versions):
    return {
        'name': 'pr-reviewer',
        'description': 'reviews pull requests',
        'owner_id': 2,
        'project_id': 2,
        'user_id': 1,
        'versions': versions,
    }


def _version(**overrides):
    version = {'instructions': 'review the diff'}
    version.update(overrides)
    return version


class TestInitialVersionMustBeBase:
    def test_a_custom_initial_version_name_is_rejected(self, pd_skill):
        """The ticket's exact payload."""
        with pytest.raises(ValidationError) as exc:
            pd_skill.SkillCreateModel.model_validate(
                _payload([_version(name='v1-custom')])
            )

        assert "must be named 'base'" in str(exc.value)

    def test_an_omitted_name_still_defaults_to_base(self, pd_skill):
        skill = pd_skill.SkillCreateModel.model_validate(_payload([_version()]))

        assert skill.versions[0].name == 'base'

    def test_base_is_accepted_explicitly(self, pd_skill):
        skill = pd_skill.SkillCreateModel.model_validate(
            _payload([_version(name='base')])
        )

        assert skill.versions[0].name == 'base'

    def test_the_version_count_is_still_checked_first(self, pd_skill):
        with pytest.raises(ValidationError) as exc:
            pd_skill.SkillCreateModel.model_validate(_payload([]))

        assert 'Exactly 1 version' in str(exc.value)
