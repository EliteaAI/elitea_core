"""Issue #6408 - ensure_base_version must put 'base' FIRST, not merely present.

import_skill creates the skill from payloads[0] and appends the rest, and
Skill.versions carries no order_by, so an export of a skill with a named version
can hand back ['yoda', 'base']. Once the create path requires a 'base' initial
version, passing that order straight through fails the import - the skill is
silently dropped and the agent's attachment goes with it.

Run via:
    python tests/run_tests.py unit/test_6408_import_hoists_base.py -v
"""
import sys
import types
import pathlib
import importlib.util
import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

PKG = 'elitea_core_6408b'


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


def _heal_sqlalchemy_stubs():
    """Earlier test modules replace sqlalchemy with stubs missing the names this
    module imports at load time. Only importability matters here."""
    for modname, names in (
        ('sqlalchemy', ('func', 'or_', 'asc', 'desc')),
        ('sqlalchemy.orm', ('selectinload', 'joinedload', 'Session')),
    ):
        stub = sys.modules.get(modname)
        if stub is None:
            continue
        for attr in names:
            if not hasattr(stub, attr):
                setattr(stub, attr, lambda *a, **k: None)


@pytest.fixture(scope='module')
def export_import():
    _heal_sqlalchemy_stubs()
    _package(PKG)
    _package(f'{PKG}.models')
    _package(f'{PKG}.models.pd')
    _package(f'{PKG}.utils')

    _module(f'{PKG}.models.skill', Skill=object, SkillVersion=object)
    _module(f'{PKG}.models.pd.skill', SkillExportModel=object)
    _module(f'{PKG}.utils.export_import_utils', slugify=lambda value: value)
    _module(
        f'{PKG}.utils.skill_utils',
        _skill_session=None,
        get_skill_details=None,
        import_skill=None,
    )

    name = f'{PKG}.utils.skill_export_import'
    spec = importlib.util.spec_from_file_location(
        name, PLUGIN_ROOT / 'utils/skill_export_import.py'
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _names(versions):
    return [v['name'] for v in versions]


class TestEnsureBaseVersion:
    def test_base_is_hoisted_to_the_front(self, export_import):
        """The regression this guards: export order is not guaranteed."""
        result = export_import.ensure_base_version([
            {'name': 'yoda', 'instructions': 'hmm'},
            {'name': 'base', 'instructions': 'plain'},
        ])

        assert _names(result) == ['base', 'yoda']
        assert result[0]['instructions'] == 'plain'

    def test_an_already_leading_base_is_left_alone(self, export_import):
        versions = [
            {'name': 'base', 'instructions': 'plain'},
            {'name': 'yoda', 'instructions': 'hmm'},
        ]

        assert _names(export_import.ensure_base_version(versions)) == ['base', 'yoda']

    def test_the_other_versions_keep_their_relative_order(self, export_import):
        result = export_import.ensure_base_version([
            {'name': 'yoda', 'instructions': 'a'},
            {'name': 'vader', 'instructions': 'b'},
            {'name': 'base', 'instructions': 'c'},
            {'name': 'leia', 'instructions': 'd'},
        ])

        assert _names(result) == ['base', 'yoda', 'vader', 'leia']

    def test_a_base_clone_is_still_prepended_when_none_exists(self, export_import):
        result = export_import.ensure_base_version([
            {'name': 'yoda', 'instructions': 'hmm'},
        ])

        assert _names(result) == ['base', 'yoda']
        assert result[0]['instructions'] == 'hmm'

    def test_an_empty_list_is_unchanged(self, export_import):
        assert export_import.ensure_base_version([]) == []
        assert export_import.ensure_base_version(None) == []
