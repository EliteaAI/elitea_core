"""#6926: a runtime skill registry entry names its skill version.

The indexer writes one usage row per skill an agent loads and resolves the version from the
registry entry alone, for the root agent and for every sub-agent it runs.
"""

import importlib.util
import pathlib
import sys
import types
from unittest import mock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'plugins.elitea_core'


class AnyModule(types.ModuleType):
    """Answers every `from x import name` so skill_utils imports without its collaborators."""

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return mock.MagicMock(name=f'{self.__name__}.{name}')


@pytest.fixture
def skill_utils(monkeypatch):
    for name in (
        'tools', 'sqlalchemy', 'sqlalchemy.orm', 'pylon', 'pylon.core', 'pylon.core.tools',
        f'{PKG}.utils.authors', f'{PKG}.utils.utils', f'{PKG}.utils.like_utils',
        f'{PKG}.utils.folder_access', f'{PKG}.utils.skill_run_settings', f'{PKG}.models.skill',
        f'{PKG}.models.all', f'{PKG}.models.enums.all', f'{PKG}.models.enums.events',
        f'{PKG}.models.pd.skill', f'{PKG}.models.pd.skill_version', f'{PKG}.models.pd.skill_run_settings',
    ):
        monkeypatch.setitem(sys.modules, name, AnyModule(name))
    for name in ('plugins', PKG, f'{PKG}.utils', f'{PKG}.models', f'{PKG}.models.enums', f'{PKG}.models.pd'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    spec = importlib.util.spec_from_file_location(f'{PKG}.utils.skill_utils', PLUGIN_ROOT / 'utils' / 'skill_utils.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def _mapping(skill_id, version_id, name):
    return {
        'skill_id': skill_id, 'skill_version_id': version_id, 'name': name,
        'description': f'{name} skill', 'icon_meta': None, 'instructions': f'Be {name}.',
    }


def test_each_registry_entry_names_its_skill_version(skill_utils):
    version_details = {'instructions': 'Help.', 'skills': [_mapping(1, 10, 'shout'), _mapping(2, 20, 'terse')]}

    registry = skill_utils.resolve_runtime_skills(version_details)

    assert [(s['skill_id'], s['skill_version_id'], s['name']) for s in registry] == [
        (1, 10, 'shout'), (2, 20, 'terse'),
    ]
