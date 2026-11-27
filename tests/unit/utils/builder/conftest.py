"""Loads the pure Elitea Builder modules (#7383) as a package without the pylon runtime."""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[3]
PLUGIN_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs  # noqa: E402

PKG = 'plugins_builder.elitea_core.utils.builder'
MODULES = ('limits', 'changeset', 'catalog', 'prompts', 'service')


@pytest.fixture(scope='session')
def builder():
    for name in ('plugins_builder', 'plugins_builder.elitea_core', 'plugins_builder.elitea_core.utils', PKG):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules.setdefault(name, package)
    loaded = {
        name: load_module_with_stubs(PLUGIN_ROOT / 'utils' / 'builder' / f'{name}.py', f'{PKG}.{name}')
        for name in MODULES
    }
    return types.SimpleNamespace(**loaded)
