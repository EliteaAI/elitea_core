"""Shared fixture: utils/internal_tools.py loaded without the Pylon runtime."""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module


@pytest.fixture(scope='module')
def internal_tools():
    import tools

    for attr, value in (
        ('config', types.SimpleNamespace()),
        ('VaultClient', object),
        ('rpc_tools', types.SimpleNamespace(RpcMixin=object)),
        ('this', types.SimpleNamespace()),
        ('auth', types.SimpleNamespace()),
    ):
        if not hasattr(tools, attr):
            setattr(tools, attr, value)
    tools.config.APP_HOST = 'http://localhost'

    mcp_config_mod = types.ModuleType('mcp_config')
    mcp_config_mod.is_mcp_exposure_enabled = lambda: True
    support_utils_mod = types.ModuleType('support_utils')
    support_utils_mod.get_support_config = lambda: {'enabled': False, 'project_id': None}

    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'internal_tools',
        extra_stubs={
            'plugins.elitea_core.utils.mcp_config': mcp_config_mod,
            'plugins.elitea_core.utils.support_utils': support_utils_mod,
        },
    )
