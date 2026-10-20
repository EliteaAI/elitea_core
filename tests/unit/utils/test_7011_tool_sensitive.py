"""EL-7011 US-17 R2.3: `toolkit_security.sensitive_tools` marks a tool as needing approval."""
import pathlib
import types

import pytest
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture
def security(monkeypatch):
    import tools

    state = types.SimpleNamespace(config={})
    descriptor = types.SimpleNamespace(config=types.SimpleNamespace(get=lambda key, default=None: state.config.get(key, default)))
    monkeypatch.setattr(tools, 'this', types.SimpleNamespace(module=types.SimpleNamespace(descriptor=descriptor)))
    state.module = load_module_with_stubs(
        PLUGIN_ROOT / 'utils' / 'toolkit_security.py', 'plugins.elitea_core.utils.toolkit_security_7011'
    )
    return state


def _set(security, **policy):
    security.config = {'toolkit_security': policy}


def test_el7011_tc_us17_05_listed_tool_is_sensitive(security):
    _set(security, sensitive_tools={'github': ['create_issue']})
    assert security.module.is_tool_sensitive('github', 'create_issue') is True


def test_el7011_tc_us17_06_match_ignores_case(security):
    _set(security, sensitive_tools={'GitHub': ['Create_Issue']})
    assert security.module.is_tool_sensitive('GITHUB', 'create_issue') is True


def test_el7011_tc_us17_07_unlisted_tool_is_safe(security):
    _set(security, sensitive_tools={'github': ['create_issue']})
    assert security.module.is_tool_sensitive('github', 'list_issues') is False
    assert security.module.is_tool_sensitive('jira', 'create_issue') is False


def test_el7011_tc_us17_08_no_policy_means_safe(security):
    assert security.module.is_tool_sensitive('github', 'create_issue') is False
    _set(security, sensitive_tools=None)
    assert security.module.is_tool_sensitive('github', 'create_issue') is False


def test_el7011_tc_us17_09_config_returns_sensitive_tools(security):
    _set(security, sensitive_tools={'github': ['create_issue']})
    cfg = security.module.get_toolkit_security_config()
    assert cfg['sensitive_tools'] == {'github': ['create_issue']}
    assert cfg['blocked_toolkits'] == []
    security.config = {}
    assert security.module.get_toolkit_security_config()['sensitive_tools'] == {}
