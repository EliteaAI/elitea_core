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


DEFAULT_INFO = {'badge': 'needs_approval', 'effect': 'This tool can change data outside ELITEA.'}


# Owner CR 2026-10-21 (risk review): the badge and effect text of a sensitive tool
def test_el7011_cr_tool_info_comes_from_the_policy_ignoring_case(security):
    _set(security, sensitive_tool_info={'Outlook': {'Send_Email': {'badge': 'sends_messages', 'effect': 'Sends e-mail'}}})
    assert security.module.sensitive_tool_info('outlook', 'send_email') == {
        'badge': 'sends_messages', 'effect': 'Sends e-mail'}


def test_el7011_cr_tool_without_info_gets_the_default_badge_and_effect(security):
    assert security.module.sensitive_tool_info('jira', 'delete_issue') == DEFAULT_INFO
    _set(security, sensitive_tool_info={'jira': {'delete_issue': {'effect': 'Deletes an issue'}}})
    assert security.module.sensitive_tool_info('jira', 'delete_issue') == {
        'badge': 'needs_approval', 'effect': 'Deletes an issue'}


def test_el7011_cr_an_unknown_badge_falls_back_to_needs_approval(security):
    _set(security, sensitive_tool_info={'jira': {'delete_issue': {'badge': 'red', 'effect': 'Deletes'}}})
    assert security.module.sensitive_tool_info('jira', 'delete_issue')['badge'] == 'needs_approval'


@pytest.mark.parametrize('name', ['CreateFile', 'create-file', 'Create File', 'github___create_file',
                                  'github:create_file', 'elitea_core:CREATE_FILE'])
def test_tool_name_aliases_and_separators_match_a_listed_tool(security, name):
    _set(security, sensitive_tools={'github': ['create_file']})
    assert security.module.is_tool_sensitive('github', name) is True


def test_a_listed_name_in_another_style_matches(security):
    _set(security, sensitive_tools={'github': ['Create-File']})
    assert security.module.is_tool_sensitive('github', 'create_file') is True


def test_a_star_toolkit_entry_lists_the_tool_for_any_toolkit(security):
    _set(security, sensitive_tools={'*': ['delete_file']})
    assert security.module.is_tool_sensitive('gitlab', 'delete_file') is True
    assert security.module.is_tool_sensitive('gitlab', 'list_files') is False


def test_a_legacy_toolkit_type_spelling_matches_the_registered_type(security):
    _set(security, sensitive_tools={'azure_devops_repos': ['create_branch']})
    assert security.module.is_tool_sensitive('ado_repos', 'create_branch') is True
    _set(security, sensitive_tools={'ado_repos': ['create_branch']})
    assert security.module.is_tool_sensitive('azure_devops_repos', 'create_branch') is True


def test_tool_info_matches_aliases_and_the_star_toolkit(security):
    _set(security, sensitive_tool_info={'*': {'delete_file': {'badge': 'deletes_data', 'effect': 'Deletes a file'}}})
    assert security.module.sensitive_tool_info('github', 'github___DeleteFile') == {
        'badge': 'deletes_data', 'effect': 'Deletes a file'}


def test_listed_sensitive_tools_returns_the_entries_of_the_type_and_the_star(security):
    _set(security, sensitive_tools={'github': ['create_pull_request'], 'jira': ['x'], '*': ['delete_file']})
    assert security.module.listed_sensitive_tools('GitHub') == ['create_pull_request', 'delete_file']
