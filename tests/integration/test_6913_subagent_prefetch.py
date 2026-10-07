"""Issue #6913 — predict-time prefetch of expanded sub-agent version details.

``collect_subagent_version_details`` walks the application-tool tree once and returns
``{"app_id:version_id": {name, description, version_details}}`` for the SDK. It must dedup shared
children (diamonds), skip cross-project children and children the end user cannot write
(mirroring PATCH version's folder check), honour node/size caps and never raise — anything it
skips is simply fetched by the SDK as before. Expansion, access and summary lookups are faked.

It must also never widen access: PATCH version requires `models.applications.version.details`,
so a user whose role lacks it (a custom role — every default role has it) used to get a 403 per
child and the SDK skipped them. The prefetch then returns nothing so that behaviour is unchanged.

Run via:
    python tests/run_tests.py integration/test_6913_subagent_prefetch.py -v
"""

import importlib.util
import json
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def sp(monkeypatch):
    folder_access = types.ModuleType('plugins.elitea_core.utils.folder_access')
    folder_access.APPLICATION_ENTITY_TYPES = ['agent', 'pipeline']
    folder_access.resolve_entities_access = lambda *a, **k: {}
    publish_utils = types.ModuleType('plugins.elitea_core.utils.publish_utils')
    publish_utils.MAX_SUB_AGENT_VALIDATION_DEPTH = 25
    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.utils.folder_access', folder_access)
    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.utils.publish_utils', publish_utils)
    # Other suites replace `tools` with narrower stubs; pin our own so test order does not matter
    tools_stub = types.ModuleType('tools')
    tools_stub.rpc_tools = types.SimpleNamespace(RpcMixin=None)
    monkeypatch.setitem(sys.modules, 'tools', tools_stub)

    spec = importlib.util.spec_from_file_location(
        'plugins.elitea_core.utils.subagent_prefetch',
        PLUGIN_ROOT / 'utils' / 'subagent_prefetch.py',
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def _app_tool(app_id, ver_id, project_id=None):
    tool = {'type': 'application', 'name': f'app_{app_id}',
            'settings': {'application_id': app_id, 'application_version_id': ver_id}}
    if project_id is not None:
        tool['project_id'] = project_id
    return tool


def _install_tree(sp, monkeypatch, tree, denied=(), failing=(), raising=(), real_permission_check=False):
    """tree: {(app, ver): [child tool dicts]} — fakes expansion, access and app summaries."""
    expanded = []

    def fake_expand(project_id, application_id, version_id, user_id):
        expanded.append((application_id, version_id))
        if (application_id, version_id) in raising:
            raise RuntimeError('db went away')
        if (application_id, version_id) in failing:
            return {'error': 'not found'}
        return {'id': version_id, 'tools': tree.get((application_id, version_id), [])}

    monkeypatch.setattr(sp, 'expand_version_for_sdk', fake_expand)
    if not real_permission_check:
        monkeypatch.setattr(sp, '_can_read_version_details', lambda project_id, user_id: True)
    monkeypatch.setattr(sp, '_denied_application_ids', lambda project_id, app_ids, user_id: set(denied))
    monkeypatch.setattr(sp, '_application_summaries', lambda project_id, app_ids: {
        a: {'name': f'Agent {a}', 'description': f'desc {a}'} for a in app_ids})
    return expanded


def test_diamond_children_are_expanded_once(sp, monkeypatch):
    # TOP -> P1, P2 ; P1 -> S ; P2 -> S ; S -> L  (S and L shared)
    tree = {(1, 11): [_app_tool(3, 33)], (2, 22): [_app_tool(3, 33)], (3, 33): [_app_tool(4, 44)]}
    expanded = _install_tree(sp, monkeypatch, tree)

    result = sp.collect_subagent_version_details(7, [_app_tool(1, 11), _app_tool(2, 22)], user_id=5)

    assert sorted(result) == ['1:11', '2:22', '3:33', '4:44']
    assert sorted(expanded) == [(1, 11), (2, 22), (3, 33), (4, 44)]
    assert result['3:33'] == {'name': 'Agent 3', 'description': 'desc 3',
                              'version_details': {'id': 33, 'tools': [_app_tool(4, 44)]}}


def test_non_application_and_cross_project_tools_are_skipped(sp, monkeypatch):
    expanded = _install_tree(sp, monkeypatch, {})
    root = [{'type': 'artifact', 'settings': {'bucket': 'b'}}, _app_tool(1, 11, project_id=999),
            _app_tool(2, 22, project_id=7), {'type': 'application', 'settings': {}}]

    result = sp.collect_subagent_version_details(7, root, user_id=5)

    assert list(result) == ['2:22']
    assert expanded == [(2, 22)]


def test_denied_child_is_omitted_and_not_descended(sp, monkeypatch):
    tree = {(1, 11): [_app_tool(3, 33)]}
    expanded = _install_tree(sp, monkeypatch, tree, denied={1})

    result = sp.collect_subagent_version_details(7, [_app_tool(1, 11), _app_tool(2, 22)], user_id=5)

    assert list(result) == ['2:22']
    assert (1, 11) not in expanded and (3, 33) not in expanded


def test_expansion_error_is_omitted_others_kept(sp, monkeypatch):
    _install_tree(sp, monkeypatch, {}, failing={(1, 11)})

    result = sp.collect_subagent_version_details(7, [_app_tool(1, 11), _app_tool(2, 22)], user_id=5)

    assert list(result) == ['2:22']


def test_expansion_exception_skips_only_that_child(sp, monkeypatch):
    _install_tree(sp, monkeypatch, {}, raising={(2, 22)})
    root = [_app_tool(1, 11), _app_tool(2, 22), _app_tool(3, 33)]

    result = sp.collect_subagent_version_details(7, root, user_id=5)

    assert sorted(result) == ['1:11', '3:33']


def test_access_lookup_failure_returns_what_was_collected(sp, monkeypatch):
    tree = {(1, 11): [_app_tool(2, 22)]}
    _install_tree(sp, monkeypatch, tree)
    calls = []

    def flaky_denied(project_id, app_ids, user_id):
        calls.append(app_ids)
        if len(calls) > 1:
            raise RuntimeError('social rpc down')
        return set()

    monkeypatch.setattr(sp, '_denied_application_ids', flaky_denied)

    result = sp.collect_subagent_version_details(7, [_app_tool(1, 11)], user_id=5)

    assert list(result) == ['1:11']


def test_node_cap(sp, monkeypatch):
    _install_tree(sp, monkeypatch, {})
    root = [_app_tool(i, i * 10) for i in range(1, 6)]

    result = sp.collect_subagent_version_details(7, root, user_id=5, max_nodes=3)

    assert len(result) == 3


def test_size_cap_stops_before_exceeding(sp, monkeypatch):
    _install_tree(sp, monkeypatch, {})
    entry_size = len(json.dumps({'name': 'Agent 1', 'description': 'desc 1',
                                 'version_details': {'id': 10, 'tools': []}}))

    result = sp.collect_subagent_version_details(
        7, [_app_tool(1, 10), _app_tool(2, 20), _app_tool(3, 30)], user_id=5,
        max_bytes=entry_size * 2 + 1,
    )

    assert sorted(result) == ['1:10', '2:20']


def test_cycle_terminates(sp, monkeypatch):
    tree = {(1, 11): [_app_tool(2, 22)], (2, 22): [_app_tool(1, 11)]}
    expanded = _install_tree(sp, monkeypatch, tree)

    result = sp.collect_subagent_version_details(7, [_app_tool(1, 11)], user_id=5)

    assert sorted(result) == ['1:11', '2:22']
    assert len(expanded) == 2


def test_empty_or_missing_tools(sp, monkeypatch):
    _install_tree(sp, monkeypatch, {})

    assert sp.collect_subagent_version_details(7, None, user_id=5) == {}
    assert sp.collect_subagent_version_details(7, [], user_id=5) == {}



def _stub_auth(monkeypatch, permissions=(), error=None):
    # The real check reads `tools.auth` / `tools.config`; record what it asks for.
    calls = []

    def get_user_permissions(user_id, mode=None, project_id=None):
        calls.append((user_id, mode, project_id))
        if error:
            raise error
        return set(permissions)

    tools_mod = sys.modules['tools']
    monkeypatch.setattr(tools_mod, 'auth', types.SimpleNamespace(get_user_permissions=get_user_permissions), raising=False)
    monkeypatch.setattr(tools_mod, 'config', types.SimpleNamespace(DEFAULT_MODE='default'), raising=False)
    return calls


def test_user_with_version_details_permission_gets_prefetch(sp, monkeypatch):
    _install_tree(sp, monkeypatch, {}, real_permission_check=True)
    calls = _stub_auth(monkeypatch, permissions={'models.applications.predict.post',
                                                 'models.applications.version.details'})

    result = sp.collect_subagent_version_details(7, [_app_tool(1, 11)], user_id=5)

    assert list(result) == ['1:11']
    assert calls == [(5, 'default', 7)]  # checked once per predict, for the end user, in this project


def test_custom_role_without_version_details_gets_no_prefetch(sp, monkeypatch):
    # Can run agents (predict.post) but not read version details: PATCH version used to 403 every
    # child and the SDK skipped them. Prefetch must not make those children reachable.
    expanded = _install_tree(sp, monkeypatch, {}, real_permission_check=True)
    _stub_auth(monkeypatch, permissions={'models.applications.predict.post'})

    result = sp.collect_subagent_version_details(7, [_app_tool(1, 11), _app_tool(2, 22)], user_id=5)

    assert result == {}
    assert expanded == []  # nothing is even expanded


def test_permission_lookup_failure_fails_closed(sp, monkeypatch):
    # An auth RPC error must not turn into "allowed"; the SDK falls back to PATCH, which re-checks.
    expanded = _install_tree(sp, monkeypatch, {}, real_permission_check=True)
    _stub_auth(monkeypatch, error=RuntimeError('auth rpc timeout'))

    result = sp.collect_subagent_version_details(7, [_app_tool(1, 11)], user_id=5)

    assert result == {}
    assert expanded == []


def test_permission_not_checked_when_there_are_no_sub_agents(sp, monkeypatch):
    _install_tree(sp, monkeypatch, {}, real_permission_check=True)
    calls = _stub_auth(monkeypatch, permissions={'models.applications.version.details'})

    assert sp.collect_subagent_version_details(7, [{'type': 'artifact', 'settings': {}}], user_id=5) == {}
    assert calls == []
