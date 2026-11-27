"""#7433: MCP and ui_host project-access checks use one membership lookup, not the user's full project list.

list_user_projects loads every project on a cache miss (~16.9k on next), stalling the gevent hub for seconds.
The single-project check must keep the old semantics: members pass, non-members and suspended projects don't.

    python tests/run_tests.py unit/test_7433_project_access.py -v
"""
import ast
import pathlib
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
HELPER = PLUGIN_ROOT / "utils" / "project_access.py"
ROUTES = ("mcp_sse.py", "ui_host.py")
MEMBER, OUTSIDER = 3, 6


class FakeRpc:
    def __init__(self, projects, members):
        self.projects, self.members, self.calls = projects, members, []

    def admin_check_user_in_project(self, project_id, user_id):
        self.calls.append("check")
        return (project_id, user_id) in self.members

    def project_get_by_id(self, project_id):
        self.calls.append("get")
        return self.projects.get(project_id)

    def list_user_projects(self, *_a, **_k):
        raise AssertionError("access check must not build the full project list")


@pytest.fixture
def make():
    def _make(projects, members):
        rpc = FakeRpc(projects, members)
        namespace = {"context": types.SimpleNamespace(rpc_manager=types.SimpleNamespace(call=rpc))}
        tree = ast.parse(HELPER.read_text())
        tree.body = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
        exec(compile(tree, str(HELPER), "exec"), namespace)  # pylint: disable=W0122
        return namespace["get_accessible_project"], rpc
    return _make


def test_7433_member_gets_the_project_row(make):
    check, _ = make({10: {"id": 10, "name": "team", "suspended": False}}, {(10, MEMBER)})
    assert check(10, MEMBER)["name"] == "team"


def test_7433_non_member_is_rejected_without_loading_the_project(make):
    check, rpc = make({10: {"id": 10, "name": "team", "suspended": False}}, {(10, MEMBER)})
    assert check(10, OUTSIDER) is None
    assert rpc.calls == ["check"]


def test_7433_suspended_project_stays_rejected_for_members(make):
    # list_user_projects dropped suspended projects by default; the new check has to do it explicitly
    check, _ = make({10: {"id": 10, "name": "team", "suspended": True}}, {(10, MEMBER)})
    assert check(10, MEMBER) is None


def test_7433_dangling_membership_on_a_missing_project_is_rejected(make):
    check, _ = make({}, {(10, MEMBER)})
    assert check(10, MEMBER) is None


@pytest.mark.parametrize("route", ROUTES)
def test_7433_routes_no_longer_call_list_user_projects(route):
    source = (PLUGIN_ROOT / "routes" / route).read_text()
    assert "list_user_projects" not in source
    assert "get_accessible_project" in source
