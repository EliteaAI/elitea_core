"""#6789: team-project settings endpoints are admin-only; a private project's owner keeps them."""

import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE_NAME = "elitea_core_6789_project_settings_access"


class FakeRpc:
    def __init__(self, roles=None, personal=None, raises=False):
        self.roles = roles or {}
        self.personal = personal or {}
        self.raises = raises

    def timeout(self, _seconds):
        return self

    def admin_get_user_roles(self, project_id, user_id):
        if self.raises:
            raise RuntimeError("rpc unavailable")
        return [{"name": name} for name in self.roles.get((project_id, user_id), [])]

    def projects_get_personal_project_id(self, user_id):
        return self.personal.get(user_id)


@pytest.fixture
def load(monkeypatch):
    def _load(rpc, user_id=99):
        log = types.SimpleNamespace(warning=lambda *a, **k: None)
        monkeypatch.setitem(sys.modules, "pylon", types.ModuleType("pylon"))
        monkeypatch.setitem(sys.modules, "pylon.core", types.ModuleType("pylon.core"))
        monkeypatch.setitem(sys.modules, "pylon.core.tools", types.SimpleNamespace(log=log))
        tools = types.SimpleNamespace(
            auth=types.SimpleNamespace(current_user=lambda: {"id": user_id}),
            rpc_tools=types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
        )
        monkeypatch.setitem(sys.modules, "tools", tools)
        spec = importlib.util.spec_from_file_location(
            MODULE_NAME, PLUGIN_ROOT / "utils/project_settings_access.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    return _load


def _guarded(module):
    class Handler:
        @module.require_project_settings_admin
        def put(self, project_id, **kwargs):
            return {"saved": project_id}, 200
    return Handler()


def test_team_project_admin_may_change_settings(load):
    module = load(FakeRpc(roles={(7, 99): ["admin"]}))

    assert _guarded(module).put(7) == ({"saved": 7}, 200)


def test_team_project_editor_is_refused(load):
    module = load(FakeRpc(roles={(7, 99): ["editor", "viewer"]}, personal={99: 3}))

    assert _guarded(module).put(7) == (module.PROJECT_SETTINGS_DENIED, 403)


def test_role_merely_containing_admin_is_not_an_admin(load):
    module = load(FakeRpc(roles={(7, 99): ["billing-admin"]}))

    assert module.can_manage_project_settings(7, 99) is False


def test_private_project_owner_may_change_settings_as_editor(load):
    module = load(FakeRpc(roles={(3, 99): ["editor", "viewer", "monitor"]}, personal={99: 3}))

    assert _guarded(module).put(3) == ({"saved": 3}, 200)


def test_role_lookup_failure_fails_closed_in_a_team_project_only(load):
    module = load(FakeRpc(raises=True, personal={99: 3}))

    assert module.can_manage_project_settings(7, 99) is False
    assert module.can_manage_project_settings(3, 99) is True


def test_anonymous_caller_is_refused(load):
    module = load(FakeRpc(roles={(7, 99): ["admin"]}), user_id=None)

    assert _guarded(module).put(7) == (module.PROJECT_SETTINGS_DENIED, 403)
