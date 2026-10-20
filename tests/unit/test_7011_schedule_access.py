"""EL-7011 US-02: schedule permission family, scope and allowed_actions (R1.1-R3.4).

The helper module is loaded from source with a fake `tools` (auth, config); no Pylon runtime.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PREFIX = "models.applications.schedules."
VERBS = ("list", "details", "create", "update", "delete", "run")
ADMIN_ONLY = ("admin", "export", "import")
EDITOR_PAIR = {"admin": True, "editor": True, "viewer": False}
ADMIN_PAIR = {"admin": True, "editor": False, "viewer": False}
MODES = ("administration", "default")


class FakeAuth:
    def __init__(self, user_id=5, permissions=None, error=None):
        self.user_id = user_id
        self.permissions = permissions if permissions is not None else {}
        self.error = error
        self.registered = []
        self.resolve_calls = []

    def register_permissions(self, spec):
        self.registered.append(spec)

    def current_user(self):
        return {"id": self.user_id}

    def resolve_permissions(self, **kwargs):
        self.resolve_calls.append(kwargs)
        if self.error:
            raise self.error
        return self.permissions[kwargs["project_id"]]


@pytest.fixture
def load(monkeypatch):
    def _load(auth):
        config = types.SimpleNamespace(ADMINISTRATION_MODE="administration", DEFAULT_MODE="default")
        monkeypatch.setitem(sys.modules, "tools", types.SimpleNamespace(auth=auth, config=config))
        spec = importlib.util.spec_from_file_location(
            "schedule_access_7011", PLUGIN_ROOT / "utils" / "schedule_access.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    return _load


def _registered(auth):
    return {s["permissions"][0]: s["recommended_roles"] for s in auth.registered}


def _perms(*names):
    return frozenset(PREFIX + n for n in names)


def _scope(module, user_id, *names):
    return module.ScheduleScope(user_id=user_id, is_admin="admin" in names, permissions=_perms(*names))


def _row(owner=5, enabled=True, version_id=10):
    return types.SimpleNamespace(created_by=owner, enabled=enabled, version_id=version_id)


# R1.1, R1.2, R1.3 (TC-US02-01, 02, 04, 05)
def test_r1_1_el7011_verbs_registered_for_admin_and_editor(load):
    auth = FakeAuth()
    load(auth).register_schedule_permissions()
    roles = _registered(auth)
    for verb in VERBS:
        assert {m: roles[PREFIX + verb][m] for m in MODES} == {m: EDITOR_PAIR for m in MODES}


def test_r1_2_el7011_viewer_gets_no_verb_permission(load):
    auth = FakeAuth()
    load(auth).register_schedule_permissions()
    roles = _registered(auth)
    assert all(roles[PREFIX + v][m]["viewer"] is False for v in VERBS for m in MODES)


def test_r1_3_el7011_admin_export_import_for_admin_only(load):
    auth = FakeAuth()
    load(auth).register_schedule_permissions()
    roles = _registered(auth)
    for name in ADMIN_ONLY:
        assert {m: roles[PREFIX + name][m] for m in MODES} == {m: ADMIN_PAIR for m in MODES}


def test_r1_3_el7011_exactly_nine_names_registered_once(load):
    auth = FakeAuth()
    load(auth).register_schedule_permissions()
    names = [s["permissions"][0] for s in auth.registered]
    assert sorted(names) == sorted(PREFIX + n for n in VERBS + ADMIN_ONLY)


# R1.4 (TC-US02-03)
@pytest.mark.parametrize("action", VERBS)
def test_r1_4_el7011_check_spec_same_pair_in_both_modes(load, action):
    assert load(FakeAuth()).schedule_check_spec(action) == {
        "permissions": [PREFIX + action],
        "recommended_roles": {m: EDITOR_PAIR for m in MODES},
    }


# R2.1, R2.2, R2.3 (TC-US02-06..09)
def test_r2_1_el7011_admin_permission_gives_project_wide_scope(load):
    auth = FakeAuth(user_id=5, permissions={1: _perms("admin", "list")})
    scope = load(auth).schedule_scope(1)
    assert (scope.user_id, scope.is_admin) == (5, True)
    assert auth.resolve_calls == [{"mode": "default", "project_id": 1}]


def test_r2_2_el7011_admin_in_one_project_is_own_only_in_another(load):
    auth = FakeAuth(permissions={1: _perms("admin"), 2: _perms("list")})
    module = load(auth)
    assert module.schedule_scope(2).is_admin is False
    assert module.schedule_scope(1).is_admin is True


def test_r2_2_el7011_editor_without_admin_permission_is_own_only(load):
    auth = FakeAuth(user_id=6, permissions={1: _perms(*VERBS)})
    assert load(auth).schedule_scope(1).is_admin is False


def test_r2_3_el7011_unreadable_permissions_raise(load):
    auth = FakeAuth(error=RuntimeError("auth down"))
    with pytest.raises(RuntimeError):
        load(auth).schedule_scope(1)


# R3.1, R3.2, R3.3, R3.4 (TC-US02-10..16)
def test_r3_1_el7011_owner_gets_run_and_verbs_in_order(load):
    module = load(FakeAuth())
    scope = _scope(module, 5, *VERBS)
    assert module.allowed_actions(_row(), scope) == ["run", "edit", "pause", "delete"]


@pytest.mark.parametrize(
    "names, version_id", [(("update", "delete"), 10), (VERBS, None)]
)
def test_r3_1_el7011_owner_run_needs_permission_and_version(load, names, version_id):
    module = load(FakeAuth())
    assert "run" not in module.allowed_actions(_row(version_id=version_id), _scope(module, 5, *names))


def test_r3_2_el7011_admin_gets_no_run_on_other_owner_row(load):
    module = load(FakeAuth())
    assert "run" not in module.allowed_actions(_row(owner=5), _scope(module, 9, "admin", "run"))


def test_r3_3_el7011_admin_gets_change_owner_and_verbs_on_any_row(load):
    module = load(FakeAuth())
    scope = _scope(module, 9, "admin", "update", "delete")
    assert module.allowed_actions(_row(enabled=True), scope) == ["edit", "pause", "delete", "change_owner"]
    assert module.allowed_actions(_row(enabled=False), scope) == ["edit", "resume", "delete", "change_owner"]


def test_r3_3_el7011_admin_without_delete_permission_gets_no_delete(load):
    module = load(FakeAuth())
    actions = module.allowed_actions(_row(), _scope(module, 9, "admin", "update"))
    assert "delete" not in actions and {"edit", "change_owner"} <= set(actions)


def test_r3_4_el7011_regular_caller_gets_nothing_on_other_owner_row(load):
    module = load(FakeAuth())
    assert module.allowed_actions(_row(owner=5), _scope(module, 6, *VERBS)) == []


def test_r3_3_el7011_regular_owner_never_gets_change_owner(load):
    module = load(FakeAuth())
    assert "change_owner" not in module.allowed_actions(_row(), _scope(module, 5, *VERBS))
