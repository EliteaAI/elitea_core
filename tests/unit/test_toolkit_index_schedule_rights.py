"""Toolkit Index schedules: the action list, the owner rights check, the new-owner check and the governance notice."""
import sys
import types
from queue import Empty
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load
from unit.test_7011_schedule_access import FakeAuth, _scope, load as access_loader  # noqa: F401 -- fixture

LOST = "owner lost access"
UNAVAILABLE = "access check unavailable"
LIST = "models.applications.schedules.list"
RUN = "models.applications.schedules.run"
INDEX_EDIT = "models.applications.index_meta.edit"
FOLDER = "plugins.elitea_core.utils.folder_access"
OWNER, ADMIN = 5, 9
EDITOR_VERBS = ("list", "details", "create", "update", "delete", "run")


def index_row(owner=OWNER, enabled=True, toolkit_id=7, index_name="docs"):
    return SimpleNamespace(created_by=owner, enabled=enabled, version_id=None, trigger_kind="cron",
                           entity_type="toolkit_index", toolkit_id=toolkit_id, index_name=index_name)


@pytest.fixture
def access(access_loader):
    return access_loader(FakeAuth())


def test_index_row_owner_may_run_edit_and_delete(access):
    actions = access.allowed_actions(index_row(), _scope(access, OWNER, *EDITOR_VERBS))
    assert actions == ["run", "edit", "pause", "delete"]


@pytest.mark.parametrize("row", [index_row(toolkit_id=None), index_row(index_name=None)])
def test_index_row_without_its_target_cannot_run(access, row):
    assert "run" not in access.allowed_actions(row, _scope(access, OWNER, *EDITOR_VERBS))


def test_index_row_is_hidden_from_a_user_who_is_not_owner_or_admin(access):
    assert access.allowed_actions(index_row(), _scope(access, 11, *EDITOR_VERBS)) == []


def test_agent_row_without_a_version_still_cannot_run(access):
    row = SimpleNamespace(created_by=OWNER, enabled=True, version_id=None, trigger_kind="cron",
                          entity_type="agent")
    assert "run" not in access.allowed_actions(row, _scope(access, OWNER, *EDITOR_VERBS))


def test_agent_row_with_a_version_still_runs(access):
    row = SimpleNamespace(created_by=OWNER, enabled=True, version_id=3, trigger_kind="cron", entity_type="agent")
    assert access.allowed_actions(row, _scope(access, OWNER, *EDITOR_VERBS))[0] == "run"


class FakeRpc:
    """Answers the permission call; an answer that is an exception is raised."""

    def __init__(self, permissions):
        self.permissions = permissions
        self.calls = []

    def timeout(self, seconds):
        return self

    def auth_get_project_user_permissions(self, project_id, user_id):
        self.calls.append((project_id, user_id))
        if isinstance(self.permissions, Exception):
            raise self.permissions
        return self.permissions


@pytest.fixture
def prerun(monkeypatch):
    """schedule_prerun with a fake folder check; `folder` answers entity_access_error."""
    tools = sys.modules.setdefault("tools", types.ModuleType("tools"))
    monkeypatch.setattr(tools, "auth", getattr(tools, "auth", SimpleNamespace()), raising=False)
    monkeypatch.setattr(tools, "config", getattr(tools, "config", SimpleNamespace()), raising=False)
    load("utils/schedule_access.py", fresh=True)
    module = load("utils/schedule_prerun.py", fresh=True)
    folder = types.ModuleType(FOLDER)
    folder.TOOLKIT_ENTITY_TYPES = ("toolkit", "datasource")
    folder.answer = None
    folder.calls = []

    def entity_access_error(project_id, entity_type, entity_id, write=False, user_id=None):
        folder.calls.append((project_id, entity_type, entity_id, write, user_id))
        if isinstance(folder.answer, Exception):
            raise folder.answer
        return folder.answer
    folder.entity_access_error = entity_access_error
    monkeypatch.setitem(sys.modules, FOLDER, folder)
    module.folder = folder

    def use(permissions):
        rpc = FakeRpc(permissions)
        monkeypatch.setattr(module, "rpc_tools", SimpleNamespace(RpcMixin=lambda: SimpleNamespace(rpc=rpc)))
        return rpc
    module.use = use
    return module


def test_index_owner_with_both_permissions_and_a_writable_folder_passes(prerun):
    prerun.use({RUN, INDEX_EDIT})
    assert prerun.check_owner_rights(34, OWNER, "toolkit_index", 7) is None
    assert prerun.folder.calls == [(34, ("toolkit", "datasource"), 7, True, OWNER)]


def test_index_owner_without_the_index_edit_permission_loses_access(prerun):
    prerun.use({RUN})
    assert prerun.check_owner_rights(34, OWNER, "toolkit_index", 7) == LOST
    assert not prerun.folder.calls


def test_index_owner_without_the_run_permission_loses_access(prerun):
    prerun.use({INDEX_EDIT})
    assert prerun.check_owner_rights(34, OWNER, "toolkit_index", 7) == LOST


def test_index_owner_refused_by_the_toolkit_folder_loses_access(prerun):
    prerun.use({RUN, INDEX_EDIT})
    prerun.folder.answer = ({"error": "no"}, 403)
    assert prerun.check_owner_rights(34, OWNER, "toolkit_index", 7) == LOST


def test_index_check_without_a_toolkit_id_loses_access(prerun):
    prerun.use({RUN, INDEX_EDIT})
    assert prerun.check_owner_rights(34, OWNER, "toolkit_index", None) == LOST


@pytest.mark.parametrize("failure", [Empty(), RuntimeError("database is down")])
def test_index_check_failure_is_unavailable_not_lost(prerun, failure):
    prerun.use({RUN, INDEX_EDIT})
    prerun.folder.answer = failure
    assert prerun.check_owner_rights(34, OWNER, "toolkit_index", 7) == UNAVAILABLE


def test_agent_owner_needs_no_index_permission_and_no_write_access(prerun):
    prerun.use({RUN})
    assert prerun.check_owner_rights(34, OWNER, "agent", 55) is None
    assert prerun.folder.calls == [(34, "agent", 55, False, OWNER)]


def test_new_owner_check_passes_the_toolkit_through(prerun, monkeypatch):
    owner = load("utils/schedule_owner.py", fresh=True)
    monkeypatch.setattr(owner, "check_owner_access", lambda project_id, owner_id: None)
    seen = []
    monkeypatch.setattr(owner, "check_owner_rights", lambda *args, **kwargs: seen.append((args, kwargs)) or LOST)
    assert owner.validate_new_owner(34, 20, "toolkit_index", 7) == owner.CANNOT_USE_SCHEDULES
    assert seen == [((34, 20, "toolkit_index", 7), {"verbs": ("list", "run")})]


def test_new_owner_without_the_index_edit_permission_cannot_use_schedules(prerun, monkeypatch):
    owner = load("utils/schedule_owner.py", fresh=True)
    monkeypatch.setattr(owner, "check_owner_access", lambda project_id, owner_id: None)
    prerun.use({LIST, RUN})
    assert owner.validate_new_owner(34, 20, "toolkit_index", 7) == owner.CANNOT_USE_SCHEDULES


def test_new_owner_with_every_right_is_accepted(prerun, monkeypatch):
    owner = load("utils/schedule_owner.py", fresh=True)
    monkeypatch.setattr(owner, "check_owner_access", lambda project_id, owner_id: None)
    prerun.use({LIST, RUN, INDEX_EDIT})
    assert owner.validate_new_owner(34, 20, "toolkit_index", 7) is None


@pytest.fixture
def governance(monkeypatch):
    """trigger_governance with governance on and no binding, so an agent schedule would get a notice."""
    stub = types.ModuleType("plugins.elitea_core.models.agent_identity")
    stub.AgentIdentityBinding = type("AgentIdentityBinding", (), {})
    monkeypatch.setitem(sys.modules, stub.__name__, stub)
    rules = types.ModuleType("plugins.elitea_core.utils.governance")
    rules.is_governed = lambda module: True
    rules.is_auditing = lambda module: True
    monkeypatch.setitem(sys.modules, rules.__name__, rules)
    module = load("utils/trigger_governance.py", fresh=True)
    monkeypatch.setattr(module, "version_is_bound", lambda *args: False)
    return module


def test_governance_notice_is_none_for_an_index_schedule(governance):
    assert governance.schedule_governance_notice("m", None, 1, None, None, OWNER, OWNER, "toolkit_index") is None
    assert governance.governance_notice_for("m", None, 1, None, None, "schedule", "toolkit_index") is None


def test_governance_notice_still_comes_for_an_agent_schedule(governance):
    notice = governance.governance_notice_for("m", None, 1, 2, 3, "schedule", "agent")
    assert notice == governance.UNBOUND_NOTICE
