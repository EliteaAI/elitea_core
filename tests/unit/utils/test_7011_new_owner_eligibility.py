"""EL-7011 US-10 R2.1, R2.2: the eligibility check of a new schedule owner."""
import sys
import types
from queue import Empty
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

NOT_ACTIVE = "the new owner is not an active project member"
CANNOT_USE = "the new owner cannot use schedules"
UNAVAILABLE = "access check unavailable"
LIST = "models.applications.schedules.list"
RUN = "models.applications.schedules.run"


class FakeRpc:
    """Answers each auth call by name with its timeout recorded; an answer that is an exception is raised."""

    def __init__(self, member=True, user=None, permissions=None):
        self.answers = {"auth_check_user_in_project": member,
                        "auth_get_user": {"id": 20, "suspended": False} if user is None else user,
                        "auth_get_project_user_permissions": {LIST, RUN} if permissions is None else permissions}
        self.calls = []
        self._timeout = None

    def timeout(self, seconds):
        self._timeout = seconds
        return self

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append((name, self._timeout, args))
            answer = self.answers[name]
            if isinstance(answer, Exception):
                raise answer
            return answer
        return call


def load_owner(monkeypatch):
    """Load schedule_owner with its imports; another test may leave a tools stub without auth or config."""
    tools = sys.modules.setdefault("tools", types.ModuleType("tools"))
    monkeypatch.setattr(tools, "auth", getattr(tools, "auth", SimpleNamespace()), raising=False)
    monkeypatch.setattr(tools, "config", getattr(tools, "config", SimpleNamespace()), raising=False)
    load("utils/schedule_access.py", fresh=True)
    prerun = load("utils/schedule_prerun.py", fresh=True)
    return prerun, load("utils/schedule_owner.py", fresh=True)


def _validate(monkeypatch, rpc, owner_id=20, folder=None):
    """`folder` is what entity_access_error answers: None (open), a (payload, status) or an exception."""
    prerun, owner = load_owner(monkeypatch)
    monkeypatch.setattr(prerun, "rpc_tools", SimpleNamespace(RpcMixin=lambda: SimpleNamespace(rpc=rpc)))

    def entity_access_error(project_id, entity_type, entity_id, write=False, user_id=None):
        rpc.calls.append(("entity_access_error", None, (project_id, entity_type, entity_id, user_id)))
        if isinstance(folder, Exception):
            raise folder
        return folder
    monkeypatch.setitem(sys.modules, "plugins.elitea_core.utils.folder_access",
                        types.ModuleType("plugins.elitea_core.utils.folder_access"))
    sys.modules["plugins.elitea_core.utils.folder_access"].entity_access_error = entity_access_error
    return owner.validate_new_owner(34, owner_id, "agent", 55)


def test_7011_r2_1_r2_2_eligible_new_owner_passes(monkeypatch):
    """TC-US10-11: an active member with schedules.list is accepted; every auth call has the 3 s timeout."""
    rpc = FakeRpc(permissions={LIST, RUN, "models.applications.schedules.details"})
    assert _validate(monkeypatch, rpc) is None
    assert ("auth_get_project_user_permissions", 3, (34, 20)) in rpc.calls
    assert {timeout for name, timeout, _ in rpc.calls if name != "entity_access_error"} == {3}


@pytest.mark.parametrize("rpc", [
    FakeRpc(member=False),
    FakeRpc(user={"id": 22, "suspended": True}),
    FakeRpc(user=RuntimeError("No such user found: id=22")),
], ids=["not-member", "suspended", "unknown-user"])
def test_7011_r2_1_inactive_new_owner_is_not_an_active_member(monkeypatch, rpc):
    """TC-US10-12: a non-member, a suspended or an unknown user is refused before the permission read."""
    assert _validate(monkeypatch, rpc) == NOT_ACTIVE
    assert "auth_get_project_user_permissions" not in [name for name, *_ in rpc.calls]


@pytest.mark.parametrize("permissions", [set(), {"models.applications.schedules.details"}, {RUN}, {LIST}],
                         ids=["no-role", "no-list-no-run", "no-list", "no-run"])
def test_7011_r2_2_member_without_schedules_list_cannot_use_schedules(monkeypatch, permissions):
    """TC-US10-13: an active member without schedules.list or schedules.run is refused."""
    assert _validate(monkeypatch, FakeRpc(permissions=permissions), owner_id=23) == CANNOT_USE


@pytest.mark.parametrize("failing", ["auth_check_user_in_project", "auth_get_user",
                                     "auth_get_project_user_permissions"])
@pytest.mark.parametrize("error", [Empty(), RuntimeError("database is down"), ConnectionError("rpc node down")],
                         ids=["timeout", "rpc-error", "other-error"])
def test_7011_r2_1_auth_failure_is_unavailable_not_ineligible(monkeypatch, failing, error):
    """Design NFR: a timeout or an RPC error is "unavailable" (503), never a 400 reason."""
    rpc = FakeRpc()
    rpc.answers[failing] = error
    assert _validate(monkeypatch, rpc) == UNAVAILABLE


def test_the_new_owner_who_cannot_open_the_entity_folder_cannot_use_schedules(monkeypatch):
    rpc = FakeRpc()
    assert _validate(monkeypatch, rpc, folder=({"ok": False, "error": "Not found"}, 404)) == CANNOT_USE
    assert ("entity_access_error", None, (34, "agent", 55, 20)) in rpc.calls


def test_a_failing_folder_check_is_unavailable_not_ineligible(monkeypatch):
    assert _validate(monkeypatch, FakeRpc(), folder=RuntimeError("social down")) == UNAVAILABLE
