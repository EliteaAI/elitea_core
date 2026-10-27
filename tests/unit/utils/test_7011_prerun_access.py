"""EL-7011 US-09 R1.1-R1.3: the principal check and the owner access check before a run."""
import sys
from queue import Empty
from types import ModuleType, SimpleNamespace

import pytest
from unit._tool_spend_loader import load

PRERUN = "utils/schedule_prerun.py"
LOST = "owner lost access"
UNAVAILABLE = "access check unavailable"
RUN = "models.applications.schedules.run"


class FakeRpc:
    """Records each auth call with its timeout; an answer that is an exception is raised."""

    def __init__(self, member=True, user=None):
        self.answers = {"auth_check_user_in_project": member,
                        "auth_get_user": {"id": 12, "suspended": False} if user is None else user,
                        "auth_get_project_user_permissions": {RUN}}
        self.calls = []
        self._timeout = None

    def timeout(self, seconds):
        self._timeout = seconds
        return self

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append((name, self._timeout, args, kwargs))
            answer = self.answers[name]
            if isinstance(answer, Exception):
                raise answer
            return answer
        return call


@pytest.fixture
def prerun():
    return load(PRERUN, fresh=True)


def _access(prerun, monkeypatch, rpc):
    monkeypatch.setattr(prerun, "rpc_tools", SimpleNamespace(RpcMixin=lambda: SimpleNamespace(rpc=rpc)))
    return prerun.check_owner_access(34, 12)


def test_7011_r1_1_r1_2_active_member_passes_after_membership_then_user(prerun, monkeypatch):
    rpc = FakeRpc()
    assert _access(prerun, monkeypatch, rpc) is None
    assert [(name, timeout) for name, timeout, *_ in rpc.calls] == [
        ("auth_check_user_in_project", 3), ("auth_get_user", 3)]
    assert rpc.calls[0][2:] == ((34, 12), {})
    assert rpc.calls[1][2:] == ((), {"user_id": 12})


def test_7011_r1_1_non_member_loses_access_without_the_user_read(prerun, monkeypatch):
    rpc = FakeRpc(member=False)
    assert _access(prerun, monkeypatch, rpc) == LOST
    assert [name for name, *_ in rpc.calls] == ["auth_check_user_in_project"]


def test_7011_r1_2_suspended_member_loses_access(prerun, monkeypatch):
    assert _access(prerun, monkeypatch, FakeRpc(user={"id": 12, "suspended": True})) == LOST


def test_7011_r1_2_missing_user_record_loses_access(prerun, monkeypatch):
    rpc = FakeRpc(user=RuntimeError("No such user found: id=12"))
    assert _access(prerun, monkeypatch, rpc) == LOST


@pytest.mark.parametrize("failing", ["auth_check_user_in_project", "auth_get_user"])
@pytest.mark.parametrize("error", [Empty(), RuntimeError("database is down")])
def test_7011_r1_1_auth_call_failure_is_unavailable_not_lost(prerun, monkeypatch, failing, error):
    rpc = FakeRpc()
    rpc.answers[failing] = error
    assert _access(prerun, monkeypatch, rpc) == UNAVAILABLE


def test_7011_r1_2_an_empty_user_answer_loses_access(prerun, monkeypatch):
    rpc = FakeRpc()
    rpc.answers["auth_get_user"] = None
    assert _access(prerun, monkeypatch, rpc) == LOST


def test_7011_r1_1_any_other_auth_error_is_unavailable(prerun, monkeypatch):
    rpc = FakeRpc(member=ConnectionError("rpc node down"))
    assert _access(prerun, monkeypatch, rpc) == UNAVAILABLE


def _schedule(kind="delegation", ref=12, created_by=12):
    return SimpleNamespace(principal={"kind": kind, "ref": ref}, created_by=created_by)


def test_7011_r1_3_delegation_principal_of_the_owner_passes(prerun):
    assert prerun.check_principal(_schedule()) is None


@pytest.mark.parametrize("schedule", [
    _schedule(ref=99),
    _schedule(kind="user"),
    _schedule(kind="legacy_system"),
    _schedule(ref="12"),
    SimpleNamespace(principal={}, created_by=12),
], ids=["other-ref", "user-kind", "legacy-kind", "string-ref", "empty"])
def test_7011_r1_3_principal_that_does_not_match_the_owner_fails(prerun, schedule):
    assert prerun.check_principal(schedule) == "principal does not match the owner"


def test_7011_r1_3_a_row_without_an_owner_fails_the_principal_check(prerun):
    assert prerun.check_principal(_schedule(ref=None, created_by=None)) == "principal does not match the owner"


def _rights(prerun, monkeypatch, rpc, folder=None, entity=("agent", 55), **kwargs):
    """check_owner_rights with a fake permission table and a fake folder check (None opens, else refuses)."""
    monkeypatch.setattr(prerun, "rpc_tools", SimpleNamespace(RpcMixin=lambda: SimpleNamespace(rpc=rpc)))
    access = ModuleType("plugins.elitea_core.utils.schedule_access")
    access.SCHEDULE_PERMISSIONS = {"run": RUN, "list": "models.applications.schedules.list"}
    folders = ModuleType("plugins.elitea_core.utils.folder_access")

    def entity_access_error(project_id, entity_type, entity_id, write=False, user_id=None):
        rpc.calls.append(("entity_access_error", None, (project_id, entity_type, entity_id, user_id), {}))
        if isinstance(folder, Exception):
            raise folder
        return folder
    folders.entity_access_error = entity_access_error
    monkeypatch.setitem(sys.modules, access.__name__, access)
    monkeypatch.setitem(sys.modules, folders.__name__, folders)
    return prerun.check_owner_rights(34, 12, *entity, **kwargs)


def test_an_owner_with_the_run_permission_and_folder_access_passes(prerun, monkeypatch):
    rpc = FakeRpc()
    assert _rights(prerun, monkeypatch, rpc) is None
    assert [call[0] for call in rpc.calls] == ["auth_get_project_user_permissions", "entity_access_error"]
    assert rpc.calls[0][1:3] == (3, (34, 12))
    assert rpc.calls[1][2] == (34, "agent", 55, 12)


def test_an_owner_without_the_run_permission_loses_access_before_the_folder_check(prerun, monkeypatch):
    rpc = FakeRpc()
    rpc.answers["auth_get_project_user_permissions"] = {"models.applications.schedules.list"}
    assert _rights(prerun, monkeypatch, rpc) == LOST
    assert "entity_access_error" not in [call[0] for call in rpc.calls]


def test_an_owner_who_lost_the_folder_loses_access(prerun, monkeypatch):
    assert _rights(prerun, monkeypatch, FakeRpc(), folder=({"ok": False}, 404)) == LOST


@pytest.mark.parametrize("error", [Empty(), RuntimeError("social down")])
def test_a_failing_rights_check_is_unavailable_not_lost(prerun, monkeypatch, error):
    rpc = FakeRpc()
    rpc.answers["auth_get_project_user_permissions"] = error
    assert _rights(prerun, monkeypatch, rpc) == UNAVAILABLE
    assert _rights(prerun, monkeypatch, FakeRpc(), folder=error) == UNAVAILABLE


def test_no_entity_skips_the_folder_check(prerun, monkeypatch):
    rpc = FakeRpc()
    assert _rights(prerun, monkeypatch, rpc, entity=(None, None)) is None
    assert "entity_access_error" not in [call[0] for call in rpc.calls]
