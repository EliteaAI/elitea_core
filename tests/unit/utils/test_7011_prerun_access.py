"""EL-7011 US-09 R1.1-R1.3: the principal check and the owner access check before a run."""
from queue import Empty
from types import SimpleNamespace

import pytest
from unit._tool_spend_loader import load

PRERUN = "utils/schedule_prerun.py"
LOST = "owner lost access"
UNAVAILABLE = "access check unavailable"


class FakeRpc:
    """Records each auth call with its timeout; an answer that is an exception is raised."""

    def __init__(self, member=True, user=None):
        self.answers = {"auth_check_user_in_project": member,
                        "auth_get_user": {"id": 12, "suspended": False} if user is None else user}
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
