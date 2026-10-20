"""C-1009 (#6756): agent identity binding lifecycle - legal changes, activation guards, one conditional UPDATE."""

import itertools
import sys
import types
from pathlib import Path

import pytest
PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import identity  # noqa: E402

STATES = ("draft", "provisioning", "pending_approval", "active", "suspended", "retiring", "retired")
# Written out here on purpose: the table in the module must equal this reading of the design.
ALLOWED = {
    ("draft", "provisioning"), ("provisioning", "pending_approval"), ("pending_approval", "active"),
    ("pending_approval", "draft"), ("active", "suspended"), ("suspended", "active"),
    ("active", "retiring"), ("suspended", "retiring"), ("retiring", "retired"),
    ("draft", "retired"), ("provisioning", "retired"), ("pending_approval", "retired"),
    ("active", "pending_approval"), ("suspended", "pending_approval"),   # rebind (#7105)
}
ENTERING_REVOKED = {"suspended", "retiring", "retired"}
REBINDS = {("active", "pending_approval"), ("suspended", "pending_approval")}
DIGEST = "adv1:" + "a" * 64
GOOD = dict(actor_user_id=7, approval_ref="appr-1", policy_revision="rev-3", current_digest=DIGEST)


POSTGRESQL = []


@pytest.fixture(scope="module")
def state():
    saved = dict(sys.modules)
    ns = identity.install(PLUGIN_ROOT)
    POSTGRESQL.append(ns.sa.postgresql)
    yield ns.state
    sys.modules.clear()
    sys.modules.update(saved)


class FakeSession:
    """Answers the row read with ``row`` and the UPDATE with ``rowcount``; records every statement."""

    def __init__(self, row=("dedicated_agent", DIGEST, None, None), rowcount=1):
        self.row, self.rowcount, self.statements = row, rowcount, []

    def execute(self, statement):
        self.statements.append(statement)
        if statement.is_select:
            row = None if self.row is None else types.SimpleNamespace(
                identity_mode=self.row[0], definition_digest=self.row[1],
                created_by=self.row[2] if len(self.row) > 2 else None,
                state_changed_by=self.row[3] if len(self.row) > 3 else None)
            return types.SimpleNamespace(one_or_none=lambda: row)
        return types.SimpleNamespace(rowcount=self.rowcount)

    def commit(self):
        raise AssertionError("transition_binding must not commit")

    rollback = commit

    @property
    def updates(self):
        return [s for s in self.statements if s.is_update]


def compiled(update):
    result = update.compile(dialect=POSTGRESQL[0].dialect())
    return str(result), result.params


def set_clause(sql):
    return sql.split(" SET ", 1)[1].split(" WHERE ", 1)[0]


# --- the table ------------------------------------------------------------------------------

def test_the_table_is_exactly_the_designed_one(state):
    assert set(state.TRANSITIONS) == ALLOWED
    assert set(state.BUMPS_EPOCH) == ENTERING_REVOKED


def test_nothing_leaves_retired(state):
    assert not [pair for pair in state.TRANSITIONS if pair[0] == "retired"]


@pytest.mark.parametrize("expected,target", list(itertools.product(STATES, STATES)))
def test_full_matrix(state, expected, target):
    session = FakeSession()
    if (expected, target) in ALLOWED:
        assert state.transition_binding(session, "b1", expected, target, **GOOD) is True
        assert len(session.updates) == 1
    else:
        with pytest.raises(state.InvalidTransition) as err:
            state.transition_binding(session, "b1", expected, target, **GOOD)
        assert err.value.reason_code == "INVALID_TRANSITION"
        assert session.statements == []


# --- activation guards ------------------------------------------------------------------------

@pytest.mark.parametrize("missing", ["approval_ref", "policy_revision"])
@pytest.mark.parametrize("value", [None, ""])
def test_activation_needs_an_approval_and_a_policy_revision(state, missing, value):
    session = FakeSession()
    with pytest.raises(state.InvalidTransition) as err:
        state.transition_binding(session, "b1", "pending_approval", "active", **{**GOOD, missing: value})
    assert err.value.reason_code == "APPROVAL_REQUIRED"
    assert session.statements == []


def test_resuming_a_suspended_binding_needs_the_approval_again(state):
    with pytest.raises(state.InvalidTransition) as err:
        state.transition_binding(FakeSession(), "b1", "suspended", "active", actor_user_id=7, current_digest=DIGEST)
    assert err.value.reason_code == "APPROVAL_REQUIRED"


@pytest.mark.parametrize("current", [None, "", "adv1:" + "b" * 64])
def test_activation_with_another_definition_is_refused(state, current):
    session = FakeSession()
    with pytest.raises(state.InvalidTransition) as err:
        state.transition_binding(session, "b1", "pending_approval", "active", **{**GOOD, "current_digest": current})
    assert err.value.reason_code == "BINDING_DIGEST_MISMATCH"
    assert session.updates == []


def test_the_refusal_does_not_carry_either_digest(state):
    other = "adv1:" + "b" * 64
    with pytest.raises(state.InvalidTransition) as err:
        state.transition_binding(FakeSession(), "b1", "pending_approval", "active", **{**GOOD, "current_digest": other})
    assert DIGEST not in str(err.value) and other not in str(err.value)


def test_runtime_domain_binding_is_denied_unless_allowed(state):
    row = ("runtime_domain_binding", DIGEST)
    session = FakeSession(row=row)
    with pytest.raises(state.InvalidTransition) as err:
        state.transition_binding(session, "b1", "pending_approval", "active", **GOOD)
    assert err.value.reason_code == "DENIED_BINDING"
    assert session.updates == []
    allowed = FakeSession(row=row)
    assert state.transition_binding(allowed, "b1", "pending_approval", "active",
                                    allow_runtime_domain_binding=True, **GOOD) is True


def test_the_allow_flag_does_not_excuse_a_digest_mismatch(state):
    with pytest.raises(state.InvalidTransition) as err:
        state.transition_binding(FakeSession(row=("runtime_domain_binding", DIGEST)), "b1", "pending_approval",
                                 "active", allow_runtime_domain_binding=True, **{**GOOD, "current_digest": "x"})
    assert err.value.reason_code == "BINDING_DIGEST_MISMATCH"


def test_activating_a_binding_that_does_not_exist_is_a_lost_race(state):
    session = FakeSession(row=None)
    assert state.transition_binding(session, "gone", "pending_approval", "active", **GOOD) is False
    assert session.updates == []


@pytest.mark.parametrize("target", ["suspended", "retiring", "retired"])
def test_leaving_use_needs_no_approval_or_digest(state, target):
    session = FakeSession()
    assert state.transition_binding(session, "b1", "active" if target != "retired" else "retiring", target,
                                    actor_user_id=7) is True
    assert not [s for s in session.statements if s.is_select]


# --- the statement ------------------------------------------------------------------------

@pytest.mark.parametrize("expected,target", sorted(ALLOWED))
def test_revocation_epoch_rises_only_when_entering_suspended_retiring_or_retired_or_rebinding(state, expected, target):
    session = FakeSession()
    state.transition_binding(session, "b1", expected, target, **GOOD)
    sql, _ = compiled(session.updates[0])
    assert ("revocation_epoch=" in set_clause(sql).replace(" ", "")) == (target in ENTERING_REVOKED or (expected, target) in REBINDS)


def test_the_epoch_is_incremented_in_sql_not_read_modified_written(state):
    session = FakeSession()
    state.transition_binding(session, "b1", "active", "suspended", actor_user_id=7)
    sql, params = compiled(session.updates[0])
    assert "revocation_epoch=(tenant.agent_identity_binding.revocation_epoch + " in set_clause(sql)
    assert 1 in params.values()


def test_one_conditional_update_guarded_by_id_and_expected_state(state):
    session = FakeSession()
    state.transition_binding(session, "b1", "active", "suspended", actor_user_id=7)
    assert len(session.statements) == 1
    sql, params = compiled(session.updates[0])
    where = sql.split(" WHERE ", 1)[1]
    assert "agent_identity_binding.id = " in where and "agent_identity_binding.state = " in where
    assert "b1" in params.values() and "active" in params.values() and "suspended" in params.values()
    assert 7 in params.values()
    assert "updated_at" in set_clause(sql) and "state_changed_by" in set_clause(sql)


def test_approval_and_policy_are_written_only_when_given(state):
    session = FakeSession()
    state.transition_binding(session, "b1", "active", "suspended", actor_user_id=7)
    sql, _ = compiled(session.updates[0])
    assert "approval_ref" not in sql and "policy_revision" not in sql
    state.transition_binding(session, "b1", "pending_approval", "active", **GOOD)
    sql, params = compiled(session.updates[1])
    assert "approval_ref" in set_clause(sql) and "policy_revision" in set_clause(sql)
    assert "appr-1" in params.values() and "rev-3" in params.values()


@pytest.mark.parametrize("rowcount,expected", [(1, True), (0, False)])
def test_the_result_is_whether_this_call_won(state, rowcount, expected):
    session = FakeSession(rowcount=rowcount)
    assert state.transition_binding(session, "b1", "active", "suspended", actor_user_id=7) is expected


def test_a_stale_expected_state_changes_nothing_and_reports_false(state):
    """Another writer moved the row first: the WHERE matches no row, rowcount is 0, the caller re-reads."""
    session = FakeSession(rowcount=0)
    assert state.transition_binding(session, "b1", "pending_approval", "active", **GOOD) is False


# --- rebind (#7105) -----------------------------------------------------------------------------

NEW_DIGEST = "adv1:" + "c" * 64


@pytest.mark.parametrize("expected", ["active", "suspended"])
def test_a_rebind_stores_the_new_digest_clears_the_approval_and_raises_the_epoch(state, expected):
    session = FakeSession()
    assert state.transition_binding(
        session, "b1", expected, "pending_approval", actor_user_id=9, current_digest=NEW_DIGEST) is True
    [update] = session.updates
    sql, params = compiled(update)
    clause = set_clause(sql)
    assert "definition_digest" in clause and NEW_DIGEST in params.values()
    assert "approval_ref" in clause and "policy_revision" in clause
    assert params["approval_ref"] is None and params["policy_revision"] is None
    assert "revocation_epoch=(tenant.agent_identity_binding.revocation_epoch + " in clause.replace("  ", " ")
    assert 9 in params.values()
    assert not [s for s in session.statements if s.is_select]


@pytest.mark.parametrize("expected", ["active", "suspended"])
@pytest.mark.parametrize("digest", [None, ""])
def test_a_rebind_needs_the_current_digest(state, expected, digest):
    session = FakeSession()
    with pytest.raises(state.InvalidTransition) as err:
        state.transition_binding(session, "b1", expected, "pending_approval", actor_user_id=9, current_digest=digest)
    assert err.value.reason_code == "DIGEST_REQUIRED" and session.statements == []


def test_submitting_does_not_touch_the_digest_or_the_epoch(state):
    session = FakeSession()
    state.transition_binding(session, "b1", "provisioning", "pending_approval", actor_user_id=9)
    sql, _ = compiled(session.updates[0])
    assert "definition_digest" not in sql and "revocation_epoch" not in set_clause(sql)


def test_the_helpers_agree_with_the_table(state):
    assert {pair for pair in state.TRANSITIONS if state.is_rebind(*pair)} == REBINDS
    assert {pair for pair in state.TRANSITIONS if state.bumps_epoch(*pair)} == (
        {pair for pair in ALLOWED if pair[1] in ENTERING_REVOKED} | REBINDS)


# --- self-approval (#7105) --------------------------------------------------------------------------

@pytest.mark.parametrize("created_by, changed_by, actor, refused", [
    (7, None, 7, True),          # the creator
    (3, 7, 7, True),             # whoever submitted it (last changed its state)
    (7, 7, 7, True),
    (3, 4, 7, False),            # a third operator
    (None, None, 7, False),      # rows made before the columns were filled
])
def test_only_another_operator_may_approve(state, created_by, changed_by, actor, refused):
    session = FakeSession(row=("dedicated_agent", DIGEST, created_by, changed_by))
    call = lambda: state.transition_binding(  # noqa: E731
        session, "b1", "pending_approval", "active", **{**GOOD, "actor_user_id": actor})
    if refused:
        with pytest.raises(state.InvalidTransition) as err:
            call()
        assert err.value.reason_code == "SELF_APPROVAL" and session.updates == []
    else:
        assert call() is True


def test_an_unidentified_operator_cannot_approve(state):
    session = FakeSession()
    with pytest.raises(state.InvalidTransition) as err:
        state.transition_binding(session, "b1", "pending_approval", "active", **{**GOOD, "actor_user_id": 0})
    assert err.value.reason_code == "SELF_APPROVAL" and session.updates == []


def test_the_statement_repeats_the_rule_so_a_change_between_read_and_write_cannot_slip_by(state):
    session = FakeSession()
    state.transition_binding(session, "b1", "pending_approval", "active", **GOOD)
    sql, _ = compiled(session.updates[0])
    where = sql.split(" WHERE ", 1)[1]
    assert "created_by IS DISTINCT FROM" in where and "state_changed_by IS DISTINCT FROM" in where


def test_resuming_is_not_an_approval(state):
    """A suspended binding comes back with the approval it already has, by whoever suspended it."""
    session = FakeSession(row=("dedicated_agent", DIGEST, 7, 7))
    assert state.transition_binding(session, "b1", "suspended", "active", **GOOD) is True
    sql, _ = compiled(session.updates[0])
    assert "DISTINCT FROM" not in sql


# --- only an approval writes the approval (#7105 review) ----------------------------------------------

@pytest.mark.parametrize("expected, target", sorted(ALLOWED - {("pending_approval", "active")}))
def test_no_other_change_writes_the_approval_columns_even_when_it_is_handed_some(state, expected, target):
    session = FakeSession()
    state.transition_binding(session, "b1", expected, target, **{**GOOD, "approval_ref": "EVIL", "policy_revision": "EVIL"})
    sql, params = compiled(session.updates[0])
    if (expected, target) in REBINDS:   # a rebind clears them
        assert params["approval_ref"] is None and params["policy_revision"] is None
    else:
        assert "approval_ref" not in sql and "policy_revision" not in sql
        assert "EVIL" not in params.values()


def test_an_approval_writes_exactly_what_the_approver_gave(state):
    session = FakeSession()
    state.transition_binding(session, "b1", "pending_approval", "active", **GOOD)
    _, params = compiled(session.updates[0])
    assert params["approval_ref"] == "appr-1" and params["policy_revision"] == "rev-3"
