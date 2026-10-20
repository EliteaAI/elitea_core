"""C-1009 (#6947): autonomous mandate lifecycle - the table, the conditional UPDATEs, constraints, immutability.

The statements run on SQLite files (see ``fixtures/mandate_env.py``); the PostgreSQL DDL is compiled and read.
"""
import itertools
import sys
import types
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import mandate_env  # noqa: E402  pylint: disable=C0413
from fixtures.mandate_env import NOW, OWNER, add_mandate, mandate_row  # noqa: E402  pylint: disable=C0413

STATES = ("draft", "pending_approval", "active", "suspended", "expired", "revoked")
# Written out here on purpose: the table in the module must equal this reading of the design.
ALLOWED = {
    ("draft", "pending_approval"), ("pending_approval", "draft"), ("pending_approval", "active"),
    ("active", "suspended"), ("suspended", "active"), ("active", "expired"), ("suspended", "expired"),
    ("draft", "revoked"), ("pending_approval", "revoked"), ("active", "revoked"), ("suspended", "revoked"),
}
LEAVING_ACTIVE = {"suspended", "expired", "revoked"}
CREATOR, APPROVER = 3, 1
GOOD = dict(actor=APPROVER)


@pytest.fixture(scope="module")
def ns(tmp_path_factory):
    saved = dict(sys.modules)
    yield mandate_env.install(PLUGIN_ROOT, tmp_path_factory.mktemp("mandate_state"))
    sys.modules.clear()
    sys.modules.update(saved)


@pytest.fixture
def env(ns):
    return mandate_env.prepare(ns, ns.env)


def move(ns, env, mandate_id, expected, target, **kwargs):
    with env.get_session(1) as session:
        changed = ns.mandate_state.transition_mandate(session, mandate_id, expected, target, **{**GOOD, **kwargs})
        session.commit()
    return changed


def approve(ns, env, mandate_id, **kwargs):
    values = dict(actor=APPROVER, approval_ref="CHG-9", policy_revision="rev-9")
    with env.get_session(1) as session:
        changed = ns.mandate_state.approve_mandate(session, mandate_id, **{**values, **kwargs})
        session.commit()
    return changed


# --- the table ------------------------------------------------------------------------------

def test_the_table_is_exactly_the_designed_one(ns):
    assert set(ns.mandate_state.TRANSITIONS) == ALLOWED
    assert set(ns.mandate_state.BUMPS_EPOCH) == LEAVING_ACTIVE


@pytest.mark.parametrize("final", ["expired", "revoked"])
def test_nothing_leaves_a_final_state(ns, final):
    assert not [pair for pair in ns.mandate_state.TRANSITIONS if pair[0] == final]


def test_the_validity_ceiling_is_180_days(ns):
    assert ns.mandate_state.MAX_MANDATE_VALIDITY_DAYS == 180


class FakeSession:
    """Answers the UPDATE with ``rowcount``; keeps every statement."""

    def __init__(self, rowcount=1):
        self.rowcount, self.statements = rowcount, []

    def execute(self, statement):
        self.statements.append(statement)
        return types.SimpleNamespace(rowcount=self.rowcount)


@pytest.mark.parametrize("expected,target", list(itertools.product(STATES, STATES)))
def test_full_matrix(ns, expected, target):
    session = FakeSession()
    if (expected, target) == ("pending_approval", "active"):
        with pytest.raises(ns.state.InvalidTransition) as err:
            ns.mandate_state.transition_mandate(session, "m1", expected, target, **GOOD)
        assert err.value.reason_code == "APPROVAL_REQUIRED"
    elif (expected, target) in ALLOWED:
        assert ns.mandate_state.transition_mandate(session, "m1", expected, target, **GOOD) is True
        assert len(session.statements) == 1
    else:
        with pytest.raises(ns.state.InvalidTransition) as err:
            ns.mandate_state.transition_mandate(session, "m1", expected, target, **GOOD)
        assert err.value.reason_code == "INVALID_TRANSITION"
    if (expected, target) not in ALLOWED or (expected, target) == ("pending_approval", "active"):
        assert session.statements == []


def test_a_lost_race_is_false_not_an_error(ns):
    assert ns.mandate_state.transition_mandate(FakeSession(rowcount=0), "m1", "active", "suspended", **GOOD) is False


@pytest.mark.parametrize("reason", ["", "owner", "OWNER", None])
def test_only_known_suspension_reasons_are_accepted(ns, reason):
    session = FakeSession()
    if reason in (None, ""):
        assert ns.mandate_state.transition_mandate(session, "m1", "active", "suspended", reason=reason, **GOOD)
        return
    with pytest.raises(ns.state.InvalidTransition) as err:
        ns.mandate_state.transition_mandate(session, "m1", "active", "suspended", reason=reason, **GOOD)
    assert err.value.reason_code == "INVALID_REASON" and session.statements == []


@pytest.mark.parametrize("missing", ["approval_ref", "policy_revision"])
@pytest.mark.parametrize("value", [None, ""])
def test_approval_needs_an_approval_and_a_policy_revision(ns, missing, value):
    session = FakeSession()
    with pytest.raises(ns.state.InvalidTransition) as err:
        ns.mandate_state.approve_mandate(session, "m1", **{"actor": 1, "approval_ref": "a", "policy_revision": "p",
                                                           missing: value})
    assert err.value.reason_code == "APPROVAL_REQUIRED" and session.statements == []


def test_no_state_function_commits(ns):
    class Strict(FakeSession):
        def commit(self):
            raise AssertionError("must not commit")

        rollback = commit

    session = Strict()
    ns.mandate_state.transition_mandate(session, "m1", "active", "suspended", **GOOD)
    ns.mandate_state.approve_mandate(session, "m1", actor=1, approval_ref="a", policy_revision="p")
    ns.mandate_state.expire_if_due(session, "m1")


# --- transitions on real rows --------------------------------------------------------------------

@pytest.mark.parametrize("expected,target", sorted(ALLOWED - {("pending_approval", "active")}))
def test_every_allowed_move_changes_the_row_and_bumps_the_epoch_only_when_leaving_active(ns, env, expected, target):
    mandate_id = add_mandate(env, ns, state=expected, epoch=4,
                             suspended_reason="MANUAL" if expected == "suspended" else None)
    assert move(ns, env, mandate_id, expected, target) is True
    row = mandate_row(env, ns, mandate_id)
    assert row.state == target and row.state_changed_by == APPROVER
    assert row.revocation_epoch == (5 if target in LEAVING_ACTIVE else 4)


def test_a_mandate_in_another_state_is_left_alone(ns, env):
    mandate_id = add_mandate(env, ns, state="draft")
    assert move(ns, env, mandate_id, "active", "suspended") is False
    row = mandate_row(env, ns, mandate_id)
    assert (row.state, row.revocation_epoch, row.state_changed_by) == ("draft", 0, None)


def test_a_missing_mandate_is_false(ns, env):
    assert move(ns, env, uuid.uuid4(), "active", "suspended") is False


def test_suspending_records_the_reason_and_resuming_clears_it(ns, env):
    mandate_id = add_mandate(env, ns, state="active")
    assert move(ns, env, mandate_id, "active", "suspended") is True
    assert mandate_row(env, ns, mandate_id).suspended_reason == "MANUAL"
    assert move(ns, env, mandate_id, "suspended", "active") is True
    assert mandate_row(env, ns, mandate_id).suspended_reason is None
    assert move(ns, env, mandate_id, "active", "suspended", reason="OWNER_SUSPENDED") is True
    assert mandate_row(env, ns, mandate_id).suspended_reason == "OWNER_SUSPENDED"


def test_the_owner_cannot_resume_their_own_mandate(ns, env):
    mandate_id = add_mandate(env, ns, state="suspended", suspended_reason="MANUAL")
    assert move(ns, env, mandate_id, "suspended", "active", actor=OWNER) is False
    assert mandate_row(env, ns, mandate_id).state == "suspended"
    assert move(ns, env, mandate_id, "suspended", "active", actor=APPROVER) is True


def test_a_mandate_past_its_validity_cannot_be_resumed(ns, env):
    mandate_id = add_mandate(env, ns, state="suspended", suspended_reason="MANUAL", valid_until=NOW - timedelta(seconds=1))
    assert move(ns, env, mandate_id, "suspended", "active") is False
    assert mandate_row(env, ns, mandate_id).state == "suspended"


def test_a_revoked_mandate_cannot_be_revived(ns, env):
    mandate_id = add_mandate(env, ns, state="active")
    assert move(ns, env, mandate_id, "active", "revoked") is True
    with pytest.raises(ns.state.InvalidTransition):
        move(ns, env, mandate_id, "revoked", "active")
    assert mandate_row(env, ns, mandate_id).state == "revoked"


# --- approval ------------------------------------------------------------------------------------

def test_approval_activates_and_records_who_and_under_which_policy(ns, env):
    mandate_id = add_mandate(env, ns, state="pending_approval")
    assert approve(ns, env, mandate_id) is True
    row = mandate_row(env, ns, mandate_id)
    assert (row.state, row.approved_by, row.approval_ref, row.policy_revision) == ("active", APPROVER, "CHG-9", "rev-9")
    assert row.approved_at is not None and row.revocation_epoch == 0


def test_the_creator_cannot_approve(ns, env):
    mandate_id = add_mandate(env, ns, state="pending_approval", created_by=CREATOR)
    assert approve(ns, env, mandate_id, actor=CREATOR) is False
    row = mandate_row(env, ns, mandate_id)
    assert (row.state, row.approved_by) == ("pending_approval", None)


def test_the_owner_cannot_approve(ns, env):
    mandate_id = add_mandate(env, ns, state="pending_approval")
    assert approve(ns, env, mandate_id, actor=OWNER) is False
    assert mandate_row(env, ns, mandate_id).state == "pending_approval"


def test_a_mandate_past_its_validity_cannot_be_approved(ns, env):
    mandate_id = add_mandate(env, ns, state="pending_approval", valid_until=NOW)
    assert approve(ns, env, mandate_id) is False
    assert mandate_row(env, ns, mandate_id).state == "pending_approval"


def test_approval_is_once(ns, env):
    mandate_id = add_mandate(env, ns, state="pending_approval")
    assert approve(ns, env, mandate_id) is True
    assert approve(ns, env, mandate_id, actor=9, approval_ref="CHG-10") is False
    row = mandate_row(env, ns, mandate_id)
    assert (row.approved_by, row.approval_ref) == (APPROVER, "CHG-9")


@pytest.mark.parametrize("state", ["draft", "active", "suspended", "expired", "revoked"])
def test_only_a_pending_mandate_can_be_approved(ns, env, state):
    mandate_id = add_mandate(env, ns, state=state, suspended_reason="MANUAL" if state == "suspended" else None,
                             approved_by=7 if state in ("expired", "revoked") else None)
    before = mandate_row(env, ns, mandate_id).approved_by
    assert approve(ns, env, mandate_id) is False
    assert mandate_row(env, ns, mandate_id).approved_by == before


# --- expiry ---------------------------------------------------------------------------------------

def expire(ns, env, mandate_id, now):
    with env.get_session(1) as session:
        changed = ns.mandate_state.expire_if_due(session, mandate_id, now=now)
        session.commit()
    return changed


@pytest.mark.parametrize("state", ["active", "suspended"])
def test_the_boundary_belongs_to_expired(ns, env, state):
    until = NOW
    mandate_id = add_mandate(env, ns, state=state, valid_until=until, epoch=2,
                             suspended_reason="MANUAL" if state == "suspended" else None)
    assert expire(ns, env, mandate_id, until - timedelta(microseconds=1)) is False
    assert mandate_row(env, ns, mandate_id).state == state
    assert expire(ns, env, mandate_id, until) is True
    row = mandate_row(env, ns, mandate_id)
    assert (row.state, row.revocation_epoch, row.suspended_reason) == ("expired", 3, None)


def test_expiry_is_once_and_only_for_live_use(ns, env):
    mandate_id = add_mandate(env, ns, state="active", valid_until=NOW - timedelta(days=1))
    assert expire(ns, env, mandate_id, NOW) is True
    assert expire(ns, env, mandate_id, NOW) is False
    assert mandate_row(env, ns, mandate_id).revocation_epoch == 1
    for state in ("draft", "pending_approval", "revoked"):
        other = add_mandate(env, ns, state=state, version=71 + len(state), valid_until=NOW - timedelta(days=1),
                            approved_by=8 if state == "revoked" else None)
        assert expire(ns, env, other, NOW) is False
        assert mandate_row(env, ns, other).state == state


def test_expiry_defaults_to_the_clock(ns, env):
    mandate_id = add_mandate(env, ns, state="active", valid_until=NOW)
    with env.get_session(1) as session:
        assert ns.mandate_state.expire_if_due(session, mandate_id) is True


# --- owner suspension ---------------------------------------------------------------------------

def test_an_inactive_owner_loses_only_the_active_mandates(ns, env):
    active = add_mandate(env, ns, state="active", version=70, epoch=1)
    other_version = add_mandate(env, ns, state="active", version=71)
    pending = add_mandate(env, ns, state="pending_approval", version=72)
    suspended = add_mandate(env, ns, state="suspended", suspended_reason="MANUAL", version=73)
    someone_else = add_mandate(env, ns, state="active", version=74, owner=6)
    with env.get_session(1) as session:
        moved = ns.mandate_state.suspend_owner_mandates(session, OWNER)
        session.commit()
    assert set(moved) == {active, other_version}
    row = mandate_row(env, ns, active)
    assert (row.state, row.suspended_reason, row.revocation_epoch) == ("suspended", "OWNER_SUSPENDED", 2)
    assert mandate_row(env, ns, pending).state == "pending_approval"
    assert (mandate_row(env, ns, suspended).suspended_reason, mandate_row(env, ns, suspended).revocation_epoch) == ("MANUAL", 0)
    assert mandate_row(env, ns, someone_else).state == "active"
    with env.get_session(1) as session:
        assert ns.mandate_state.suspend_owner_mandates(session, OWNER) == []


# --- revisions and the live-unique index --------------------------------------------------------

def test_the_next_revision_counts_every_mandate_of_the_version(ns, env):
    with env.get_session(1) as session:
        assert ns.mandate_state.next_revision(session, 1, 70) == 1
    add_mandate(env, ns, state="revoked", revision=1, approved_by=8)
    add_mandate(env, ns, state="revoked", revision=2, approved_by=8)
    add_mandate(env, ns, state="draft", revision=3, version=71)
    with env.get_session(1) as session:
        assert ns.mandate_state.next_revision(session, 1, 70) == 3
        assert ns.mandate_state.next_revision(session, 2, 70) == 1


@pytest.mark.parametrize("first", ["draft", "pending_approval", "active", "suspended"])
def test_a_second_live_mandate_for_the_version_is_refused(ns, env, first):
    add_mandate(env, ns, state=first, suspended_reason="MANUAL" if first == "suspended" else None)
    with pytest.raises(ns.sa.exc.IntegrityError):
        add_mandate(env, ns, state="draft", revision=2)


def test_a_new_revision_is_accepted_once_the_old_one_is_not_live(ns, env):
    old = add_mandate(env, ns, state="active")
    assert move(ns, env, old, "active", "revoked") is True
    add_mandate(env, ns, state="draft", revision=2)
    add_mandate(env, ns, state="draft", revision=3, version=71)   # another version is independent
    add_mandate(env, ns, state="draft", revision=1, project_id=2)  # and so is another project (own schema)


def test_a_revision_number_is_not_reused(ns, env):
    add_mandate(env, ns, state="revoked", revision=1, approved_by=8)
    with pytest.raises(ns.sa.exc.IntegrityError):
        add_mandate(env, ns, state="draft", revision=1)


# --- constraints ----------------------------------------------------------------------------------

@pytest.mark.parametrize("over", [
    dict(state="active", approved_by=None),                       # set below: add_mandate fills it, so patched
    dict(state="draft", created_by=3, approved_by=3),             # approver = creator
    dict(state="draft", approved_by=OWNER),                       # approver = owner
    dict(operations=()),                                          # no operation
    dict(runtime=59), dict(runtime=86401),
    dict(state="paused"),
    dict(state="suspended", suspended_reason="BORED", approved_by=1),
])
def test_the_table_refuses_what_the_design_forbids(ns, env, over):
    if over == dict(state="active", approved_by=None):
        row = ns.Mandate(id=uuid.uuid4(), project_id=1, application_id=7, agent_version=70,
                         agent_binding_ref=uuid.uuid4(), owner_user_id=5, owner_user_ref={}, allowed_operations=["run:schedule"],
                         allowed_targets=["*"], max_runtime_s=600, valid_until=NOW, state="active", created_by=3, revision=1)
        with ns.sa_orm.Session(env.engine(1)) as session, pytest.raises(ns.sa.exc.IntegrityError):
            session.add(row)
            session.commit()
        return
    with pytest.raises(ns.sa.exc.IntegrityError):
        add_mandate(env, ns, **over)


@pytest.mark.parametrize("over", [dict(runtime=60), dict(runtime=86400), dict(state="suspended", suspended_reason="OWNER_SUSPENDED")])
def test_the_edges_of_the_ranges_are_accepted(ns, env, over):
    add_mandate(env, ns, **over)


def test_the_postgresql_ddl_matches_the_design(ns):
    dialect = ns.postgresql.dialect()
    table = ns.Mandate.__table__
    ddl = str(ns.sa.schema.CreateTable(table).compile(dialect=dialect))
    assert "CONSTRAINT autonomous_mandate_revision_uc UNIQUE (project_id, agent_version, revision)" in ddl
    assert "CHECK (state IN ('draft', 'pending_approval', 'active', 'suspended', 'expired', 'revoked'))" in ddl
    assert ("CHECK (state NOT IN ('active', 'suspended') OR "
            "(approved_by IS NOT NULL AND approval_ref IS NOT NULL AND policy_revision IS NOT NULL))") in ddl
    assert "CHECK (approved_by IS NULL OR (approved_by <> created_by AND approved_by <> owner_user_id))" in ddl
    assert "CHECK (jsonb_array_length(allowed_operations) >= 1)" in ddl
    assert "CHECK (max_runtime_s BETWEEN 60 AND 86400)" in ddl
    assert "state VARCHAR(24) DEFAULT 'draft' NOT NULL" in ddl and "revocation_epoch BIGINT DEFAULT '0' NOT NULL" in ddl
    assert "valid_until TIMESTAMP WITH TIME ZONE NOT NULL" in ddl
    indexes = {str(ns.sa.schema.CreateIndex(index).compile(dialect=dialect)).strip() for index in table.indexes}
    assert indexes == {
        "CREATE UNIQUE INDEX uq_autonomous_mandate_live_version ON tenant.autonomous_mandate (project_id, agent_version) "
        "WHERE state IN ('draft', 'pending_approval', 'active', 'suspended')",
        "CREATE INDEX ix_autonomous_mandate_owner_state ON tenant.autonomous_mandate (owner_user_id, state)",
    }


def test_the_row_holds_references_and_limits_only(ns):
    columns = set(ns.Mandate.__table__.columns.keys())
    assert not [name for name in columns if any(word in name for word in ("token", "cookie", "secret", "password", "key"))]


# --- immutability ---------------------------------------------------------------------------------

CHANGES = {
    "id": lambda row: uuid.uuid4(), "project_id": lambda row: 9, "application_id": lambda row: 9,
    "agent_version": lambda row: 9, "agent_binding_ref": lambda row: uuid.uuid4(), "owner_user_id": lambda row: 99,
    "owner_user_ref": lambda row: {"user_id": 99}, "allowed_operations": lambda row: ["run:webhook"],
    "allowed_targets": lambda row: ["other"], "max_runtime_s": lambda row: 61,
    "valid_until": lambda row: row.valid_until + timedelta(days=1), "revision": lambda row: 9, "created_by": lambda row: 99,
    "created_at": lambda row: row.created_at + timedelta(days=1),
}


def test_every_immutable_column_is_covered(ns):
    models = sys.modules[ns.Mandate.__module__]
    assert set(CHANGES) == set(models.MANDATE_IMMUTABLE)


@pytest.mark.parametrize("column", sorted(CHANGES))
def test_an_immutable_column_cannot_be_changed_through_the_orm(ns, env, column):
    mandate_id = add_mandate(env, ns, state="draft")
    models = sys.modules[ns.Mandate.__module__]
    with ns.sa_orm.Session(env.engine(1)) as session:
        row = session.get(ns.Mandate, mandate_id)
        setattr(row, column, CHANGES[column](row))
        with pytest.raises(models.ImmutableFieldChanged):
            session.flush()


def test_the_mutable_columns_can_be_changed(ns, env):
    mandate_id = add_mandate(env, ns, state="pending_approval")
    with ns.sa_orm.Session(env.engine(1)) as session:
        row = session.get(ns.Mandate, mandate_id)
        row.state, row.suspended_reason, row.approval_ref, row.policy_revision = "active", "MANUAL", "CHG-2", "rev-2"
        row.approved_by, row.state_changed_by, row.revocation_epoch = 1, 1, 1
        session.commit()
    assert mandate_row(env, ns, mandate_id).approved_by == 1


def test_the_approver_is_set_once(ns, env):
    mandate_id = add_mandate(env, ns, state="active", approved_by=1)
    models = sys.modules[ns.Mandate.__module__]
    with ns.sa_orm.Session(env.engine(1)) as session:
        session.get(ns.Mandate, mandate_id).approved_by = 2
        with pytest.raises(models.ImmutableFieldChanged):
            session.flush()
