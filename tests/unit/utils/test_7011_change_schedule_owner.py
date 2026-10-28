"""EL-7011 US-10 R1.1, R1.2, R1.4: the owner and the principal move together in one commit; state stays."""
import pathlib
import uuid
from datetime import UTC, datetime

import pytest
from fixtures import schedule_sqlite as sqlite_db
from fixtures.helpers import load_module_with_stubs
from unit.test_7011_schedule_crud_api import ADMIN_ID, OTHER_ID, OWNER_ID, PACKAGE
from unit.test_7011_schedule_crud_api import harness as _crud_harness

harness = _crud_harness  # the CRUD harness fixture: real schedule table in SQLite
PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture
def owner(harness):
    load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_prerun.py', f'{PACKAGE}.utils.schedule_prerun')
    return load_module_with_stubs(PLUGIN_ROOT / 'utils/schedule_owner.py', f'{PACKAGE}.utils.schedule_owner')


def change(harness, owner, schedule_id, owner_id, fail_flush=False):
    """Run the owner change on a fresh session; returns (result, commit count)."""
    commits = []
    with harness.session_factory() as session:
        sqlite_db.sa.event.listen(session, 'after_commit', commits.append)
        if fail_flush:
            def fail(*args):
                raise sqlite_db.sa.exc.OperationalError('UPDATE entity_schedules', {}, Exception('db down'))
            sqlite_db.sa.event.listen(session, 'before_flush', fail)
        row = session.get(harness.EntitySchedule, schedule_id)
        return owner.change_schedule_owner(session, row, owner_id, ADMIN_ID), len(commits)


def test_7011_r1_1_r1_2_owner_and_principal_move_in_one_commit(harness, owner):
    """TC-US10-01: created_by and principal.ref both become the new owner; the admin is the editor."""
    schedule_id = harness.seed(created_by=OWNER_ID)
    result, commits = change(harness, owner, schedule_id, OTHER_ID)
    assert result == {'old_owner_id': OWNER_ID, 'new_owner_id': OTHER_ID}
    assert commits == 1
    row = harness.row(schedule_id)
    assert (row.created_by, row.principal, row.updated_by) == (
        OTHER_ID, {'kind': 'delegation', 'ref': OTHER_ID}, ADMIN_ID)


def test_7011_r1_4_paused_schedule_keeps_its_state(harness, owner):
    """TC-US10-02: enabled and disabled_reason stay as they were."""
    schedule_id = harness.seed(created_by=OWNER_ID, enabled=False, disabled_reason='owner_lost_access')
    change(harness, owner, schedule_id, OTHER_ID)
    row = harness.row(schedule_id)
    assert (row.created_by, row.enabled, row.disabled_reason) == (OTHER_ID, False, 'owner_lost_access')


def test_7011_r1_2_failed_write_leaves_both_owner_fields(harness, owner):
    """TC-US10-03: a database error rolls back; neither owner field changes."""
    schedule_id = harness.seed(created_by=OWNER_ID)
    with pytest.raises(sqlite_db.sa.exc.OperationalError):
        change(harness, owner, schedule_id, OTHER_ID, fail_flush=True)
    row = harness.row(schedule_id)
    assert (row.created_by, row.principal) == (OWNER_ID, {'kind': 'delegation', 'ref': OWNER_ID})


def test_7011_r1_1_same_owner_writes_nothing(harness, owner):
    """TC-US10-15: the current owner as new owner returns None and commits nothing."""
    schedule_id = harness.seed(created_by=OWNER_ID)
    assert change(harness, owner, schedule_id, OWNER_ID) == (None, 0)
    assert harness.row(schedule_id).updated_by is None


def test_the_new_owner_starts_with_pause_and_ask_and_no_confirmation(harness, owner):
    """The consent of the old owner never carries over: rule, fingerprint and confirmation are reset."""
    schedule_id = harness.seed(
        created_by=OWNER_ID, approval_rule='run_without_asking', approval_fingerprint='fp-1',
        approval_confirmed_by=OWNER_ID, approval_confirmed_at=datetime.now(UTC))
    change(harness, owner, schedule_id, OTHER_ID)
    row = harness.row(schedule_id)
    assert (row.approval_rule, row.approval_fingerprint, row.approval_confirmed_by, row.approval_confirmed_at) == (
        'pause_and_ask', None, None, None)


def test_owner_change_resets_chat_delivery_to_run_history(harness, owner):
    """The chat target was checked against the old owner: the new owner starts with Run History only."""
    schedule_id = harness.seed(
        created_by=OWNER_ID, delivery_mode='conversation',
        delivery_conversation_uuid=uuid.UUID('11111111-1111-4111-8111-111111111111'), delivery_participant_id=5)
    change(harness, owner, schedule_id, OTHER_ID)
    row = harness.row(schedule_id)
    assert (row.delivery_mode, row.delivery_conversation_uuid, row.delivery_participant_id) == (
        'run_history', None, None)


def test_owner_change_resets_history_count_with_chat_delivery(harness, owner):
    """The history count belongs to the chat target: it goes back to 0 with Run History."""
    schedule_id = harness.seed(
        created_by=OWNER_ID, delivery_mode='conversation', delivery_include_history=7,
        delivery_conversation_uuid=uuid.UUID('11111111-1111-4111-8111-111111111111'), delivery_participant_id=5)
    change(harness, owner, schedule_id, OTHER_ID)
    row = harness.row(schedule_id)
    assert (row.delivery_include_history, row.delivery_mode) == (0, 'run_history')
