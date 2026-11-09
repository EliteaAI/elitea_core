"""#7404: allowed_actions on webhook rows follows the owner matrix of plan-b §4.2.

Only the owner edits; a schedules admin pauses, resumes, deletes, rotates the secret and changes the owner.
After an owner change (`disabled_reason='owner_changed'`) only the new owner may switch the row on again.
Cron rows keep the EL-7011 matrix (covered by test_7011_schedule_access.py; one guard test here).
"""
import types

import pytest
from unit.test_7011_schedule_access import FakeAuth, _scope, load  # noqa: F401 -- `load` is a fixture

OWNER, ADMIN, EDITOR = 5, 9, 7
EDITOR_VERBS = ("list", "details", "create", "update", "delete", "run")


def _hook(owner=OWNER, enabled=True, reason=None):
    return types.SimpleNamespace(created_by=owner, enabled=enabled, version_id=10, trigger_kind="webhook",
                                 disabled_reason=reason)


@pytest.fixture
def access(load):
    return load(FakeAuth())


# #7455: the owner may run a webhook with a sample body
def test_7404_owner_gets_run_edit_pause_rotate_delete(access):
    actions = access.allowed_actions(_hook(), _scope(access, OWNER, *EDITOR_VERBS))
    assert actions == ["run", "edit", "pause", "rotate_secret", "delete"]


def test_7404_admin_on_another_owners_webhook_cannot_edit(access):
    actions = access.allowed_actions(_hook(), _scope(access, ADMIN, *EDITOR_VERBS, "admin"))
    assert actions == ["pause", "rotate_secret", "delete", "change_owner"]


def test_7404_editor_sees_nothing_on_another_owners_webhook(access):
    assert access.allowed_actions(_hook(), _scope(access, EDITOR, *EDITOR_VERBS)) == []


# An owner change pauses the row; the admin who made it must not be the one to switch it back on
@pytest.mark.parametrize("user_id, names, can_resume", [
    (OWNER, EDITOR_VERBS, True),
    (ADMIN, (*EDITOR_VERBS, "admin"), False),
])
def test_7404_resume_after_owner_change_is_for_the_owner_only(access, user_id, names, can_resume):
    actions = access.allowed_actions(_hook(enabled=False, reason="owner_changed"), _scope(access, user_id, *names))
    assert ("resume" in actions) is can_resume


def test_7404_admin_may_resume_a_webhook_paused_for_another_reason(access):
    actions = access.allowed_actions(_hook(enabled=False), _scope(access, ADMIN, *EDITOR_VERBS, "admin"))
    assert "resume" in actions


def test_7404_cron_rows_keep_the_schedule_matrix(access):
    row = types.SimpleNamespace(created_by=OWNER, enabled=True, version_id=10, trigger_kind="cron")
    actions = access.allowed_actions(row, _scope(access, ADMIN, *EDITOR_VERBS, "admin"))
    assert actions == ["edit", "pause", "delete", "change_owner"]
