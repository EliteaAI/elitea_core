"""Per-row webhook secrets in the project's hidden vault store (#7404)."""
import secrets
from datetime import UTC, datetime, timedelta

from pylon.core.tools import log
from sqlalchemy import func, select, text
from tools import VaultClient

ROTATION_OVERLAP = timedelta(hours=24)
# Orders the read-modify-write of the hidden store between webhook writers of one project
SECRET_LOCK = "SELECT pg_advisory_xact_lock(hashtext('webhook-secrets'), :project_id)"


def secret_key(uuid) -> str:
    return f'webhook_secret_{uuid}'


def signing_key(uuid) -> str:
    return f'webhook_signing_secret_{uuid}'


def prev_key(uuid) -> str:
    return f'webhook_secret_{uuid}_prev'


def _ref(key: str) -> str:
    return f'{{{{secret.{key}}}}}'


def new_secret() -> str:
    return secrets.token_urlsafe(32)


def is_signing_row(row) -> bool:
    return row.webhook_type == 'gitlab' and row.gitlab_auth_method == 'signing_token'


def _current_key(row) -> str:
    return signing_key(row.uuid) if is_signing_row(row) else secret_key(row.uuid)


def _locked_store(session, project_id: int):
    """Take the lock in the caller's transaction, then read the store with a client whose cache is not older."""
    if session.get_bind().dialect.name == 'postgresql':  # SQLite tests have no advisory locks
        session.execute(text(SECRET_LOCK), {'project_id': project_id})
    client = VaultClient(project_id)
    return client, dict(client.get_hidden_secrets() or {})


def _write_hidden(session, project_id: int, changes: dict, client=None, before=None) -> None:
    """Apply `changes` under the lock; a None value pops the key."""
    if client is None:
        client, before = _locked_store(session, project_id)
    add = {key: value for key, value in changes.items() if value is not None}
    remove = [key for key, value in changes.items() if value is None and key in before]
    if not add and not remove:
        return
    # The database and corporate engines merge under a row lock, safe against non-webhook writers too
    if hasattr(client, 'update_hidden_secrets'):
        client.update_hidden_secrets(add=add, remove=remove)
        return
    after = {key: value for key, value in before.items() if key not in remove}
    client.set_hidden_secrets({**after, **add})


def store_row_secret(session, project_id: int, row, value: str | None = None) -> str:
    """Write the first secret of a new row and set its ref; `value` is the pasted GitLab signing token."""
    value = value or new_secret()
    key = _current_key(row)
    _write_hidden(session, project_id, {key: value})
    if is_signing_row(row):
        row.signing_secret_ref = _ref(key)
    else:
        row.secret_ref = _ref(key)
    return value


class RotationInProgress(Exception):
    """A previous secret still works; a second rotate would end its 24 h early."""


def rotate_row_secret(session, project_id: int, row, now: datetime, value: str | None = None,
                      updated_by: int | None = None) -> str:
    """Write a new secret and keep the current one valid for 24 h; commits, or restores the store on failure."""
    value = value or new_secret()
    key = _current_key(row)
    client, stored = _locked_store(session, project_id)
    session.refresh(row)  # under the lock: a rotation that committed meanwhile is seen
    row.updated_by = updated_by if updated_by is not None else row.updated_by
    if row.previous_secret_until is not None and _aware(row.previous_secret_until) > now:
        raise RotationInProgress()
    changes = {key: value}
    if key in stored:
        changes[prev_key(row.uuid)] = stored[key]
        row.previous_secret_ref, row.previous_secret_until = _ref(prev_key(row.uuid)), now + ROTATION_OVERLAP
    else:  # no current value to keep: nothing overlaps
        changes[prev_key(row.uuid)] = None
        row.previous_secret_ref = row.previous_secret_until = None
    _commit_or_restore(session, project_id, client, stored, changes)
    return value


def _commit_or_restore(session, project_id: int, client, stored: dict, changes: dict) -> None:
    """Write the store, then commit the row; a failed commit puts the old values back under the lock."""
    _write_hidden(session, project_id, changes, client, stored)
    try:
        session.commit()
    except Exception:
        session.rollback()
        _restore(session, project_id, stored, changes)
        raise


def _restore(session, project_id: int, stored: dict, changes: dict) -> None:
    """Put back the keys that still hold what this call wrote; a key another writer changed since is left alone."""
    try:
        client, current = _locked_store(session, project_id)
        back = {key: stored.get(key) for key, value in changes.items() if current.get(key) == value}
        _write_hidden(session, project_id, back, client, current)
        session.commit()
    except Exception as e:  # noqa: BLE001 -- the caller re-raises the commit error
        session.rollback()
        log.error('Webhook secret: the store could not be restored after a failed commit: %s', type(e).__name__)


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)  # SQLite drops the zone


def regenerate_row_secret(session, project_id: int, row, value: str | None = None,
                          updated_by: int | None = None) -> str:
    """Replace the secret at once and drop any previous one; commits, or restores the store on failure."""
    value = value or new_secret()
    client, stored = _locked_store(session, project_id)
    session.refresh(row)
    row.updated_by = updated_by if updated_by is not None else row.updated_by
    row.previous_secret_ref = row.previous_secret_until = None
    _commit_or_restore(session, project_id, client, stored, {_current_key(row): value, prev_key(row.uuid): None})
    return value


def application_webhook_uuids(session, application_id: int) -> list:
    """The uuids of an application's webhook rows; read before a delete, whose cascade removes the rows."""
    from ..models.entity_schedule import EntitySchedule  # late: callers load without the model graph
    return list(session.scalars(select(EntitySchedule.uuid).where(
        EntitySchedule.application_id == application_id, EntitySchedule.trigger_kind == 'webhook')))


def drop_secrets_of(session, project_id: int, uuids) -> None:
    """Remove the keys of deleted webhook rows in one write and end the transaction holding the lock."""
    if not uuids:
        return
    changes = {}
    for uuid in uuids:
        changes.update({secret_key(uuid): None, signing_key(uuid): None, prev_key(uuid): None})
    _write_hidden(session, project_id, changes)
    session.commit()


def sweep_expired_previous(session, project_id: int, now: datetime) -> int:
    """Drop the previous secrets whose overlap ended; return how many rows were cleared."""
    from ..models.entity_schedule import EntitySchedule  # late: the tick loads without the model graph
    expired = select(EntitySchedule).where(
        EntitySchedule.previous_secret_until.is_not(None), EntitySchedule.previous_secret_until < now)
    if session.scalar(select(func.count()).select_from(expired.subquery())) == 0:
        return 0  # the common case: no lock, no store read
    client, stored = _locked_store(session, project_id)
    rows = session.scalars(expired.execution_options(populate_existing=True)).all()  # re-read under the lock
    if not rows:
        return 0
    _write_hidden(session, project_id, {prev_key(row.uuid): None for row in rows}, client, stored)
    for row in rows:
        row.previous_secret_ref = row.previous_secret_until = None
    session.commit()
    return len(rows)


def signing_value_error(value: str | None) -> str | None:
    """Why a pasted GitLab signing token is unusable, or None."""
    from .pipeline_trigger import validate_gitlab_signing_token_format  # late: it pulls in the trigger helpers
    return validate_gitlab_signing_token_format(value or '')
