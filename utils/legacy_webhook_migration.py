"""One-way migration of legacy pipeline webhook triggers to webhook rows (#7402)."""
import re

from pylon.core.tools import log
from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from .legacy_schedule_migration import (
    LOCK_WAIT, LOCK_WAIT_RESET, NAME_LIMIT, PROJECT_LOCK, _unlock_project, parse_task_params,
)
from .pipeline_trigger import WEBHOOK_TYPE_CONFIG, is_gitlab_signing_mode
from .schedule_prerun import ACCESS_CHECK_UNAVAILABLE, check_owner_access

__all__ = ['parse_task_params', 'migrate_project']

SECRET_MISSING = 'secret_missing'
OWNER_MISSING = 'owner_missing'
OWNER_LOST_ACCESS = 'owner_lost_access'
UNKNOWN_TYPE = 'unknown_webhook_type'
REF_RE = re.compile(r'\{\{secret\.([\w.-]+)\}\}')


class LegacyWebhookError(Exception):
    """The version cannot be migrated: it keeps its legacy trigger and is reported."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def ref_key(ref) -> str | None:
    """The vault key of a `{{secret.key}}` reference, None for anything else."""
    match = REF_RE.fullmatch(ref.strip()) if isinstance(ref, str) else None
    return match.group(1) if match else None


def trigger_keys(trigger: dict) -> set[str]:
    """Every vault key a legacy trigger refers to (the inactive GitLab one too)."""
    keys = {ref_key(trigger.get(name)) for name in ('webhook_secret', 'webhook_signing_secret')}
    return keys - {None}


def active_ref(trigger: dict) -> str | None:
    signing = is_gitlab_signing_mode(trigger.get('webhook_type'), trigger.get('gitlab_auth_method'))
    return trigger.get('webhook_signing_secret' if signing else 'webhook_secret')


def secret_value(trigger: dict, visible: dict) -> str:
    """The stored value of the active secret, verbatim (the door normalizes it as the old route did)."""
    value = visible.get(ref_key(active_ref(trigger)) or '')
    if not value:
        raise LegacyWebhookError(SECRET_MISSING)
    return value


def migrated_row(application_id: int, version_id: int, pipeline_name: str, version_name: str,
                 trigger: dict) -> dict:
    """EntitySchedule values for one legacy webhook trigger; raises LegacyWebhookError when none can be made."""
    if trigger.get('webhook_type') not in WEBHOOK_TYPE_CONFIG:
        raise LegacyWebhookError(UNKNOWN_TYPE)
    try:
        owner = int(trigger.get('created_by'))
    except (TypeError, ValueError):
        raise LegacyWebhookError(OWNER_MISSING) from None
    if owner <= 0:
        raise LegacyWebhookError(OWNER_MISSING)
    gitlab = trigger['webhook_type'] == 'gitlab'
    return {
        'entity_type': 'pipeline', 'application_id': application_id, 'version_id': version_id,
        'name': f'{pipeline_name} ({version_name})'[:NAME_LIMIT], 'description': None,
        'trigger_kind': 'webhook', 'webhook_type': trigger['webhook_type'],
        # Rows need a method on GitLab; a legacy trigger without one used the secret token
        'gitlab_auth_method': (trigger.get('gitlab_auth_method') or 'secret_token') if gitlab else None,
        'run_mode': 'entry_point', 'prompt': None, 'variables': [],
        'enabled': True, 'approval_rule': 'pause_and_ask',
        'created_by': owner, 'updated_by': owner, 'principal': {'kind': 'delegation', 'ref': owner},
        'legacy_version_id': version_id,
    }


def removable_keys(migrated_keys: set[str], remaining_triggers: list[dict]) -> tuple[set[str], set[str]]:
    """(removed, kept): an old key is removed only when no trigger left in the project refers to it."""
    still_used = set().union(*(trigger_keys(t) for t in remaining_triggers)) if remaining_triggers else set()
    return migrated_keys - still_used, migrated_keys & still_used


def _legacy_versions(session) -> list:
    """(version, pipeline name) of every pipeline version with a legacy webhook trigger, by version id."""
    from ..models.all import Application, ApplicationVersion
    from ..models.enums.all import AgentTypes

    return session.execute(
        select(ApplicationVersion, Application.name)
        .join(Application, Application.id == ApplicationVersion.application_id)
        .where(
            ApplicationVersion.agent_type == AgentTypes.pipeline.value,
            ApplicationVersion.pipeline_settings.op('->')('trigger').op('->>')('type') == 'webhook',
        )
        .order_by(ApplicationVersion.id)
    ).all()


def _migrated_ids(session) -> set[int]:
    from ..models.entity_schedule import EntitySchedule

    return set(session.scalars(select(EntitySchedule.legacy_version_id).where(
        EntitySchedule.legacy_version_id.is_not(None))))


def _drop_legacy_trigger(version) -> None:
    version.pipeline_settings = {**(version.pipeline_settings or {}), 'trigger': {'type': 'chat_message'}}
    flag_modified(version, 'pipeline_settings')


def migrate_version(session, project_id: int, version, row: dict, value: str) -> None:
    """Add the row with its own hidden secret and drop the version trigger in one commit; the store is restored on failure."""
    from ..models.entity_schedule import EntitySchedule
    from .webhook_secrets import _commit_or_restore, _locked_store, _ref, current_key, is_signing_row

    try:
        schedule = EntitySchedule(**row)
        session.add(schedule)
        session.flush()  # the uuid default is applied on insert
        key = current_key(schedule)
        if is_signing_row(schedule):
            schedule.signing_secret_ref = _ref(key)
        else:
            schedule.secret_ref = _ref(key)
        _drop_legacy_trigger(version)
        client, stored = _locked_store(session, project_id)
    except Exception:
        session.rollback()
        raise
    _commit_or_restore(session, project_id, client, stored, {key: value})


def _remove_old_keys(project_id: int, keys: set[str]) -> None:
    from tools import VaultClient

    client = VaultClient(project_id)
    if hasattr(client, 'update_secrets'):
        client.update_secrets(remove=sorted(keys))
        return
    visible = client.get_secrets() or {}
    client.set_secrets({key: value for key, value in visible.items() if key not in keys})


def _migrate_versions(session, project_id: int, dry_run: bool) -> dict:
    """The project report; each version moves alone, a failed one keeps its trigger and is logged."""
    from tools import VaultClient

    report = {'project_id': project_id, 'found': 0, 'migrated': [], 'skipped': [], 'failed': [],
              'old_secrets_removed': 0, 'old_secrets_kept': 0}
    visible = VaultClient(project_id).get_secrets() or {}
    done = _migrated_ids(session)
    remaining, migrated_keys = [], set()
    for version, pipeline_name in _legacy_versions(session):
        version_id, trigger = version.id, (version.pipeline_settings or {}).get('trigger') or {}
        report['found'] += 1
        try:
            if version_id in done:  # migrated before: only the stale trigger goes
                if not dry_run:
                    _drop_legacy_trigger(version)
                    session.commit()
                report['skipped'].append(version_id)
                migrated_keys |= trigger_keys(trigger)
                continue
            row = migrated_row(version.application_id, version_id, pipeline_name, version.name, trigger)
            value = secret_value(trigger, visible)
            verdict = check_owner_access(project_id, row['created_by'])
            if verdict:
                raise LegacyWebhookError(ACCESS_CHECK_UNAVAILABLE if verdict == ACCESS_CHECK_UNAVAILABLE
                                         else OWNER_LOST_ACCESS)
            if not dry_run:
                migrate_version(session, project_id, version, row, value)
        except Exception as error:  # noqa: BLE001 -- one bad version must not stop the project
            reason = error.reason if isinstance(error, LegacyWebhookError) else type(error).__name__
            log.error('migrate_legacy_pipeline_webhooks: project %s version %s not migrated: %s',
                      project_id, version_id, reason)
            report['failed'].append({'version_id': version_id, 'reason': reason})
            remaining.append(trigger)
            continue
        report['migrated'].append(version_id)
        migrated_keys |= trigger_keys(trigger)
    removed, kept = removable_keys(migrated_keys, remaining)
    removed &= set(visible)
    report['old_secrets_removed'], report['old_secrets_kept'] = len(removed), len(kept)
    if removed and not dry_run:
        try:
            _remove_old_keys(project_id, removed)
        except Exception as error:  # noqa: BLE001 -- the rows are committed; the keys are only redundant
            log.error('migrate_legacy_pipeline_webhooks: project %s old secrets not removed: %s',
                      project_id, type(error).__name__)
            report['old_secrets_removed'] = 0
    return report


def migrate_project(project_id: int, dry_run: bool) -> dict:
    """Migrate one project under the legacy advisory lock (shared with the schedule migration); the project report."""
    from tools import db

    from .schedule_claim import legacy_lock_key

    key = {'key': legacy_lock_key(project_id)}
    with db.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
        connection.execute(LOCK_WAIT)
        try:
            connection.execute(PROJECT_LOCK, key)
        finally:
            connection.execute(LOCK_WAIT_RESET)
        try:
            with db.get_session(project_id) as session:
                return _migrate_versions(session, project_id, dry_run)
        finally:
            _unlock_project(connection, key, project_id)
