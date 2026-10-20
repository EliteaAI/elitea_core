"""Params and rows of the legacy pipeline schedule migration (EL-7011 US-24)."""
from datetime import UTC, datetime, timedelta

from pydantic import ValidationError
from sqlalchemy import func, select, text

from ..models.pd.index import _cron_gaps
from ..models.pd.pipeline_trigger import PipelineTriggerSchedule
from .schedule_prerun import (
    ACCESS_CHECK_UNAVAILABLE,
    DISABLED_OWNER_LOST_ACCESS,
    check_owner_access,
)
from .schedule_timing import compute_next_run, schedule_limits

DISABLED_INVALID_CRON = 'invalid_cron'
PARAMS_FORMAT = 'project_id= is required. Format: project_id=<all|N>[;dry_run=false]'
NAME_LIMIT = 256  # entity_schedules.name is String(256)
# Blocking, on the key the tick try-locks: the tick skips the project while it moves
PROJECT_LOCK = text('SELECT pg_advisory_lock(:key)')
PROJECT_UNLOCK = text('SELECT pg_advisory_unlock(:key)')


class LegacyTriggerError(Exception):
    """The trigger cannot become a row: the version keeps its legacy trigger and is reported."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def parse_task_params(raw: str) -> tuple[tuple, bool]:
    """((kind, value) of project_id, dry_run) of "project_id=<all|N>[;dry_run=false]"; a dry run by default."""
    # Imported here: llm_migration_utils pulls the model and RPC graph into every importer
    from .llm_migration_utils import _parse_bool, parse_project_id_spec

    params = {key.strip(): value.strip() for key, _, value in (part.partition('=') for part in raw.split(';'))}
    try:
        spec = parse_project_id_spec(params.get('project_id', ''))
    except ValueError as error:
        raise ValueError(PARAMS_FORMAT) from error
    return spec, _parse_bool(params.get('dry_run', 'true'), 'dry_run')


def below_minimum(cron: str) -> bool:
    """True when the cron fires closer than the schedule minimum interval (reported, never blocking)."""
    return min(_cron_gaps(cron)) < timedelta(minutes=schedule_limits().min_interval_minutes)


def _last_run(value) -> datetime | None:
    try:
        moment = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def migrated_row(
    project_id: int, application_id: int, version_id: int, pipeline_name: str, version_name: str,
    trigger: dict, now: datetime,
) -> dict:
    """EntitySchedule values for one legacy schedule trigger; `now` must be timezone-aware.

    Raises LegacyTriggerError when no row can be made: no usable creator (the row needs one) or the
    owner check could not answer.
    """
    cron, zone = str(trigger.get('cron') or '').strip(), str(trigger.get('timezone') or '')
    reason = None
    try:
        PipelineTriggerSchedule.model_validate(trigger)
    except ValidationError as error:
        failed = {problem['loc'][0] for problem in error.errors()}
        if 'created_by' in failed:
            raise LegacyTriggerError(DISABLED_OWNER_LOST_ACCESS) from error
        if failed & {'cron', 'timezone'}:
            reason = DISABLED_INVALID_CRON
    owner = int(trigger['created_by'])
    if reason is None:
        verdict = check_owner_access(project_id, owner)
        if verdict == ACCESS_CHECK_UNAVAILABLE:
            raise LegacyTriggerError(verdict)
        reason = DISABLED_OWNER_LOST_ACCESS if verdict else None
    return {
        'entity_type': 'pipeline', 'application_id': application_id, 'version_id': version_id,
        'name': f'{pipeline_name} ({version_name})'[:NAME_LIMIT], 'description': None,
        'cron': cron, 'timezone': zone, 'run_mode': 'entry_point', 'prompt': None, 'variables': [],
        'enabled': reason is None, 'disabled_reason': reason, 'created_by': owner,
        'principal': {'kind': 'delegation', 'ref': owner}, 'last_run': _last_run(trigger.get('last_run')),
        'next_run': compute_next_run(cron, zone, now) if reason is None else None,
    }


def migrate_version(session, version, row: dict) -> None:
    """Add the EntitySchedule row and set the version trigger to chat_message in one commit.

    On any error both are rolled back and the error is raised again for the caller's report.
    """
    # Imported here: the model module needs the plugin db package, which the row unit tests do not load
    from ..models.entity_schedule import EntitySchedule

    try:
        session.add(EntitySchedule(**row))
        version.pipeline_settings = {**(version.pipeline_settings or {}), 'trigger': {'type': 'chat_message'}}
        session.commit()
    except Exception:
        session.rollback()
        raise


def _legacy_versions(session) -> list:
    """(version, pipeline name) of every pipeline version with a legacy schedule trigger, by version id.

    The filter of the legacy check (`rpc/pipeline_scheduling.py` `_check_project_pipelines`).
    """
    from ..models.all import Application, ApplicationVersion
    from ..models.enums.all import AgentTypes

    return session.execute(
        select(ApplicationVersion, Application.name)
        .join(Application, Application.id == ApplicationVersion.application_id)
        .where(
            ApplicationVersion.agent_type == AgentTypes.pipeline.value,
            ApplicationVersion.pipeline_settings.op('->')('trigger').op('->>')('type') == 'schedule',
        )
        .order_by(ApplicationVersion.id)
    ).all()


def _migrate_versions(session, project_id: int, dry_run: bool) -> dict:
    """The project report; each version moves alone, a failed one goes under `errors` and keeps its trigger."""
    from ..models.entity_schedule import EntitySchedule

    report = {
        'project_id': project_id, 'found': 0, 'migrated_enabled': 0, 'migrated_disabled': [],
        'below_minimum_interval': [], 'above_project_cap': False, 'errors': [], 'original_triggers': [],
    }
    enabled_before = session.scalar(
        select(func.count()).select_from(EntitySchedule).where(EntitySchedule.enabled.is_(True))
    )
    now = datetime.now(UTC)
    for version, pipeline_name in _legacy_versions(session):
        version_id, trigger = version.id, version.pipeline_settings['trigger']
        report['found'] += 1
        report['original_triggers'].append({'version_id': version_id, 'trigger': trigger})
        try:
            row = migrated_row(project_id, version.application_id, version_id, pipeline_name, version.name,
                               trigger, now)
            if not dry_run:
                migrate_version(session, version, row)
        except Exception as error:  # noqa: BLE001 -- one bad version must not stop the project
            report['errors'].append({'version_id': version_id, 'error': str(error)})
            continue
        if row['enabled']:
            report['migrated_enabled'] += 1
        else:
            report['migrated_disabled'].append(
                {'version_id': version_id, 'created_by': row['created_by'], 'reason': row['disabled_reason']}
            )
        if row['disabled_reason'] != DISABLED_INVALID_CRON and below_minimum(row['cron']):
            report['below_minimum_interval'].append({'version_id': version_id, 'cron': row['cron']})
    cap = schedule_limits().max_enabled_per_project
    report['above_project_cap'] = enabled_before + report['migrated_enabled'] > cap
    return report


def migrate_project(project_id: int, dry_run: bool) -> dict:
    """Move every legacy schedule trigger of one project while holding its advisory lock; the project report.

    A dry run writes nothing. The lock is released when the project ends or fails; a failure is raised.
    """
    # Imported here, as above: tools.db and the claim helpers need the running plugin
    from tools import db

    from .schedule_claim import legacy_lock_key

    key = {'key': legacy_lock_key(project_id)}
    # A session lock is freed only by the connection that took it; autocommit keeps no transaction open
    with db.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
        connection.execute(PROJECT_LOCK, key)
        try:
            with db.get_session(project_id) as session:
                return _migrate_versions(session, project_id, dry_run)
        finally:
            try:
                connection.execute(PROJECT_UNLOCK, key)
            except Exception:
                connection.invalidate()  # closing the server session frees the lock for the tick
                raise
