"""Move the legacy index schedules of `elitea_tools.meta.indexes_meta` into `entity_schedules` rows, and back."""
from datetime import UTC, datetime, timedelta

from pydantic import ValidationError
from pylon.core.tools import log
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.attributes import flag_modified

from ..models.pd.index import ToolkitIndexingSchedule
from .legacy_schedule_migration import (
    DISABLED_INVALID_CRON,
    NAME_LIMIT,
    LegacyTriggerError,
    _last_run,
    below_minimum,
    migrate_project,
)
from .orphaned_index_schedules import find_orphans
from .schedule_prerun import (
    ACCESS_CHECK_UNAVAILABLE,
    DISABLED_OWNER_LOST_ACCESS,
    check_owner_access,
    check_owner_rights,
)
from .schedule_timing import compute_next_run, schedule_limits
from .trigger_principal import TEAM_SCHEDULE_KEY, index_schedule_owner

TOOLKIT_INDEX = 'toolkit_index'
INDEX_LOOKUP_FAILED = 'index_lookup_failed'
INDEX_ALREADY_SCHEDULED = 'index_already_scheduled'
CREDENTIAL_REVIEW = 'credential_review'
EXPIRED = 'expired'
FIRST_RUN_DELAY = timedelta(seconds=60)


def _rank_key(item: tuple) -> tuple:
    key, body = item
    moment = _last_run(body.get('last_run')) or datetime.min.replace(tzinfo=UTC)
    return (not body.get('enabled'), key != TEAM_SCHEDULE_KEY, -moment.timestamp())


def choose_schedule(schedules: dict) -> tuple[str, dict, list[str]]:
    """(key, body, merged keys) of the schedule that becomes the row of an index.

    An enabled schedule first, then the team schedule (key -1), then the latest `last_run`.
    """
    ranked = sorted(((str(key), body) for key, body in schedules.items() if isinstance(body, dict)), key=_rank_key)
    key, body = ranked[0]
    return key, body, [other for other, _ in ranked[1:]]


def _credential_title(value) -> tuple | None:
    if not isinstance(value, dict):
        return None
    return value.get('elitea_title') or value.get('alita_title'), bool(value.get('private'))


def _own_credential_differs(toolkit, key: str, body: dict) -> bool:
    """True when a personal schedule carries a credential other than the one saved on the toolkit."""
    own = _credential_title(body.get('credentials'))
    if key == TEAM_SCHEDULE_KEY or own is None:
        return False
    # Late: this module loads the settings helpers of the index scheduler only for a credential check
    from .index_scheduling import resolve_credential_config_key

    settings = toolkit.settings or {}
    slot = resolve_credential_config_key(settings, toolkit.type)
    return own != _credential_title(settings.get(slot)) if slot else True


def _bad_fields(body: dict) -> set:
    try:
        ToolkitIndexingSchedule.model_validate(body)
    except ValidationError as error:
        return {loc for problem in error.errors() for loc in problem['loc'][:1]}
    return set()


def index_row(project_id: int, toolkit, index_name: str, key: str, body: dict, now: datetime) -> dict:
    """EntitySchedule values of one index from its chosen legacy schedule; `now` must be timezone-aware.

    The first reason that applies pauses the row: the owner (lost access or unknown), a bad cron or a cron
    below the minimum interval, an expired schedule, a credential other than the toolkit's, a schedule
    that was disabled (no reason). Raises LegacyTriggerError when no owner can be named or checked.
    """
    cron, zone = str(body.get('cron') or '').strip(), str(body.get('timezone') or '')
    owner = index_schedule_owner(key, body.get('created_by'))
    reason = None
    if owner is None:
        if toolkit.author_id is None:
            raise LegacyTriggerError(DISABLED_OWNER_LOST_ACCESS)
        owner, reason = toolkit.author_id, DISABLED_OWNER_LOST_ACCESS
    else:
        verdict = check_owner_access(project_id, owner) or check_owner_rights(
            project_id, owner, TOOLKIT_INDEX, toolkit.id)
        if verdict == ACCESS_CHECK_UNAVAILABLE:
            raise LegacyTriggerError(verdict)
        reason = DISABLED_OWNER_LOST_ACCESS if verdict else None
    cron_ok = not _bad_fields(body) & {'cron', 'timezone'}
    fast = cron_ok and below_minimum(cron, TOOLKIT_INDEX)
    stop = _last_run(body.get('expires_at'))
    if reason is None and (not cron_ok or fast):
        reason = DISABLED_INVALID_CRON
    if reason is None and (body.get('expired') or (stop is not None and stop <= now)):
        reason = EXPIRED
    if reason is None and _own_credential_differs(toolkit, key, body):
        reason = CREDENTIAL_REVIEW
    enabled = bool(body.get('enabled')) and reason is None
    last_run = _last_run(body.get('last_run'))
    return {
        'entity_type': TOOLKIT_INDEX, 'toolkit_id': toolkit.id, 'index_name': index_name,
        'name': f'{toolkit.name} / {index_name}'[:NAME_LIMIT], 'description': None,
        'cron': cron, 'timezone': zone, 'run_mode': None, 'prompt': None, 'variables': [],
        'enabled': enabled, 'disabled_reason': reason, 'created_by': owner,
        'principal': {'kind': 'delegation', 'ref': owner},
        'last_run': last_run if last_run and last_run.year > 1970 else None,
        'next_run': compute_next_run(cron, zone, now + FIRST_RUN_DELAY) if enabled else None,
        'expires_at': stop, 'original_trigger': None, 'fast': fast,
    }


def pgvector_index_names(project_id: int, toolkit) -> set:
    """Names of the indexes in the vector store of one toolkit; raises when the store cannot be read."""
    # Late: the RPC tools and the vector store helpers load only for a migration
    from tools import rpc_tools

    from .application_tools import _get_pgvector_engine
    from .orphaned_index_schedules import pgvector_settings, present_index_names

    expanded = rpc_tools.RpcMixin().rpc.timeout(5).configurations_expand(
        project_id=project_id, settings=pgvector_settings(toolkit.settings),
        user_id=toolkit.author_id, unsecret=True,
    )
    connection_string = (expanded.get('pgvector_configuration') or {}).get('connection_string')
    if not connection_string:
        raise ValueError('the toolkit has no pgvector connection string')
    return present_index_names(_get_pgvector_engine(connection_string), str(toolkit.id))


def _enabled_count(session) -> int:
    from ..models.entity_schedule import EntitySchedule

    return session.scalar(select(func.count()).select_from(EntitySchedule).where(EntitySchedule.enabled.is_(True)))


def _legacy_toolkits(session) -> list:
    from ..models.all import EliteATool

    # Only the `indexes_meta` JSON of each toolkit is read; the whole row loads for the few that hold a schedule
    rows = session.execute(
        select(EliteATool.id, EliteATool.meta['indexes_meta']).where(EliteATool.meta['indexes_meta'].is_not(None))
        .order_by(EliteATool.id)
    ).all()
    ids = [
        toolkit_id for toolkit_id, indexes_meta in rows
        if any(isinstance(entry, dict) and entry.get('schedules') for entry in (indexes_meta or {}).values())
    ]
    if not ids:
        return []
    return list(session.scalars(select(EliteATool).where(EliteATool.id.in_(ids)).order_by(EliteATool.id)).all())


def _write_index(session, toolkit_id: int, row: dict, index_name: str, keys: list[str]) -> bool:
    """Add the row and drop the moved keys from the toolkit JSON in one commit; False when the index has a row.

    The toolkit row is locked first, so a concurrent save of the same toolkit waits. A losing insert rolls
    back its savepoint only and leaves the JSON as it is.
    """
    from ..models.all import EliteATool
    from ..models.entity_schedule import EntitySchedule
    from .schedule_service import _is_index_conflict
    from .schedule_test_runs import UNIQUE_VIOLATION, error_code

    toolkit = session.execute(select(EliteATool).where(EliteATool.id == toolkit_id).with_for_update()).scalar_one()
    try:
        with session.begin_nested():
            session.add(EntitySchedule(**{name: value for name, value in row.items() if name != 'fast'}))
    except IntegrityError as error:
        if error_code(error) == UNIQUE_VIOLATION and _is_index_conflict(error):
            session.rollback()
            return False
        raise
    meta = dict(toolkit.meta or {})
    indexes_meta = {name: dict(entry) if isinstance(entry, dict) else entry
                    for name, entry in (meta.get('indexes_meta') or {}).items()}
    schedules = dict(indexes_meta[index_name].get('schedules') or {})
    for key in keys:
        schedules.pop(key, None)
    indexes_meta[index_name]['schedules'] = schedules
    toolkit.meta = {**meta, 'indexes_meta': indexes_meta}
    flag_modified(toolkit, 'meta')
    session.commit()
    return True


def _migrate_indexes(session, project_id: int, dry_run: bool, enforce_limits: bool = False,
                     lookup=pgvector_index_names) -> dict:
    """The project report; each index moves alone, a failed one goes under `errors` and keeps its JSON.

    `lookup(project_id, toolkit)` gives the index names of one toolkit, read once per toolkit. When it fails,
    every index of that toolkit is held and keeps its JSON. With `enforce_limits` an enabled row over the
    project cap is held.
    """
    report = {
        'project_id': project_id, 'found': 0, 'migrated_enabled': 0, 'migrated_paused': [],
        'below_minimum_interval': [], 'above_project_cap': False, 'held': [], 'orphaned': [], 'merged': [],
        'errors': [],
    }
    enabled_before = _enabled_count(session)
    cap = schedule_limits().max_enabled_per_project
    now = datetime.now(UTC)
    for toolkit in _legacy_toolkits(session):
        toolkit_id = toolkit.id
        indexes = {name: entry['schedules'] for name, entry in toolkit.meta['indexes_meta'].items()
                   if isinstance(entry, dict) and entry.get('schedules')}
        report['found'] += len(indexes)
        try:
            present = lookup(project_id, toolkit)
        except Exception as error:  # noqa: BLE001 -- an unreadable store must not stop the project
            log.warning('Project %s: toolkit %s: the index lookup failed: %s', project_id, toolkit_id, error)
            report['held'] += [{'toolkit_id': toolkit_id, 'index': name, 'reason': INDEX_LOOKUP_FAILED}
                               for name in indexes]
            continue
        orphans = find_orphans(indexes, present)
        report['orphaned'] += [{'toolkit_id': toolkit_id, 'index': name} for name in orphans]
        for index_name, schedules in indexes.items():
            if index_name in orphans:
                continue
            try:
                key, body, merged = choose_schedule(schedules)
                row = index_row(project_id, toolkit, index_name, key, body, now)
                if enforce_limits and row['enabled'] and enabled_before + report['migrated_enabled'] >= cap:
                    report['held'].append({'toolkit_id': toolkit_id, 'index': index_name, 'reason': 'project_cap'})
                    continue
                row['original_trigger'] = {'schedules': {name: schedules[name] for name in [key, *merged]}}
                if not dry_run and not _write_index(session, toolkit_id, row, index_name, [key, *merged]):
                    report['held'].append(
                        {'toolkit_id': toolkit_id, 'index': index_name, 'reason': INDEX_ALREADY_SCHEDULED})
                    continue
            except Exception as error:  # noqa: BLE001 -- one bad index must not stop the project
                session.rollback()
                log.exception('Project %s: toolkit %s: the index schedule could not be moved', project_id, toolkit_id)
                report['errors'].append({'toolkit_id': toolkit_id, 'index': index_name, 'error': str(error)})
                continue
            if row['enabled']:
                report['migrated_enabled'] += 1
            else:
                report['migrated_paused'].append({'toolkit_id': toolkit_id, 'index': index_name,
                                                  'created_by': row['created_by'], 'reason': row['disabled_reason']})
            if row['fast']:
                report['below_minimum_interval'].append({'toolkit_id': toolkit_id, 'index': index_name,
                                                         'cron': row['cron']})
            if merged:
                report['merged'].append({'toolkit_id': toolkit_id, 'index': index_name, 'kept': key, 'dropped': merged})
    report['above_project_cap'] = enabled_before + report['migrated_enabled'] > cap
    return report


def migrate_index_project(project_id: int, dry_run: bool, enforce_limits: bool = False) -> dict:
    """Move the legacy index schedules of one project under its advisory lock; the project report."""
    return migrate_project(project_id, dry_run, enforce_limits, body=_migrate_indexes)
