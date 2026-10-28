"""Platform schedules whose cadence is owned by the elitea_core admin config."""

from croniter import croniter
import sqlalchemy as sa

from pylon.core.tools import log

SCHEDULER_CONFIG_BINDINGS = {
    'index_scheduling': {
        'section': 'runtime',
        'fields': ['index_scheduling_enabled', 'index_scheduling_cron'],
    },
    'pipeline_scheduling': {
        'section': 'runtime',
        'fields': ['pipeline_scheduling_enabled', 'pipeline_scheduling_cron'],
    },
}


SCHEDULER_CONFIG_DEFAULTS = {
    'index_scheduling': {'cron': '* * * * *', 'enabled': True},
    'pipeline_scheduling': {'cron': '* * * * *', 'enabled': True},
}


# The pipeline row keeps the handler name its stored scheduler row already has, so the sync finds that
# row and creates no second one; the schedule tick registers this name beside its own
SCHEDULER_CONFIG_RPC_FUNCS = {
    'index_scheduling': 'applications_check_index_scheduling',
    'pipeline_scheduling': 'pipelines_check_scheduling',
}


# Handlers that earlier builds stored on a managed row, per row name. A stack that ran such a build keeps
# that row beside the one the boot call matches by handler, so the tick would run from two rows.
LEGACY_RPC_FUNCS = {
    'pipeline_scheduling': 'entity_schedules_check',
}

# A table without a schema, so the session's schema_translate_map puts it in the platform schema; raw SQL
# would depend on the connection's search_path instead
_SCHEDULE = sa.Table(
    'schedule', sa.MetaData(),
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('name', sa.String),
    sa.Column('project_id', sa.Integer),
    sa.Column('rpc_func', sa.String),
    sa.Column('active', sa.Boolean),
)


def rebind_legacy_rows(session):
    """Point the oldest stray-handler platform row at the managed handler and switch the other stray rows off.

    No stray row is rebound when a managed row exists, so the tick runs from one row only.
    """
    for name, old in LEGACY_RPC_FUNCS.items():
        new = SCHEDULER_CONFIG_RPC_FUNCS[name]
        rows = session.execute(
            sa.select(_SCHEDULE.c.id, _SCHEDULE.c.rpc_func)
            .where(_SCHEDULE.c.name == name, _SCHEDULE.c.project_id.is_(None),
                   _SCHEDULE.c.rpc_func.in_([old, new]))
            .order_by(_SCHEDULE.c.id)
        ).all()
        stray = [row.id for row in rows if row.rpc_func == old]
        if not stray:
            continue
        if not any(row.rpc_func == new for row in rows):
            session.execute(sa.update(_SCHEDULE).where(_SCHEDULE.c.id == stray[0]).values(rpc_func=new))
            log.info("scheduler row %s %s: handler %s -> %s", name, stray[0], old, new)
            stray = stray[1:]
        if stray:
            parked = session.execute(
                sa.update(_SCHEDULE).where(_SCHEDULE.c.id.in_(stray), _SCHEDULE.c.active.is_(True))
                .values(active=False)
            ).rowcount
            log.info("scheduler rows %s %s: %s switched off, handler %s", name, stray, parked, old)
    session.commit()


def advertised_bindings(active):
    if not active:
        return {}
    return {
        name: {
            'managed_by': managed_by,
            'rpc_func': SCHEDULER_CONFIG_RPC_FUNCS[name],
        }
        for name, managed_by in SCHEDULER_CONFIG_BINDINGS.items()
    }


def _usable_cron(name, cron, default):
    if cron is None:
        return default
    if isinstance(cron, str) and croniter.is_valid(cron):
        return cron
    log.error(
        "scheduler config for %s holds an unusable cron; "
        "falling back to %r until it is corrected in Configuration -> Runtime",
        name, default,
    )
    return default


def _usable_flag(name, enabled, default):
    if enabled is None:
        return default
    if isinstance(enabled, bool):
        return enabled
    log.error(
        "scheduler config for %s holds a non-boolean enabled flag; "
        "falling back to %r until it is corrected in Configuration -> Runtime",
        name, default,
    )
    return default


def _as_mapping(name, value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    log.error("scheduler config for %s is not a mapping; ignoring it", name)
    return {}


def build_scheduler_sync_plan(scheduler_cfg):
    """Pair each managed schedule with the cron/active its config asks for."""
    scheduler_cfg = _as_mapping('scheduler', scheduler_cfg)
    plan = []
    for name, managed_by in SCHEDULER_CONFIG_BINDINGS.items():
        sub = _as_mapping(name, scheduler_cfg.get(name))
        defaults = SCHEDULER_CONFIG_DEFAULTS[name]
        plan.append({
            'name': name,
            'cron': _usable_cron(name, sub.get('cron'), defaults['cron']),
            'active': _usable_flag(name, sub.get('enabled'), defaults['enabled']),
            'rpc_func': SCHEDULER_CONFIG_RPC_FUNCS[name],
            'managed_by': managed_by,
        })
    return plan
