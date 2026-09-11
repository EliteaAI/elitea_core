"""Platform schedules whose cadence is owned by the elitea_core admin config.

The scheduling plugin stores cron/active on a DB row, but for these two
schedules the plugin configuration is authoritative: it is re-pushed onto the
row at every boot and reconfig. The bindings below tell the Admin Portal where
each row is actually edited, so the Schedules tab can present them read-only.
"""

from croniter import croniter

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


# Mirrors the defaults in admin_schema.json for the fields above. The Runtime
# screen shows these whenever the stored config is missing or null, so
# reconciliation has to push the same values or the screens disagree.
SCHEDULER_CONFIG_DEFAULTS = {
    'index_scheduling': {'cron': '* * * * *', 'enabled': True},
    'pipeline_scheduling': {'cron': '* * * * *', 'enabled': True},
}


# The handler each managed schedule is supposed to call. Used to pick the
# canonical row when duplicates exist, so a hand-made row cannot win on age.
SCHEDULER_CONFIG_RPC_FUNCS = {
    'index_scheduling': 'applications_check_index_scheduling',
    'pipeline_scheduling': 'pipelines_check_scheduling',
}


def advertised_bindings(active):
    """What to hand the scheduling plugin when it rebuilds its registry.

    Nothing at all once these rows have been released: that plugin asks again
    on its own ready(), so answering from the static constant would re-lock
    rows this one has already admitted it cannot drive.
    """
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
    """Fall back to the default for a cron the schedule row would reject.

    Config predates any validation of this field, so a stored value can be
    unusable. Passing it on makes ScheduleModelPD raise inside ready(), which
    pylon swallows whole -- the platform would come up missing everything that
    runs after the schedule bootstrap, with nothing in the log to say why.
    """
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
    """Fall back to the default for anything that is not a real boolean.

    Python truthiness would read a persisted string "false" as enabled, which
    silently starts a schedule an operator meant to stop. The admin save path
    does not enforce the declared type unless a field carries a value_schema,
    so a raw override or a direct API call can put one here.
    """
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
    """Config is operator-editable, so a scalar can appear where a block goes."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    log.error("scheduler config for %s is not a mapping; ignoring it", name)
    return {}


def build_scheduler_sync_plan(scheduler_cfg):
    """Pair each managed schedule with the cron/active its config asks for.

    Config that is silent about a schedule falls back to the schema default
    rather than being skipped. The Runtime screen already displays that default
    for a missing or null value, and a save of an unchanged value is a no-op --
    so skipping here would leave the two screens showing different values with
    no way to reconcile them from either one.
    """
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
