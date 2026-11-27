"""The project setting "Allow Run without asking" of Schedules (EL-7011, owner CR 2026-10-21).

One `schedule_settings` record per project in the configurations plugin, section `project_settings`,
beside the other Settings > Project General values. The Schedules API reads and writes it.
"""
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt
from pylon.core.tools import log

SETTINGS_TYPE = "schedule_settings"
RPC_TIMEOUT = 5
OWNER = "elitea_core"
MANAGE_URL = "/api/v2/elitea_core/schedules/{mode}/{project_id}/settings"


class ScheduleProjectSettings(BaseModel):
    """The Schedules values of one project; the configurations plugin validates each write with this model."""

    model_config = ConfigDict(extra="forbid", json_schema_extra={"metadata": {
        "label": "Schedules", "section": "project_settings", "type": SETTINGS_TYPE,
    }})

    allow_run_without_asking: StrictBool = Field(
        description='True lets schedules use the "Run without asking" approval rule: their runs approve '
                    'sensitive actions by themselves. False makes those runs pause and ask.',
        examples=[True],
    )
    # The platform maximum is checked by the settings PUT: this model loads without the plugin config
    webhook_max_concurrent_runs: StrictInt | None = Field(
        None, ge=1, description='Running runs one webhook of the project may have at the same time; '
                                'null uses the platform default. At most the platform maximum.',
        examples=[2],
    )


def _rpc():
    from tools import rpc_tools  # pylint: disable=E0401,C0415 -- late: the model loads without the platform
    return rpc_tools.RpcMixin().rpc.timeout(RPC_TIMEOUT)


def register_schedule_settings_type() -> None:
    """Register the record type as owned by elitea_core; a failure is logged, the plugin still starts.

    The owner makes the configurations REST API refuse writes of the record, so only the schedules
    settings PUT (schedules admin) changes it. Any failure, a configurations plugin from before owned
    types included, leaves the type unregistered: without the owner the generic REST API would let
    anyone with configuration write access set the value.
    """
    try:
        _rpc().configurations_register(type_name=SETTINGS_TYPE, section="project_settings",
                                       model=ScheduleProjectSettings, replace=True,
                                       managed_by=OWNER, manage_url=MANAGE_URL)
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the plugin still starts; an old plugin's TypeError ends here too
        log.exception("Could not register the %s configuration type; it stays unregistered. "
                      "Deploy configurations with owned settings types (managed_by, owner) before elitea_core",
                      SETTINGS_TYPE)


def _request_cache() -> dict | None:
    """The per-request answers by project id; None outside a request, where nothing is cached."""
    try:
        from flask import g, has_request_context  # late: the model loads without the web layer
    except ImportError:
        return None
    return g.setdefault("schedule_run_without_asking", {}) if has_request_context() else None


def _read_settings(project_id: int) -> ScheduleProjectSettings | None:
    """The saved record of the project, None when never saved; a store error propagates."""
    config = _rpc().configurations_get_first_filtered_project(
        project_id=project_id, filter_fields={"type": SETTINGS_TYPE, "elitea_title": SETTINGS_TYPE},
    )
    return None if config is None else ScheduleProjectSettings.model_validate(config["data"])


def read_saved_settings(project_id: int) -> dict:
    """The stored keys of the project's record, {} when never saved; a store error propagates."""
    config = _rpc().configurations_get_first_filtered_project(
        project_id=project_id, filter_fields={"type": SETTINGS_TYPE, "elitea_title": SETTINGS_TYPE},
    )
    return dict(config["data"]) if config is not None else {}


def saved_webhook_cap(project_id: int) -> int | None:
    """The project's own per-webhook value for the settings form; None when unset or the store fails."""
    try:
        settings = _read_settings(project_id)
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the form then shows the platform default
        log.exception("Could not read the %s of project %s", SETTINGS_TYPE, project_id)
        return None
    return settings.webhook_max_concurrent_runs if settings is not None else None


def project_webhook_cap(project_id: int, limits) -> int:
    """The running-run cap of one webhook of the project: its lower value, else the platform default; never over the maximum."""
    try:
        settings = _read_settings(project_id)
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the platform default still bounds the runs
        log.exception("Could not read the %s of project %s", SETTINGS_TYPE, project_id)
        return limits.per_webhook
    value = settings.webhook_max_concurrent_runs if settings is not None else None
    return min(value or limits.per_webhook, limits.per_webhook_limit)


def is_run_without_asking_allowed(project_id: int) -> bool:
    """True when the project allows the "Run without asking" rule; false when never saved.

    A read error or a broken record refuses the rule (fail closed). A request reads the store once per
    project; a failed read is not kept, so the next call tries again.
    """
    cache = _request_cache()
    if cache is not None and project_id in cache:
        return cache[project_id]
    try:
        settings = _read_settings(project_id)
        allowed = settings is not None and settings.allow_run_without_asking
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the rule stays off when the store fails
        log.exception("Could not read the %s of project %s", SETTINGS_TYPE, project_id)
        return False
    if cache is not None:
        cache[project_id] = allowed
    return allowed


def _owned(call, **kwargs):
    """Call a configurations write as the owner; a plugin from before owned types raises TypeError, and it propagates."""
    return call(**kwargs, owner=OWNER)


def save_schedule_settings(project_id: int, settings: ScheduleProjectSettings) -> dict:
    """Create or replace the project's record and answer the saved values; a store error propagates."""
    data = settings.model_dump(exclude_none=True)  # an unset webhook cap is left out, not stored as null
    rpc = _rpc()
    config, created = _owned(rpc.configurations_create_if_not_exists, payload={
        "project_id": project_id, "elitea_title": SETTINGS_TYPE, "label": "Schedules",
        "type": SETTINGS_TYPE, "data": data,
    })
    if not created:
        _owned(rpc.configurations_update, project_id=project_id, config_id=config["id"], payload={"data": data})
    cache = _request_cache()
    if cache is not None:
        cache.pop(project_id, None)  # a read after the save in this request sees the new value
    return data
