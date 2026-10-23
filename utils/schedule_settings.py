"""The project setting "Allow Run without asking" of Schedules (EL-7011, owner CR 2026-10-21).

One `schedule_settings` record per project in the configurations plugin, section `project_settings`,
beside the other Settings > Project General values. The Schedules API reads and writes it.
"""
from pydantic import BaseModel, ConfigDict, Field, StrictBool
from pylon.core.tools import log

SETTINGS_TYPE = "schedule_settings"
RPC_TIMEOUT = 5
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


def _rpc():
    from tools import rpc_tools  # pylint: disable=E0401,C0415 -- late: the model loads without the platform
    return rpc_tools.RpcMixin().rpc.timeout(RPC_TIMEOUT)


def register_schedule_settings_type() -> None:
    """Register the record type as owned by elitea_core; a failure is logged, the plugin still starts.

    The owner makes the configurations REST API refuse writes of the record, so only the schedules
    settings PUT (schedules admin) changes it. A configurations plugin from before owned types raises
    TypeError for the unknown `managed_by` argument; only then the type is registered without the
    owner arguments, unguarded as before. Any other error (a timeout, a failed call) never drops the
    guard: it is logged and the type stays unregistered or as the first call left it.
    """
    plain = {"type_name": SETTINGS_TYPE, "section": "project_settings", "model": ScheduleProjectSettings,
             "replace": True}
    try:
        _rpc().configurations_register(**plain, managed_by="elitea_core", manage_url=MANAGE_URL)
        return
    except TypeError as error:
        if "managed_by" not in str(error):
            log.exception("Could not register the %s configuration type", SETTINGS_TYPE)
            return
        log.warning("The configurations plugin has no owner arguments (%s); registering %s unguarded",
                    error, SETTINGS_TYPE)
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the guard must stay; the plugin still starts
        log.exception("Could not register the %s configuration type", SETTINGS_TYPE)
        return
    try:
        _rpc().configurations_register(**plain)
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- writes then fail with an unknown type
        log.exception("Could not register the %s configuration type", SETTINGS_TYPE)


def is_run_without_asking_allowed(project_id: int) -> bool:
    """True when the project allows the "Run without asking" rule; false when never saved.

    A read error or a broken record refuses the rule (fail closed).
    """
    try:
        config = _rpc().configurations_get_first_filtered_project(
            project_id=project_id, filter_fields={"type": SETTINGS_TYPE, "elitea_title": SETTINGS_TYPE},
        )
        return config is not None and ScheduleProjectSettings.model_validate(config["data"]).allow_run_without_asking
    except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the rule stays off when the store fails
        log.exception("Could not read the %s of project %s", SETTINGS_TYPE, project_id)
        return False


def save_schedule_settings(project_id: int, settings: ScheduleProjectSettings) -> dict:
    """Create or replace the project's record and answer the saved values; a store error propagates."""
    data = settings.model_dump()
    rpc = _rpc()
    config, created = rpc.configurations_create_if_not_exists(payload={
        "project_id": project_id, "elitea_title": SETTINGS_TYPE, "label": "Schedules",
        "type": SETTINGS_TYPE, "data": data,
    })
    if not created:
        rpc.configurations_update(project_id=project_id, config_id=config["id"], payload={"data": data})
    return data
