"""Repair of the platform tick row on stacks that ran an interim build of the generic schedule tick.

Such a stack holds a global `pipeline_scheduling` row with handler `entity_schedules_check` beside the row the
boot call matches by `pipelines_check_scheduling`, so the tick is called from two rows each minute.
"""

ROW_NAME = "pipeline_scheduling"
STRAY_HANDLER = "entity_schedules_check"
MANAGED_HANDLER = "pipelines_check_scheduling"


def parse_params(param: str | None) -> bool:
    """``dry_run=false`` -> False; a dry run unless that is spelled out."""
    params = {}
    for part in (param or "").split(";"):
        key, sep, value = part.partition("=")
        if sep:
            params[key.strip().lower()] = value.strip()
    return params.get("dry_run", "true").lower() != "false"


def plan_repair(rows) -> dict:
    """Row ids to rename and to switch off, from the global rows of `ROW_NAME` (objects with id, rpc_func, active).

    The oldest stray row takes the managed handler only when no managed row exists, so one row drives the tick.
    """
    by_age = sorted(rows, key=lambda row: row.id)
    stray = [row for row in by_age if row.rpc_func == STRAY_HANDLER]
    rename = []
    if stray and not any(row.rpc_func == MANAGED_HANDLER for row in by_age):
        rename, stray = [stray[0].id], stray[1:]
    return {
        "found": [row.id for row in by_age if row.rpc_func == STRAY_HANDLER],
        "rename": rename,
        "switch_off": [row.id for row in stray if row.active],
    }


def repair(session, schedule_model, dry_run: bool = True) -> dict:
    """Plan the repair on the rows of `schedule_model` and, unless `dry_run`, write it and commit."""
    rows = (
        session.query(schedule_model)
        .filter(
            schedule_model.name == ROW_NAME,
            schedule_model.project_id.is_(None),
            schedule_model.rpc_func.in_([STRAY_HANDLER, MANAGED_HANDLER]),
        )
        .all()
    )
    plan = plan_repair(rows)
    if dry_run:
        return plan
    for row in rows:
        if row.id in plan["rename"]:
            row.rpc_func = MANAGED_HANDLER  # cron and active flag stay as they are
        elif row.id in plan["switch_off"]:
            row.active = False
    session.commit()
    return plan
