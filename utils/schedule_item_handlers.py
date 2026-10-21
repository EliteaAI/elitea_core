"""Handlers of one schedule (EL-7011, ADR-0004): create, read, edit, delete, pause, resume, run, runs, owner.

`api/v2/schedules.py` serves them; this module registers no route.
"""
from flask import request
from pydantic import BaseModel, Field, ValidationError
from pylon.core.tools import log
from sqlalchemy import func, select
from tools import auth, db

from ..models.entity_schedule import EntityScheduleRun
from ..models.enums.all import NotificationEventTypes
from ..models.pd.entity_schedule import (
    ScheduleCreate,
    ScheduleDetails,
    ScheduleEdit,
    ScheduleUpdate,
)
from ..models.pd.schedule_list import RunStatus
from ..models.pd.schedule_run import ScheduleRunRequest
from .schedule_access import (
    ScheduleScope,
    allowed_actions,
    schedule_check_spec,
    schedule_scope,
)
from .schedule_claim import fail_run, insert_run
from .schedule_dispatch import START_FAILED, start_schedule_run
from .schedule_notifications import notify_schedule_event
from .schedule_service import (
    ScheduleError,
    create_schedule,
    delete_schedule,
    load_schedule_in_scope,
    update_schedule,
)
from .schedule_timing import ScheduleLimitError

MAX_RUNS_LIMIT = 100
RUN_FIELDS = (
    "id", "trigger", "status", "reason", "conversation_id", "requested_by", "started_at", "finished_at",
)
ENABLED_NOT_EDITABLE = {
    "error": "enabled cannot be changed by an edit; use the pause or resume action of the schedule",
    "field": "enabled",
}


class RunsQuery(BaseModel):
    """Query of the run records list; `limit` above the cap is lowered to it (R3.3, R3.5)."""

    limit: int = Field(default=20, ge=1)
    offset: int = Field(default=0, ge=0)
    status: RunStatus | None = None


def _iso(moment) -> str | None:
    return moment.isoformat() if moment else None


def _owner_name(owner_id: int) -> str | None:
    """The owner's name, else the email, as the list shows it; a failed lookup gives None (US-32 R2.2)."""
    try:
        # Late import: the auth lookup is needed only for the details answer
        from .authors import get_authors_data  # pylint: disable=C0415
        user = next(iter(get_authors_data([owner_id])), None) or {}
    except Exception as e:  # noqa: BLE001  # pylint: disable=W0718 -- a name must never break the details
        log.warning("Schedule owner name lookup failed: %s", e)
        return None
    return user.get("name") or user.get("email")


def _details(row, scope: ScheduleScope) -> dict:
    """Shape a row as the API answers it, with the owner's name for the edit page (US-32)."""
    fields = ScheduleDetails.model_fields.keys() - {"owner", "allowed_actions"}
    return ScheduleDetails.model_validate({
        **{name: getattr(row, name) for name in fields},
        "owner": {"id": row.created_by, "name": _owner_name(row.created_by)},
        "allowed_actions": allowed_actions(row, scope),
    }).model_dump(mode="json")


def _refuse_unallowed_rule(project_id: int, body):
    """Answer 400 when the body asks for "run without asking" and the project does not allow it."""
    from .schedule_settings import is_run_without_asking_allowed
    if body.approval_rule == "run_without_asking" and not is_run_without_asking_allowed(project_id):
        return {"error": "approval_rule_not_allowed", "field": "approval_rule"}, 400
    return None


def _saved(row, scope: ScheduleScope, project_id: int) -> dict:
    """Details of a saved row, with every finding of the risk review as the save `warnings` (R2.4)."""
    from .schedule_risk import scan_schedule
    review = scan_schedule(project_id, row.version_id, row.created_by)
    warnings = review["blocking"] + review["sensitive"] + review["notices"]
    return {**_read_answer(row, scope, project_id), "warnings": warnings}


def _read_answer(row, scope: ScheduleScope, project_id: int) -> dict:
    from .schedule_settings import is_run_without_asking_allowed
    return {**_details(row, scope), "run_without_asking_allowed": bool(is_run_without_asking_allowed(project_id))}


def _parse_body(model):
    """The request body as `model`, or the 400 answer of its first error."""
    try:
        return model.model_validate(request.get_json(silent=True)), None
    except ValidationError as e:
        return None, _validation_response(e)


def _validation_response(error: ValidationError) -> tuple[dict, int]:
    """Answer 400 with the first broken field and its plain reason."""
    first = error.errors()[0]
    field = first["loc"][0] if first["loc"] else "body"
    return {"error": first["msg"].removeprefix("Value error, "), "field": field}, 400


def _apply_update(project_id: int, schedule_id: int, body: ScheduleUpdate | ScheduleEdit):
    """Save an edit, a pause or a resume through the service and answer the details.

    The service checks a resume (a version is needed, the project cap), computes the next run, and
    marks the audit span and tells the owner when an admin changes another owner's schedule.
    """
    scope = schedule_scope(project_id)
    with db.get_session(project_id) as session:
        try:
            row = load_schedule_in_scope(session, schedule_id, scope)
            return _saved(update_schedule(session, project_id, row, body, scope), scope, project_id), 200
        except ScheduleError as e:
            return e.response
        except ScheduleLimitError as e:
            return {"error": str(e)}, 409
        except ValidationError as e:  # the stop date does not fit the resulting cron and time zone
            return _validation_response(e)


def _notify_owner_change(project_id: int, row, change: dict) -> None:
    """Tell the old and the new owner, both ids in the meta (R3.2); a failure is logged, the change stays."""
    for user_id in change.values():
        try:
            notify_schedule_event(project_id, user_id, NotificationEventTypes.schedule_owner_changed, row, extra=change)
        except Exception as e:  # noqa: BLE001 -- the change is committed; a lost notice must not fail it
            log.error("Schedule %s: cannot send the owner change notice: %s", row.id, e)


class ScheduleItemHandlers:
    """One schedule. A caller without `schedules.admin` sees only their own schedules; others answer 404.

    Each handler is called with keyword arguments: check_api reads `project_id` from them.
    """

    @auth.decorators.check_api(schedule_check_spec("create"))
    def _create(self, project_id: int):
        """Create a schedule owned by the caller; the owner is never taken from the body.

        Who may call: a project member with `schedules.create`.
        Errors: 400 invalid field or "run without asking" not allowed; 403; 404 agent or pipeline
        not found or not visible; 409 project limit of active schedules.
        """
        body, error = _parse_body(ScheduleCreate)
        if error:
            return error
        refused = _refuse_unallowed_rule(project_id, body)
        if refused:
            return refused
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                row = create_schedule(session, project_id, body, scope)
            except ScheduleError as e:
                return e.response
            except ScheduleLimitError as e:
                return {"error": str(e)}, 409
            return _saved(row, scope, project_id), 201

    @auth.decorators.check_api(schedule_check_spec("details"))
    def _read(self, project_id: int, schedule_id: int):
        """Answer the details of one schedule.

        Who may call: the owner with `schedules.details`, or a schedules admin.
        Errors: 403; 404 not found or another owner's schedule.
        """
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                return _read_answer(load_schedule_in_scope(session, schedule_id, scope), scope, project_id), 200
            except ScheduleError as e:
                return e.response

    @auth.decorators.check_api(schedule_check_spec("update"))
    def _edit(self, project_id: int, schedule_id: int):
        """Change the fields of a schedule; the owner never changes. Pause and resume have their own actions.

        Who may call: the owner with `schedules.update`, or a schedules admin (the owner gets a notice).
        Errors: 400 invalid field, `enabled` in the body, or "run without asking" not allowed; 403; 404.
        """
        raw = request.get_json(silent=True)
        if isinstance(raw, dict) and "enabled" in raw:
            return ENABLED_NOT_EDITABLE, 400
        body, error = _parse_body(ScheduleEdit)
        if error:
            return error
        refused = _refuse_unallowed_rule(project_id, body)
        if refused:
            return refused
        return _apply_update(project_id, schedule_id, body)

    @auth.decorators.check_api(schedule_check_spec("delete"))
    def _delete(self, project_id: int, schedule_id: int):
        """Delete a schedule; the conversations of its runs stay.

        Who may call: the owner with `schedules.delete`, or a schedules admin (the owner gets a notice).
        Errors: 403; 404.
        """
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                delete_schedule(session, project_id, load_schedule_in_scope(session, schedule_id, scope), scope)
            except ScheduleError as e:
                return e.response
        return None, 204

    @auth.decorators.check_api(schedule_check_spec("update"))
    def _pause(self, project_id: int, schedule_id: int):
        """Pause a schedule: no new run starts. A run in progress goes on.

        Who may call: the owner with `schedules.update`, or a schedules admin (the owner gets a notice).
        Errors: 403; 404.
        """
        return _apply_update(project_id, schedule_id, ScheduleUpdate(enabled=False))

    @auth.decorators.check_api(schedule_check_spec("update"))
    def _resume(self, project_id: int, schedule_id: int):
        """Resume a paused schedule and compute its next run.

        Who may call: the owner with `schedules.update`, or a schedules admin (the owner gets a notice).
        Errors: 400 the schedule has no version; 403; 404; 409 project limit of active schedules.
        """
        return _apply_update(project_id, schedule_id, ScheduleUpdate(enabled=True))

    @auth.decorators.check_api(schedule_check_spec("run"))
    def _run(self, project_id: int, schedule_id: int):
        """Start a run now (`manual`) or a test run (`test`) of the caller's own schedule.

        Who may call: the owner with `schedules.run`; an admin sees the schedule but cannot run it.
        Errors: 400 invalid trigger; 403 not the owner; 404; 409 deleted version or an active run
        (the body has `active_run_id`); 500 the run could not start.
        """
        body, error = _parse_body(ScheduleRunRequest)
        if error:
            return error
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                row = load_schedule_in_scope(session, schedule_id, scope)
            except ScheduleError as e:
                return e.response
            if row.created_by != scope.user_id:  # an admin sees the row but never runs it as the owner
                return {"error": "not_owner"}, 403
            if row.version_id is None:
                return {"error": "version_deleted"}, 409
            entity_type = row.entity_type
            run_id = insert_run(session, schedule_id, body.trigger, scope.user_id)
            if run_id is None:
                active_id = session.scalar(select(EntityScheduleRun.id).where(
                    EntityScheduleRun.schedule_id == schedule_id,
                    EntityScheduleRun.status.in_(("running", "waiting_for_approval")),
                ))
                return {"error": "A run of this schedule is active", "active_run_id": active_id}, 409
            session.commit()
            if not start_schedule_run(entity_type, project_id, schedule_id, run_id):
                fail_run(session, run_id, START_FAILED)
                session.commit()
                return {"error": "The run could not start"}, 500
            return {"run_id": run_id}, 202

    @auth.decorators.check_api(schedule_check_spec("details"))
    def _runs(self, project_id: int, schedule_id: int):
        """Answer the run records of a schedule, newest first, with the total before paging.

        Who may call: the owner with `schedules.details`, or a schedules admin.
        Errors: 400 invalid query option; 403; 404.
        """
        try:
            query = RunsQuery.model_validate(dict(request.args))
        except ValidationError as e:
            return _validation_response(e)
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                load_schedule_in_scope(session, schedule_id, scope)
            except ScheduleError as e:
                return e.response
            where = [EntityScheduleRun.schedule_id == schedule_id]
            if query.status:
                where.append(EntityScheduleRun.status == query.status)
            rows = session.execute(
                select(*(getattr(EntityScheduleRun, name) for name in RUN_FIELDS)).where(*where)
                .order_by(EntityScheduleRun.started_at.desc(), EntityScheduleRun.id.desc())
                .limit(min(query.limit, MAX_RUNS_LIMIT)).offset(query.offset)
            ).mappings().all()
            total = session.scalar(select(func.count()).select_from(EntityScheduleRun).where(*where))
            return {"total": total, "rows": [{
                **row, "started_at": _iso(row["started_at"]), "finished_at": _iso(row["finished_at"]),
            } for row in rows]}, 200

    @auth.decorators.check_api(schedule_check_spec("admin"))
    def _change_owner(self, project_id: int, schedule_id: int):
        """Give a schedule to another project member; the old and the new owner get a notice.

        Who may call: a schedules admin only.
        Errors: 400 `owner_id` missing or not a member who may own it; 403; 404; 503 the access
        check is not available.
        """
        # late, like schedule_risk: only the owner route needs the owner and auth RPC checks
        from .schedule_owner import (
            ACCESS_CHECK_UNAVAILABLE,
            change_schedule_owner,
            mark_owner_change_span,
            validate_new_owner,
        )
        body = request.get_json(silent=True)
        owner_id = body.get("owner_id") if isinstance(body, dict) else None
        if type(owner_id) is not int:  # strict: "20", true and 20.0 are no owner ids (R2.3)
            return {"error": "owner_id must be an integer", "field": "owner_id"}, 400
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                row = load_schedule_in_scope(session, schedule_id, scope)
            except ScheduleError as e:
                return e.response
            reason = validate_new_owner(project_id, owner_id)
            if reason == ACCESS_CHECK_UNAVAILABLE:
                return {"error": reason}, 503
            if reason:
                return {"error": reason, "field": "owner_id"}, 400
            change = change_schedule_owner(session, row, owner_id, scope.user_id)
            if change:
                mark_owner_change_span(row)
                _notify_owner_change(project_id, row, change)
            return _read_answer(row, scope, project_id), 200
