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
    ScheduleRiskCheck,
    ScheduleUpdate,
)
from ..models.pd.schedule_list import RunStatus
from ..models.pd.schedule_run import ScheduleRunRequest
from .schedule_access import (
    SCHEDULE_PERMISSIONS,
    ScheduleScope,
    allowed_actions,
    schedule_check_spec,
    schedule_scope,
)
from .schedule_claim import fail_run, insert_run, merge_events
from .schedule_dispatch import START_FAILED, start_schedule_run
from .schedule_notifications import notify_schedule_event
from .schedule_service import (
    INVALID_INPUT,
    ScheduleError,
    check_entity,
    create_schedule,
    delete_schedule,
    load_schedule_in_scope,
    refusal,
    update_schedule,
)
from .schedule_timing import ScheduleLimitError
from .trigger_governance import governance_notice_for

MAX_RUNS_LIMIT = 100
RUN_FIELDS = (
    "id", "trigger", "status", "reason", "conversation_id", "requested_by", "started_at", "finished_at",
    "version_id",
)
RUN_NOT_FOUND = refusal("run_not_found", "Run not found", 404)
ENABLED_NOT_EDITABLE = refusal(
    INVALID_INPUT, "enabled cannot be changed by an edit; use the pause or resume action of the schedule",
    field="enabled",
)
ENABLED_NOT_BOOLEAN = refusal(INVALID_INPUT, "enabled must be true or false", field="enabled")
LIMIT_REACHED = "schedule_limit_reached"
RUN_WITHOUT_ASKING = "run_without_asking"
# The human texts of the save refusals (owner CR 2026-10-21, risk review)
RULE_NOT_ALLOWED = 'This project does not allow "Run without asking"'
BLOCKED = "You can't save this schedule: a scheduled run cannot do what the blocking findings need"
SCAN_INCOMPLETE = 'Some tools could not be checked, so "Run without asking" is not allowed; use "Pause and ask me"'
CONFIRM = "Review the sensitive tools, then save again with `risk_ack` and this fingerprint"
CHANGED = "The sensitive tools changed since you reviewed them; review them again and confirm"
NAME_MISMATCH = 'Type the schedule name exactly to confirm "Run without asking"'


class RunsQuery(BaseModel):
    """Query of the run records list; `limit` above the cap is lowered to it (R3.3, R3.5)."""

    limit: int = Field(default=20, ge=1)
    offset: int = Field(default=0, ge=0)
    status: RunStatus | None = None


def _iso(moment) -> str | None:
    return moment.isoformat() if moment else None


def _run_row(row) -> dict:
    """A run as the lists and the run detail answer it: ISO times and `duration_ms` once the run ended."""
    started, finished = row["started_at"], row["finished_at"]
    return {
        **row, "started_at": _iso(started), "finished_at": _iso(finished),
        "duration_ms": round((finished - started).total_seconds() * 1000) if started and finished else None,
    }


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


def _delivery(row) -> dict:
    """The delivery object of a row; the table keeps it as three flat columns."""
    target = row.delivery_conversation_uuid
    return {
        "mode": row.delivery_mode,
        "conversation_uuid": str(target) if target is not None else None,
        "participant_id": row.delivery_participant_id,
    }


def _details(row, scope: ScheduleScope) -> dict:
    """Shape a row as the API answers it, with the owner's and the confirmer's name for the edit page (US-32)."""
    fields = ScheduleDetails.model_fields.keys() - {"owner", "allowed_actions", "approval_confirmed_by", "delivery"}
    confirmer = row.approval_confirmed_by
    return ScheduleDetails.model_validate({
        **{name: getattr(row, name) for name in fields},
        "owner": {"id": row.created_by, "name": _owner_name(row.created_by)},
        "approval_confirmed_by": _owner_name(confirmer) if confirmer is not None else None,
        "allowed_actions": allowed_actions(row, scope),
        "delivery": _delivery(row),
    }).model_dump(mode="json")


def _refuse_unallowed_rule(project_id: int, body):
    """Answer 400 when the body asks for "run without asking" and the project does not allow it."""
    from .schedule_settings import is_run_without_asking_allowed
    if body.approval_rule == RUN_WITHOUT_ASKING and not is_run_without_asking_allowed(project_id):
        return refusal("approval_rule_not_allowed", RULE_NOT_ALLOWED, field="approval_rule")
    return None


def _kept_fingerprint(row) -> str | None:
    """The stored consent an edit or a check may keep: only a schedule that runs without asking has one (R9.1)."""
    return row.approval_fingerprint if row.approval_rule == RUN_WITHOUT_ASKING else None


def _check_scope_error(row, body, scope: ScheduleScope) -> dict | None:
    """Tie a check with `schedule_id` to that schedule, so it never shows the owner's scan of another entity.

    The agent or pipeline must be the schedule's. Another version needs the right to edit the schedule,
    as an edit would; the same version only needs the visibility the 404 rule already gives.
    """
    for field in ("entity_type", "application_id"):
        if getattr(body, field) != getattr(row, field):
            return refusal(INVALID_INPUT, f"{field} must be the one of the schedule", field=field)
    if body.version_id != row.version_id and SCHEDULE_PERMISSIONS["update"] not in scope.permissions:
        return refusal("not_owner", "Only a user who may edit this schedule can check another version", 403)
    return None


def _review_save(project_id: int, version_id: int | None, owner_id: int, name: str, rule: str, ack,
                 stored_fingerprint: str | None = None):
    """Check a save against the risk review; return the review and, for a confirmed "Run without asking", it again.

    An edit without `risk_ack` that keeps "Run without asking" and the stored fingerprint keeps the stored
    consent (R9.1): it returns the review and no new confirmation, also for a rename.

    Raises:
        ScheduleError: 409 `schedule_blocked`, `risk_confirmation_required` or `risk_changed`; 400
            `approval_rule_not_allowed` (incomplete scan) or `risk_name_mismatch` (`field: risk_ack`).
    """
    from .schedule_risk import scan_complete, scan_schedule
    review = scan_schedule(project_id, version_id, owner_id)
    without_asking = rule == RUN_WITHOUT_ASKING
    if review["blocking"]:
        raise ScheduleError("schedule_blocked", BLOCKED, 409, **review)
    if without_asking and not scan_complete(review):
        raise ScheduleError("approval_rule_not_allowed", SCAN_INCOMPLETE, field="approval_rule", **review)
    if not review["sensitive"]:
        return review, None
    if ack is None and without_asking and stored_fingerprint and stored_fingerprint == review["fingerprint"]:
        return review, None
    if ack is None:
        raise ScheduleError("risk_confirmation_required", CONFIRM, 409, **review)
    if ack.fingerprint != review["fingerprint"]:
        raise ScheduleError("risk_changed", CHANGED, 409, **review)
    if not without_asking:
        return review, None
    if ack.confirmed_name.strip() != name.strip():
        raise ScheduleError("risk_name_mismatch", NAME_MISMATCH, field="risk_ack", **review)
    return review, review


def _saved(row, scope: ScheduleScope, project_id: int, review: dict | None = None, notice: str | None = None) -> dict:
    """Details of a saved row, with every finding of the risk review as the save `warnings` (R2.4).

    `review` is the scan the save already ran; a pause or a resume scans here. `notice` is the governance
    notice of a create or an edit: the schedule is saved, but its version has no active binding.
    """
    if review is None:
        from .schedule_risk import scan_schedule
        review = scan_schedule(project_id, row.version_id, row.created_by)
    warnings = review["blocking"] + review["sensitive"] + review["notices"]
    return {**_read_answer(row, scope, project_id), "warnings": warnings, "governance_notice": notice}


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
    return refusal(INVALID_INPUT, first["msg"].removeprefix("Value error, "), field=field)


def _apply_update(project_id: int, schedule_id: int, body: ScheduleUpdate | ScheduleEdit, reviewed: bool = False,
                  module=None):
    """Save an edit, a pause or a resume through the service and answer the details.

    `reviewed` runs the risk review of an edit first and adds the governance notice; a pause or a resume
    skips both. The service checks
    a resume (a version is needed, the project cap), computes the next run, and marks the audit span
    and tells the owner when an admin changes another owner's schedule.
    """
    scope = schedule_scope(project_id)
    with db.get_session(project_id) as session:
        try:
            row = load_schedule_in_scope(session, schedule_id, scope)
            review = confirmed = None
            if reviewed:
                if body.version_id is not None:  # no scan of a version the caller may not use
                    check_entity(session, project_id, row.entity_type, row.application_id, body.version_id)
                review, confirmed = _review_save(
                    project_id, body.version_id or row.version_id, row.created_by, body.name or row.name,
                    body.approval_rule or row.approval_rule, body.risk_ack, _kept_fingerprint(row),
                )
            row = update_schedule(session, project_id, row, body, scope, confirmed)
            notice = governance_notice_for(
                module, session, project_id, row.application_id, row.version_id, "schedule") if reviewed else None
            return _saved(row, scope, project_id, review, notice), 200
        except ScheduleError as e:
            return e.response
        except ScheduleLimitError as e:
            return refusal(LIMIT_REACHED, str(e), 409)
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
    def _check_risks(self, project_id: int):
        """Answer the risk review of a version before a save; nothing is written.

        The answer has `blocking` (the save is refused), `sensitive` (sensitive tools and personal
        credentials, `personal_credential` with `tool` null: the save needs `risk_ack`),
        `notices`, `run_without_asking_allowed` and the `fingerprint` that `risk_ack` must name.
        With `schedule_id` (an edit) the scan runs as the schedule owner, so an admin gets the owner's
        fingerprint, and the answer adds `confirmed`: true when the schedule keeps "Run without asking"
        with this fingerprint stored (R9.2).
        Who may call: a project member with `schedules.create`, as for a create.
        Errors: 400 invalid field, a version that is not of the agent or pipeline, or an agent or pipeline
        that is not the one of `schedule_id`; 403, also `not_owner` for another version without the right
        to edit the schedule; 404 not visible, also a `schedule_id` the caller may not see.
        """
        from .schedule_risk import scan_complete, scan_schedule
        from .schedule_settings import is_run_without_asking_allowed
        body, error = _parse_body(ScheduleRiskCheck)
        if error:
            return error
        owner_id, stored = auth.current_user()["id"], None
        with db.get_session(project_id) as session:
            try:
                if body.schedule_id is not None:
                    scope = schedule_scope(project_id)
                    row = load_schedule_in_scope(session, body.schedule_id, scope)
                    refused = _check_scope_error(row, body, scope)
                    if refused:
                        return refused
                    owner_id, stored = row.created_by, _kept_fingerprint(row)
                check_entity(session, project_id, body.entity_type, body.application_id, body.version_id)
            except ScheduleError as e:
                return e.response
        review = scan_schedule(project_id, body.version_id, owner_id)
        allowed = bool(is_run_without_asking_allowed(project_id)) and scan_complete(review)
        answer = {**review, "run_without_asking_allowed": allowed}
        if body.schedule_id is not None:
            answer["confirmed"] = stored is not None and stored == review["fingerprint"]
        return answer, 200

    @auth.decorators.check_api(schedule_check_spec("create"))
    def _create(self, project_id: int):
        """Create a schedule owned by the caller; the owner is never taken from the body.

        The risk review runs first. A blocking finding refuses the save; sensitive tools need
        `risk_ack` with the fingerprint, and with the schedule name for "Run without asking".
        Who may call: a project member with `schedules.create`.
        Errors: 400 invalid field, "run without asking" not allowed (`approval_rule_not_allowed`) or a
        wrong confirmed name (`risk_name_mismatch`, `field: risk_ack`); 403; 404 agent or pipeline not
        found or not visible; 409 `schedule_blocked`, `risk_confirmation_required`, `risk_changed`, or
        the project limit of active schedules.
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
                check_entity(session, project_id, body.entity_type, body.application_id, body.version_id)
                review, confirmed = _review_save(
                    project_id, body.version_id, scope.user_id, body.name, body.approval_rule, body.risk_ack,
                )
                row = create_schedule(session, project_id, body, scope, confirmed=confirmed)
            except ScheduleError as e:
                return e.response
            except ScheduleLimitError as e:
                return refusal(LIMIT_REACHED, str(e), 409)
            notice = governance_notice_for(
                self.module, session, project_id, row.application_id, row.version_id, "schedule")
            return _saved(row, scope, project_id, review, notice), 201

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
        """Change the fields of a schedule; the owner never changes.

        `enabled` false or true pauses or resumes the schedule after the other fields are saved.
        The risk review runs on the resulting version, name and rule, as for a create.
        Who may call: the owner with `schedules.update`, or a schedules admin (the owner gets a notice).
        Errors: 400 invalid field, `enabled` that is not a boolean, "run without asking" not allowed or a
        wrong confirmed name (`field: risk_ack`); 403; 404; 409 `schedule_blocked`,
        `risk_confirmation_required`, `risk_changed` or the project limit of active schedules.
        """
        raw = request.get_json(silent=True)
        enabled = raw.get("enabled") if isinstance(raw, dict) else None
        if isinstance(raw, dict) and "enabled" in raw and not isinstance(enabled, bool):
            return ENABLED_NOT_BOOLEAN
        body, error = _parse_body(ScheduleEdit)
        if error:
            return error
        if enabled is None or body.model_fields_set:
            refused = _refuse_unallowed_rule(project_id, body)
            if refused:
                return refused
            answer = _apply_update(project_id, schedule_id, body, reviewed=True, module=self.module)
            if enabled is None or answer[1] != 200:
                return answer
        return _apply_update(project_id, schedule_id, ScheduleUpdate(enabled=enabled))

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
                return refusal("not_owner", "Only the owner of this schedule can run it", 403)
            if row.version_id is None:
                return refusal("version_deleted", "The version of this schedule was deleted; choose a version", 409)
            entity_type = row.entity_type
            run_id = insert_run(session, schedule_id, body.trigger, scope.user_id)
            if run_id is None:
                active_id = session.scalar(select(EntityScheduleRun.id).where(
                    EntityScheduleRun.schedule_id == schedule_id,
                    EntityScheduleRun.status.in_(("running", "waiting_for_approval")),
                ))
                return refusal("run_active", "A run of this schedule is active", 409, active_run_id=active_id)
            session.commit()
            if not start_schedule_run(entity_type, project_id, schedule_id, run_id):
                fail_run(session, run_id, START_FAILED)
                session.commit()
                return refusal("run_start_failed", "The run could not start", 500)
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
            return {"total": total, "rows": [_run_row(row) for row in rows]}, 200

    @auth.decorators.check_api(schedule_check_spec("details"))
    def _run_detail(self, project_id: int, schedule_id: int, run_id: int):
        """Answer one run of a schedule for the Test schedule panel, with its usage summary and event log.

        The events are the stored ones and the tool calls of the run conversation, in time order, at most 200.
        A tool call carries the full tool result in `summary`: raw tool output, up to 20 000 characters.
        Tokens and cost are null while the usage store has no row for the conversation.
        Who may call: the owner with `schedules.details`, or a schedules admin.
        Errors: 403; 404 the schedule or the run is not found, or another owner's schedule.
        """
        # Late, like schedule_risk: only this route reads the conversation and the usage store
        from .schedule_run_detail import tool_call_events, usage_summary, version_label
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                schedule = load_schedule_in_scope(session, schedule_id, scope)
            except ScheduleError as e:
                return e.response
            run = session.scalar(select(EntityScheduleRun).where(
                EntityScheduleRun.id == run_id, EntityScheduleRun.schedule_id == schedule_id))
            if run is None:
                return RUN_NOT_FOUND
            row = _run_row({name: getattr(run, name) for name in RUN_FIELDS})
            tokens, cost_usd = usage_summary(session, project_id, run)
            events = merge_events(run.events, tool_call_events(session, run.conversation_id))
            return {
                **row,
                "version_label": version_label(session, run.version_id),
                "principal_label": run.principal_label,
                "risk_outcome": run.risk_outcome,
                "approval_rule": schedule.approval_rule,
                "approval_expires_at": _iso(run.approval_expires_at),
                "summary": {"duration_ms": row["duration_ms"], "tokens": tokens, "cost_usd": cost_usd},
                "events": events,
            }, 200

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
            return refusal(INVALID_INPUT, "owner_id must be an integer", field="owner_id")
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                row = load_schedule_in_scope(session, schedule_id, scope)
            except ScheduleError as e:
                return e.response
            reason = validate_new_owner(project_id, owner_id, row.entity_type, row.application_id)
            if reason == ACCESS_CHECK_UNAVAILABLE:
                return refusal("access_check_unavailable", reason, 503)
            if reason:
                return refusal(INVALID_INPUT, reason, field="owner_id")
            change = change_schedule_owner(session, row, owner_id, scope.user_id)
            if change:
                mark_owner_change_span(row)
                _notify_owner_change(project_id, row, change)
            return _read_answer(row, scope, project_id), 200
