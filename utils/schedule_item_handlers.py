"""Handlers of one schedule (EL-7011, ADR-0004): create, read, edit, delete, pause, resume, run, runs, owner.

`api/v2/schedules.py` serves them; this module registers no route.
"""
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Literal

from flask import request
from pydantic import BaseModel, Field, ValidationError, constr
from pylon.core.tools import log
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError
from tools import VaultClient, auth, db

from ..models.all import EliteATool
from ..models.entity_schedule import ACTIVE_STATUSES, EntitySchedule, EntityScheduleRun
from ..models.enums.all import NotificationEventTypes
from ..models.pd.entity_schedule import (
    INDEX_NAME_MAX,
    ScheduleCreate,
    ScheduleDetails,
    ScheduleEdit,
    ScheduleRiskCheck,
    ScheduleUpdate,
    WebhookSecretAction,
)
from ..models.pd.schedule_list import MAX_DB_ID, DbId, RunFilters
from ..models.pd.schedule_run import ScheduleRunDecision, ScheduleRunRequest
from .error_text import public_event, public_reason
from .schedule_access import (
    SCHEDULE_PERMISSIONS,
    ScheduleScope,
    allowed_actions,
    row_webhook_url,
    schedule_check_spec,
    webhook_signing,
    schedule_scope,
)
from .schedule_claim import fail_run, insert_run, merge_events
from .schedule_dispatch import START_FAILED, start_schedule_run
from .schedule_notifications import notify_schedule_event
from .schedule_service import (
    INVALID_INPUT,
    TOOLKIT_INDEX,
    ScheduleError,
    audit_schedule_event,
    check_entity,
    check_index_body,
    check_schedule_body,
    check_webhook_owner_edit,
    create_schedule,
    delete_schedule,
    load_schedule_in_scope,
    notify_owner,
    refusal,
    update_changes,
    update_schedule,
)
from .schedule_test_runs import (
    OLD_SCHEMA,
    TestRunUnavailable,
    active_index_test_run_id,
    active_test_run_id,
    error_code,
    form_of_test_run,
    insert_test_run,
    load_test_run,
    remove_trimmed_chats,
    test_run_scope,
    trim_test_runs,
)
from .schedule_timing import ScheduleLimitError
from .trigger_governance import schedule_governance_notice

MAX_RUNS_LIMIT = 100
RUN_FIELDS = (
    "id", "trigger", "status", "reason", "conversation_id", "requested_by", "started_at", "finished_at",
    "version_id",
)
RUN_NOT_FOUND = refusal("run_not_found", "Run not found", 404)
TEST_RUN_UNAVAILABLE = refusal(
    "test_run_unavailable", "Test runs are not available yet: the administrator must run the task migrate_schedule_test_runs",
    503,
)
ENABLED_NOT_EDITABLE = refusal(
    INVALID_INPUT, "enabled cannot be changed by an edit; use the pause or resume action of the schedule",
    field="enabled",
)
LIMIT_REACHED = "schedule_limit_reached"
RUN_WITHOUT_ASKING = "run_without_asking"
# The human texts of the save refusals (owner CR 2026-10-21, risk review)
CREDENTIAL_STILL_MISSING = "The credential is still missing. Add it, then approve again."
RULE_NOT_ALLOWED = 'This project does not allow "Run without asking"'
BLOCKED = "You can't save this schedule: a scheduled run cannot do what the blocking findings need"
SCAN_INCOMPLETE = 'Some tools could not be checked, so "Run without asking" is not allowed; use "Pause and ask me"'
CONFIRM = "Review the sensitive tools, then save again with `risk_ack` and this fingerprint"
CHANGED = "The sensitive tools changed since you reviewed them; review them again and confirm"
ROTATION_IN_PROGRESS = "The previous secret still works until the time shown; use regenerate to replace it now"
OWNER_CHANGED_PAUSED = "it is paused until the new owner checks its connections and switches it on"
NAME_MISMATCH = 'Type the schedule name exactly to confirm "Run without asking"'


def _started(entity_type: str, project_id: int, schedule_id: DbId | None, run_id: int) -> bool:
    """Start an approved run; an error counts as "not started", so the caller fails the run with a reason.

    The decision is already committed and the run is running. An error that escaped would leave it running
    with no thread, and the owner would get a bare 500.
    """
    try:
        return start_schedule_run(entity_type, project_id, schedule_id, run_id)
    except Exception:  # noqa: BLE001 -- the run must end failed, not stay running
        log.exception("Schedule %s run %s: the approved run could not start", schedule_id, run_id)
        return False


class RunsQuery(RunFilters):
    """Query of the run records list; `limit` above the cap is lowered to it (R3.3, R3.5)."""

    limit: int = Field(default=20, ge=1)
    offset: int = Field(default=0, ge=0, le=MAX_DB_ID)


class TestRunTarget(BaseModel):
    """What a Test Run list or detail names: an agent or pipeline, or a Toolkit Index (both names)."""

    __test__ = False  # not a pytest class, although the name starts with Test

    application_id: DbId | None = None
    toolkit_id: DbId | None = None
    index_name: constr(min_length=1, max_length=INDEX_NAME_MAX) | None = None


class TestRunsQuery(RunsQuery, TestRunTarget):
    """Query of the Test Run list: the runs list options and the target of the runs."""

    __test__ = False  # not a pytest class, although the name starts with Test


def _test_run_target(query: TestRunTarget):
    """The scope keywords of the target a query names, or the 400 answer when it names none or two.

    Returns:
        ``(keywords, None)`` or ``(None, refusal)``.
    """
    index = (query.toolkit_id, query.index_name)
    if query.application_id is not None and any(part is not None for part in index):
        return None, refusal(INVALID_INPUT, "Name an agent or pipeline, or a toolkit and an index, not both",
                             field="application_id")
    if query.application_id is not None:
        return {"application_id": query.application_id}, None
    if all(part is not None for part in index):
        return {"toolkit_id": query.toolkit_id, "index_name": query.index_name}, None
    field = "application_id" if not any(part is not None for part in index) else (
        "index_name" if query.index_name is None else "toolkit_id")
    return None, refusal(INVALID_INPUT, "Name an agent or pipeline (application_id), or a toolkit and an index", field=field)


def _reply_target(row) -> dict:
    """The target fields of a run reply: the toolkit and index of a Toolkit Index, else the agent or pipeline."""
    if row.entity_type == TOOLKIT_INDEX:
        return {"toolkit_id": row.toolkit_id, "index_name": row.index_name}
    return {"application_id": row.application_id}


def _governance_notice(module, session, project_id: int, row, saver_id: int):
    """The governance notice of a saved schedule; a Toolkit Index row names its type, because it has no version to bind."""
    extra = {"entity_type": row.entity_type} if row.entity_type == TOOLKIT_INDEX else {}
    return schedule_governance_notice(
        module, session, project_id, row.application_id, row.version_id, row.created_by, saver_id, **extra)


def _start_run(session, project_id: int, schedule_id: int, entity_type: str, target: dict, trigger: str,
               user_id: int, owner_request):
    """Insert the running run, start it and answer 202; 409 `run_active` when the schedule has an active run."""
    run_id = insert_run(session, schedule_id, trigger, user_id)
    if run_id is None:
        session.rollback()  # a draft written for this call must not stay half-saved
        active_id = session.scalar(select(EntityScheduleRun.id).where(
            EntityScheduleRun.schedule_id == schedule_id,
            EntityScheduleRun.status.in_(ACTIVE_STATUSES),
        ))
        return refusal("run_active", "A run of this schedule is active", 409, active_run_id=active_id)
    session.commit()
    if not start_schedule_run(entity_type, project_id, schedule_id, run_id, owner_request):
        fail_run(session, run_id, START_FAILED)
        session.commit()
        return refusal("run_start_failed", "The run could not start", 500)
    return {"run_id": run_id, **target}, 202


def _sample_body_error(row, body):
    """A webhook row runs only as `test` with a sample `payload`; a cron row takes no `payload` (#7455)."""
    if row.trigger_kind != "webhook":
        if body.payload is not None:
            return refusal(INVALID_INPUT, "Only a webhook takes a sample payload", field="payload")
        return None
    if body.trigger != "test":
        return refusal(INVALID_INPUT, "A webhook is run as a test with a sample payload", field="trigger")
    if body.payload is None:
        return refusal(INVALID_INPUT, "A webhook run needs a sample payload", field="payload")
    return None


def _start_sample_run(session, project_id: int, row, payload: str):
    """Run a webhook row with the owner's sample body through the delivery path; the body is never stored."""
    from .webhook_door import Capped, admit_webhook_run  # pylint: disable=C0415 -- late: only webhook rows reach it
    schedule_id = row.id
    run_id = admit_webhook_run(session, project_id, schedule_id, trigger="test")
    if isinstance(run_id, Capped):  # no skipped row: the owner sees the answer at once
        session.rollback()
        return refusal(run_id.code, f"Too many runs at once ({run_id.running} running, cap {run_id.cap}); try again later",
                       429, running=run_id.running, cap=run_id.cap)
    if run_id is None:  # one sample at a time; deliveries are not limited by the active-run index
        session.rollback()
        active_id = session.scalar(select(EntityScheduleRun.id).where(
            EntityScheduleRun.schedule_id == schedule_id, EntityScheduleRun.trigger == "test",
            EntityScheduleRun.status.in_(ACTIVE_STATUSES)))
        return refusal("run_active", "A sample run of this webhook is active", 409, active_run_id=active_id)
    session.commit()
    if not start_schedule_run(row.entity_type, project_id, schedule_id, run_id, payload=payload.encode("utf-8")):
        fail_run(session, run_id, START_FAILED)
        session.commit()
        return refusal("run_start_failed", "The run could not start", 500)
    return {"run_id": run_id, "application_id": row.application_id}, 202


def _runs_page(session, where: list, query: RunsQuery) -> tuple[dict, list]:
    """The `{total, rows}` page of the runs picked by `where`, newest first, for the runs list and the Test Run list.

    The rows have no usage figures yet. The second value is what `_add_usage` needs, so the caller reads the usage
    store after it closed the project session.
    """
    # Late: only the run routes read the usage store
    from .schedule_run_detail import conversation_uuids, version_labels
    where = [*where, *_run_filters(query)]
    rows = session.execute(
        select(*(getattr(EntityScheduleRun, name) for name in RUN_FIELDS)).where(*where)
        .order_by(EntityScheduleRun.started_at.desc(), EntityScheduleRun.id.desc())
        .limit(min(query.limit, MAX_RUNS_LIMIT)).offset(query.offset)
    ).mappings().all()
    total = session.scalar(select(func.count()).select_from(EntityScheduleRun).where(*where))
    # One lookup for the whole page: a call per row would cost a query per run
    labels = version_labels(session, [row["version_id"] for row in rows])
    uuids = conversation_uuids(session, [row["conversation_id"] for row in rows])
    usage_runs = [SimpleNamespace(id=row["id"], conversation_uuid=uuids.get(row["conversation_id"]),
                                  conversation_id=row["conversation_id"], started_at=row["started_at"],
                                  finished_at=row["finished_at"]) for row in rows]
    return {"total": total, "rows": [
        {**_run_row(row), "version_label": labels.get(row["version_id"])} for row in rows
    ]}, usage_runs


def _add_usage(project_id: int, page: dict, usage_runs: list) -> dict:
    """Add `tokens` and `cost_usd` to the rows of a page. Called after the project session closed, so a request
    holds one pooled connection at a time."""
    from .schedule_run_detail import usage_summaries  # late, as `_runs_page`
    usage = usage_summaries(project_id, usage_runs)
    for row in page["rows"]:
        row["tokens"], row["cost_usd"] = usage[row["id"]]
    return page


def _run_filters(query: RunFilters) -> list:
    where = []
    if query.status:
        where.append(EntityScheduleRun.status == query.status)
    if query.trigger:
        where.append(EntityScheduleRun.trigger == query.trigger)
    if query.version_id:
        where.append(EntityScheduleRun.version_id == query.version_id)
    return where


def _run_detail_body(session, project_id: int, run, approval_rule: str | None):
    """The `(body, status)` of one run, with the approval rule of its schedule or of its Test Run form."""
    # Late: only the detail routes read the conversation and the usage store
    from .schedule_run_detail import tool_call_events, usage_summary, version_label
    row = _run_row({name: getattr(run, name) for name in RUN_FIELDS})
    tokens, cost_usd = usage_summary(session, project_id, run)
    events = [public_event(e) for e in merge_events(run.events, tool_call_events(session, run.conversation_id))]
    return {
        **row,
        "version_label": version_label(session, run.version_id),
        "principal_label": run.principal_label,
        "risk_outcome": run.risk_outcome,
        "approval_rule": approval_rule,
        "approval_expires_at": _iso(run.approval_expires_at),
        "summary": {"duration_ms": row["duration_ms"], "tokens": tokens, "cost_usd": cost_usd},
        "events": events,
    }, 200


def _iso(moment) -> str | None:
    return moment.isoformat() if moment else None


def _run_row(row) -> dict:
    """A run as the lists and the run detail answer it: ISO times and `duration_ms` once the run ended."""
    started, finished = row["started_at"], row["finished_at"]
    return {
        **row, "reason": public_reason(row.get("reason")),
        "started_at": _iso(started), "finished_at": _iso(finished),
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


def _details(row, scope: ScheduleScope) -> dict:
    """Shape a row as the API answers it, with the owner's and the confirmer's name for the edit page (US-32)."""
    fields = ScheduleDetails.model_fields.keys() - {"owner", "allowed_actions", "approval_confirmed_by", "webhook_url"}
    confirmer = row.approval_confirmed_by
    return ScheduleDetails.model_validate({
        **{name: getattr(row, name) for name in fields},
        "owner": {"id": row.created_by, "name": _owner_name(row.created_by)},
        "approval_confirmed_by": _owner_name(confirmer) if confirmer is not None else None,
        "allowed_actions": allowed_actions(row, scope),
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
    review = scan_schedule(project_id, version_id, owner_id, rule)
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
    notice of a create or an edit: the schedule is saved, but its version has no active binding or its owner
    no mandate.
    """
    if review is None:
        from .schedule_risk import scan_schedule
        review = scan_schedule(project_id, row.version_id, row.created_by, row.approval_rule)
    warnings = review["blocking"] + review["sensitive"] + review["notices"]
    return {**_read_answer(row, scope, project_id, review), "warnings": warnings, "governance_notice": notice}


def _needs_reconfirm(row, project_id: int, review: dict | None) -> bool | None:
    """True when "Run without asking" would not hold on the next run (as `_may_auto_approve`); None if unscanned."""
    if row.approval_rule != RUN_WITHOUT_ASKING:
        return False
    try:
        if review is None:
            from .schedule_risk import scan_schedule  # pylint: disable=C0415 -- late: the scan expands toolkits
            review = scan_schedule(project_id, row.version_id, row.created_by, row.approval_rule)
    except Exception:  # noqa: BLE001 -- a failed scan must not break the read
        log.exception("Schedule %s: cannot scan for needs_reconfirm", row.id)
        return None
    if not review["sensitive"]:
        return False
    return review["fingerprint"] != row.approval_fingerprint or row.approval_confirmed_by != row.created_by


def _read_answer(row, scope: ScheduleScope, project_id: int, review: dict | None = None) -> dict:
    """One row as detail, edit and save answer it; only here `needs_reconfirm` is scanned, never per list row."""
    from .schedule_settings import is_run_without_asking_allowed
    return {**_details(row, scope), "webhook_url": row_webhook_url(project_id, row),
            "run_without_asking_allowed": bool(is_run_without_asking_allowed(project_id)),
            "needs_reconfirm": _needs_reconfirm(row, project_id, review), **webhook_signing(row)}


def _credential_check(project_id: int, owner) -> str:
    """The same credential check as the pre-run step for `owner`'s schedule: "missing", "clean" or "unknown"."""
    from .schedule_prerun import check_owner_credentials, credential_wait  # late, as the other pre-run helpers
    try:
        reason = check_owner_credentials(project_id, owner)
    except Exception:  # noqa: BLE001 -- an unreadable check must not block the owner; the restart runs it again
        log.exception("Project %s: the credential check before an approval failed", project_id)
        return "unknown"
    if credential_wait(reason):
        return "missing"
    return "clean" if reason is None else "unknown"


def _remember_checked(run_id: int) -> None:
    """Skip the repeat of a clean credential check in the restarted run, so it does not expand the version twice."""
    from .schedule_prerun import mark_credentials_checked  # late, as the other pre-run helpers
    mark_credentials_checked(run_id)


def _forget_checked(run_id: int) -> None:
    from .schedule_prerun import forget_credentials_checked  # late, as the other pre-run helpers
    forget_credentials_checked(run_id)


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
            # Before the risk scan: an admin edit of a webhook is refused without scanning
            check_webhook_owner_edit(row, scope, update_changes(body))
            review = confirmed = None
            if reviewed:
                if body.version_id is not None:  # no scan of a version the caller may not use
                    check_entity(session, project_id, row.entity_type, row.application_id, body.version_id)
                review, confirmed = _review_save(
                    project_id, body.version_id or row.version_id, row.created_by, body.name or row.name,
                    body.approval_rule or row.approval_rule, body.risk_ack, _kept_fingerprint(row),
                )
            row = update_schedule(session, project_id, row, body, scope, confirmed)
            notice = _governance_notice(module, session, project_id, row, scope.user_id) if reviewed else None
            return _saved(row, scope, project_id, review, notice), 200
        except ScheduleError as e:
            return e.response
        except ScheduleLimitError as e:
            return refusal(LIMIT_REACHED, str(e), 409)
        except ValidationError as e:  # the stop date does not fit the resulting cron and time zone
            return _validation_response(e)


def _notify_owner_change(project_id: int, row, change: dict) -> None:
    """Tell the old and the new owner, both ids in the meta (R3.2); a failure is logged, the change stays."""
    extra = {**change, "reason": OWNER_CHANGED_PAUSED} if row.trigger_kind == "webhook" else change
    for user_id in change.values():
        try:
            notify_schedule_event(project_id, user_id, NotificationEventTypes.schedule_owner_changed, row, extra=extra)
        except Exception as e:  # noqa: BLE001 -- the change is committed; a lost notice must not fail it
            log.error("Schedule %s: cannot send the owner change notice: %s", row.id, e)


def _decide(project_id: int, session, scope, run, entity_type: str, owner, body):
    """The decision on a waiting run, shared by a saved run and a Test Run; `owner` has the values the credential
    check needs. Reads in `session`, which this function leaves open for the caller to close."""
    # Late: the decision writers load the run models, which the handler tests fake
    from .schedule_runs import (
        ScheduleDecisionForbidden,
        ScheduleRunNotWaiting,
        apply_schedule_decision,
        check_schedule_decision,
        run_owner_id,
    )
    if run.conversation_id is not None:
        return refusal("run_has_chat", "Decide this run in its chat", 409)
    run_id, schedule_id = run.id, run.schedule_id
    owner.created_by = run_owner_id(session, run)
    session.close()  # the checks below open their own sessions
    try:
        decision = check_schedule_decision(project_id, run_id, scope.user_id, [body.action])
        checked = _credential_check(project_id, owner) if decision == "approve" else "unknown"
        if checked == "missing":
            return refusal("credential_missing", CREDENTIAL_STILL_MISSING, 409)
        apply_schedule_decision(project_id, run_id, decision)
        if checked == "clean":  # only after the write: a lost race must leave no mark behind
            _remember_checked(run_id)
    except ScheduleDecisionForbidden:
        return refusal("not_owner", "Only the schedule owner can approve or reject this run", 403)
    except ScheduleRunNotWaiting:
        return refusal("run_not_waiting", "This schedule run is not waiting for approval", 409)
    if decision == "approve" and not _started(entity_type, project_id, schedule_id, run_id):
        _forget_checked(run_id)
        with db.get_session(project_id) as failed_session:
            fail_run(failed_session, run_id, START_FAILED)
            failed_session.commit()
        return refusal("run_start_failed", "The run could not start", 500)
    return {"run_id": run_id, "status": "running" if decision == "approve" else "failed"}, 200


def _unavailable_or_raise(error: DBAPIError):
    """The 503 answer for a database error of a schema without the Test Run columns; any other error goes on."""
    if error_code(error) in OLD_SCHEMA:
        return TEST_RUN_UNAVAILABLE
    raise error


INDEX_RUN_ACTIVE_TEXT = "A run of this index is active"


class IndexRiskCheck(BaseModel):
    """The body of the dry-run check of a Toolkit Index: the toolkit and the index, no version."""

    entity_type: Literal["toolkit_index"]
    toolkit_id: DbId
    index_name: constr(min_length=1, max_length=INDEX_NAME_MAX)
    schedule_id: DbId | None = None


def _check_index_target(session, project_id: int, toolkit_id: int | None, index_name: str | None) -> None:
    """`check_entity` for an index, without the "already scheduled" refusal.

    A Test Run and a risk check try a form, so the index may already hold the schedule the form belongs to.
    """
    try:
        check_entity(session, project_id, TOOLKIT_INDEX, None, None, toolkit_id, index_name)
    except ScheduleError as e:
        if e.response[0]["code"] != "index_scheduled":
            raise


def _check_test_body(session, project_id: int, body: ScheduleCreate) -> None:
    """The Save rules of a Test Run body; a Toolkit Index body is checked by the index rules."""
    if body.entity_type != TOOLKIT_INDEX:
        return check_schedule_body(session, project_id, body)
    check_index_body(body)
    return _check_index_target(session, project_id, body.toolkit_id, body.index_name)


def _body_target(body: ScheduleCreate) -> dict:
    """The target fields of a Test Run reply, as `_reply_target` gives them for a saved row."""
    if body.entity_type == TOOLKIT_INDEX:
        return {"toolkit_id": body.toolkit_id, "index_name": body.index_name}
    return {"application_id": body.application_id}


def _index_row_is_live(project_id: int, toolkit, index_name: str) -> bool:
    """True when the index row says `in_progress` and its run still beats; false when it cannot be read.

    A failed read does not block the run: the starter reads the same row again and skips a live run itself.
    """
    # Late: the vector store helpers load only when an index run is asked for
    from .application_tools import DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC, resolve_index_staleness
    from .schedule_index_starter import _read_index, _toolkit  # pylint: disable=C0415,W0212
    try:
        _, connection_string = _toolkit(project_id, toolkit.author_id, toolkit.id)
        index = _read_index(connection_string, toolkit.id, index_name, with_heartbeat=True)
        state = str((index.meta if index else {}).get("state") or "").lower()
        if state != "in_progress":
            return False
        timeout = int(VaultClient(project_id).get_secrets().get(
            "task_disconnected_timeout_sec", DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC))
        # No heartbeat horizon: the same control rule as the starter and the dispatch guard
        return not resolve_index_staleness(state, index.meta.get("updated_on", 0), timeout, index.heartbeat)
    except Exception as error:  # noqa: BLE001 -- the starter decides again at the start
        log.warning("Index %s of toolkit %s: liveness not read: %r", index_name, toolkit.id, error)
        return False


def _index_busy(session, project_id: int, toolkit_id: int, index_name: str, schedule_id: int | None = None):
    """The 409 `run_active` answer while a run of the index is active, else None.

    The unique index of an active Test Run keys on the agent, which an index run does not have, so the toolkit row
    is locked first: a second caller waits for the commit of the first and then sees its run.
    """
    toolkit = session.scalars(select(EliteATool).where(EliteATool.id == toolkit_id).with_for_update()).one_or_none()
    try:
        with session.begin_nested():  # a missing Test Run column must not abort the outer transaction
            active_id = active_index_test_run_id(session, toolkit_id, index_name)
    except TestRunUnavailable:
        active_id = None  # no Test Run column yet, so no Test Run exists
    if active_id is None and schedule_id is not None:
        active_id = session.scalar(select(EntityScheduleRun.id).where(
            EntityScheduleRun.schedule_id == schedule_id, EntityScheduleRun.status.in_(ACTIVE_STATUSES)).limit(1))
    if active_id is not None or (toolkit is not None and _index_row_is_live(project_id, toolkit, index_name)):
        return refusal("run_active", INDEX_RUN_ACTIVE_TEXT, 409, active_run_id=active_id)
    return None


def _index_admission(module, project_id: int, toolkit_id: int, trigger_ref: str):
    """The admission request of an index Run now or Test run; None when governance is off.

    An index has no version, so `owner_run_request` does not fit: the entry point is the on-demand index run.
    """
    from ..models.pd.admission import ENTRY_INDEX_ON_DEMAND, admission_request_from  # pylint: disable=C0415
    request = admission_request_from(
        module, sid=None, project_id=project_id, binding_project_id=project_id,
        version_row=SimpleNamespace(id=None, application_id=None), run_kind="interactive",
        entry_point=ENTRY_INDEX_ON_DEMAND, toolkit_id=toolkit_id)
    return None if request is None else request.model_copy(update={"trigger_ref": trigger_ref})


def _index_review(session, project_id: int, toolkit_id: int, owner_id: int) -> dict:
    """The risk review of a Toolkit Index: the credential of its toolkit, as the owner would run it.

    An index has no tool to approve, so `sensitive` stays empty and there is no fingerprint to confirm.
    A credential the owner cannot use blocks; a personal credential is a notice.
    """
    # Late and private: the scan helpers of an agent version are reused, not copied
    from .application_tools import ConfigurationExpandError, expand_toolkit_settings
    from .schedule_risk import (  # pylint: disable=C0415,W0212
        CREDENTIAL_ERRORS,
        INCOMPLETE_CODE,
        _error_types,
        _personal_credential,
        _uses_private_credential,
        _warning,
    )
    review = {"blocking": [], "sensitive": [], "notices": [], "fingerprint": None}
    toolkit = session.get(EliteATool, toolkit_id)
    name = toolkit.name or toolkit.type
    try:
        expand_toolkit_settings(toolkit.type, toolkit.settings or {}, project_id, owner_id)
    except ConfigurationExpandError as error:
        if _error_types(error.errors) & CREDENTIAL_ERRORS:
            review["blocking"].append(_warning(
                "credential_missing", f'Toolkit "{name}" has no usable credential for the owner',
                toolkit=name, toolkit_type=toolkit.type))
        else:
            review["notices"].append(_warning(INCOMPLETE_CODE, f'Toolkit "{name}" could not be checked',
                                              toolkit=name, toolkit_type=toolkit.type))
    except Exception:  # noqa: BLE001 -- a failed scan is an incomplete notice, never a 500
        log.exception("Index check: cannot expand the settings of toolkit %s", toolkit_id)
        review["notices"].append(_warning(INCOMPLETE_CODE, f'Toolkit "{name}" could not be checked',
                                          toolkit=name, toolkit_type=toolkit.type))
    else:
        if _uses_private_credential(toolkit):
            review["notices"].append(_personal_credential(toolkit, name))
    return review


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
        A Toolkit Index (`entity_type` `toolkit_index`, with `toolkit_id` and `index_name`, no version) is
        checked by the credential of its toolkit: `blocking` has `credential_missing`, `notices` has
        `personal_credential`, `sensitive` is empty and `fingerprint` is null.
        Who may call: a project member with `schedules.create`, as for a create.
        Errors: 400 invalid field, a version that is not of the agent or pipeline, or an agent or pipeline
        that is not the one of `schedule_id`; 403, also `not_owner` for another version without the right
        to edit the schedule; 404 not visible, also a `schedule_id` the caller may not see.
        """
        payload = request.get_json(silent=True)
        if isinstance(payload, dict) and payload.get("entity_type") == TOOLKIT_INDEX:
            return self._check_index_risks(project_id)
        from .schedule_risk import scan_complete, scan_schedule
        from .schedule_settings import is_run_without_asking_allowed
        body, error = _parse_body(ScheduleRiskCheck)
        if error:
            return error
        owner_id, stored, rule = auth.current_user()["id"], None, body.approval_rule
        with db.get_session(project_id) as session:
            try:
                if body.schedule_id is not None:
                    scope = schedule_scope(project_id)
                    row = load_schedule_in_scope(session, body.schedule_id, scope)
                    refused = _check_scope_error(row, body, scope)
                    if refused:
                        return refused
                    owner_id, stored = row.created_by, _kept_fingerprint(row)
                    rule = rule or row.approval_rule
                check_entity(session, project_id, body.entity_type, body.application_id, body.version_id)
            except ScheduleError as e:
                return e.response
        review = scan_schedule(project_id, body.version_id, owner_id, rule)
        allowed = bool(is_run_without_asking_allowed(project_id)) and scan_complete(review)
        answer = {**review, "run_without_asking_allowed": allowed}
        if body.schedule_id is not None:
            answer["confirmed"] = stored is not None and stored == review["fingerprint"]
        return answer, 200

    def _check_index_risks(self, project_id: int):
        """The risk check of a Toolkit Index; the rights are the ones of `_check_risks`, which calls it."""
        body, error = _parse_body(IndexRiskCheck)
        if error:
            return error
        owner_id = auth.current_user()["id"]
        with db.get_session(project_id) as session:
            try:
                if body.schedule_id is not None:
                    row = load_schedule_in_scope(session, body.schedule_id, schedule_scope(project_id))
                    for field in ("entity_type", "toolkit_id", "index_name"):
                        if getattr(body, field) != getattr(row, field):
                            return refusal(INVALID_INPUT, f"{field} must be the one of the schedule", field=field)
                    owner_id = row.created_by
                _check_index_target(session, project_id, body.toolkit_id, body.index_name)
            except ScheduleError as e:
                return e.response
            answer = {**_index_review(session, project_id, body.toolkit_id, owner_id),
                      "run_without_asking_allowed": False}
        if body.schedule_id is not None:
            answer["confirmed"] = False
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
                check_entity(session, project_id, body.entity_type, body.application_id, body.version_id,
                             body.toolkit_id, body.index_name)
                review, confirmed = _review_save(
                    project_id, body.version_id, scope.user_id, body.name, body.approval_rule, body.risk_ack,
                )
                row = create_schedule(session, project_id, body, scope, confirmed=confirmed)
                session.commit()
            except ScheduleError as e:
                return e.response
            except ScheduleLimitError as e:
                return refusal(LIMIT_REACHED, str(e), 409)
            notice = _governance_notice(self.module, session, project_id, row, scope.user_id)
            answer = _saved(row, scope, project_id, review, notice)
            if getattr(row, "new_secret", None):  # shown once: no read answers it again
                answer["secret"] = row.new_secret
            return answer, 201

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

        The risk review runs on the resulting version, name and rule, as for a create.
        Who may call: the owner with `schedules.update`, or a schedules admin (the owner gets a notice).
        Errors: 400 invalid field, `enabled` in the body, "run without asking" not allowed or a wrong
        confirmed name (`field: risk_ack`); 403; 404; 409 `schedule_blocked`,
        `risk_confirmation_required` or `risk_changed`.
        """
        raw = request.get_json(silent=True)
        if isinstance(raw, dict) and "enabled" in raw:
            return ENABLED_NOT_EDITABLE
        body, error = _parse_body(ScheduleEdit)
        if error:
            return error
        refused = _refuse_unallowed_rule(project_id, body)
        if refused:
            return refused
        return _apply_update(project_id, schedule_id, body, reviewed=True, module=self.module)

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

        A webhook runs only as `test` with a sample `payload`, through the delivery path (#7455).

        Who may call: the owner with `schedules.run`; an admin sees the schedule but cannot run it.
        A Toolkit Index has no version: it answers 409 `run_active` while a run of the index is active, and the
        202 names `toolkit_id` and `index_name` instead of `application_id`.
        Errors: 400 invalid trigger or payload; 403 not the owner; 404; 409 deleted version or an active run
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
            is_index = row.entity_type == TOOLKIT_INDEX
            if row.version_id is None and not is_index:  # an index has no version by design
                return refusal("version_deleted", "The version of this schedule was deleted; choose a version", 409)
            sample_error = _sample_body_error(row, body)
            if sample_error:
                return sample_error
            if row.trigger_kind == "webhook":
                return _start_sample_run(session, project_id, row, body.payload)
            entity_type = row.entity_type
            try:   # the owner's own run (#7254), from this signed-in request; None when off. Built before the run row
                if is_index:
                    busy = _index_busy(session, project_id, row.toolkit_id, row.index_name)
                    if busy:
                        return busy
                from .mandate_admission import owner_run_request, schedule_trigger_ref  # pylint: disable=C0415
                if is_index:
                    owner_request = _index_admission(
                        self.module, project_id, row.toolkit_id, schedule_trigger_ref(schedule_id))
                else:
                    owner_request = owner_run_request(
                        self.module, project_id=project_id, application_id=row.application_id,
                        version_id=row.version_id, trigger_ref=schedule_trigger_ref(schedule_id))
            except Exception:  # noqa: BLE001 -- nothing is stored yet; the owner can try again
                log.exception('Schedule %s: cannot build the owner run request', schedule_id)
                return refusal("run_start_failed", "The run could not start", 500)
            return _start_run(
                session, project_id, schedule_id, entity_type, _reply_target(row), body.trigger, scope.user_id,
                owner_request)

    @auth.decorators.check_api(schedule_check_spec("run"))
    def _test_run(self, project_id: int):
        """Test the form of a schedule that is not saved yet; no schedule row is written.

        The body is the Save body. It is stored on one run row of trigger `test` that has no schedule, and the run
        starts with the same checks as a saved run. The run never reaches the schedule list, Last Run, Next Run,
        the tick, the cap or an export. The caller keeps the newest 50 Test Runs of an agent, pipeline or index.
        A Toolkit Index body names `toolkit_id` and `index_name`; an active run of that index, whoever started it,
        refuses the Test Run.
        Who may call: a project member with `schedules.run`.
        Errors: 400 invalid field (the Save rules, `approval_rule_not_allowed`); 403; 404 agent or pipeline not
        found or not visible; 409 `run_active` (a Test Run of this agent, pipeline or index is active, the body has
        `active_run_id`); 500 `run_start_failed`; 503 `test_run_unavailable` (the admin task
        `migrate_schedule_test_runs` did not run in this project).
        """
        body, error = _parse_body(ScheduleCreate)
        if error:
            return error
        if body.trigger_kind == "webhook":  # a webhook is tried with a sample body (#7399), not with a Test Run
            return refusal(INVALID_INPUT, "A Test Run is for schedules; a webhook is tried with a sample delivery",
                           field="trigger_kind")
        refused = _refuse_unallowed_rule(project_id, body)
        if refused:
            return refused
        scope = schedule_scope(project_id)
        is_index = body.entity_type == TOOLKIT_INDEX
        with db.get_session(project_id) as session:
            try:
                _check_test_body(session, project_id, body)
                if is_index:
                    busy = _index_busy(session, project_id, body.toolkit_id, body.index_name)
                    if busy:
                        return busy
                run_id = insert_test_run(session, scope.user_id, body)
                if run_id is None:
                    active_id = active_test_run_id(session, scope.user_id, body.application_id)
                    return refusal("run_active", "A test run of this schedule is active", 409, active_run_id=active_id)
            except ScheduleError as e:
                return e.response
            except TestRunUnavailable:
                session.rollback()
                return TEST_RUN_UNAVAILABLE
            try:
                from .mandate_admission import owner_run_request, test_run_trigger_ref  # pylint: disable=C0415
                if is_index:
                    owner_request = _index_admission(
                        self.module, project_id, body.toolkit_id, test_run_trigger_ref(run_id))
                else:
                    owner_request = owner_run_request(
                        self.module, project_id=project_id, application_id=body.application_id,
                        version_id=body.version_id, trigger_ref=test_run_trigger_ref(run_id))
            except Exception:  # noqa: BLE001 -- the owner can try again; the rollback leaves nothing stored
                session.rollback()
                log.exception('Test run of %s: cannot build the owner run request', _body_target(body))
                return refusal("run_start_failed", "The run could not start", 500)
            trimmed = trim_test_runs(session, scope.user_id, **_body_target(body))
            session.commit()
            started = start_schedule_run(body.entity_type, project_id, None, run_id, owner_request)
            if not started:
                fail_run(session, run_id, START_FAILED)
                session.commit()
        remove_trimmed_chats(project_id, trimmed)  # after the commit: a bucket cannot be rolled back
        if not started:
            return refusal("run_start_failed", "The run could not start", 500)
        return {"run_id": run_id, **_body_target(body)}, 202

    @auth.decorators.check_api(schedule_check_spec("run"))
    def _decide_run(self, project_id: int, schedule_id: int, run_id: int):
        """Approve or reject a waiting run that has no chat to answer in (it waits for credentials).

        Approve checks the credentials first: while one is missing the answer is 409 and the run keeps waiting.
        Otherwise approve starts the run again. A run with a chat is decided in that chat.
        Who may call: the owner with `schedules.run`; an admin cannot decide.
        Errors: 400 invalid action; 403 not the owner; 404; 409 the run is not waiting, has a chat, or a credential is still missing.
        """
        body, error = _parse_body(ScheduleRunDecision)
        if error:
            return error
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                row = load_schedule_in_scope(session, schedule_id, scope)
            except ScheduleError as e:
                return e.response
            run = session.get(EntityScheduleRun, run_id)
            if run is None or run.schedule_id != schedule_id:
                return RUN_NOT_FOUND
            owner = SimpleNamespace(created_by=None, application_id=row.application_id, version_id=row.version_id)
            return _decide(project_id, session, scope, run, row.entity_type, owner, body)

    @auth.decorators.check_api(schedule_check_spec("run"))
    def _decide_test_run(self, project_id: int, run_id: int):
        """Approve or reject a waiting Test Run that has no chat to answer in (it waits for credentials).

        The same checks as the decision on a saved run. The credential check uses the agent or pipeline and the
        version of the stored form.
        Who may call: the caller who started the Test Run, with `schedules.run`.
        Errors: 400 invalid action; 403; 404 not the caller's Test Run; 409 `run_not_waiting`, `run_has_chat` or
        `credential_missing`; 500 `run_start_failed`; 503 `test_run_unavailable`.
        """
        body, error = _parse_body(ScheduleRunDecision)
        if error:
            return error
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                run = load_test_run(session, scope.user_id, run_id)
                if run is None:
                    return RUN_NOT_FOUND
                form = form_of_test_run(session, run_id)
            except TestRunUnavailable:
                return TEST_RUN_UNAVAILABLE
            owner = SimpleNamespace(created_by=None, application_id=form.get("application_id"),
                                    version_id=form.get("version_id"))
            return _decide(project_id, session, scope, run, form.get("entity_type"), owner, body)

    @auth.decorators.check_api(schedule_check_spec("run"))
    def _test_runs(self, project_id: int):
        """List the caller's own Test Runs of an agent, pipeline or Toolkit Index, newest first.

        Query: `application_id`, or `toolkit_id` with `index_name` (one of the two is required), `limit`, `offset`, and the `status`, `trigger`, `version_id` filters of the
        runs list. The answer is `{rows, total}` with the row fields of the runs list.
        Who may call: a project member with `schedules.run`; only the caller's own runs are read.
        Errors: 400 invalid query option, or no target or two targets; 403; 503 `test_run_unavailable`.
        """
        try:
            query = TestRunsQuery.model_validate(dict(request.args))
        except ValidationError as e:
            return _validation_response(e)
        target, refused = _test_run_target(query)
        if refused:
            return refused
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                page, usage_runs = _runs_page(session, test_run_scope(scope.user_id, **target), query)
            except DBAPIError as e:
                return _unavailable_or_raise(e)
        return _add_usage(project_id, page, usage_runs), 200

    @auth.decorators.check_api(schedule_check_spec("run"))
    def _test_run_detail(self, project_id: int, run_id: int):
        """Answer one of the caller's own Test Runs, with the body of the run detail of a saved schedule.

        Query: `application_id`, or `toolkit_id` with `index_name` (one of the two is required). The `approval_rule`
        is the one of the stored form.
        Who may call: a project member with `schedules.run`; another owner's run, a run of a saved schedule and a
        run of another agent, pipeline or index are 404.
        Errors: 400 invalid target, or no target or two targets; 403; 404 `run_not_found`; 503 `test_run_unavailable`.
        """
        try:
            query = TestRunTarget.model_validate(dict(request.args))
        except ValidationError as e:
            return _validation_response(e)
        target, refused = _test_run_target(query)
        if refused:
            return refused
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                run = load_test_run(session, scope.user_id, run_id, **target)
                if run is None:
                    return RUN_NOT_FOUND
                rule = form_of_test_run(session, run_id).get("approval_rule")
            except TestRunUnavailable:
                return TEST_RUN_UNAVAILABLE
            return _run_detail_body(session, project_id, run, rule)

    @auth.decorators.check_api(schedule_check_spec("details"))
    def _runs(self, project_id: int, schedule_id: int):
        """Answer the run records of a schedule, newest first, with the total before paging.

        Who may call: the owner with `schedules.details`, or a schedules admin.
        Each row has `version_label`, `duration_ms`, `tokens` and `cost_usd` (null while the version is gone or
        the usage store has no row). `trigger` is the stored value: `cron`, `manual` or `test`.
        Filters: `status`, `trigger` and `version_id`; they combine.
        Errors: 400 invalid query option (an unknown `status` or `trigger`, or a `version_id` that is not an id);
        403; 404.
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
            page, usage_runs = _runs_page(session, [EntityScheduleRun.schedule_id == schedule_id], query)
        return _add_usage(project_id, page, usage_runs), 200

    @auth.decorators.check_api(schedule_check_spec("details"))
    def _run_detail(self, project_id: int, schedule_id: int, run_id: int):
        """Answer one run of a schedule for the Test schedule panel, with its usage summary and event log.

        The events are the stored ones and the tool calls of the run conversation, in time order, at most 200.
        A tool call carries the full tool result in `summary`: raw tool output, up to 20 000 characters.
        Tokens and cost are null while the usage store has no row for the conversation.
        Who may call: the owner with `schedules.details`, or a schedules admin.
        Errors: 403; 404 the schedule or the run is not found, or another owner's schedule.
        """
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                schedule = load_schedule_in_scope(session, schedule_id, scope)
            except ScheduleError as e:
                return e.response
            run = session.scalar(select(EntityScheduleRun).where(
                EntityScheduleRun.id == run_id, EntityScheduleRun.schedule_id == schedule.id))
            if run is None:
                return RUN_NOT_FOUND
            return _run_detail_body(session, project_id, run, schedule.approval_rule)

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
                if row.trigger_kind == "webhook":
                    audit_schedule_event(project_id, row, scope.user_id, "webhook_owner_changed",
                                         f"{change['old_owner_id']} -> {owner_id}")
                _notify_owner_change(project_id, row, change)
            return _read_answer(row, scope, project_id), 200

    @auth.decorators.check_api(schedule_check_spec("update"))
    def _secret(self, project_id: int, schedule_id: int):
        """Rotate (old secret valid 24 h more) or regenerate (at once) a webhook secret; answers it once."""
        body, error = _parse_body(WebhookSecretAction)
        if error:
            return error
        from .webhook_secrets import (  # pylint: disable=C0415 -- late: only this route writes secrets
            RotationInProgress,
            is_signing_row,
            regenerate_row_secret,
            rotate_row_secret,
            signing_value_error,
        )
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                row = load_schedule_in_scope(session, schedule_id, scope)
            except ScheduleError as e:
                return e.response
            if row.trigger_kind != "webhook":
                return refusal("not_a_webhook", "Only a webhook has a secret", 400)
            signing = is_signing_row(row)
            if signing != (body.signing_secret_value is not None):
                return refusal(INVALID_INPUT, "Send signing_secret_value for GitLab signing tokens, and only then",
                               field="signing_secret_value")
            if signing and (reason := signing_value_error(body.signing_secret_value)):
                return refusal(INVALID_INPUT, reason, field="signing_secret_value")
            try:
                if body.mode == "rotate":
                    value = rotate_row_secret(session, project_id, row, datetime.now(UTC), body.signing_secret_value,
                                              updated_by=scope.user_id)
                else:
                    value = regenerate_row_secret(session, project_id, row, body.signing_secret_value,
                                                  updated_by=scope.user_id)
            except RotationInProgress:
                until = row.previous_secret_until
                session.rollback()
                return refusal("rotation_in_progress", ROTATION_IN_PROGRESS, 409, previous_secret_until=_iso(until))
            except Exception as e:  # noqa: BLE001 -- the vault answer must not leak; the old secret stays
                session.rollback()
                log.error("Schedule %s: cannot %s the webhook secret: %s", schedule_id, body.mode, type(e).__name__)
                return refusal("secret_store_unavailable", "The webhook secret could not be changed; try again", 503)
            audit_schedule_event(project_id, row, scope.user_id, f"webhook_secret_{body.mode}d")
            if row.created_by != scope.user_id:
                notify_owner(project_id, row.created_by, row, action=f"{body.mode}d the secret of")
            answer = _read_answer(row, scope, project_id)
            if not signing:
                answer["secret"] = value
            return answer, 200
