"""Create, read, update and delete of one entity schedule (EL-7011, ADR-0004)."""
from typing import ClassVar, get_args

from flask import request
from pydantic import BaseModel, Field, ValidationError
from pylon.core.tools import log
from sqlalchemy import func, select
from tools import api_tools, auth, db, register_openapi

from ...models.entity_schedule import EntityScheduleRun
from ...models.enums.all import NotificationEventTypes
from ...models.pd.entity_schedule import ScheduleCreate, ScheduleDetails, ScheduleUpdate
from ...models.pd.schedule_list import RunStatus
from ...models.pd.schedule_run import ScheduleRunRequest
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.constants import SCHEDULES_OPENAPI_TAG as OPENAPI_TAG
from ...utils.constants import SCHEDULES_PROJECT_PATH as PROJECT_PATH
from ...utils.schedule_access import (
    ScheduleScope,
    allowed_actions,
    schedule_check_spec,
    schedule_scope,
)
from ...utils.schedule_claim import fail_run, insert_run
from ...utils.schedule_dispatch import START_FAILED, start_schedule_run
from ...utils.schedule_notifications import notify_schedule_event
from ...utils.schedule_service import (
    ScheduleError,
    create_schedule,
    delete_schedule,
    load_schedule_in_scope,
    update_schedule,
)
from ...utils.schedule_timing import ScheduleLimitError

SCHEDULE_PATH = f"{PROJECT_PATH}/<int:schedule_id>"
SCHEDULE_URL = f"/api/v2/elitea_core/schedule/{SCHEDULE_PATH}"
RESPONSE_TEXT = {
    200: "Success",
    201: "Created",
    202: "The run started; the body has `run_id`",
    204: "Deleted",
    400: "Invalid input; the body names the error and the field",
    403: "No permission, or not the owner",
    404: "Schedule not found, or another owner's schedule",
    409: "Conflict: schedule limit, deleted version or an active run",
    500: "Server error",
    503: "A service the check needs is unavailable",
}


def _responses(*codes: int) -> dict:
    """The documented answers of an operation, by status code."""
    return {str(code): {"description": RESPONSE_TEXT[code]} for code in codes}


def _iso(moment) -> str | None:
    return moment.isoformat() if moment else None


def _owner_name(owner_id: int) -> str | None:
    """The owner's name, else the email, as the list shows it; a failed lookup gives None (US-32 R2.2)."""
    try:
        # Late import: the auth lookup is needed only for the details answer
        from ...utils.authors import get_authors_data  # pylint: disable=C0415
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
    from ...utils.schedule_risk import is_run_without_asking_allowed
    if body.approval_rule == "run_without_asking" and not is_run_without_asking_allowed(project_id):
        return {"error": "approval_rule_not_allowed", "field": "approval_rule"}, 400
    return None


def _saved(row, scope: ScheduleScope, project_id: int) -> dict:
    """Details of a saved row, with the save warnings; a warning never blocks or changes the save (R2.4)."""
    from ...utils.schedule_risk import scan_schedule_risks
    return {**_read(row, scope, project_id), "warnings": scan_schedule_risks(project_id, row.version_id, row.created_by)}


def _read(row, scope: ScheduleScope, project_id: int) -> dict:
    from ...utils.schedule_risk import is_run_without_asking_allowed
    return {**_details(row, scope), "run_without_asking_allowed": bool(is_run_without_asking_allowed(project_id))}


NOT_ALLOWED = {"error": "Method not allowed"}, 405


def _on_run_path() -> bool:
    """True for the `<project>/<schedule>/run` URL, which shares this resource with the schedule URL."""
    return request.path.rstrip("/").endswith("/run")


def _on_owner_path() -> bool:
    """True for the `<project>/<schedule>/owner` URL, which shares this resource with the schedule URL."""
    return request.path.rstrip("/").endswith("/owner")


def _on_runs_path() -> bool:
    """True for the `<project>/<schedule>/runs` URL, which shares this resource with the schedule URL."""
    return request.path.rstrip("/").endswith("/runs")


class RunsQuery(BaseModel):
    """Query of the run records list; `limit` above the cap is lowered to it (R3.3, R3.5)."""

    limit: int = Field(default=20, ge=1)
    offset: int = Field(default=0, ge=0)
    status: RunStatus | None = None


MAX_RUNS_LIMIT = 100
PATH_PARAMS = [
    {"name": "mode", "in": "path", "required": True, "schema": {"type": "string", "default": PROMPT_LIB_MODE},
     "description": f"Mode (default: {PROMPT_LIB_MODE})"},
    {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Project id"},
    {"name": "schedule_id", "in": "path", "required": True, "schema": {"type": "integer"},
     "description": "Schedule id"},
]
RUNS_QUERY_PARAMS = [
    {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 20, "minimum": 1},
     "description": f"Rows per page; a value above {MAX_RUNS_LIMIT} is lowered to {MAX_RUNS_LIMIT}"},
    {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0, "minimum": 0},
     "description": "Rows to skip"},
    {"name": "status", "in": "query", "schema": {"type": "string", "enum": list(get_args(RunStatus))},
     "description": "Only runs with this status"},
]
OWNER_BODY = {"required": True, "content": {"application/json": {"schema": {
    "type": "object", "required": ["owner_id"],
    "properties": {"owner_id": {"type": "integer", "description": "User id of the new owner, a project member"}},
}, "examples": {"example1": {"value": {"owner_id": 20}}}}}}
# The router serves these URLs with the handlers below, so register_api_class cannot list them;
# Module._register_openapi registers them (BUG-11)
OPENAPI_SUB_ACTIONS = [
    {"path": f"{SCHEDULE_URL}/run", "method": "post", "name": "Run Schedule",
     "description": "Start a run now (`manual`) or a test run (`test`) of the caller's own schedule.",
     "parameters": PATH_PARAMS, "request_body": ScheduleRunRequest,
     "responses": _responses(202, 400, 403, 404, 409, 500)},
    {"path": f"{SCHEDULE_URL}/owner", "method": "post", "name": "Change Schedule Owner",
     "description": "Set a new owner. Schedules admin only; the old and the new owner get a notice.",
     "parameters": PATH_PARAMS, "request_body": OWNER_BODY, "responses": _responses(200, 400, 403, 404, 503)},
    {"path": f"{SCHEDULE_URL}/runs", "method": "get", "name": "List Schedule Runs",
     "description": "Run records of one schedule, newest first, with the total before paging.",
     "parameters": PATH_PARAMS + RUNS_QUERY_PARAMS, "responses": _responses(200, 400, 403, 404)},
]
RUN_FIELDS = (
    "id", "trigger", "status", "reason", "conversation_id", "requested_by", "started_at", "finished_at",
)


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


def _notify_owner_change(project_id: int, row, change: dict) -> None:
    """Tell the old and the new owner, both ids in the meta (R3.2); a failure is logged, the change stays."""
    for user_id in change.values():
        try:
            notify_schedule_event(project_id, user_id, NotificationEventTypes.schedule_owner_changed, row, extra=change)
        except Exception as e:  # noqa: BLE001 -- the change is committed; a lost notice must not fail it
            log.error("Schedule %s: cannot send the owner change notice: %s", row.id, e)


class PromptLibAPI(api_tools.APIModeHandler):
    """Schedule of the caller; a user without `schedules.admin` sees only their own rows."""

    @register_openapi(
        name="Create Schedule",
        description="Create a schedule owned by the caller. The owner is never taken from the body.",
        tags=[OPENAPI_TAG],
        request_body=ScheduleCreate,
        responses=_responses(201, 400, 403, 404, 409),
        available_to_users=True,
        path_suffix_override=PROJECT_PATH,
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, schedule_id: int | None = None, **kwargs):
        # The router matches by verb only, so the URL tail tells a run from a post to the schedule itself.
        # Keyword arguments: check_api reads project_id from them for the project-scoped permission check.
        if schedule_id is None:
            return self._create(project_id=project_id)
        if _on_owner_path():
            return self._change_owner(project_id=project_id, schedule_id=schedule_id)
        if not _on_run_path():
            return NOT_ALLOWED
        return self._run(project_id=project_id, schedule_id=schedule_id)

    @auth.decorators.check_api(schedule_check_spec("create"))
    def _create(self, project_id: int):
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

    @auth.decorators.check_api(schedule_check_spec("run"))
    def _run(self, project_id: int, schedule_id: int):
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

    @auth.decorators.check_api(schedule_check_spec("admin"))
    def _change_owner(self, project_id: int, schedule_id: int):
        # late, like schedule_risk: only the owner route needs the owner and auth RPC checks
        from ...utils.schedule_owner import (
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
            return _read(row, scope, project_id), 200

    @register_openapi(
        name="Get Schedule",
        description="Read one schedule. Another owner's schedule answers 404 unless the caller is a schedules admin.",
        tags=[OPENAPI_TAG],
        responses=_responses(200, 403, 404),
        available_to_users=True,
        path_suffix_override=SCHEDULE_PATH,
    )
    @auth.decorators.check_api(schedule_check_spec("details"))
    @api_tools.endpoint_metrics
    def get(self, project_id: int, schedule_id: int, **kwargs):
        if _on_run_path() or _on_owner_path():  # only POST serves the run and owner URLs
            return NOT_ALLOWED
        if _on_runs_path():
            return self._runs(project_id=project_id, schedule_id=schedule_id)
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                return _read(load_schedule_in_scope(session, schedule_id, scope), scope, project_id), 200
            except ScheduleError as e:
                return e.response

    def _runs(self, project_id: int, schedule_id: int):
        """Run records of a schedule in scope, newest first, with the total before paging."""
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

    @register_openapi(
        name="Update Schedule",
        description="Edit, pause or resume a schedule. The owner never changes.",
        tags=[OPENAPI_TAG],
        request_body=ScheduleUpdate,
        responses=_responses(200, 400, 403, 404, 409),
        available_to_users=True,
        path_suffix_override=SCHEDULE_PATH,
    )
    @auth.decorators.check_api(schedule_check_spec("update"))
    @api_tools.endpoint_metrics
    def put(self, project_id: int, schedule_id: int, **kwargs):
        if _on_run_path() or _on_owner_path() or _on_runs_path():  # POST serves run and owner, GET runs
            return NOT_ALLOWED
        body, error = _parse_body(ScheduleUpdate)
        if error:
            return error
        refused = _refuse_unallowed_rule(project_id, body)
        if refused:
            return refused
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

    @register_openapi(
        name="Delete Schedule",
        description="Delete a schedule. Run conversations stay.",
        tags=[OPENAPI_TAG],
        responses=_responses(204, 403, 404),
        available_to_users=True,
        path_suffix_override=SCHEDULE_PATH,
    )
    @auth.decorators.check_api(schedule_check_spec("delete"))
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, schedule_id: int, **kwargs):
        if _on_run_path() or _on_owner_path() or _on_runs_path():  # POST serves run and owner, GET runs
            return NOT_ALLOWED
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                delete_schedule(session, project_id, load_schedule_in_scope(session, schedule_id, scope), scope)
            except ScheduleError as e:
                return e.response
        return None, 204


class API(api_tools.APIBase):
    url_params: ClassVar[list] = api_tools.with_modes([
        "<int:project_id>", "<int:project_id>/<int:schedule_id>", "<int:project_id>/<int:schedule_id>/run",
        "<int:project_id>/<int:schedule_id>/owner", "<int:project_id>/<int:schedule_id>/runs",
    ])

    mode_handlers: ClassVar[dict] = {PROMPT_LIB_MODE: PromptLibAPI}
