"""Create, read, update and delete of one entity schedule (EL-7011, ADR-0004)."""
from typing import ClassVar, Literal

from flask import request
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import func, select
from tools import api_tools, auth, db, register_openapi

from ...models.entity_schedule import EntityScheduleRun
from ...models.pd.entity_schedule import ScheduleCreate, ScheduleDetails, ScheduleUpdate
from ...models.pd.schedule_run import ScheduleRunRequest
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.schedule_access import (
    ScheduleScope,
    allowed_actions,
    schedule_check_spec,
    schedule_scope,
)
from ...utils.schedule_claim import fail_run, insert_run
from ...utils.schedule_dispatch import START_FAILED, start_schedule_run
from ...utils.schedule_service import (
    ScheduleError,
    create_schedule,
    delete_schedule,
    load_schedule_in_scope,
    update_schedule,
)
from ...utils.schedule_timing import ScheduleLimitError

OPENAPI_TAG = "elitea_core/schedules"


def _iso(moment) -> str | None:
    return moment.isoformat() if moment else None


def _details(row, scope: ScheduleScope) -> dict:
    """Shape a row as the API answers it; the owner name stays empty until a story needs it."""
    fields = ScheduleDetails.model_fields.keys() - {"owner", "allowed_actions"}
    return ScheduleDetails.model_validate({
        **{name: getattr(row, name) for name in fields},
        "owner": {"id": row.created_by},
        "allowed_actions": allowed_actions(row, scope),
    }).model_dump(mode="json")


NOT_ALLOWED = {"error": "Method not allowed"}, 405


def _on_run_path() -> bool:
    """True for the `<project>/<schedule>/run` URL, which shares this resource with the schedule URL."""
    return request.path.rstrip("/").endswith("/run")


def _on_runs_path() -> bool:
    """True for the `<project>/<schedule>/runs` URL, which shares this resource with the schedule URL."""
    return request.path.rstrip("/").endswith("/runs")


class RunsQuery(BaseModel):
    """Query of the run records list; `limit` above the cap is lowered to it (R3.3, R3.5)."""

    limit: int = Field(default=20, ge=1)
    offset: int = Field(default=0, ge=0)
    status: Literal["running", "success", "failed", "skipped", "blocked", "waiting_for_approval"] | None = None


MAX_RUNS_LIMIT = 100
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


class PromptLibAPI(api_tools.APIModeHandler):
    """Schedule of the caller; a user without `schedules.admin` sees only their own rows."""

    @register_openapi(
        name="Create Schedule",
        description="Create a schedule owned by the caller. The owner is never taken from the body. "
                    "POST .../{schedule_id}/run starts a run now or a test run of the caller's own schedule.",
        tags=[OPENAPI_TAG],
        request_body=ScheduleCreate,
        available_to_users=True,
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, schedule_id: int | None = None, **kwargs):
        # The router matches by verb only, so the URL tail tells a run from a post to the schedule itself.
        # Keyword arguments: check_api reads project_id from them for the project-scoped permission check.
        if schedule_id is None:
            return self._create(project_id=project_id)
        if not _on_run_path():
            return NOT_ALLOWED
        return self._run(project_id=project_id, schedule_id=schedule_id)

    @auth.decorators.check_api(schedule_check_spec("create"))
    def _create(self, project_id: int):
        body, error = _parse_body(ScheduleCreate)
        if error:
            return error
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                row = create_schedule(session, project_id, body, scope)
            except ScheduleError as e:
                return e.response
            except ScheduleLimitError as e:
                return {"error": str(e)}, 409
            return _details(row, scope), 201

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

    @register_openapi(
        name="Get Schedule",
        description="Read one schedule. Another owner's schedule answers 404 unless the caller is a schedules admin.",
        tags=[OPENAPI_TAG],
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("details"))
    @api_tools.endpoint_metrics
    def get(self, project_id: int, schedule_id: int, **kwargs):
        if _on_run_path():  # only POST serves the run URL
            return NOT_ALLOWED
        if _on_runs_path():
            return self._runs(project_id=project_id, schedule_id=schedule_id)
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                return _details(load_schedule_in_scope(session, schedule_id, scope), scope), 200
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
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("update"))
    @api_tools.endpoint_metrics
    def put(self, project_id: int, schedule_id: int, **kwargs):
        if _on_run_path() or _on_runs_path():  # only POST serves the run URL, only GET the runs URL
            return NOT_ALLOWED
        body, error = _parse_body(ScheduleUpdate)
        if error:
            return error
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                row = load_schedule_in_scope(session, schedule_id, scope)
                return _details(update_schedule(session, project_id, row, body, scope), scope), 200
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
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("delete"))
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, schedule_id: int, **kwargs):
        if _on_run_path() or _on_runs_path():  # only POST serves the run URL, only GET the runs URL
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
        "<int:project_id>/<int:schedule_id>/runs",
    ])

    mode_handlers: ClassVar[dict] = {PROMPT_LIB_MODE: PromptLibAPI}
