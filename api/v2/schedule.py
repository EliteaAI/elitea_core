"""Create, read, update and delete of one entity schedule (EL-7011, ADR-0004)."""
from typing import ClassVar

from flask import request
from pydantic import ValidationError
from tools import api_tools, auth, db, register_openapi

from ...models.pd.entity_schedule import ScheduleCreate, ScheduleDetails, ScheduleUpdate
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.schedule_access import (
    ScheduleScope,
    allowed_actions,
    schedule_check_spec,
    schedule_scope,
)
from ...utils.schedule_service import (
    ScheduleError,
    create_schedule,
    delete_schedule,
    load_schedule_in_scope,
    update_schedule,
)
from ...utils.schedule_timing import ScheduleLimitError

OPENAPI_TAG = "elitea_core/schedules"


def _details(row, scope: ScheduleScope) -> dict:
    """Shape a row as the API answers it; the owner name stays empty until a story needs it."""
    fields = ScheduleDetails.model_fields.keys() - {"owner", "allowed_actions"}
    return ScheduleDetails.model_validate({
        **{name: getattr(row, name) for name in fields},
        "owner": {"id": row.created_by},
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


def _validation_response(error: ValidationError) -> tuple[dict, int]:
    """Answer 400 with the first broken field and its plain reason."""
    first = error.errors()[0]
    field = first["loc"][0] if first["loc"] else "body"
    return {"error": first["msg"].removeprefix("Value error, "), "field": field}, 400


class PromptLibAPI(api_tools.APIModeHandler):
    """Schedule of the caller; a user without `schedules.admin` sees only their own rows."""

    @register_openapi(
        name="Create Schedule",
        description="Create a schedule owned by the caller. The owner is never taken from the body.",
        tags=[OPENAPI_TAG],
        request_body=ScheduleCreate,
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("create"))
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        try:
            body = ScheduleCreate.model_validate(request.get_json(silent=True))
        except ValidationError as e:
            return _validation_response(e)
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

    @register_openapi(
        name="Get Schedule",
        description="Read one schedule. Another owner's schedule answers 404 unless the caller is a schedules admin.",
        tags=[OPENAPI_TAG],
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("details"))
    @api_tools.endpoint_metrics
    def get(self, project_id: int, schedule_id: int, **kwargs):
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                return _read(load_schedule_in_scope(session, schedule_id, scope), scope, project_id), 200
            except ScheduleError as e:
                return e.response

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
        try:
            body = ScheduleUpdate.model_validate(request.get_json(silent=True))
        except ValidationError as e:
            return _validation_response(e)
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
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("delete"))
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, schedule_id: int, **kwargs):
        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            try:
                delete_schedule(session, project_id, load_schedule_in_scope(session, schedule_id, scope), scope)
            except ScheduleError as e:
                return e.response
        return None, 204


class API(api_tools.APIBase):
    url_params: ClassVar[list] = api_tools.with_modes(["<int:project_id>", "<int:project_id>/<int:schedule_id>"])

    mode_handlers: ClassVar[dict] = {PROMPT_LIB_MODE: PromptLibAPI}
