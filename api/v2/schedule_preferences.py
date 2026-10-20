"""The caller's own "Get notified" value for schedule notices (EL-7011, ADR-0009)."""
from typing import ClassVar

from flask import request
from pydantic import BaseModel, StrictBool, ValidationError
from tools import api_tools, auth, db, register_openapi, serialize

from ...models.entity_schedule import EntitySchedulePreference
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.schedule_access import schedule_check_spec

OPENAPI_TAG = "elitea_core/schedules"
INVALID_BODY = {"error": "get_notified must be true or false"}


class SchedulePreferenceUpdate(BaseModel):
    get_notified: StrictBool


class PromptLibAPI(api_tools.APIModeHandler):
    """A project member reads and saves their own "Get notified" value."""

    @register_openapi(
        name="Get Schedule Notification Preference",
        description="Whether the caller gets a notice when a schedule run succeeds. True when never saved.",
        tags=[OPENAPI_TAG],
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("list"))
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        with db.get_session(project_id) as session:
            row = session.get(EntitySchedulePreference, auth.current_user()["id"])
            return serialize({"get_notified": row.get_notified if row else True}), 200

    @register_openapi(
        name="Save Schedule Notification Preference",
        description="Save the caller's own value. The user id is never taken from the request.",
        tags=[OPENAPI_TAG],
        request_body=SchedulePreferenceUpdate,
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("list"))
    @api_tools.endpoint_metrics
    def put(self, project_id: int, **kwargs):
        try:
            value = SchedulePreferenceUpdate.model_validate(request.get_json(silent=True)).get_notified
        except ValidationError:
            return INVALID_BODY, 400
        with db.get_session(project_id) as session:
            user_id = auth.current_user()["id"]
            row = session.get(EntitySchedulePreference, user_id)
            if row:
                row.get_notified = value
            else:
                session.add(EntitySchedulePreference(user_id=user_id, get_notified=value))
            session.commit()
        return {"get_notified": value}, 200


class API(api_tools.APIBase):
    url_params: ClassVar[list] = api_tools.with_modes(["<int:project_id>"])

    mode_handlers: ClassVar[dict] = {PROMPT_LIB_MODE: PromptLibAPI}
