"""Schedules list endpoint: GET /api/v2/elitea_core/schedules/prompt_lib/{project_id} (EL-7011, US-05)."""
from typing import ClassVar

from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from tools import api_tools, auth, db, register_openapi

from ...models.pd.schedule_list import ScheduleListFilters, ScheduleListItem
from ...utils.authors import get_authors_data
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.schedule_access import (
    allowed_actions,
    schedule_check_spec,
    schedule_scope,
)
from ...utils.schedule_queries import list_schedules


def _owner_names(owner_ids: set[int]) -> dict[int, str | None]:
    """Map owner id to name; a failed lookup gives an empty map, so every name is null (R1.3)."""
    if not owner_ids:  # an empty id list makes auth_core return every user
        return {}
    try:
        return {user["id"]: user.get("name") for user in get_authors_data(sorted(owner_ids))}
    except Exception as e:  # noqa: BLE001  # pylint: disable=W0718 -- names must never break the list (R1.3)
        log.warning(f"Schedule owner names lookup failed: {e}")
        return {}


class PromptLibAPI(api_tools.APIModeHandler):
    """Lists the schedules of a project; a regular caller sees own rows only."""

    @register_openapi(
        name="List Schedules",
        description="List schedules with filters and paging; owner names and allowed actions per row.",
        tags=["elitea_core/schedules"],
        available_to_users=True,
    )
    @auth.decorators.check_api(schedule_check_spec("list"))
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        """Return `{"total", "rows"}` for one page (R1.1-R5.3)."""
        args = request.args.to_dict()
        owner_ids = request.args.getlist('owner_ids')
        if owner_ids:  # to_dict keeps only the first value of a repeated key
            args['owner_ids'] = ','.join(owner_ids)
        try:
            filters = ScheduleListFilters(**args)
        except ValidationError as e:
            name = e.errors()[0]["loc"][0]
            if name == "limit":
                return {"error": "limit must be between 1 and 100"}, 400
            return {"error": f"{name} is not valid"}, 400

        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            total, rows = list_schedules(session, scope, filters, filters.limit, filters.offset)
        names = _owner_names({row.created_by for row in rows})
        items = [
            ScheduleListItem(
                **row._mapping,
                owner={"id": row.created_by, "name": names.get(row.created_by)},
                allowed_actions=allowed_actions(row, scope),
            ).model_dump(mode="json")  # one ISO offset per date; serialize() adds a second 'Z'
            for row in rows
        ]
        return {"total": total, "rows": items}, 200


class API(api_tools.APIBase):
    """URL pattern: /api/v2/elitea_core/schedules/prompt_lib/{project_id}."""
    url_params: ClassVar[list[str]] = api_tools.with_modes(['<int:project_id>'])

    mode_handlers: ClassVar[dict] = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
