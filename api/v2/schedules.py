"""Schedules list endpoint and the admin-only YAML file routes (EL-7011, US-05, US-22).

GET .../schedules/prompt_lib/{project_id} lists; GET .../export writes the file; POST .../import reads one.
"""
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

NOT_ALLOWED = {"error": "Method not allowed"}, 405


def _last_segment() -> str:
    """The URL tail that tells the list from `export` and `import`; the resource shares one verb per URL."""
    return request.path.rstrip("/").rsplit("/", 1)[-1]


def _parse_ids(raw: str) -> list[int] | None:
    """Ids of `1,2`; ASCII digits only. None when the text is not such a list."""
    parts = raw.split(",")
    if not all(part.isascii() and part.isdigit() for part in parts):
        return None
    return [int(part) for part in parts]


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
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        # One check_api per helper, called with keyword arguments: each URL needs its own permission.
        if _last_segment() == "export":
            return self._export(project_id=project_id)
        return self._list(project_id=project_id)

    @register_openapi(
        name="Import Schedules",
        description="Import a schedule YAML file (multipart `file`). Every created schedule is disabled "
                    "and owned by the caller. Answers the created, skipped and failed items.",
        tags=["elitea_core/schedules"],
        available_to_users=True,
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        if _last_segment() != "import":
            return NOT_ALLOWED
        return self._import(project_id=project_id)

    @auth.decorators.check_api(schedule_check_spec("list"))
    def _list(self, project_id: int):
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
        from ...utils.schedule_risk import is_run_without_asking_allowed
        return {"total": total, "rows": items, "run_without_asking_allowed": bool(is_run_without_asking_allowed(project_id))}, 200


    @auth.decorators.check_api(schedule_check_spec("export"))
    def _export(self, project_id: int):
        """Answer the YAML file of every schedule of the project, or of `?ids=1,2` (R1.1, R1.2, R1.4)."""
        from flask import Response  # late: the list unit test fakes flask without it

        from ...utils.export_import_utils import content_disposition_attachment
        from ...utils.schedule_service import ScheduleError
        from ...utils.schedule_yaml import export_schedules_yaml
        raw_ids = request.args.get("ids")
        ids = None if raw_ids is None else _parse_ids(raw_ids)
        if raw_ids is not None and ids is None:
            return {"error": "ids must be a comma-separated list of schedule ids"}, 400
        try:
            text = export_schedules_yaml(project_id, ids)
        except ScheduleError as e:
            return e.response
        return Response(text, mimetype="application/x-yaml", headers={
            "Content-Disposition": content_disposition_attachment(f"schedules_{project_id}.yaml"),
        })

    @auth.decorators.check_api(schedule_check_spec("import"))
    def _import(self, project_id: int):
        """Create a disabled schedule per matched item of the uploaded file; answer the report (R2.6, R3.1)."""
        from ...utils.schedule_yaml import (
            MAX_FILE_BYTES,
            NOT_YAML,
            TOO_LARGE,
            import_schedules,
            parse_schedules_file,
        )
        upload = request.files.get("file")
        if upload is None:
            return {"error": "No file provided"}, 400
        if upload.seek(0, 2) > MAX_FILE_BYTES:
            return {"error": TOO_LARGE}, 400
        upload.seek(0)
        try:
            file = parse_schedules_file(upload.read().decode())
        except UnicodeDecodeError:
            return {"error": NOT_YAML}, 400
        except ValueError as e:
            return {"error": str(e)}, 400
        return import_schedules(project_id, schedule_scope(project_id).user_id, file), 200


class API(api_tools.APIBase):
    """URL patterns: /api/v2/elitea_core/schedules/prompt_lib/{project_id}, with `/export` and `/import`."""
    url_params: ClassVar[list[str]] = api_tools.with_modes([
        '<int:project_id>', '<int:project_id>/export', '<int:project_id>/import',
    ])

    mode_handlers: ClassVar[dict] = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
