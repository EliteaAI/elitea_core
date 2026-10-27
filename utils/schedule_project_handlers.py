"""Handlers of the schedules of a project (EL-7011, US-05, US-06, US-22): list, export, import, preferences, settings.

`api/v2/schedules.py` serves them; this module registers no route.
"""
from uuid import UUID

from flask import request
from pydantic import BaseModel, Field, StrictBool, ValidationError
from pylon.core.tools import log
from sqlalchemy.exc import IntegrityError
from tools import auth, db

from ..models.entity_schedule import EntitySchedulePreference
from ..models.pd.schedule_list import ScheduleListFilters, ScheduleListItem
from .authors import get_authors_data
from .schedule_access import allowed_actions, schedule_check_spec, schedule_scope
from .schedule_queries import list_schedules

MAX_ID = 2147483647  # the schedule id column is a 32-bit integer
MAX_EXPORT_IDS = 500
# The refusal body of `schedule_service.refusal`, written out: the handler tests load this module without the service
INVALID_INPUT = "invalid_input"
INVALID_PREFERENCE = {"code": INVALID_INPUT, "error": "get_notified must be true or false", "field": "get_notified"}
INVALID_SETTING = {"code": INVALID_INPUT, "error": "allow_run_without_asking must be true or false",
                   "field": "allow_run_without_asking"}
SETTINGS_UNAVAILABLE = {"code": "settings_unavailable", "error": "The project settings store is unavailable; try again later"}


class SchedulePreferenceUpdate(BaseModel):
    get_notified: StrictBool = Field(description="True sends the caller a notice when a schedule run succeeds",
                                     examples=[False])


def _parse_ids(raw: str) -> list[int] | None:
    """Ids of `1,2`; ASCII digits only, each a 32-bit id, at most 500. None when the text is not such a list."""
    parts = raw.split(",")
    if len(parts) > MAX_EXPORT_IDS or not all(part.isascii() and part.isdigit() for part in parts):
        return None
    ids = [int(part) for part in parts]
    return ids if max(ids) <= MAX_ID else None


def _bad_file(text: str) -> tuple[dict, int]:
    return {"code": INVALID_INPUT, "error": text, "field": "file"}, 400


def _owner_names(owner_ids: set[int]) -> dict[int, str | None]:
    """Map owner id to name, or the email when the user has no name; a failed lookup gives an empty map (R1.3)."""
    if not owner_ids:  # an empty id list makes auth_core return every user
        return {}
    try:
        return {user["id"]: user.get("name") or user.get("email") for user in get_authors_data(sorted(owner_ids))}
    except Exception as e:  # noqa: BLE001  # pylint: disable=W0718 -- names must never break the list (R1.3)
        log.warning(f"Schedule owner names lookup failed: {e}")
        return {}


class ScheduleProjectHandlers:
    """The schedules of a project. Each handler is called with keyword arguments: check_api reads `project_id`."""

    @auth.decorators.check_api(schedule_check_spec("list"))
    def _list(self, project_id: int):
        """Answer one page of schedules as `{"total", "rows"}`, with owner names and allowed actions (R1.1-R5.3).

        Who may call: a project member with `schedules.list`; only a schedules admin sees other owners' schedules.
        Errors: 400 invalid query option; 403.
        """
        args = request.args.to_dict()
        owner_ids = request.args.getlist('owner_ids')
        if owner_ids:  # to_dict keeps only the first value of a repeated key
            args['owner_ids'] = ','.join(owner_ids)
        try:
            filters = ScheduleListFilters(**args)
        except ValidationError as e:
            name = e.errors()[0]["loc"][0]
            text = "limit must be between 1 and 100" if name == "limit" else f"{name} is not valid"
            return {"code": INVALID_INPUT, "error": text, "field": name}, 400

        chat = args.get("delivery_conversation_uuid")
        if chat is not None:
            try:
                chat = UUID(chat)
            except ValueError:
                return {"code": INVALID_INPUT, "error": "delivery_conversation_uuid is not valid",
                        "field": "delivery_conversation_uuid"}, 400
            # the filters model has no such field; the query reads it from the filters object
            filters = filters.model_copy(update={"delivery_conversation_uuid": chat})

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
        from .schedule_settings import is_run_without_asking_allowed
        return {"total": total, "rows": items, "run_without_asking_allowed": bool(is_run_without_asking_allowed(project_id))}, 200

    @auth.decorators.check_api(schedule_check_spec("export"))
    def _export(self, project_id: int):
        """Answer the YAML file of every schedule of the project, or of `?ids=1,2` (R1.1, R1.2, R1.4).

        Who may call: a schedules admin only.
        Errors: 400 invalid or unknown ids; 403.
        """
        from flask import Response  # late: the list unit test fakes flask without it

        from .export_import_utils import content_disposition_attachment
        from .schedule_service import ScheduleError
        from .schedule_yaml import export_schedules_yaml
        raw_ids = request.args.get("ids")
        ids = None if raw_ids is None else _parse_ids(raw_ids)
        if raw_ids is not None and ids is None:
            return {"code": INVALID_INPUT, "error": "ids must be a comma-separated list of schedule ids", "field": "ids"}, 400
        try:
            text = export_schedules_yaml(project_id, ids)
        except ScheduleError as e:
            return e.response
        return Response(text, mimetype="application/x-yaml", headers={
            "Content-Disposition": content_disposition_attachment(f"schedules_{project_id}.yaml"),
        })

    @auth.decorators.check_api(schedule_check_spec("import"))
    def _import(self, project_id: int):
        """Update or create a schedule per item of the uploaded file and answer the report (R2.6, R3.1, R4).

        Who may call: a schedules admin only.
        Errors: 400 no file, a file over 1 MB, or not a schedule file; 403.
        """
        from .schedule_yaml import (
            MAX_FILE_BYTES,
            NOT_YAML,
            TOO_LARGE,
            import_schedules,
            parse_schedules_file,
        )
        upload = request.files.get("file")
        if upload is None:
            return _bad_file("No file provided")
        if upload.seek(0, 2) > MAX_FILE_BYTES:
            return _bad_file(TOO_LARGE)
        upload.seek(0)
        try:
            file = parse_schedules_file(upload.read().decode())
        except UnicodeDecodeError:
            return _bad_file(NOT_YAML)
        except ValueError as e:
            return _bad_file(str(e))
        return import_schedules(project_id, schedule_scope(project_id).user_id, file), 200

    @auth.decorators.check_api(schedule_check_spec("list"))
    def _get_preferences(self, project_id: int):
        """Answer whether the caller gets a notice when a schedule run succeeds; true when never saved.

        Who may call: a project member with `schedules.list`; each caller reads only their own value.
        Errors: 403.
        """
        with db.get_session(project_id) as session:
            row = session.get(EntitySchedulePreference, auth.current_user()["id"])
            return {"get_notified": row.get_notified if row else True}, 200

    @auth.decorators.check_api(schedule_check_spec("list"))
    def _save_preferences(self, project_id: int):
        """Save the caller's own "Get notified" value; the user id is never taken from the request.

        Who may call: a project member with `schedules.list`.
        Errors: 400 `get_notified` is not true or false; 403.
        """
        try:
            value = SchedulePreferenceUpdate.model_validate(request.get_json(silent=True)).get_notified
        except ValidationError:
            return INVALID_PREFERENCE, 400
        with db.get_session(project_id) as session:
            user_id = auth.current_user()["id"]
            row = session.get(EntitySchedulePreference, user_id)
            if row:
                row.get_notified = value
            else:
                session.add(EntitySchedulePreference(user_id=user_id, get_notified=value))
            try:
                session.commit()
            except IntegrityError:  # a parallel first save of the same user inserted the row first
                session.rollback()
                session.get(EntitySchedulePreference, user_id).get_notified = value
                session.commit()
        return {"get_notified": value}, 200

    @auth.decorators.check_api(schedule_check_spec("list"))
    def _get_settings(self, project_id: int):
        """Answer whether the project allows the "Run without asking" approval rule; false when never saved.

        Who may call: a project member with `schedules.list`.
        Errors: 403. A store error answers false, as every other reader of the setting does (fail closed).
        """
        from .schedule_settings import is_run_without_asking_allowed
        return {"allow_run_without_asking": is_run_without_asking_allowed(project_id)}, 200

    @auth.decorators.check_api(schedule_check_spec("admin"))
    def _save_settings(self, project_id: int):
        """Allow or forbid "Run without asking" in the project. Saved schedules keep their rule; while it is
        off, their runs pause and ask.

        Who may call: a schedules admin (`schedules.admin`) only.
        Errors: 400 an unknown key or a value that is not true or false, with `field`; 403; 503 the store fails.
        """
        from .schedule_settings import ScheduleProjectSettings, save_schedule_settings
        try:
            settings = ScheduleProjectSettings.model_validate(request.get_json(silent=True))
        except ValidationError as e:
            unknown = [error["loc"][0] for error in e.errors() if error["type"] == "extra_forbidden"]
            if unknown:
                return {"code": INVALID_INPUT, "error": f"{unknown[0]} is not a schedule setting", "field": unknown[0]}, 400
            return INVALID_SETTING, 400
        try:
            return save_schedule_settings(project_id, settings), 200
        except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the configurations store failed
            log.exception("Project %s: the schedule settings could not be saved", project_id)
            return SETTINGS_UNAVAILABLE, 503
