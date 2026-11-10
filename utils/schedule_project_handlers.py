"""Handlers of the schedules of a project (EL-7011, US-05, US-06, US-22): list, export, import, preferences, settings.

`api/v2/schedules.py` serves them; this module registers no route.
"""
from flask import request
from pydantic import BaseModel, Field, StrictBool, ValidationError
from pylon.core.tools import log
from sqlalchemy.exc import IntegrityError
from tools import auth, db

from ..models.entity_schedule import EntitySchedulePreference
from ..models.pd.schedule_list import ScheduleListFilters, ScheduleListItem
from .authors import get_authors_data
from .error_text import public_reason
from .schedule_access import allowed_actions, row_webhook_url, schedule_check_spec, schedule_scope
from .schedule_queries import list_schedules

MAX_ID = 2147483647  # the schedule id column is a 32-bit integer
MAX_EXPORT_IDS = 500
# The refusal body of `schedule_service.refusal`, written out: the handler tests load this module without the service
INVALID_INPUT = "invalid_input"
INVALID_PREFERENCE = {"code": INVALID_INPUT, "error": "get_notified must be true or false", "field": "get_notified"}
INVALID_SETTING = {"code": INVALID_INPUT, "error": "allow_run_without_asking must be true or false",
                   "field": "allow_run_without_asking"}
INVALID_WEBHOOK_CAP = {"code": INVALID_INPUT, "error": "Running runs per webhook must be a whole number of at least 1",
                      "field": "webhook_max_concurrent_runs"}
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


def _webhook_load(session, project_id: int, rows) -> dict[int, dict]:
    """`running_runs` and `run_cap` of the webhook rows of one page: one grouped count, never a query per row."""
    ids = [row.id for row in rows if getattr(row, "trigger_kind", None) == "webhook"]
    if not ids:
        return {}
    from .schedule_settings import project_webhook_cap
    from .schedule_timing import webhook_limits
    from .schedule_claim import running_webhook_runs
    cap = project_webhook_cap(project_id, webhook_limits())
    running = running_webhook_runs(session, ids)
    return {row_id: {"running_runs": running.get(row_id, 0), "run_cap": cap} for row_id in ids}


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

        scope = schedule_scope(project_id)
        with db.get_session(project_id) as session:
            total, rows = list_schedules(session, scope, filters, filters.limit, filters.offset)
            running = _webhook_load(session, project_id, rows)
        names = _owner_names({row.created_by for row in rows})
        items = [
            ScheduleListItem(
                **{**row._mapping, "last_run_reason": public_reason(row.last_run_reason),
                   "webhook_url": row_webhook_url(project_id, row), **running.get(row.id, {})},
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
        from .schedule_settings import is_run_without_asking_allowed, saved_webhook_cap
        from .schedule_timing import webhook_limits
        limits = webhook_limits()
        return {"allow_run_without_asking": is_run_without_asking_allowed(project_id),
                "webhook_max_concurrent_runs": saved_webhook_cap(project_id),
                "webhook_max_concurrent_runs_default": limits.per_webhook,
                "webhook_max_concurrent_runs_limit": limits.per_webhook_limit}, 200

    @auth.decorators.check_api(schedule_check_spec("admin"))
    def _save_settings(self, project_id: int):
        """Allow or forbid "Run without asking", or set the project's per-webhook running cap; a key left out keeps its value.
        Saved schedules keep their rule; while "Run without asking" is off, their runs pause and ask.

        Who may call: a schedules admin (`schedules.admin`) only.
        Errors: 400 an unknown key or a value that is not true or false, with `field`; 403; 503 the store fails.
        """
        from .schedule_settings import (ScheduleProjectSettings, is_run_without_asking_allowed, save_schedule_settings,
                                        saved_webhook_cap)
        body = request.get_json(silent=True)
        # Each control saves only its own key, so a save of one never resets the other
        if isinstance(body, dict) and "webhook_max_concurrent_runs" not in body:
            body = {**body, "webhook_max_concurrent_runs": saved_webhook_cap(project_id)}
        elif isinstance(body, dict) and "allow_run_without_asking" not in body:
            body = {**body, "allow_run_without_asking": is_run_without_asking_allowed(project_id)}
        try:
            settings = ScheduleProjectSettings.model_validate(body)
        except ValidationError as e:
            unknown = [error["loc"][0] for error in e.errors() if error["type"] == "extra_forbidden"]
            if unknown:
                return {"code": INVALID_INPUT, "error": f"{unknown[0]} is not a schedule setting", "field": unknown[0]}, 400
            if any(error["loc"][:1] == ("webhook_max_concurrent_runs",) for error in e.errors()):
                return INVALID_WEBHOOK_CAP, 400
            return INVALID_SETTING, 400
        from .schedule_timing import webhook_limits
        limit = webhook_limits().per_webhook_limit
        if (settings.webhook_max_concurrent_runs or 0) > limit:
            return {**INVALID_WEBHOOK_CAP, "error": f"Running runs per webhook can be at most {limit}", "limit": limit}, 400
        try:
            return save_schedule_settings(project_id, settings), 200
        except Exception:  # noqa: BLE001  # pylint: disable=W0718 -- the configurations store failed
            log.exception("Project %s: the schedule settings could not be saved", project_id)
            return SETTINGS_UNAVAILABLE, 503
