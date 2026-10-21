"""The Schedules API (EL-7011): every operation under `/api/v2/elitea_core/schedules/{mode}/{project_id}`.

Pylon serves all URLs of this module with one class, so `PromptLibAPI` picks the handler by the
HTTP method and the URL tail, as `ROUTES` says. The handlers live in three helper modules:
one schedule, the project's schedules, and the AI draft. The Swagger text of each operation is in
`utils/schedule_openapi.py`; `Module._register_openapi` registers it.
"""
from typing import ClassVar

from flask import request
from tools import api_tools

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.schedule_draft_handler import ScheduleDraftHandler
from ...utils.schedule_item_handlers import ScheduleItemHandlers
from ...utils.schedule_project_handlers import ScheduleProjectHandlers

ITEM = "<int:schedule_id>"
RUN = f"{ITEM}/runs/<int:run_id>"
# (HTTP method, URL tail after the project id): handler method. The Swagger table has the same keys.
ROUTES = {
    ("get", ""): "_list",
    ("post", ""): "_create",
    ("get", ITEM): "_read",
    ("put", ITEM): "_edit",
    ("delete", ITEM): "_delete",
    ("post", f"{ITEM}/pause"): "_pause",
    ("post", f"{ITEM}/resume"): "_resume",
    ("post", f"{ITEM}/run"): "_run",
    ("get", f"{ITEM}/runs"): "_runs",
    ("get", RUN): "_run_detail",
    ("post", f"{ITEM}/owner"): "_change_owner",
    ("get", "export"): "_export",
    ("post", "import"): "_import",
    ("post", "draft"): "_draft",
    ("get", "preferences"): "_get_preferences",
    ("put", "preferences"): "_save_preferences",
    ("get", "settings"): "_get_settings",
    ("put", "settings"): "_save_settings",
    ("post", "check"): "_check_risks",
}
# The body of `schedule_service.refusal`; the handler tests load this module without the service
NOT_ALLOWED = {"code": "method_not_allowed", "error": "Method not allowed"}, 405


def _tail(schedule_id: int | None, run_id: int | None) -> str:
    """The URL tail after the project id, in the form of the `ROUTES` keys."""
    if run_id is not None:
        return RUN
    last = request.path.rstrip("/").rsplit("/", 1)[-1]
    action = "" if last.isdigit() else last
    if schedule_id is None:
        return action
    return f"{ITEM}/{action}" if action else ITEM


class PromptLibAPI(ScheduleItemHandlers, ScheduleProjectHandlers, ScheduleDraftHandler, api_tools.APIModeHandler):
    """Answers every Schedules URL; an HTTP method a URL does not serve answers 405."""

    def _dispatch(self, method: str, project_id: int, schedule_id: int | None, run_id: int | None):
        name = ROUTES.get((method, _tail(schedule_id, run_id)))
        if name is None:
            return NOT_ALLOWED
        # Keyword arguments: check_api reads project_id from them for the project-scoped permission check
        ids = {key: value for key, value in (("schedule_id", schedule_id), ("run_id", run_id)) if value is not None}
        return getattr(self, name)(project_id=project_id, **ids)

    @api_tools.endpoint_metrics
    def get(self, project_id: int, schedule_id: int | None = None, run_id: int | None = None, **kwargs):
        """List, read one, run records, one run, export, preferences and project settings."""
        return self._dispatch("get", project_id, schedule_id, run_id)

    @api_tools.endpoint_metrics
    def post(self, project_id: int, schedule_id: int | None = None, run_id: int | None = None, **kwargs):
        """Create, check risks, pause, resume, run, change owner, import and draft."""
        return self._dispatch("post", project_id, schedule_id, run_id)

    @api_tools.endpoint_metrics
    def put(self, project_id: int, schedule_id: int | None = None, run_id: int | None = None, **kwargs):
        """Edit a schedule, save preferences and save project settings."""
        return self._dispatch("put", project_id, schedule_id, run_id)

    @api_tools.endpoint_metrics
    def delete(self, project_id: int, schedule_id: int | None = None, run_id: int | None = None, **kwargs):
        """Delete a schedule."""
        return self._dispatch("delete", project_id, schedule_id, run_id)


class API(api_tools.APIBase):
    url_params: ClassVar[list[str]] = api_tools.with_modes(sorted({
        f"<int:project_id>/{tail}".rstrip("/") for _, tail in ROUTES
    }))

    mode_handlers: ClassVar[dict] = {PROMPT_LIB_MODE: PromptLibAPI}
