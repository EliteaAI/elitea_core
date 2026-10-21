"""Swagger text of the Schedules API (EL-7011, owner change request 2026-10-21).

One entry per row of `ROUTES` in `api/v2/schedules.py`, with the same (method, URL tail) key; a test
checks that both tables match. `Module._register_openapi` registers every entry under the
`elitea_core/schedules` tag and keeps it out of the MCP tools.
"""
from typing import get_args

from ..models.pd.entity_schedule import ScheduleCreate, ScheduleEdit
from ..models.pd.generate_schedule_draft import GenerateScheduleDraftRequest
from ..models.pd.schedule_list import EntityType, RunStatus, SortBy, SortOrder
from ..models.pd.schedule_run import ScheduleRunRequest
from .constants import PROMPT_LIB_MODE, SCHEDULES_PROJECT_PATH
from .schedule_item_handlers import ENABLED_NOT_EDITABLE, MAX_RUNS_LIMIT, RUN_NOT_FOUND
from .schedule_project_handlers import MAX_EXPORT_IDS, SchedulePreferenceUpdate
from .schedule_settings import ScheduleProjectSettings

BASE_URL = f"/api/v2/elitea_core/schedules/{SCHEDULES_PROJECT_PATH}"
ITEM = "<int:schedule_id>"
OWNER_OR_ADMIN = "the owner with `{permission}`, or a schedules admin (`schedules.admin`)"

PROJECT_PARAMS = [
    {"name": "mode", "in": "path", "required": True, "schema": {"type": "string", "default": PROMPT_LIB_MODE},
     "description": f"Mode (default: {PROMPT_LIB_MODE})"},
    {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Project id"},
]
ITEM_PARAMS = [*PROJECT_PARAMS, {"name": "schedule_id", "in": "path", "required": True,
                                 "schema": {"type": "integer"}, "description": "Schedule id"}]
RUN = f"{ITEM}/runs/<int:run_id>"
RUN_PARAMS = [*ITEM_PARAMS, {"name": "run_id", "in": "path", "required": True,
                             "schema": {"type": "integer"}, "description": "Run id, from the runs list or the run answer"}]
LIST_QUERY_PARAMS = [
    {"name": "entity_type", "in": "query", "schema": {"type": "string", "enum": list(get_args(EntityType))},
     "description": "Only schedules of this entity kind"},
    {"name": "status", "in": "query", "schema": {"type": "string", "enum": list(get_args(RunStatus))},
     "description": "Only schedules whose last run has this status"},
    {"name": "enabled", "in": "query", "schema": {"type": "boolean"}, "description": "True: active, false: paused"},
    {"name": "owner_ids", "in": "query", "schema": {"type": "array", "items": {"type": "integer"}},
     "description": "Only schedules of these owners, repeated or comma-separated; schedules admin only"},
    {"name": "entity_id", "in": "query", "schema": {"type": "integer"},
     "description": "Only schedules of this agent or pipeline"},
    {"name": "search", "in": "query", "schema": {"type": "string"}, "description": "Match on schedule or entity name"},
    {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
     "description": "Rows per page"},
    {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0, "minimum": 0},
     "description": "Rows to skip"},
    {"name": "sort_by", "in": "query", "schema": {"type": "string", "default": "next_run", "enum": list(get_args(SortBy))},
     "description": "Sort column"},
    {"name": "sort_order", "in": "query",
     "schema": {"type": "string", "enum": list(get_args(SortOrder)), "default": "asc"}, "description": "Sort direction"},
]
RUNS_QUERY_PARAMS = [
    {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 20, "minimum": 1},
     "description": f"Rows per page; a value above {MAX_RUNS_LIMIT} is lowered to {MAX_RUNS_LIMIT}"},
    {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0, "minimum": 0},
     "description": "Rows to skip"},
    {"name": "status", "in": "query", "schema": {"type": "string", "enum": list(get_args(RunStatus))},
     "description": "Only runs with this status"},
]
EXPORT_QUERY_PARAMS = [
    {"name": "ids", "in": "query", "schema": {"type": "string"},
     "description": f"Comma-separated schedule ids, at most {MAX_EXPORT_IDS}; all schedules when absent"},
]
OWNER_BODY = {"required": True, "content": {"application/json": {"schema": {
    "type": "object", "required": ["owner_id"],
    "properties": {"owner_id": {"type": "integer", "description": "User id of the new owner, a project member",
                                "examples": [20]}},
}, "examples": {"example1": {"value": {"owner_id": 20}}}}}}
IMPORT_BODY = {"required": True, "content": {"multipart/form-data": {"schema": {
    "type": "object", "required": ["file"],
    "properties": {"file": {"type": "string", "format": "binary",
                            "description": "Schedule YAML file, as Export writes it"}},
}}}}

DETAILS_EXAMPLE = {
    "id": 12, "name": "Daily digest", "description": "Send the team digest every work day",
    "entity_type": "agent", "application_id": 42, "version_id": 420, "run_mode": None,
    "prompt": "Write the digest for {{ team }}", "variables": [{"name": "team", "value": "Platform"}],
    "cron": "0 9 * * 1-5", "timezone": "Europe/Warsaw", "expires_at": None, "enabled": True,
    "created_by": 20, "owner": {"id": 20, "name": "Ann Smith"}, "updated_by": 20,
    "next_run": "2026-10-22T07:00:00+00:00", "last_run": None, "last_run_status": None, "disabled_reason": None,
    "approval_rule": "pause_and_ask", "allowed_actions": ["run", "edit", "pause", "delete"],
    "run_without_asking_allowed": False,
}
SAVED_EXAMPLE = {**DETAILS_EXAMPLE, "warnings": []}
LIST_EXAMPLE = {"total": 1, "run_without_asking_allowed": False, "rows": [{
    "id": 12, "name": "Daily digest", "description": None, "entity_type": "agent", "run_mode": None,
    "application_id": 42, "entity_name": "Digest agent", "version_id": 420, "version_name": "base",
    "cron": "0 9 * * 1-5", "timezone": "Europe/Warsaw", "enabled": True, "disabled_reason": None,
    "next_run": "2026-10-22T07:00:00+00:00", "last_run": "2026-10-21T07:00:00+00:00", "last_run_status": "success",
    "last_run_reason": None, "last_conversation_id": 901, "expires_at": None,
    "owner": {"id": 20, "name": "Ann Smith"}, "allowed_actions": ["run", "edit", "pause", "delete"],
}]}
RUNS_EXAMPLE = {"total": 1, "rows": [{
    "id": 77, "trigger": "cron", "status": "success", "reason": None, "conversation_id": 901, "requested_by": 20,
    "started_at": "2026-10-21T07:00:00+00:00", "finished_at": "2026-10-21T07:01:10+00:00",
    "version_id": 420, "duration_ms": 70000,
}]}
RUN_DETAIL_EXAMPLE = {
    "id": 78, "trigger": "test", "status": "success", "reason": None, "conversation_id": 902, "requested_by": 20,
    "started_at": "2026-10-21T09:00:00+00:00", "finished_at": "2026-10-21T09:00:38+00:00", "version_id": 420,
    "duration_ms": 38000, "version_label": "v3", "principal_label": "Ann Smith (schedule owner)",
    "risk_outcome": "needs_approval", "approval_rule": "pause_and_ask", "approval_expires_at": None,
    "summary": {"duration_ms": 38000, "tokens": 2140, "cost_usd": 0.03},
    "events": [
        {"at": "2026-10-21T09:00:00.120000+00:00", "kind": "started", "level": "info",
         "text": "Test run started · trigger: test · version v3", "trigger": "test", "version_id": 420},
        {"at": "2026-10-21T09:00:00.410000+00:00", "kind": "account", "level": "info",
         "text": "Account: Ann Smith (schedule owner)"},
        {"at": "2026-10-21T09:00:00.900000+00:00", "kind": "risk_check", "level": "warning",
         "text": "Risk check: needs approval for github · create_issue"},
        {"at": "2026-10-21T09:00:01.050000+00:00", "kind": "agent_started", "level": "info",
         "text": "Agent started: Digest agent"},
        {"at": "2026-10-21T09:00:06.300000+00:00", "kind": "tool_call", "level": "info",
         "text": "Tool call: github · list_issues (1200 ms): 12 open issues", "toolkit": "github",
         "tool": "list_issues", "summary": "12 open issues", "duration_ms": 1200},
        {"at": "2026-10-21T09:00:09.000000+00:00", "kind": "paused", "level": "warning",
         "text": "Paused for approval: github · create_issue", "toolkit": "github", "tool": "create_issue"},
        {"at": "2026-10-21T09:00:30.000000+00:00", "kind": "decision", "level": "info",
         "text": "Approved by the schedule owner", "action": "approve", "by": 20},
        {"at": "2026-10-21T09:00:38.000000+00:00", "kind": "finished", "level": "success",
         "text": "Run finished: success", "status": "success"},
    ],
}
IMPORT_EXAMPLE = {
    "created": [{"name": "Weekly report", "schedule_id": 13}], "updated": [{"name": "Daily digest", "schedule_id": 12}],
    "skipped": [], "failed": [{"name": "Old job", "reason": "The agent of this schedule is not in the project"}],
}
DRAFT_EXAMPLE = {"draft": {
    "name": "Daily digest", "description": None, "entity_type": "agent", "application_id": 42, "version_id": 420,
    "run_mode": None, "prompt": "Write the digest", "variables": [], "cron": "0 9 * * 1-5",
    "timezone": "Europe/Warsaw", "expires_at": None, "approval_rule": "pause_and_ask",
}, "warnings": [{"field": "version_id", "reason": "unknown"}]}

ERRORS = {
    400: "Invalid input; the body names the error and, for a field, the `field`",
    403: "No permission for this operation",
    404: "Schedule not found, or another owner's schedule",
    409: "Conflict",
    500: "Server error",
    502: "The model failed or its answer could not be used",
    503: "A service the operation needs is unavailable",
}


def _responses(code: int, text: str, example: dict | None, errors: dict) -> dict:
    """The success answer with its JSON example, then the main error answers; an error text None takes the default."""
    success = {"description": text}
    if example is not None:
        success["content"] = {"application/json": {"example": example}}
    answers = {str(code): success}
    for error, value in errors.items():
        description, error_example = value if isinstance(value, tuple) else (value, None)
        answers[str(error)] = {"description": description or ERRORS[error]}
        if error_example:
            answers[str(error)]["content"] = {"application/json": {"example": error_example}}
    return answers


def _text(what: str, who: str, errors: str) -> str:
    """A description for people: what the operation does, who may call it, the main errors."""
    return f"{what}\n\nWho may call: {who}.\n\nErrors: {errors}."


OPERATIONS = {
    ("get", ""): {
        "name": "List schedules",
        "summary": "List schedules",
        "description": _text(
            "Answer one page of schedules with filters, sort and paging. Each row has the owner's name "
            "and the actions the caller may take on it.",
            "a project member with `schedules.list`; only a schedules admin sees other owners' schedules",
            "400 an invalid query option; 403 no permission"),
        "parameters": PROJECT_PARAMS + LIST_QUERY_PARAMS,
        "responses": _responses(200, "One page: `total` and `rows`", LIST_EXAMPLE, {
            400: "An invalid query option; the error names it", 403: None}),
    },
    ("post", ""): {
        "name": "Create a schedule",
        "summary": "Create a schedule",
        "description": _text(
            "Create a schedule for an agent or a pipeline version. The caller becomes the owner; "
            "the body never sets the owner. The answer has save `warnings`, which never block the save.",
            "a project member with `schedules.create`",
            "400 an invalid field, or \"run without asking\" is not allowed in the project; 403 no permission; "
            "404 the agent or pipeline is not found or not visible; 409 the project limit of active schedules"),
        "parameters": PROJECT_PARAMS,
        "request_body": ScheduleCreate,
        "responses": _responses(201, "Created: the schedule details and `warnings`", SAVED_EXAMPLE, {
            400: None, 403: None, 404: "The agent, pipeline or version is not found or not visible",
            409: "The project limit of active schedules is reached"}),
    },
    ("get", ITEM): {
        "name": "Read a schedule",
        "summary": "Read a schedule",
        "description": _text(
            "Answer the details of one schedule, with the owner's name and the allowed actions.",
            OWNER_OR_ADMIN.format(permission="schedules.details"),
            "403 no permission; 404 not found, or another owner's schedule for a regular user"),
        "parameters": ITEM_PARAMS,
        "responses": _responses(200, "The schedule details", DETAILS_EXAMPLE, {403: None, 404: None}),
    },
    ("put", ITEM): {
        "name": "Edit a schedule",
        "summary": "Edit a schedule",
        "description": _text(
            "Change the fields of a schedule; send only the fields to change. The owner never changes. "
            "This call does not pause or resume: a body with `enabled` answers 400, use the pause and "
            "resume actions. When an admin edits another owner's schedule, the owner gets a notice.",
            OWNER_OR_ADMIN.format(permission="schedules.update"),
            "400 an invalid field, `enabled` in the body, or \"run without asking\" is not allowed; "
            "403 no permission; 404 not found"),
        "parameters": ITEM_PARAMS,
        "request_body": ScheduleEdit,
        "responses": _responses(200, "The schedule details and `warnings`", SAVED_EXAMPLE, {
            400: (None, ENABLED_NOT_EDITABLE), 403: None, 404: None}),
    },
    ("delete", ITEM): {
        "name": "Delete a schedule",
        "summary": "Delete a schedule",
        "description": _text(
            "Delete a schedule. The conversations of its runs stay. When an admin deletes another "
            "owner's schedule, the owner gets a notice.",
            OWNER_OR_ADMIN.format(permission="schedules.delete"),
            "403 no permission; 404 not found"),
        "parameters": ITEM_PARAMS,
        "responses": _responses(204, "Deleted; no body", None, {403: None, 404: None}),
    },
    ("post", f"{ITEM}/pause"): {
        "name": "Pause a schedule",
        "summary": "Pause a schedule",
        "description": _text(
            "Stop the schedule from starting new runs. A run in progress goes on. No request body. "
            "When an admin pauses another owner's schedule, the owner gets a notice.",
            OWNER_OR_ADMIN.format(permission="schedules.update"),
            "403 no permission; 404 not found"),
        "parameters": ITEM_PARAMS,
        "responses": _responses(200, "The paused schedule", {**SAVED_EXAMPLE, "enabled": False, "allowed_actions": [
            "run", "edit", "resume", "delete"]}, {403: None, 404: None}),
    },
    ("post", f"{ITEM}/resume"): {
        "name": "Resume a schedule",
        "summary": "Resume a schedule",
        "description": _text(
            "Start a paused schedule again and compute its next run. No request body. The schedule "
            "needs a version, and the project must stay within its limit of active schedules.",
            OWNER_OR_ADMIN.format(permission="schedules.update"),
            "400 the schedule has no version (`field: version_id`); 403 no permission; 404 not found; "
            "409 the project limit of active schedules"),
        "parameters": ITEM_PARAMS,
        "responses": _responses(200, "The active schedule with its next run", SAVED_EXAMPLE, {
            400: ("The schedule has no version", {
                "error": "Choose a version before you resume this schedule", "field": "version_id"}),
            403: None, 404: None, 409: "The project limit of active schedules is reached"}),
    },
    ("post", f"{ITEM}/run"): {
        "name": "Run a schedule now",
        "summary": "Run a schedule now",
        "description": _text(
            "Start a run now (`manual`) or a test run (`test`). Only one run of a schedule is active at a time.",
            "the owner with `schedules.run`; an admin sees the schedule but cannot run it",
            "400 an invalid trigger; 403 no permission or not the owner; 404 not found; "
            "409 the version is deleted or a run is active (`active_run_id`); 500 the run could not start"),
        "parameters": ITEM_PARAMS,
        "request_body": ScheduleRunRequest,
        "responses": _responses(202, "The run started", {"run_id": 77}, {
            400: None, 403: "No permission, or not the owner", 404: None,
            409: ("The version is deleted, or a run is active", {
                "error": "A run of this schedule is active", "active_run_id": 76}),
            500: "The run could not start"}),
    },
    ("get", f"{ITEM}/runs"): {
        "name": "List the runs of a schedule",
        "summary": "List the runs of a schedule",
        "description": _text(
            "Answer the run records of one schedule, newest first, with the total before paging.",
            OWNER_OR_ADMIN.format(permission="schedules.details"),
            "400 an invalid query option; 403 no permission; 404 not found"),
        "parameters": ITEM_PARAMS + RUNS_QUERY_PARAMS,
        "responses": _responses(200, "`total` and one page of `rows`", RUNS_EXAMPLE, {400: None, 403: None, 404: None}),
    },
    ("get", RUN): {
        "name": "Read a run of a schedule",
        "summary": "Read a run with its event log",
        "description": _text(
            "Answer one run for the Test schedule panel: the run fields, the version and its name, the account "
            "(`principal_label`), the risk check outcome, a `summary` (duration, tokens, cost) and `events`. "
            "Each event has `at`, `kind`, `text` and `level`; the kinds are started, account, risk_check, "
            "agent_started, tool_call, paused, decision and finished. Tool calls come from the run conversation. "
            "Only the owner sees the tool result: for a caller who is not the owner, such as a schedules admin, "
            "a `tool_call` event keeps `toolkit`, `tool` and `duration_ms`, has no `summary`, and its `text` has "
            "no result part (\"Tool call: github · get_issues (487 ms)\"). "
            "Tokens and cost are null while the usage store has no row for the conversation. Poll every 2 s "
            "while the status is `running` or `waiting_for_approval`.",
            OWNER_OR_ADMIN.format(permission="schedules.details"),
            "403 no permission; 404 the schedule or the run is not found, or another owner's schedule"),
        "parameters": RUN_PARAMS,
        "responses": _responses(200, "The run with `summary` and `events`", RUN_DETAIL_EXAMPLE, {
            403: None, 404: ("The schedule or the run is not found", RUN_NOT_FOUND)}),
    },
    ("post", f"{ITEM}/owner"): {
        "name": "Change the owner",
        "summary": "Change the owner of a schedule",
        "description": _text(
            "Give the schedule to another project member. The old and the new owner get a notice.",
            "a schedules admin (`schedules.admin`) only",
            "400 `owner_id` is missing or the user cannot own the schedule; 403 no permission; 404 not found; "
            "503 the access check is unavailable"),
        "parameters": ITEM_PARAMS,
        "request_body": OWNER_BODY,
        "responses": _responses(200, "The schedule details with the new owner", {
            **DETAILS_EXAMPLE, "created_by": 21, "owner": {"id": 21, "name": "Bob Lee"}}, {
            400: None, 403: None, 404: None, 503: "The access check is unavailable"}),
    },
    ("get", "export"): {
        "name": "Export schedules",
        "summary": "Export schedules to YAML",
        "description": _text(
            "Download the schedules of the project as one YAML file, or only the schedules in `ids`.",
            "a schedules admin (`schedules.export`) only",
            "400 invalid or unknown ids; 403 no permission"),
        "parameters": PROJECT_PARAMS + EXPORT_QUERY_PARAMS,
        "responses": _responses(200, "The YAML file (application/x-yaml)", None, {
            400: "Invalid or unknown ids", 403: None}),
    },
    ("post", "import"): {
        "name": "Import schedules",
        "summary": "Import schedules from YAML",
        "description": _text(
            "Read a schedule YAML file (multipart `file`). An item that matches a schedule by key, else by "
            "name, updates it. Every other item creates a paused schedule owned by the caller.",
            "a schedules admin (`schedules.import`) only",
            "400 no file, a file over 1 MB, or not a schedule file; 403 no permission"),
        "parameters": PROJECT_PARAMS,
        "request_body": IMPORT_BODY,
        "responses": _responses(200, "The `created`, `updated`, `skipped` and `failed` items", IMPORT_EXAMPLE, {
            400: "No file, or the file is not a schedule file", 403: None}),
    },
    ("post", "draft"): {
        "name": "Draft a schedule with AI",
        "summary": "Draft a schedule with AI",
        "description": _text(
            "Draft the fields of a schedule form from a plain-text description and the current form values. "
            "Uses the project's default LLM. The server checks the draft and adds a warning for each field "
            "it could not use. Nothing is saved.",
            "a project member with `schedules.create`",
            "400 an invalid body or no default LLM; 403 no permission; 500 the service prompt is not "
            "configured; 502 the model failed; 503 busy, retry after `retry_after` seconds"),
        "parameters": PROJECT_PARAMS,
        "request_body": GenerateScheduleDraftRequest,
        "responses": _responses(200, "The checked `draft` and its `warnings`", DRAFT_EXAMPLE, {
            400: "An invalid body, or no default LLM in the project", 403: None,
            500: "The service prompt is not configured", 502: None,
            503: ("Busy; retry after `retry_after` seconds", {
                "error": "temporarily_unavailable", "retry_after": 5,
                "message": "The service is busy. Please try again in a few seconds."})}),
    },
    ("get", "preferences"): {
        "name": "Read my notice setting",
        "summary": "Read my run notice setting",
        "description": _text(
            "Answer whether the caller gets a notice when a schedule run succeeds. True when never saved.",
            "a project member with `schedules.list`; each caller reads only their own value",
            "403 no permission"),
        "parameters": PROJECT_PARAMS,
        "responses": _responses(200, "The caller's value", {"get_notified": True}, {403: None}),
    },
    ("put", "preferences"): {
        "name": "Save my notice setting",
        "summary": "Save my run notice setting",
        "description": _text(
            "Save the caller's own value. The user id is never taken from the request.",
            "a project member with `schedules.list`",
            "400 `get_notified` is not true or false; 403 no permission"),
        "parameters": PROJECT_PARAMS,
        "request_body": SchedulePreferenceUpdate,
        "responses": _responses(200, "The saved value", {"get_notified": False}, {
            400: ("`get_notified` is not true or false", {"error": "get_notified must be true or false"}),
            403: None}),
    },
    ("get", "settings"): {
        "name": "Read the project schedule settings",
        "summary": "Read the project schedule settings",
        "description": _text(
            "Answer whether the project allows the \"Run without asking\" approval rule. False when never "
            "saved, and false when the setting cannot be read.",
            "a project member with `schedules.list`",
            "403 no permission"),
        "parameters": PROJECT_PARAMS,
        "responses": _responses(200, "The project's value", {"allow_run_without_asking": False}, {403: None}),
    },
    ("put", "settings"): {
        "name": "Save the project schedule settings",
        "summary": "Save the project schedule settings",
        "description": _text(
            "Allow or forbid the \"Run without asking\" approval rule in the project. Saved schedules keep "
            "their rule; while it is off, their runs pause and ask, and a save or an import with that rule "
            "is refused or changed to \"Pause and ask me\".",
            "a schedules admin (`schedules.admin`) only",
            "400 an unknown key or a value that is not true or false (`field` names it); 403 no permission; "
            "503 the settings store is unavailable"),
        "parameters": PROJECT_PARAMS,
        "request_body": ScheduleProjectSettings,
        "responses": _responses(200, "The saved value", {"allow_run_without_asking": True}, {
            400: ("An unknown key, or a value that is not true or false", {
                "error": "enabled is not a schedule setting", "field": "enabled"}),
            403: None, 503: "The settings store is unavailable"}),
    },
}
OPENAPI_ENDPOINTS = [
    {"method": method, "path": f"{BASE_URL}/{tail}".rstrip("/"), **operation}
    for (method, tail), operation in OPERATIONS.items()
]
