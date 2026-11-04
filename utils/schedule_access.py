"""Schedule permission family, endpoint check spec and caller scope (EL-7011, ADR-0004)."""
from dataclasses import dataclass

from tools import auth
from tools import config as c

_PREFIX = "models.applications.schedules."
_VERBS = ("list", "details", "create", "update", "delete", "run")
_ADMIN_ONLY = ("admin", "export", "import")

SCHEDULE_PERMISSIONS = {action: _PREFIX + action for action in _VERBS + _ADMIN_ONLY}

_EDITOR_ROLES = {"admin": True, "editor": True, "viewer": False}
_ADMIN_ROLES = {"admin": True, "editor": False, "viewer": False}


def schedule_check_spec(action: str) -> dict:
    """Return the check_api spec of one schedule action; an unknown action raises KeyError."""
    roles = _ADMIN_ROLES if action in _ADMIN_ONLY else _EDITOR_ROLES
    return {
        "permissions": [SCHEDULE_PERMISSIONS[action]],
        "recommended_roles": {c.ADMINISTRATION_MODE: dict(roles), c.DEFAULT_MODE: dict(roles)},
    }


def register_schedule_permissions() -> None:
    for action in SCHEDULE_PERMISSIONS:
        auth.register_permissions(schedule_check_spec(action))


@dataclass(frozen=True)
class ScheduleScope:
    user_id: int
    is_admin: bool
    permissions: frozenset[str]


def schedule_scope(project_id: int) -> ScheduleScope:
    """Read the caller's rights in the path project once; a read error propagates, never a wider scope."""
    permissions = frozenset(auth.resolve_permissions(mode=c.DEFAULT_MODE, project_id=project_id))
    return ScheduleScope(
        user_id=auth.current_user()["id"],
        is_admin=SCHEDULE_PERMISSIONS["admin"] in permissions,
        permissions=permissions,
    )


def allowed_actions(row, scope: ScheduleScope) -> list[str]:
    """Actions the caller may take on one schedule row; a pure function of the row and the scope."""
    is_owner = row.created_by == scope.user_id
    if not (is_owner or scope.is_admin):
        return []
    granted = {verb for verb in _VERBS if SCHEDULE_PERMISSIONS[verb] in scope.permissions}
    if getattr(row, "trigger_kind", "cron") == "webhook":
        return _webhook_actions(row, scope, is_owner, granted)
    actions = []
    if is_owner and "run" in granted and row.version_id is not None:
        actions.append("run")
    if "update" in granted:
        actions += ["edit", "pause" if row.enabled else "resume"]
    if "delete" in granted:
        actions.append("delete")
    if scope.is_admin:
        actions.append("change_owner")
    return actions


def _webhook_actions(row, scope: ScheduleScope, is_owner: bool, granted: set[str]) -> list[str]:
    """Plan-b §4.2: only the owner edits; an admin pauses, deletes, rotates and changes the owner, all audited."""
    actions = []
    if "update" in granted:
        if is_owner:
            actions.append("edit")
        # after an owner change only the new owner switches it on again
        if row.enabled:
            actions.append("pause")
        elif is_owner or row.disabled_reason != "owner_changed":
            actions.append("resume")
        actions.append("rotate_secret")
    if "delete" in granted:
        actions.append("delete")
    if scope.is_admin:
        actions.append("change_owner")
    return actions


def row_webhook_url(project_id: int, row) -> str | None:
    """The delivery path of a webhook row, keyed by its unchanging uuid; None for cron rows."""
    if getattr(row, "trigger_kind", "cron") != "webhook":
        return None
    return f"/api/v2/elitea_core/hooks/prompt_lib/{int(project_id)}/{row.uuid}"
