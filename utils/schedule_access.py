"""Schedule permission family, endpoint check spec and caller scope (EL-7011, ADR-0004)."""
from dataclasses import dataclass

from tools import auth, config as c

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
