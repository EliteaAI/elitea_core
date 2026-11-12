"""Who may take an admin-only schedule action: the permission holder, a project admin, or the owner of a personal project.

The owner of a personal project holds no admin role (`projects.create_personal_project`), yet is the admin of that
project. `check_api` cannot express that, so the route lets the editor level in (`admin_action_spec`) and the
handler asks `admin_refusal`.
"""
from .project_settings_access import can_manage_project_settings
from .schedule_access import SCHEDULE_PERMISSIONS, ScheduleScope, schedule_check_spec


def admin_action_spec(action: str) -> dict:
    """The check_api spec of an admin-only action, which also lets the editor level in; the handler decides the rest.

    Both permissions are any-of, so a role that holds only the action permission still gets in. The roles stay
    those of the admin-only action: check_api registers every listed permission with them, and an editor
    recommendation would hand the action permission to editors.
    """
    spec = schedule_check_spec(action)
    spec["permissions"] = [SCHEDULE_PERMISSIONS["update"], SCHEDULE_PERMISSIONS[action]]
    return spec


def admin_refusal(project_id: int, action: str, scope: ScheduleScope):
    """None when the caller may take the admin-only `action`, else the `(body, 403)` answer.

    Allowed: the holder of the action permission, a project admin, or the owner of their own personal project.
    A failed role or project lookup counts as no (`can_manage_project_settings` fails closed).
    The body keeps the `ok`, `error` and `required` keys of the `check_api` answer; the client reads `error`.
    """
    if SCHEDULE_PERMISSIONS[action] in scope.permissions or can_manage_project_settings(project_id, scope.user_id):
        return None
    return {"ok": False, "error": "access_denied", "required": [SCHEDULE_PERMISSIONS[action]]}, 403
