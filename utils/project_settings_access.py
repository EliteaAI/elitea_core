"""Team-project settings are changed by project admins only (#6789).

Not a permission string: editors already hold ``models.project_context.edit`` in existing projects,
and a personal project's owner has no admin role (``projects.create_personal_project``), so the
rule is "project admin OR own personal project", as in ``configurations.tracing_access``.
"""
import functools

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from tools import auth, rpc_tools  # pylint: disable=E0401

ADMIN_ROLE_NAMES = frozenset({"admin", "super_admin", "system"})
PROJECT_SETTINGS_DENIED = {"error": "Project settings are managed by project admins"}
_RPC_TIMEOUT = 5


def _is_project_admin(project_id: int, user_id: int) -> bool:
    try:
        roles = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).admin_get_user_roles(
            project_id=project_id, user_id=user_id,
        ) or []
        return any(role.get("name") in ADMIN_ROLE_NAMES for role in roles)
    except Exception as e:  # pylint: disable=W0703
        log.warning("project settings: admin role lookup failed for user %s in project %s: %s",
                    user_id, project_id, e)
        return False


def _is_own_personal_project(project_id: int, user_id: int) -> bool:
    try:
        personal_id = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).projects_get_personal_project_id(
            user_id=user_id,
        )
    except Exception as e:  # pylint: disable=W0703
        log.warning("project settings: personal project lookup failed for user %s: %s", user_id, e)
        return False
    return personal_id is not None and int(personal_id) == int(project_id)


def can_manage_project_settings(project_id: int, user_id: int | None) -> bool:
    if not user_id:
        return False
    # Independent signals: a failing role lookup must not lock an owner out of their own project
    return _is_project_admin(project_id, user_id) or _is_own_personal_project(project_id, user_id)


def require_project_settings_admin(func):
    @functools.wraps(func)
    def wrapper(self, project_id: int, *args, **kwargs):
        if not can_manage_project_settings(project_id, auth.current_user().get("id")):
            return PROJECT_SETTINGS_DENIED, 403
        return func(self, project_id, *args, **kwargs)
    return wrapper
