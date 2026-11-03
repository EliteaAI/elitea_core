"""The project inbox (#7269, C-1009): which waiting governance rows nobody in their project can approve.

A row waits for an approver while it is pending (a binding or a mandate in ``pending_approval``, a grant in
``pending_approval``, a credential reference ``pending`` or validated and not approved yet). Its approver is a project
admin whom the kind's own four-eyes rule does not exclude: for a binding its creator and whoever last changed its state,
for a mandate its creator and its owner, for a reference its creator and whoever wrote its value, for a grant its
creator (``binding_state``, ``mandate_state``, secrets ``credential_onboarding`` / ``credential_grants``; nothing here
changes those rules, a platform operator approves in administration mode as before). A row with no such admin (a
private project has none, or the only admin is excluded) "needs a platform operator": it is ``escalated``, and the
administration list shows exactly those rows with ``escalated=true``.

Computed from the project's role membership (one call per page, the project's system user left out), never stored.
"""
from pylon.core.tools import log
from sqlalchemy import and_, false, or_

ADMIN_ROLE = "admin"
ROLES_TIMEOUT_S = 15


class AdminsUnavailable(Exception):
    """The project's members could not be read: an inbox page is refused rather than marked wrong."""


def project_members(project_id) -> dict:
    """``{user id: role names}`` of the project, its system user left out. ``AdminsUnavailable`` when the membership
    cannot be read."""
    from tools import rpc_tools  # pylint: disable=C0415
    try:
        roles = rpc_tools.RpcMixin().rpc.timeout(ROLES_TIMEOUT_S).admin_get_users_roles_in_project(
            project_id, filter_system_user=True)
    except Exception as exc:  # pylint: disable=W0703
        log.error("governance inbox: the project's members were not read (%s)", type(exc).__name__)
        raise AdminsUnavailable() from None
    return {int(user_id): tuple(names or ()) for user_id, names in (roles or {}).items() if user_id is not None}


def project_admins(project_id) -> frozenset:
    """The user ids holding the project's ``admin`` role. ``AdminsUnavailable`` when the membership cannot be read."""
    return frozenset(user_id for user_id, names in project_members(project_id).items() if ADMIN_ROLE in names)


def escalated(admins, *excluded) -> bool:
    """No project admin may approve a row whose four-eyes rule excludes ``excluded``."""
    return not set(admins) - {user_id for user_id in excluded if user_id}


def escalated_clause(admins, pending, *columns):
    """The SQL form of ``escalated`` for a waiting row: ``pending`` and every admin is one of ``columns`` (so a project
    with more admins than the rule excludes has none). A NULL column matches nobody."""
    admins = sorted(admins)
    if len(admins) > len(columns):
        return false()
    return and_(pending, *[or_(*[column == admin for column in columns]) for admin in admins])
