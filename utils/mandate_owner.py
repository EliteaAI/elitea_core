"""A mandate owner who is no longer active takes their mandates with them (#6947, C-1009).

The guarantee is lazy and lives in admission (every firing and every worker start reads the owner's ``active``
verdict). This is the accelerator: the ``auth_invalidate`` event fired after every revocation bump of a user. Role
changes bump the epoch too and leave the user active, so only ``active is False`` goes on to suspend anything. A
lost or late event costs nothing but delay.
"""
from threading import Thread

from pylon.core.tools import log
from tools import auth, rpc_tools

from .admission import suspend_owner_in_project
from .governance import is_governed


def invalidated_user_id(payload):
    """The user id an ``auth_invalidate`` payload announces, or ``None`` (not a user, or not well formed)."""
    if not isinstance(payload, dict) or payload.get("kind") != "user":
        return None
    user_id = payload.get("id")
    return user_id if isinstance(user_id, int) and not isinstance(user_id, bool) else None


def suspend_inactive_owner(user_id, project_ids=None) -> dict:
    """If ``user_id`` is inactive, suspend their active mandates in every project (one transaction per project).
    ``{project_id: [mandate ids]}`` for what was suspended. An unreadable verdict changes nothing."""
    epochs = auth.read_identity_epochs("user", user_id)
    if not isinstance(epochs, dict) or epochs.get("active") is not False:
        return {}
    if project_ids is None:
        project_ids = [project["id"] for project in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
            filter_={"create_success": True})]
    suspended = {}
    for project_id in project_ids:
        try:
            ids = suspend_owner_in_project(project_id, user_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("mandate owner: project %s could not be scanned (%s)", project_id, type(exc).__name__)
            continue
        if ids:
            suspended[project_id] = ids
    return suspended


def on_auth_invalidate(module, payload, spawn=None) -> bool:
    """Handle one ``auth_invalidate`` event: return at once unless governed and about a user; the scan itself runs
    off the event thread. True when a scan was started."""
    if not is_governed(module):
        return False
    user_id = invalidated_user_id(payload)
    if user_id is None:
        return False

    def _run():
        try:
            suspend_inactive_owner(user_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("mandate owner: the suspension scan failed (%s)", type(exc).__name__)

    (spawn or (lambda target: Thread(target=target, daemon=True).start()))(_run)
    return True
