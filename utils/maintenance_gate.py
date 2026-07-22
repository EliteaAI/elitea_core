"""
Cross-plugin helper for cron RPCs that need to skip work while
the platform is in maintenance mode.

Cron ticks (`pipelines_check_scheduling`, `applications_check_index_scheduling`)
are wired directly to arbiter events, so they bypass the maintenance splash
router hook. Each cron RPC should call `is_maintenance_active()` and bail
out early to avoid dispatching new work to task nodes that are rejecting
and tearing down existing tasks.
"""

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from tools import this  # pylint: disable=E0401


def is_maintenance_active() -> bool:
    """Return True when bootstrap's maintenance splash is on.

    Fails open (returns False) if bootstrap is unreachable — we do not want
    a transient plugin lookup error to permanently halt scheduled work.
    """
    try:
        bootstrap = this.for_module("bootstrap")
        if bootstrap is not None and bootstrap.module is not None:
            return bool(bootstrap.module.is_maintenance_active())
    except:  # pylint: disable=W0702
        log.debug("maintenance_gate: is_maintenance_active check failed", exc_info=True)
    return False
