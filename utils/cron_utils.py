"""Local cron evaluation helper.

Replaces per-pipeline / per-toolkit `scheduling_time_to_run` RPC round-trips
with an in-process croniter call. Same algorithm as the scheduling plugin's
RPC at scheduling/rpc/main.py:56-96.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter
from pylon.core.tools import log


def is_cron_due(cron: str, last_run_iso: str, timezone: str) -> bool:
    """Return True if cron expression is due relative to last_run.

    Args:
        cron: Cron expression string.
        last_run_iso: ISO 8601 datetime string with timezone info.
        timezone: IANA timezone name for cron evaluation.
    """
    try:
        tz = ZoneInfo(timezone)
        now = datetime.now(tz)
        last_run_in_tz = datetime.fromisoformat(last_run_iso).astimezone(tz)
        next_run = croniter(cron, last_run_in_tz, datetime).get_next()
        return next_run <= now
    except Exception as error:
        log.error(
            f"is_cron_due failed: cron={cron!r}, last_run={last_run_iso!r}, "
            f"timezone={timezone!r}, error={error!r}"
        )
        return False
