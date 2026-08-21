"""
Shared date-range parsing for analytics endpoints.

Parses date_from / date_to query args, defaults to the last
DEFAULT_DATE_RANGE_DAYS days, and clamps pathological spans to
MAX_DATE_RANGE_DAYS so unbounded daily-trend queries can't return an
unbounded number of per-day rows.
"""

from datetime import datetime, timedelta, timezone

from .constants import DEFAULT_DATE_RANGE_DAYS, MAX_DATE_RANGE_DAYS


def parse_date_range(args):
    date_from = args.get("date_from")
    date_to = args.get("date_to")
    try:
        dt_from = datetime.fromisoformat(date_from) if date_from else None
    except (ValueError, TypeError):
        dt_from = None
    try:
        dt_to = datetime.fromisoformat(date_to) if date_to else None
    except (ValueError, TypeError):
        dt_to = None
    if not dt_from and not dt_to:
        dt_to = datetime.now(timezone.utc)
        dt_from = dt_to - timedelta(days=DEFAULT_DATE_RANGE_DAYS)
    elif not dt_from:
        dt_from = dt_to - timedelta(days=DEFAULT_DATE_RANGE_DAYS)
    elif not dt_to:
        dt_to = datetime.now(timezone.utc)
    if dt_from and dt_to and (dt_to - dt_from).days > MAX_DATE_RANGE_DAYS:
        dt_from = dt_to - timedelta(days=MAX_DATE_RANGE_DAYS)
    return dt_from, dt_to
