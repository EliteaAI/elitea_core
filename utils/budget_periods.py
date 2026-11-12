"""Month selection for the admin budgets listing (#7510).

Pure functions for the selectable months, validation of a requested period and the reporting
range. Months are UTC calendar months, the same boundaries the Usage counters use.
"""

import datetime

# The current month and the three before it
WINDOW_MONTHS = 4


def _utc_now(now=None):
    return now or datetime.datetime.now(datetime.timezone.utc)


def current_period(now=None):
    """'YYYYMM' of the current UTC month."""
    return f"{_utc_now(now):%Y%m}"


def _shift(year, month, delta):
    index = year * 12 + (month - 1) + delta
    #
    return index // 12, index % 12 + 1


def period_window(now=None, months=WINDOW_MONTHS):
    """The selectable months, newest first: the current one and the ones before it."""
    now = _utc_now(now)
    #
    return [f"{year:04d}{month:02d}" for year, month in (
        _shift(now.year, now.month, -offset) for offset in range(months)
    )]


def validate_period(period, now=None):
    """Return the period to report on, or raise ValueError with a message safe to show.

    None or empty means the current month, which keeps existing callers unchanged.
    """
    if not period:
        return current_period(now)
    #
    period = str(period)
    #
    if len(period) != 6 or not period.isdigit():
        raise ValueError("period must be in YYYYMM format")
    #
    if not 1 <= int(period[4:6]) <= 12:
        raise ValueError("period month must be 01-12")
    #
    if period > current_period(now):
        raise ValueError("period must not be in the future")
    #
    if period not in period_window(now):
        raise ValueError(f"period must be one of the last {WINDOW_MONTHS} months")
    #
    return period


def period_range(period, now=None):
    """(first day, last day) of the reporting range as ISO dates.

    The current month is month-to-date, so it ends today; a past month is the whole month.
    """
    now = _utc_now(now)
    year, month = int(period[:4]), int(period[4:6])
    first = datetime.date(year, month, 1)
    #
    if period == current_period(now):
        return first.isoformat(), now.date().isoformat()
    #
    next_year, next_month = _shift(year, month, 1)
    last = datetime.date(next_year, next_month, 1) - datetime.timedelta(days=1)
    #
    return first.isoformat(), last.isoformat()
