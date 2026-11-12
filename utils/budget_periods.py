"""Monthly budget history helpers (#7510).

Pure functions for the month window and validation, plus the small database helpers that
read and write the per-month snapshot of a project's effective limit. Kept out of the RPC and
API classes so both use one implementation and the logic can be tested without pylon.

Months are UTC calendar months, the same boundaries the Usage counters use.
"""

import datetime

# The current month and the three before it
WINDOW_MONTHS = 4

# Keep upserts well under any bind-parameter limit
UPSERT_CHUNK = 500


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


def limit_source(row, effective):
    """Where a project's enforced limit came from: explicit, default or unlimited.

    Must agree with _limit_source in api/v2/project_budgets.py for project rows; the
    snapshot stores exactly what the admin list shows for the current month.
    """
    exempt = bool(row) and not row.get("enabled", True)
    #
    if row and not exempt and row.get("monthly_limit") is not None:
        return "explicit"
    #
    if effective is None:
        return "unlimited"
    #
    return "default"


def desired_snapshots(project_ids, budgets, limits):
    """What each project's snapshot should say right now, keyed by project id."""
    desired = {}
    #
    for project_id in project_ids:
        row = budgets.get(project_id) or budgets.get(str(project_id)) or {}
        effective = limits.get(project_id, limits.get(str(project_id)))
        #
        desired[int(project_id)] = {
            "effective_limit": effective,
            "limit_source": limit_source(row, effective),
            "currency": row.get("currency") or "USD",
        }
    #
    return desired


def changed_snapshots(existing, desired):
    """The subset of desired that is missing or different, so an unchanged hour writes nothing."""
    changed = {}
    #
    for project_id, wanted in desired.items():
        stored = existing.get(project_id)
        #
        if stored is None or any(stored.get(key) != value for key, value in wanted.items()):
            changed[project_id] = wanted
    #
    return changed


def read_snapshots(session, period, project_ids=None):
    """Stored snapshots of one month, keyed by project id."""
    from ..models.project_budget_period import ProjectBudgetPeriod  # pylint: disable=C0415
    #
    query = session.query(ProjectBudgetPeriod).filter(ProjectBudgetPeriod.period == period)
    #
    if project_ids is not None:
        query = query.filter(ProjectBudgetPeriod.project_id.in_([int(pid) for pid in project_ids]))
    #
    return {row.project_id: row.to_json() for row in query.all()}


def upsert_snapshots(session, period, rows):
    """Write snapshots of one month. Last write wins, safe to run on several workers at once."""
    from sqlalchemy.dialects.postgresql import insert  # pylint: disable=C0415
    from sqlalchemy import func  # pylint: disable=C0415
    from ..models.project_budget_period import ProjectBudgetPeriod  # pylint: disable=C0415
    #
    items = list(rows.items())
    #
    for at in range(0, len(items), UPSERT_CHUNK):
        values = [
            {"project_id": project_id, "period": period, **snapshot}
            for project_id, snapshot in items[at:at + UPSERT_CHUNK]
        ]
        statement = insert(ProjectBudgetPeriod).values(values)
        statement = statement.on_conflict_do_update(
            index_elements=["project_id", "period"],
            set_={
                "effective_limit": statement.excluded.effective_limit,
                "limit_source": statement.excluded.limit_source,
                "currency": statement.excluded.currency,
                "captured_at": func.now(),
            },
        )
        session.execute(statement)
    #
    session.commit()
