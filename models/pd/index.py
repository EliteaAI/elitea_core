from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, root_validator, validator, ValidationError
from datetime import datetime, timedelta, timezone

from croniter import croniter


_DAILY_FLOOR = timedelta(hours=24)
# Number of consecutive firings to inspect when verifying the minimum gap.
# 32 covers monthly patterns (28-31 day gaps) and weekly multi-day patterns
# without making validation expensive.
_GAP_PROBE_FIRINGS = 32
# Probing from a fixed instant rather than "now" keeps a given expression's gaps identical
# whenever they are asked for, so an expiration window cannot depend on the minute a
# schedule happened to be saved.
_GAP_PROBE_BASE = datetime(2000, 1, 1, tzinfo=timezone.utc)

# How long a schedule may run before somebody has to renew it. The shortest gap a monthly
# cron can produce is 28 days (February), so that boundary keeps every monthly pattern on
# the low-frequency side of the split.
_MONTHLY_GAP = timedelta(days=28)
_HIGH_FREQUENCY_WINDOW = timedelta(days=90)
_LOW_FREQUENCY_WINDOW = timedelta(days=180)
_LOW_FREQUENCY_FIRINGS = 6

_DEFAULT_TIMEZONE = 'UTC'
_EPOCH_ISO = datetime(1970, 1, 1, tzinfo=timezone.utc).isoformat()


def _validate_cron_expression(v: str) -> str:
    # basic type and emptiness check for clearer errors than croniter alone
    if not isinstance(v, str) or not v.strip():
        raise ValueError('cron must be a non-empty string')
    v = v.strip()

    # strict validation via croniter
    try:
        # constructing croniter is enough to validate the expression
        croniter(v)
    except Exception as e:
        raise ValueError(f'invalid cron expression: {e}')

    return v


def _cron_gaps(cron_expr: str) -> list[timedelta]:
    """Gaps between the next ``_GAP_PROBE_FIRINGS`` + 1 firings of an expression."""
    itr = croniter(cron_expr, _GAP_PROBE_BASE)
    prev = itr.get_next(datetime)
    gaps = []
    for _ in range(_GAP_PROBE_FIRINGS):
        nxt = itr.get_next(datetime)
        gaps.append(nxt - prev)
        prev = nxt
    return gaps


def _validate_daily_floor(cron_expr: str) -> str:
    """Reject cron expressions that fire more than once per 24 hours.

    Mirrors the daily-frequency floor enforced by the index scheduling UI
    (validateMinimumDailyFrequency in indexSchedule.helpers.js). Direct API
    callers bypass the UI gate, so the same constraint is enforced here.

    Probes the next several firings and asserts every consecutive gap is
    >= 24h. This catches all sub-daily patterns (every-N-minutes, multiple
    hours per day, hour ranges) without re-implementing cron field parsing.
    """
    if min(_cron_gaps(cron_expr)) < _DAILY_FLOOR:
        raise ValueError('Frequency cannot be more than once per day')
    return cron_expr


def compute_schedule_expiration(cron_expr: str, from_dt: datetime) -> datetime:
    """When a schedule saved at ``from_dt`` stops running unless somebody renews it.

    Cadences of more than once a month get 90 days. Rarer ones get 180 days, or six
    firings' worth of time when that is longer, so a quarterly schedule is not retired
    after two runs.

    Raises ValueError for an expression croniter cannot walk (a valid-looking but
    unreachable date like Feb 30 constructs fine and only fails on iteration). That is the
    caller's cue to leave the schedule without a deadline: a schedule nobody can price is
    one to leave running, not one to guess a deadline for.
    """
    try:
        gaps = _cron_gaps(cron_expr)
    except Exception as e:
        raise ValueError(f'cannot determine the firing interval of cron {cron_expr!r}: {e}')
    # The *shortest* gap decides the class: a cron firing on the 1st and the 2nd of every
    # month is a high-frequency schedule that merely happens to have one long gap.
    if min(gaps) < _MONTHLY_GAP:
        return from_dt + _HIGH_FREQUENCY_WINDOW
    # The *mean* gap sizes the window, because it is how long six firings actually take.
    mean_gap = sum(gaps, timedelta()) / len(gaps)
    return from_dt + max(_LOW_FREQUENCY_WINDOW, _LOW_FREQUENCY_FIRINGS * mean_gap)


def _normalize_optional_utc_iso(value) -> Optional[str]:
    """An ISO 8601 UTC string, or None for anything unreadable.

    Shared by the two fields whose absence is recoverable, so they cannot drift apart.
    """
    if value is None:
        return None
    try:
        dt = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except Exception:  # TypeError for a dict/int, ValueError for a malformed string
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


class Credentials(BaseModel):
    private: Optional[bool] = False
    elitea_title: str

    @root_validator(pre=True)
    def accept_legacy_title(cls, values):
        # Schedules stored before the alita->elitea rename still carry `alita_title`.
        # configurations.expand_configuration reads either key, so rejecting them here
        # would strand rows that the rest of the pipeline can still resolve.
        if isinstance(values, dict) and not values.get('elitea_title') and values.get('alita_title'):
            values = {**values, 'elitea_title': values['alita_title']}
        return values


class UpdateIndexingSchedule(BaseModel):
    cron: str
    enabled: bool = False
    # -1 indicates no user and
    # scheduling is in project configurations
    user_id: Optional[int] = -1
    credentials: Optional[Credentials] = None
    timezone: str

    @validator('timezone')
    def validate_timezone(cls, v):
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError('timezone must be a valid IANA timezone name, e.g., "Etc/GMT-3", "Asia/Tokyo"')
        return v

    @validator('cron')
    def validate_cron(cls, v: str) -> str:
        v = _validate_cron_expression(v)
        return _validate_daily_floor(v)


class ToolkitIndexingSchedule(BaseModel):
    cron: str
    enabled: bool
    credentials: Optional[Credentials] = None
    # Schedules written before these fields existed have no author, timezone or last_run.
    # They stay parseable so the scheduler can still run them; a missing author only
    # blocks resolving a *private* credential, which resolve_credentials rejects on its own.
    created_by: Optional[int] = Field(default=None, gt=0)
    timezone: str = _DEFAULT_TIMEZONE
    # The scheduler's cron cursor (ISO 8601, UTC), not a record that an index ran: any
    # concluded attempt advances it, and saving or disabling the schedule resets it. The
    # run record is the pgvector index_meta history.
    last_run: str = _EPOCH_ISO
    # When the current run of retryable credential-lookup failures began, or None when no
    # such run is in progress. Set on the first failure and left alone until something
    # concludes, so its age measures the outage rather than the tick interval.
    retry_since: Optional[str] = None
    # The deadline (ISO 8601, UTC) after which the tick disables this schedule until its
    # owner renews it. None means "not priced yet", NOT "never expires": the tick stamps a
    # deadline the first time it sees an enabled schedule without one, so schedules that
    # predate this field are retired too — a full window from that first sight, never
    # backdated to their creation, which would disable most of the platform's schedules in
    # a single tick.
    expires_at: Optional[str] = None
    # Which expiry warnings have already been sent for the *current* deadline. Persisted
    # because the tick re-reads this schedule every minute while a warning window is days
    # wide, so without it one deadline would notify thousands of times. Rebuilt empty on
    # every save, which is what makes a renewal re-arm both warnings.
    notified_expiry_warnings: list[str] = Field(default_factory=list)

    @validator('timezone', pre=True)
    def validate_timezone(cls, v):
        if v is None:
            return _DEFAULT_TIMEZONE
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError('timezone must be a valid IANA timezone name, e.g., "Etc/GMT-3", "Asia/Tokyo"')
        return v

    @validator('last_run', pre=True)
    def normalize_last_run(cls, v):
        """Accept datetime or string, ensure tz is present, normalize to UTC, and store as ISO string."""
        if v is None:
            # An absent last_run makes the schedule immediately due, which is the
            # intended recovery for a legacy row that has never run.
            return _EPOCH_ISO
        # Convert input to datetime first
        if isinstance(v, datetime):
            dt = v
        else:
            try:
                dt = datetime.fromisoformat(v)
            except Exception:
                raise ValueError('last_run must be a valid ISO 8601 datetime string')

        # If no timezone or naive, assume UTC
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Normalize to UTC if not already
            dt = dt.astimezone(timezone.utc)

        return dt.isoformat()

    @validator('retry_since', pre=True)
    def normalize_retry_since(cls, v):
        """Degrade to None on anything unreadable — deliberately unlike normalize_last_run
        above, which raises.

        A raise here strands the schedule permanently: parse_obj fails, and the tick logs
        "invalid schedule configuration" and skips it on every scan (#6526). This value only
        decides when to escalate a report, so an unreadable one is worth another grace
        window of silence, never a schedule that can no longer run at all. None must keep
        meaning "no retry run in progress" — an epoch fallback would read as "failing since
        1970" and escalate on the first tick.
        """
        return _normalize_optional_utc_iso(v)

    @validator('expires_at', pre=True)
    def normalize_expires_at(cls, v):
        """Degrades to None for the same reason as ``retry_since`` above.

        None is read as "price this schedule on the next tick", so an unreadable deadline
        costs the schedule one more window of life. Raising would strand it forever (#6526),
        and defaulting to the epoch would disable it on the next tick.
        """
        return _normalize_optional_utc_iso(v)

    @validator('notified_expiry_warnings', pre=True)
    def normalize_notified_expiry_warnings(cls, v):
        """Same degrade-don't-raise rule; the worst case is one duplicate warning."""
        if not isinstance(v, list):
            return []
        return [str(item) for item in v]

    @validator('cron')
    def validate_cron(cls, v: str) -> str:
        return _validate_cron_expression(v)


class SaveIndexConfiguration(BaseModel):
    # The tool schema is owned by the SDK, so the payload is only shape-checked here.
    configuration: dict


class IndexDataRemovedEvent(BaseModel):
    index_name: str
    toolkit_id: int = Field(gt=0)
    project_id: int = Field(gt=0)

    class Config:
        extra = 'allow'
