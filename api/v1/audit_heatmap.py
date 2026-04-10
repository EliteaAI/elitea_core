"""
Audit trail heatmap aggregation endpoint.

Returns time-bucketed, duration-banded event counts in nivo heatmap format.
"""

from pylon.core.tools import log

try:
    from tools import api_tools, auth, config as c
    _API_AVAILABLE = True
except ImportError:
    _API_AVAILABLE = False


if _API_AVAILABLE:
    from datetime import datetime, timezone
    from flask import request
    from sqlalchemy import func, case, literal_column, Integer, Float, text

    # Duration band labels matching SQL CASE indices:
    # 0=<10ms, 1=10-100ms, 2=100ms-1s, 3=1-10s, 4=>10s
    _BAND_LABELS = ["<10ms", "10-100ms", "100ms-1s", "1-10s", ">10s"]
    _BAND_COUNT = len(_BAND_LABELS)

    # Interval selection based on range width
    _INTERVAL_TABLE = [
        (3600,        60,    "1min"),     # ≤1h → 1min buckets
        (6 * 3600,    300,   "5min"),     # ≤6h → 5min
        (24 * 3600,   900,   "15min"),    # ≤24h → 15min
        (7 * 86400,   3600,  "1h"),       # ≤7d → 1h
        (30 * 86400,  14400, "4h"),       # ≤30d → 4h
    ]
    _DEFAULT_INTERVAL = (86400, "1d")     # >30d → 1d

    def _pick_interval(range_seconds):
        """Select bucket interval based on the date range width."""
        for threshold, interval, label in _INTERVAL_TABLE:
            if range_seconds <= threshold:
                return interval, label
        return _DEFAULT_INTERVAL


    class AdminAPI(api_tools.APIModeHandler):
        """Admin API for heatmap aggregation of audit trail events."""

        @auth.decorators.check_api({
            "permissions": ["models.admin.audit_trail.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            }
        })
        def get(self, **kwargs):
            """
            GET /api/v1/elitea_core/audit_heatmap/administration

            Required: date_from, date_to (ISO datetime)
            Optional: event_type, user_id, project_id, is_error, search, trace_id
            """
            from tools import db
            from ...models.audit_event import AuditEvent

            # --- Parse required date range ---
            date_from_str = request.args.get("date_from")
            date_to_str = request.args.get("date_to")
            if not date_from_str or not date_to_str:
                return {"error": "date_from and date_to are required"}, 400

            try:
                dt_from = datetime.fromisoformat(date_from_str)
                dt_to = datetime.fromisoformat(date_to_str)
            except (ValueError, TypeError):
                return {"error": "Invalid date format"}, 400

            range_seconds = (dt_to - dt_from).total_seconds()
            if range_seconds <= 0:
                return {"error": "date_to must be after date_from"}, 400

            interval, interval_label = _pick_interval(range_seconds)

            try:
                with db.with_project_schema_session(None) as db_session:
                    # Time bucket: floor(epoch / interval) * interval
                    time_bucket = (
                        func.floor(
                            func.extract('epoch', AuditEvent.timestamp)
                            / literal_column(str(interval))
                        ) * literal_column(str(interval))
                    ).cast(Integer).label("time_bucket")

                    # Duration band: 0=<10ms, 1=10-100ms, 2=100ms-1s, 3=1-10s, 4=>10s
                    duration_band = case(
                        (AuditEvent.duration_ms < 10, 0),
                        (AuditEvent.duration_ms < 100, 1),
                        (AuditEvent.duration_ms < 1000, 2),
                        (AuditEvent.duration_ms < 10000, 3),
                        else_=4,
                    ).label("duration_band")

                    query = db_session.query(
                        time_bucket,
                        duration_band,
                        func.count().label("cnt"),
                    )

                    # --- Filters (same as audit.py) ---
                    query = query.filter(AuditEvent.timestamp >= dt_from)
                    query = query.filter(AuditEvent.timestamp <= dt_to)
                    query = query.filter(AuditEvent.duration_ms.isnot(None))

                    search = request.args.get("search")
                    if search:
                        pattern = f"%{search}%"
                        query = query.filter(
                            AuditEvent.action.ilike(pattern)
                            | AuditEvent.tool_name.ilike(pattern)
                            | AuditEvent.user_email.ilike(pattern)
                            | AuditEvent.model_name.ilike(pattern)
                        )

                    event_type = request.args.get("event_type")
                    if event_type:
                        types = [t.strip() for t in event_type.split(",")]
                        if len(types) == 1:
                            query = query.filter(AuditEvent.event_type == types[0])
                        else:
                            query = query.filter(AuditEvent.event_type.in_(types))

                    is_error = request.args.get("is_error")
                    if is_error and is_error.lower() == "true":
                        query = query.filter(AuditEvent.is_error.is_(True))

                    user_id = request.args.get("user_id")
                    if user_id:
                        try:
                            query = query.filter(AuditEvent.user_id == int(user_id))
                        except (ValueError, TypeError):
                            pass

                    project_id = request.args.get("project_id")
                    if project_id:
                        try:
                            query = query.filter(AuditEvent.project_id == int(project_id))
                        except (ValueError, TypeError):
                            pass

                    trace_id = request.args.get("trace_id")
                    if trace_id:
                        query = query.filter(AuditEvent.trace_id == trace_id)

                    # --- Aggregate ---
                    query = query.group_by(
                        literal_column("time_bucket"),
                        literal_column("duration_band"),
                    )

                    rows = query.all()

                    # --- Build nivo-ready response ---
                    # Collect all unique time buckets and counts
                    counts = {}  # {(time_bucket, band_index): count}
                    all_buckets = set()
                    total_events = 0

                    for row in rows:
                        tb = int(row.time_bucket)
                        band = int(row.duration_band)
                        cnt = int(row.cnt)
                        counts[(tb, band)] = cnt
                        all_buckets.add(tb)
                        total_events += cnt

                    # Generate complete time bucket range from user's requested dates
                    # (not from actual data — we want the full requested range)
                    min_bucket = int(dt_from.timestamp()) // interval * interval
                    max_bucket = int(dt_to.timestamp()) // interval * interval

                    time_slots = list(range(min_bucket, max_bucket + interval, interval))

                    # Build nivo data: iterate in reverse so >10s is first (top of Y-axis)
                    # Use epoch timestamps as X — frontend formats in user's local TZ
                    nivo_data = []
                    for band_idx in range(_BAND_COUNT - 1, -1, -1):
                        series = {
                            "id": _BAND_LABELS[band_idx],
                            "data": [],
                        }
                        for ts in time_slots:
                            series["data"].append({
                                "x": ts,
                                "y": counts.get((ts, band_idx), None),
                            })
                        nivo_data.append(series)

                    return {
                        "data": nivo_data,
                        "metadata": {
                            "interval_seconds": interval,
                            "interval_label": interval_label,
                            "total_events": total_events,
                            "bucket_count": len(time_slots),
                            "range_seconds": int(range_seconds),
                        },
                    }, 200

            except Exception as e:
                log.error(f"Audit heatmap query failed: {e}")
                return {"error": "Failed to query audit heatmap"}, 500

    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '',
        ])
        mode_handlers = {
            'administration': AdminAPI,
        }
else:
    API = None
