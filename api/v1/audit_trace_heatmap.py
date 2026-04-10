"""
Audit trail trace-level heatmap aggregation endpoint.

Returns time-bucketed, duration-banded trace counts in nivo heatmap format.
Each trace is counted once, using total trace duration for the duration band.
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
    from sqlalchemy import func, case, literal_column, Integer, Float

    # Duration band labels matching SQL CASE indices:
    # 0=<10ms, 1=10-100ms, 2=100ms-1s, 3=1-10s, 4=>10s
    _BAND_LABELS = ["<10ms", "10-100ms", "100ms-1s", "1-10s", ">10s"]
    _BAND_COUNT = len(_BAND_LABELS)

    # Interval selection based on range width (same as audit_heatmap.py)
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
        """Admin API for trace-level heatmap aggregation of audit trail events."""

        @auth.decorators.check_api({
            "permissions": ["models.admin.audit_trail.view"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            }
        })
        def get(self, **kwargs):
            """
            GET /api/v1/elitea_core/audit_trace_heatmap/administration

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
                    # --- Step 1: Build filtered base query ---
                    base_query = db_session.query(AuditEvent).filter(
                        AuditEvent.trace_id.isnot(None),
                        AuditEvent.timestamp >= dt_from,
                        AuditEvent.timestamp <= dt_to,
                    )

                    search = request.args.get("search")
                    if search:
                        pattern = f"%{search}%"
                        base_query = base_query.filter(
                            AuditEvent.action.ilike(pattern)
                            | AuditEvent.tool_name.ilike(pattern)
                            | AuditEvent.user_email.ilike(pattern)
                            | AuditEvent.model_name.ilike(pattern)
                        )

                    event_type = request.args.get("event_type")
                    if event_type:
                        types = [t.strip() for t in event_type.split(",")]
                        if len(types) == 1:
                            base_query = base_query.filter(AuditEvent.event_type == types[0])
                        else:
                            base_query = base_query.filter(AuditEvent.event_type.in_(types))

                    is_error = request.args.get("is_error")
                    if is_error and is_error.lower() == "true":
                        base_query = base_query.filter(AuditEvent.is_error.is_(True))

                    user_id = request.args.get("user_id")
                    if user_id:
                        try:
                            base_query = base_query.filter(AuditEvent.user_id == int(user_id))
                        except (ValueError, TypeError):
                            pass

                    project_id = request.args.get("project_id")
                    if project_id:
                        try:
                            base_query = base_query.filter(AuditEvent.project_id == int(project_id))
                        except (ValueError, TypeError):
                            pass

                    trace_id = request.args.get("trace_id")
                    if trace_id:
                        base_query = base_query.filter(AuditEvent.trace_id == trace_id)

                    # --- Step 2: Aggregate per trace_id ---
                    trace_duration = (
                        (func.max(
                            func.extract('epoch', AuditEvent.timestamp)
                            + func.coalesce(AuditEvent.duration_ms, literal_column('0'))
                            / literal_column('1000')
                        ) - func.min(
                            func.extract('epoch', AuditEvent.timestamp)
                        )) * literal_column('1000')
                    ).cast(Float).label('trace_duration_ms')

                    trace_agg = base_query.with_entities(
                        AuditEvent.trace_id,
                        func.min(AuditEvent.timestamp).label('start_time'),
                        trace_duration,
                    ).group_by(AuditEvent.trace_id).subquery('trace_agg')

                    # --- Step 3: Bucket by time and duration band ---
                    time_bucket = (
                        func.floor(
                            func.extract('epoch', trace_agg.c.start_time)
                            / literal_column(str(interval))
                        ) * literal_column(str(interval))
                    ).cast(Integer).label("time_bucket")

                    duration_band = case(
                        (trace_agg.c.trace_duration_ms < 10, 0),
                        (trace_agg.c.trace_duration_ms < 100, 1),
                        (trace_agg.c.trace_duration_ms < 1000, 2),
                        (trace_agg.c.trace_duration_ms < 10000, 3),
                        else_=4,
                    ).label("duration_band")

                    heatmap_query = db_session.query(
                        time_bucket,
                        duration_band,
                        func.count().label("cnt"),
                    ).select_from(trace_agg).group_by(
                        literal_column("time_bucket"),
                        literal_column("duration_band"),
                    )

                    rows = heatmap_query.all()

                    # --- Build nivo-ready response ---
                    counts = {}
                    all_buckets = set()
                    total_traces = 0

                    for row in rows:
                        tb = int(row.time_bucket)
                        band = int(row.duration_band)
                        cnt = int(row.cnt)
                        counts[(tb, band)] = cnt
                        all_buckets.add(tb)
                        total_traces += cnt

                    # Generate complete time bucket range
                    min_bucket = int(dt_from.timestamp()) // interval * interval
                    max_bucket = int(dt_to.timestamp()) // interval * interval

                    time_slots = list(range(min_bucket, max_bucket + interval, interval))

                    # Build nivo data: iterate in reverse so >10s is first (top of Y-axis)
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
                            "total_traces": total_traces,
                            "bucket_count": len(time_slots),
                            "range_seconds": int(range_seconds),
                        },
                    }, 200

            except Exception as e:
                log.error(f"Audit trace heatmap query failed: {e}")
                return {"error": "Failed to query audit trace heatmap"}, 500

    class API(api_tools.APIBase):
        url_params = api_tools.with_modes([
            '',
        ])
        mode_handlers = {
            'administration': AdminAPI,
        }
else:
    API = None
