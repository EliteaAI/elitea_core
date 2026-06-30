import time

import flask
from sqlalchemy import text

from pylon.core.tools import web, log


class Route:
    @web.route("/health/live")
    def health_live(self):
        checks = {}
        overall_status = "ok"

        # Redis check
        redis_start = time.time()
        try:
            client = self.get_redis_client()
            client.ping()
            checks["redis"] = {
                "status": "ok",
                "latency_ms": round((time.time() - redis_start) * 1000, 1),
            }
        except Exception as e:
            checks["redis"] = {
                "status": "unhealthy",
                "error": str(e),
                "latency_ms": round((time.time() - redis_start) * 1000, 1),
            }
            overall_status = "unhealthy"

        # PostgreSQL check
        pg_start = time.time()
        try:
            from tools import db as db_tools  # pylint: disable=C0415
            with db_tools.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            checks["postgres"] = {
                "status": "ok",
                "latency_ms": round((time.time() - pg_start) * 1000, 1),
            }
        except Exception as e:
            checks["postgres"] = {
                "status": "unhealthy",
                "error": str(e),
                "latency_ms": round((time.time() - pg_start) * 1000, 1),
            }
            overall_status = "unhealthy"

        code = 200 if overall_status == "ok" else 503
        return flask.jsonify({
            "status": overall_status,
            "checks": checks,
        }), code

    @web.route("/health/ready")
    def health_ready(self):
        checks = {}
        overall_status = "ok"

        # Check that plugin initialization is complete
        init_complete = getattr(self, "_scaling_ready", False)
        if init_complete:
            checks["init"] = {"status": "ok"}
        else:
            checks["init"] = {"status": "not_ready"}
            overall_status = "not_ready"

        # Redis check (must be reachable to serve requests)
        redis_start = time.time()
        try:
            client = self.get_redis_client()
            client.ping()
            checks["redis"] = {
                "status": "ok",
                "latency_ms": round((time.time() - redis_start) * 1000, 1),
            }
        except Exception as e:
            checks["redis"] = {
                "status": "unhealthy",
                "error": str(e),
                "latency_ms": round((time.time() - redis_start) * 1000, 1),
            }
            overall_status = "unhealthy"

        # PostgreSQL check
        pg_start = time.time()
        try:
            from tools import db as db_tools  # pylint: disable=C0415
            with db_tools.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            checks["postgres"] = {
                "status": "ok",
                "latency_ms": round((time.time() - pg_start) * 1000, 1),
            }
        except Exception as e:
            checks["postgres"] = {
                "status": "unhealthy",
                "error": str(e),
                "latency_ms": round((time.time() - pg_start) * 1000, 1),
            }
            overall_status = "unhealthy"

        code = 200 if overall_status == "ok" else 503
        return flask.jsonify({
            "status": overall_status,
            "checks": checks,
        }), code

    @web.route("/health/events")
    def health_events(self):
        from ..utils.event_metrics import EventMetrics  # pylint: disable=C0415
        try:
            client = self.get_redis_client()
            metrics = EventMetrics(client)
            summary = metrics.get_summary()
            streams = metrics.get_all_streams_health()
            status = "healthy"
            if summary.get("streams_unhealthy", 0) > 0:
                status = "degraded"
            return flask.jsonify({
                "status": status,
                "summary": summary,
                "streams": streams,
            }), 200
        except Exception as e:
            return flask.jsonify({
                "status": "unhealthy",
                "error": str(e),
            }), 503

    @web.route("/health/streams")
    def health_streams(self):
        from ..utils.streams_monitor import StreamsMonitor  # pylint: disable=C0415
        try:
            client = self.get_redis_client()
            monitor = StreamsMonitor(client)
            status = monitor.get_streams_status()
            code = 200 if status["status"] == "healthy" else 503
            return flask.jsonify(status), code
        except Exception as e:
            return flask.jsonify({
                "status": "unhealthy",
                "error": str(e),
            }), 503
