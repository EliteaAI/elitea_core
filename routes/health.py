import hmac
import os
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
            log.error("Health check: Redis ping failed: %s", e)
            checks["redis"] = {
                "status": "unhealthy",
                "latency_ms": round((time.time() - redis_start) * 1000, 1),
            }
            overall_status = "unhealthy"

        # Sentinel check (if configured) — only report status, not topology
        sentinel_info = self.get_sentinel_info()
        if sentinel_info is not None:
            if sentinel_info.get("error"):
                checks["sentinel"] = {"status": "unhealthy"}
                overall_status = "unhealthy"
            elif sentinel_info["sentinels_reachable"] == 0:
                checks["sentinel"] = {"status": "unhealthy"}
                overall_status = "unhealthy"
            else:
                checks["sentinel"] = {"status": "ok"}

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
            log.error("Health check: PostgreSQL failed: %s", e)
            checks["postgres"] = {
                "status": "unhealthy",
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
            log.error("Health check: Redis ping failed: %s", e)
            checks["redis"] = {
                "status": "unhealthy",
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
            log.error("Health check: PostgreSQL failed: %s", e)
            checks["postgres"] = {
                "status": "unhealthy",
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
        if not self._check_admin_auth(flask.request):
            return flask.jsonify({"error": "unauthorized"}), 401

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
            log.error("Health events endpoint failed: %s", e)
            return flask.jsonify({"status": "unhealthy"}), 503

    @web.route("/health/streams")
    def health_streams(self):
        if not self._check_admin_auth(flask.request):
            return flask.jsonify({"error": "unauthorized"}), 401

        from ..utils.streams_monitor import StreamsMonitor  # pylint: disable=C0415
        try:
            client = self.get_redis_client()
            monitor = StreamsMonitor(client)
            status = monitor.get_streams_status()
            code = 200 if status["status"] == "healthy" else 503
            return flask.jsonify(status), code
        except Exception as e:
            log.error("Health streams endpoint failed: %s", e)
            return flask.jsonify({"status": "unhealthy"}), 503

    @web.route("/api/admin/rate-limit/status")
    def rate_limit_status(self):
        if not self._check_admin_auth(flask.request):
            return flask.jsonify({"error": "unauthorized"}), 401

        from ..middleware.rate_limiter import RateLimiter, get_rate_limit_status  # pylint: disable=C0415

        rate_limiter = getattr(self, "_rate_limiter", None)
        if rate_limiter is None:
            redis_client = self.get_redis_client()
            rate_limiter = RateLimiter(redis_client=redis_client)
            self._rate_limiter = rate_limiter

        status = get_rate_limit_status(rate_limiter, flask.request)
        return flask.jsonify(status), 200

    @web.route("/metrics")
    def prometheus_metrics(self):
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST  # pylint: disable=C0415
        from ..utils.prometheus_metrics import MetricsCollector, get_registry  # pylint: disable=C0415

        collector = getattr(self, "_metrics_collector", None)
        if collector is None:
            redis_client = self.get_redis_client()
            sio_server = getattr(self.context, "sio", None)
            collector = MetricsCollector(
                sio_server=sio_server,
                redis_client=redis_client,
            )
            self._metrics_collector = collector
            self._metrics_registry = get_registry(collector)

        registry = self._metrics_registry
        return flask.Response(
            generate_latest(registry),
            mimetype=CONTENT_TYPE_LATEST,
        )

    @web.route("/api/admin/feature-flags", methods=["GET"])
    def admin_feature_flags_list(self):
        from ..utils.feature_flags import FeatureFlags  # pylint: disable=C0415

        if not self._check_admin_auth(flask.request):
            return flask.jsonify({"error": "unauthorized"}), 401

        project_id = flask.request.args.get("project_id")
        if project_id is not None:
            try:
                project_id = int(project_id)
            except (ValueError, TypeError):
                return flask.jsonify({"error": "project_id must be an integer"}), 400

        redis_client = self.get_redis_client()
        ff = FeatureFlags(redis_client)
        flags = ff.list_all_details(project_id=project_id)
        return flask.jsonify({"flags": flags}), 200

    @web.route("/api/admin/feature-flags", methods=["POST"])
    def admin_feature_flags_set(self):
        from ..utils.feature_flags import FeatureFlags, KNOWN_FLAGS  # pylint: disable=C0415

        if not self._check_admin_auth(flask.request):
            return flask.jsonify({"error": "unauthorized"}), 401

        data = flask.request.get_json(silent=True)
        if not data:
            return flask.jsonify({"error": "request body must be JSON"}), 400

        flag_name = data.get("flag_name")
        if not flag_name:
            return flask.jsonify({"error": "flag_name is required"}), 400
        if flag_name not in KNOWN_FLAGS:
            return flask.jsonify({
                "error": f"unknown flag: {flag_name}",
                "known_flags": list(KNOWN_FLAGS),
            }), 400

        enabled = data.get("enabled")
        if enabled is None:
            return flask.jsonify({"error": "enabled (bool) is required"}), 400

        rollout_pct = data.get("rollout_pct", 100)
        try:
            rollout_pct = int(rollout_pct)
        except (ValueError, TypeError):
            return flask.jsonify({"error": "rollout_pct must be an integer 0-100"}), 400
        if rollout_pct < 0 or rollout_pct > 100:
            return flask.jsonify({"error": "rollout_pct must be 0-100"}), 400

        project_id = data.get("project_id")
        if project_id is not None:
            try:
                project_id = int(project_id)
            except (ValueError, TypeError):
                return flask.jsonify({"error": "project_id must be an integer"}), 400

        redis_client = self.get_redis_client()
        ff = FeatureFlags(redis_client)
        ff.set_flag(flag_name, bool(enabled), project_id=project_id,
                    rollout_pct=rollout_pct)

        details = ff.get_flag_details(flag_name, project_id=project_id)
        return flask.jsonify({"updated": details}), 200

    def _check_admin_auth(self, request):
        """Verify admin authorization via internal token or auth context."""
        internal_token = os.environ.get("INTERNAL_SERVICE_TOKEN", "")
        if internal_token:
            header_token = request.headers.get("X-Internal-Token", "")
            if header_token and hmac.compare_digest(header_token, internal_token):
                return True

        auth_info = getattr(flask.g, "auth_info", None)
        if auth_info and auth_info.get("role") in ("admin", "superadmin"):
            return True

        return False
