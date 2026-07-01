"""Synthetic monitoring for proactive service health validation.

Runs periodic health probes (every 60s, leader-only) that actively verify
each critical dependency is working, as opposed to passive health checks
that only respond to scrape requests.

Probes:
- HTTP GET /health/live → expect 200 (self-check via internal request)
- Redis PING → expect PONG
- PostgreSQL SELECT 1 → expect result
- Socket.IO connect + disconnect → expect success (optional, configurable)

Results are stored in Redis for Prometheus exposition and alerting.
After 3 consecutive failures for any probe, an alert state is set.

Redis key layout:
  synthetic:probe_results:{probe_name}  — hash with latest result
  synthetic:failure_count:{probe_name}  — integer counter of consecutive failures
  synthetic:alert:{probe_name}          — set when failure_count >= threshold

Usage:
    from elitea_core.utils.synthetic_monitor import SyntheticMonitor

    monitor = SyntheticMonitor(
        redis_client=redis_client,
        db_engine=db_engine,
        health_url="http://localhost:8080/health/live",
    )
    monitor.run_probes()

    status = monitor.get_status()
    # {"status": "healthy", "probes": {...}, "alerts": [...]}
"""

import time

from pylon.core.tools import log


DEFAULT_PROBE_INTERVAL_S = 60
DEFAULT_FAILURE_THRESHOLD = 3
PROBE_RESULT_TTL = 300  # 5 minutes — results expire if monitor stops

KEY_PREFIX_RESULTS = "synthetic:probe_results"
KEY_PREFIX_FAILURES = "synthetic:failure_count"
KEY_PREFIX_ALERT = "synthetic:alert"
ALERTS_SET_KEY = "synthetic:active_alerts"


class ProbeResult:
    """Result of a single probe execution."""

    __slots__ = ("name", "success", "latency_ms", "error", "timestamp")

    def __init__(self, name: str, success: bool, latency_ms: float,
                 error: str = "", timestamp: float = 0.0):
        self.name = name
        self.success = success
        self.latency_ms = latency_ms
        self.error = error
        self.timestamp = timestamp or time.time()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "success": "1" if self.success else "0",
            "latency_ms": str(round(self.latency_ms, 2)),
            "error": self.error,
            "timestamp": str(self.timestamp),
        }


class SyntheticMonitor:
    """Runs synthetic health probes and tracks consecutive failures."""

    def __init__(self, redis_client, db_engine=None, health_url: str = "",
                 sio_url: str = "", webhook_url: str = "",
                 failure_threshold: int = DEFAULT_FAILURE_THRESHOLD):
        """Initialize synthetic monitor.

        Args:
            redis_client: Redis client instance.
            db_engine: SQLAlchemy engine for PostgreSQL probe.
            health_url: URL for HTTP health probe (e.g. http://localhost:8080/health/live).
            sio_url: URL for Socket.IO probe (optional, skipped if empty).
            webhook_url: Base URL for webhook endpoint probe (optional, skipped if empty).
                         Example: http://localhost:8080/api/v2/elitea_core/webhook/prompt_lib/1/1/custom
            failure_threshold: Number of consecutive failures before alerting.
        """
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self._client = redis_client
        self._db_engine = db_engine
        self._health_url = health_url
        self._sio_url = sio_url
        self._webhook_url = webhook_url
        self._failure_threshold = failure_threshold
        self._probes = self._build_probe_list()

    def _build_probe_list(self) -> list:
        """Build the list of active probes based on configured backends."""
        probes = [
            ("redis", self._probe_redis),
        ]
        if self._db_engine is not None:
            probes.append(("postgres", self._probe_postgres))
        if self._health_url:
            probes.append(("http_health", self._probe_http_health))
        if self._sio_url:
            probes.append(("socketio", self._probe_socketio))
        if self._webhook_url:
            probes.append(("webhook", self._probe_webhook))
        return probes

    @property
    def probe_names(self) -> list:
        """Return list of configured probe names."""
        return [name for name, _ in self._probes]

    def run_probes(self) -> list:
        """Execute all configured probes and record results.

        Returns:
            List of ProbeResult objects.
        """
        results = []
        for name, probe_fn in self._probes:
            result = self._execute_probe(name, probe_fn)
            results.append(result)
            self._record_result(result)
        return results

    def _execute_probe(self, name: str, probe_fn) -> ProbeResult:
        """Execute a single probe with timing and error handling."""
        start = time.time()
        try:
            probe_fn()
            latency_ms = (time.time() - start) * 1000
            return ProbeResult(
                name=name,
                success=True,
                latency_ms=latency_ms,
                timestamp=start,
            )
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            return ProbeResult(
                name=name,
                success=False,
                latency_ms=latency_ms,
                error=str(e)[:200],
                timestamp=start,
            )

    def _record_result(self, result: ProbeResult) -> None:
        """Store probe result in Redis and update failure counter."""
        result_key = f"{KEY_PREFIX_RESULTS}:{result.name}"
        failure_key = f"{KEY_PREFIX_FAILURES}:{result.name}"
        alert_key = f"{KEY_PREFIX_ALERT}:{result.name}"

        pipe = self._client.pipeline(transaction=False)
        pipe.hset(result_key, mapping=result.to_dict())
        pipe.expire(result_key, PROBE_RESULT_TTL)

        if result.success:
            pipe.set(failure_key, "0", ex=PROBE_RESULT_TTL)
            pipe.delete(alert_key)
            pipe.srem(ALERTS_SET_KEY, result.name)
        else:
            pipe.incr(failure_key)
            pipe.expire(failure_key, PROBE_RESULT_TTL)

        pipe.execute()

        if not result.success:
            count = self._get_failure_count(result.name)
            if count >= self._failure_threshold:
                self._set_alert(result.name, count, result.error)
                log.warning(
                    "Synthetic probe ALERT: probe=%s, consecutive_failures=%d, error=%s",
                    result.name, count, result.error,
                )
            else:
                log.info(
                    "Synthetic probe failed: probe=%s, count=%d/%d, error=%s",
                    result.name, count, self._failure_threshold, result.error,
                )
        else:
            if self._client.exists(alert_key):
                log.info("Synthetic probe recovered: probe=%s", result.name)
                self._client.delete(alert_key)
                self._client.srem(ALERTS_SET_KEY, result.name)

    def _get_failure_count(self, probe_name: str) -> int:
        """Get current consecutive failure count for a probe."""
        key = f"{KEY_PREFIX_FAILURES}:{probe_name}"
        val = self._client.get(key)
        if val is None:
            return 0
        return int(val.decode() if isinstance(val, bytes) else val)

    def _set_alert(self, probe_name: str, count: int, error: str) -> None:
        """Set alert state for a probe that has exceeded the failure threshold."""
        alert_key = f"{KEY_PREFIX_ALERT}:{probe_name}"
        pipe = self._client.pipeline(transaction=False)
        pipe.hset(alert_key, mapping={
            "probe": probe_name,
            "consecutive_failures": str(count),
            "last_error": error[:200],
            "alerted_at": str(time.time()),
        })
        pipe.expire(alert_key, PROBE_RESULT_TTL * 2)
        pipe.sadd(ALERTS_SET_KEY, probe_name)
        pipe.expire(ALERTS_SET_KEY, PROBE_RESULT_TTL * 3)
        pipe.execute()

    def get_status(self) -> dict:
        """Get current synthetic monitoring status for all probes.

        Returns:
            Dict with overall status, per-probe results, and active alerts.
        """
        probes = {}
        for name, _ in self._probes:
            result_key = f"{KEY_PREFIX_RESULTS}:{name}"
            raw = self._client.hgetall(result_key)
            if raw:
                probes[name] = self._decode_hash(raw)
            else:
                probes[name] = {"status": "no_data"}

        alerts = self._get_active_alerts()
        if alerts:
            status = "alerting"
        elif any(p.get("success") == "0" for p in probes.values() if "success" in p):
            status = "degraded"
        else:
            status = "healthy"

        return {
            "status": status,
            "probes": probes,
            "alerts": alerts,
            "failure_threshold": self._failure_threshold,
        }

    def _get_active_alerts(self) -> list:
        """Get all currently active probe alerts."""
        members = self._client.smembers(ALERTS_SET_KEY)
        if not members:
            return []

        alerts = []
        for member in members:
            name = member.decode() if isinstance(member, bytes) else member
            alert_key = f"{KEY_PREFIX_ALERT}:{name}"
            raw = self._client.hgetall(alert_key)
            if raw:
                alerts.append(self._decode_hash(raw))
            else:
                self._client.srem(ALERTS_SET_KEY, name)
        return alerts

    def get_prometheus_metrics(self) -> list:
        """Get probe results formatted for Prometheus text exposition.

        Returns:
            List of tuples: (metric_name, labels_dict, value).
        """
        metrics = []
        for name, _ in self._probes:
            result_key = f"{KEY_PREFIX_RESULTS}:{name}"
            raw = self._client.hgetall(result_key)
            if not raw:
                continue
            data = self._decode_hash(raw)

            success_val = 1.0 if data.get("success") == "1" else 0.0
            metrics.append((
                "synthetic_probe_success",
                {"probe": name},
                success_val,
            ))

            latency = float(data.get("latency_ms", "0"))
            metrics.append((
                "synthetic_probe_latency_ms",
                {"probe": name},
                latency,
            ))

        failure_count_total = 0
        for name, _ in self._probes:
            count = self._get_failure_count(name)
            failure_count_total += count
            metrics.append((
                "synthetic_probe_consecutive_failures",
                {"probe": name},
                float(count),
            ))

        alert_members = self._client.smembers(ALERTS_SET_KEY)
        alert_count = len(alert_members) if alert_members else 0
        metrics.append((
            "synthetic_probe_alerts_active",
            {},
            float(alert_count),
        ))

        return metrics

    def clear_alerts(self) -> int:
        """Clear all active alerts (manual recovery). Returns count cleared."""
        members = self._client.smembers(ALERTS_SET_KEY)
        if not members:
            return 0
        count = 0
        for member in members:
            name = member.decode() if isinstance(member, bytes) else member
            alert_key = f"{KEY_PREFIX_ALERT}:{name}"
            self._client.delete(alert_key)
            count += 1
        self._client.delete(ALERTS_SET_KEY)
        log.info("Synthetic monitoring: cleared %d alerts", count)
        return count

    def reset_probe(self, probe_name: str) -> bool:
        """Reset failure counter and alert state for a specific probe."""
        if probe_name not in self.probe_names:
            return False
        result_key = f"{KEY_PREFIX_RESULTS}:{probe_name}"
        failure_key = f"{KEY_PREFIX_FAILURES}:{probe_name}"
        alert_key = f"{KEY_PREFIX_ALERT}:{probe_name}"
        pipe = self._client.pipeline(transaction=False)
        pipe.delete(result_key)
        pipe.delete(failure_key)
        pipe.delete(alert_key)
        pipe.srem(ALERTS_SET_KEY, probe_name)
        pipe.execute()
        return True

    # --- Probe implementations ---

    def _probe_redis(self) -> None:
        """Probe: Redis PING → expect PONG."""
        result = self._client.ping()
        if not result:
            raise RuntimeError("Redis PING returned falsy")

    def _probe_postgres(self) -> None:
        """Probe: PostgreSQL SELECT 1 → expect result."""
        from sqlalchemy import text  # pylint: disable=C0415
        with self._db_engine.connect() as conn:
            row = conn.execute(text("SELECT 1"))
            result = row.fetchone()
            if result is None:
                raise RuntimeError("SELECT 1 returned no rows")

    def _probe_http_health(self) -> None:
        """Probe: HTTP GET to health endpoint → expect 200."""
        import urllib.request  # pylint: disable=C0415
        import urllib.error  # pylint: disable=C0415

        req = urllib.request.Request(self._health_url, method="GET")
        req.add_header("User-Agent", "SyntheticMonitor/1.0")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Health endpoint returned {resp.status}")
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"Health endpoint returned {e.code}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"Health endpoint unreachable: {e.reason}") from e

    def _probe_socketio(self) -> None:
        """Probe: Socket.IO connect + disconnect → expect success.

        Uses a lightweight HTTP request to the Socket.IO handshake endpoint
        rather than a full WebSocket connection to keep the probe fast.
        """
        import urllib.request  # pylint: disable=C0415
        import urllib.error  # pylint: disable=C0415

        handshake_url = self._sio_url.rstrip("/") + "/socket.io/?EIO=4&transport=polling"
        req = urllib.request.Request(handshake_url, method="GET")
        req.add_header("User-Agent", "SyntheticMonitor/1.0")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Socket.IO handshake returned {resp.status}")
                body = resp.read(256)
                if b"sid" not in body:
                    raise RuntimeError("Socket.IO handshake missing sid in response")
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"Socket.IO handshake returned {e.code}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"Socket.IO unreachable: {e.reason}") from e

    def _probe_webhook(self) -> None:
        """Probe: POST to webhook endpoint with invalid token → expect 400.

        A 400 response proves the endpoint is live and processing requests
        (signature validation is happening). A 5xx or timeout means the
        webhook handler is broken or unreachable.
        """
        import urllib.request  # pylint: disable=C0415
        import urllib.error  # pylint: disable=C0415

        data = b'{"probe": "synthetic_monitor"}'
        req = urllib.request.Request(self._webhook_url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Webhook-Token", "synthetic-probe-invalid-token")
        req.add_header("User-Agent", "SyntheticMonitor/1.0")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return
                raise RuntimeError(f"Webhook probe unexpected status {resp.status}")
        except urllib.error.HTTPError as e:
            if e.code == 400:
                return  # Expected — endpoint is live but rejected invalid signature
            raise RuntimeError(f"Webhook endpoint returned {e.code}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"Webhook endpoint unreachable: {e.reason}") from e

    @staticmethod
    def _decode_hash(raw: dict) -> dict:
        """Decode Redis hash bytes to str dict."""
        result = {}
        for k, v in raw.items():
            key = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            val = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            result[key] = val
        return result
