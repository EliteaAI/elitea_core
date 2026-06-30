"""Data consistency checker for cross-store state validation.

Periodically verifies that Redis and PostgreSQL state are consistent.
Runs every 15 minutes as a leader-only task — only one pod performs
the checks to avoid redundant load.

Checks performed:
  1. Session count: Redis session keys vs active sessions in PostgreSQL
  2. Canvas versions: Redis canvas version counters vs PostgreSQL latest versions
  3. Feature flags: All pods see identical flag state (single Redis source)

On inconsistency: logs an error and increments a Redis metric counter.
Does NOT auto-fix — operators inspect and reconcile manually.

Redis key layout:
  consistency:results:{check_name}     — hash with last check result
  consistency:metrics:{check_name}     — integer counter of inconsistencies found
  consistency:last_run                 — timestamp of last full check run

Usage:
    from elitea_core.utils.consistency_checker import ConsistencyChecker

    checker = ConsistencyChecker(redis_client, db_engine)
    results = checker.run_all_checks()
    # [CheckResult(name="sessions", consistent=True, ...), ...]

    status = checker.get_status()
    # {"status": "consistent", "checks": {...}, "last_run": "..."}
"""

import time

from pylon.core.tools import log


CHECK_INTERVAL_S = 900  # 15 minutes
RESULT_TTL = 1800  # 30 minutes — results expire if checker stops
KEY_PREFIX_RESULTS = "consistency:results"
KEY_PREFIX_METRICS = "consistency:metrics"
LAST_RUN_KEY = "consistency:last_run"

SESSION_KEY_PATTERN = "*_auth_session_*"
CANVAS_VERSION_SUFFIX = ":version"
CANVAS_AUTOSAVE_PREFIX = "canvas_autosave:"
FEATURE_FLAG_GLOBAL_PREFIX = "feature_flags:global:"


class CheckResult:
    """Result of a single consistency check."""

    __slots__ = ("name", "consistent", "details", "redis_count",
                 "db_count", "mismatches", "timestamp")

    def __init__(self, name: str, consistent: bool, details: str = "",
                 redis_count: int = 0, db_count: int = 0,
                 mismatches: int = 0, timestamp: float = 0.0):
        self.name = name
        self.consistent = consistent
        self.details = details
        self.redis_count = redis_count
        self.db_count = db_count
        self.mismatches = mismatches
        self.timestamp = timestamp or time.time()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "consistent": "1" if self.consistent else "0",
            "details": self.details,
            "redis_count": str(self.redis_count),
            "db_count": str(self.db_count),
            "mismatches": str(self.mismatches),
            "timestamp": str(self.timestamp),
        }


class ConsistencyChecker:
    """Cross-store data consistency validator.

    Compares state between Redis and PostgreSQL to detect drift caused by
    partial failures, network partitions, or bugs in state synchronization.
    """

    def __init__(self, redis_client, db_engine=None,
                 session_key_pattern: str = SESSION_KEY_PATTERN,
                 session_table: str = "auth_session"):
        """Initialize the consistency checker.

        Args:
            redis_client: Redis client instance.
            db_engine: SQLAlchemy engine for PostgreSQL queries.
            session_key_pattern: Glob pattern for Redis session keys.
            session_table: Name of the DB table holding active sessions.
        """
        self._client = redis_client
        self._db_engine = db_engine
        self._session_key_pattern = session_key_pattern
        self._session_table = session_table
        self._checks = self._build_check_list()

    def _build_check_list(self) -> list:
        """Build list of checks based on configured backends."""
        checks = [
            ("feature_flags", self._check_feature_flags),
        ]
        if self._db_engine is not None:
            checks.insert(0, ("sessions", self._check_sessions))
            checks.insert(1, ("canvas_versions", self._check_canvas_versions))
        return checks

    @property
    def check_names(self) -> list:
        """Return names of all configured checks."""
        return [name for name, _ in self._checks]

    def run_all_checks(self) -> list:
        """Execute all consistency checks and record results.

        Returns:
            List of CheckResult objects.
        """
        results = []
        for name, check_fn in self._checks:
            result = self._execute_check(name, check_fn)
            results.append(result)
            self._record_result(result)

        self._client.set(LAST_RUN_KEY, str(time.time()), ex=RESULT_TTL)
        return results

    def _execute_check(self, name: str, check_fn) -> CheckResult:
        """Execute a single check with error handling."""
        try:
            return check_fn()
        except Exception as e:
            log.error(
                "Consistency check error: check=%s, error=%r", name, e,
            )
            return CheckResult(
                name=name,
                consistent=False,
                details=f"Check failed with error: {str(e)[:200]}",
                mismatches=-1,
            )

    def _record_result(self, result: CheckResult) -> None:
        """Store check result in Redis and update inconsistency counter."""
        result_key = f"{KEY_PREFIX_RESULTS}:{result.name}"
        metrics_key = f"{KEY_PREFIX_METRICS}:{result.name}"

        pipe = self._client.pipeline(transaction=False)
        pipe.hset(result_key, mapping=result.to_dict())
        pipe.expire(result_key, RESULT_TTL)

        if not result.consistent and result.mismatches > 0:
            pipe.incrby(metrics_key, result.mismatches)
        pipe.execute()

        if not result.consistent:
            log.error(
                "CONSISTENCY VIOLATION: check=%s, redis_count=%d, "
                "db_count=%d, mismatches=%d, details=%s",
                result.name, result.redis_count, result.db_count,
                result.mismatches, result.details,
            )

    def get_status(self) -> dict:
        """Get current consistency check status.

        Returns:
            Dict with overall status, per-check results, metrics, and last_run.
        """
        checks = {}
        for name, _ in self._checks:
            result_key = f"{KEY_PREFIX_RESULTS}:{name}"
            raw = self._client.hgetall(result_key)
            if raw:
                checks[name] = self._decode_hash(raw)
            else:
                checks[name] = {"status": "no_data"}

        metrics = {}
        for name, _ in self._checks:
            metrics_key = f"{KEY_PREFIX_METRICS}:{name}"
            val = self._client.get(metrics_key)
            if val is not None:
                metrics[name] = int(val.decode() if isinstance(val, bytes) else val)
            else:
                metrics[name] = 0

        last_run_raw = self._client.get(LAST_RUN_KEY)
        last_run = ""
        if last_run_raw:
            last_run = last_run_raw.decode() if isinstance(last_run_raw, bytes) else str(last_run_raw)

        any_inconsistent = any(
            c.get("consistent") == "0"
            for c in checks.values()
            if "consistent" in c
        )
        status = "inconsistent" if any_inconsistent else "consistent"

        return {
            "status": status,
            "checks": checks,
            "metrics": metrics,
            "last_run": last_run,
        }

    def get_metrics(self) -> list:
        """Get metrics in Prometheus-compatible format.

        Returns:
            List of tuples: (metric_name, labels_dict, value).
        """
        metrics = []
        for name, _ in self._checks:
            result_key = f"{KEY_PREFIX_RESULTS}:{name}"
            raw = self._client.hgetall(result_key)
            if raw:
                data = self._decode_hash(raw)
                consistent_val = 1.0 if data.get("consistent") == "1" else 0.0
                metrics.append((
                    "consistency_check_result",
                    {"check": name},
                    consistent_val,
                ))
                mismatches = float(data.get("mismatches", "0"))
                metrics.append((
                    "consistency_check_mismatches",
                    {"check": name},
                    mismatches,
                ))

            metrics_key = f"{KEY_PREFIX_METRICS}:{name}"
            val = self._client.get(metrics_key)
            total = int(val.decode() if isinstance(val, bytes) else val) if val else 0
            metrics.append((
                "consistency_check_total_inconsistencies",
                {"check": name},
                float(total),
            ))

        return metrics

    def reset_metrics(self, check_name: str = "") -> int:
        """Reset inconsistency counters. If check_name is empty, resets all.

        Returns:
            Number of counters reset.
        """
        if check_name:
            if check_name not in self.check_names:
                return 0
            metrics_key = f"{KEY_PREFIX_METRICS}:{check_name}"
            self._client.delete(metrics_key)
            return 1

        count = 0
        for name, _ in self._checks:
            metrics_key = f"{KEY_PREFIX_METRICS}:{name}"
            self._client.delete(metrics_key)
            count += 1
        return count

    # --- Check implementations ---

    def _check_sessions(self) -> CheckResult:
        """Check: Redis session count approximately matches active DB sessions.

        Uses SCAN to count Redis session keys (pattern-based) and compares
        against a COUNT query on the session table. A mismatch indicates
        sessions leaked in one store (e.g., Redis expiry with DB still active,
        or DB deletion without Redis cleanup).

        Tolerance: ±5 sessions (accounts for in-flight login/logout).
        """
        redis_count = self._count_redis_keys(self._session_key_pattern)
        db_count = self._count_db_sessions()

        tolerance = 5
        diff = abs(redis_count - db_count)
        consistent = diff <= tolerance
        details = ""
        if not consistent:
            details = (
                f"Session drift: Redis has {redis_count}, DB has {db_count} "
                f"(diff={diff}, tolerance={tolerance})"
            )

        return CheckResult(
            name="sessions",
            consistent=consistent,
            details=details,
            redis_count=redis_count,
            db_count=db_count,
            mismatches=diff if not consistent else 0,
        )

    def _check_canvas_versions(self) -> CheckResult:
        """Check: Canvas versions in Redis match latest versions in PostgreSQL.

        Scans all canvas_autosave:* keys in Redis, extracts the version counter,
        and compares against the version stored in the canvas DB table. Mismatches
        indicate a failed save or missed update.

        Only checks canvases that are active in Redis (have autosave state).
        """
        redis_canvases = self._get_redis_canvas_versions()
        if not redis_canvases:
            return CheckResult(
                name="canvas_versions",
                consistent=True,
                details="No active canvases in Redis to check",
                redis_count=0,
                db_count=0,
            )

        db_versions = self._get_db_canvas_versions(list(redis_canvases.keys()))
        mismatches = 0
        mismatch_details = []

        for canvas_id, redis_version in redis_canvases.items():
            db_version = db_versions.get(canvas_id)
            if db_version is None:
                continue  # Canvas not in DB yet or already deleted — not a consistency issue
            if redis_version != db_version and redis_version > 0:
                mismatches += 1
                if len(mismatch_details) < 3:
                    mismatch_details.append(
                        f"{canvas_id}: redis_v={redis_version}, db_v={db_version}"
                    )

        consistent = mismatches == 0
        details = ""
        if not consistent:
            details = (
                f"Canvas version mismatches: {mismatches}. "
                f"Examples: {'; '.join(mismatch_details)}"
            )

        return CheckResult(
            name="canvas_versions",
            consistent=consistent,
            details=details,
            redis_count=len(redis_canvases),
            db_count=len(db_versions),
            mismatches=mismatches,
        )

    def _check_feature_flags(self) -> CheckResult:
        """Check: Feature flags in Redis are self-consistent.

        Verifies that all global feature flag keys in Redis contain valid JSON
        with the expected schema (enabled: bool, rollout_pct: int). Invalid or
        corrupt flag state would cause pods to disagree on flag values.

        Since all pods read from the same Redis instance, flag consistency
        across pods is guaranteed by design. This check detects data corruption
        or schema violations that could cause parsing errors.
        """
        flag_keys = self._scan_keys(f"{FEATURE_FLAG_GLOBAL_PREFIX}*")
        if not flag_keys:
            return CheckResult(
                name="feature_flags",
                consistent=True,
                details="No global feature flags found in Redis",
                redis_count=0,
                db_count=0,
            )

        invalid_flags = []
        for key in flag_keys:
            raw = self._client.get(key)
            if raw is None:
                continue
            value = raw.decode() if isinstance(raw, bytes) else str(raw)
            if not self._validate_flag_value(value):
                flag_name = key.decode() if isinstance(key, bytes) else str(key)
                flag_name = flag_name.replace(FEATURE_FLAG_GLOBAL_PREFIX, "")
                invalid_flags.append(flag_name)

        mismatches = len(invalid_flags)
        consistent = mismatches == 0
        details = ""
        if not consistent:
            details = (
                f"Invalid feature flag state: {', '.join(invalid_flags[:5])}"
                + (f" (+{mismatches - 5} more)" if mismatches > 5 else "")
            )

        return CheckResult(
            name="feature_flags",
            consistent=consistent,
            details=details,
            redis_count=len(flag_keys),
            db_count=0,
            mismatches=mismatches,
        )

    # --- Helper methods ---

    def _count_redis_keys(self, pattern: str) -> int:
        """Count Redis keys matching a glob pattern using SCAN."""
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor=cursor, match=pattern, count=100)
            count += len(keys)
            if cursor == 0:
                break
        return count

    def _scan_keys(self, pattern: str) -> list:
        """Collect all Redis keys matching a pattern via SCAN."""
        keys = []
        cursor = 0
        while True:
            cursor, batch = self._client.scan(cursor=cursor, match=pattern, count=100)
            keys.extend(batch)
            if cursor == 0:
                break
        return keys

    def _count_db_sessions(self) -> int:
        """Count active sessions in PostgreSQL."""
        from sqlalchemy import text  # pylint: disable=C0415
        with self._db_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {self._session_table}")  # noqa: S608
            )
            row = result.fetchone()
            return row[0] if row else 0

    def _get_redis_canvas_versions(self) -> dict:
        """Get all canvas autosave states from Redis with their versions.

        Returns:
            Dict mapping canvas_id → version (int).
        """
        keys = self._scan_keys(f"{CANVAS_AUTOSAVE_PREFIX}*")
        if not keys:
            return {}

        versions = {}
        pipe = self._client.pipeline(transaction=False)
        decoded_keys = []
        for key in keys:
            decoded = key.decode() if isinstance(key, bytes) else str(key)
            decoded_keys.append(decoded)
            pipe.hget(key, "version")

        results = pipe.execute()
        for decoded_key, version_raw in zip(decoded_keys, results):
            canvas_id = decoded_key.replace(CANVAS_AUTOSAVE_PREFIX, "")
            version = int(version_raw) if version_raw else 0
            versions[canvas_id] = version

        return versions

    def _get_db_canvas_versions(self, canvas_ids: list) -> dict:
        """Get canvas versions from PostgreSQL for given IDs.

        The canvas_id format from Redis is "{project_id}_{canvas_uuid}".
        This queries the canvas table by uuid.

        Returns:
            Dict mapping canvas_id → version (int).
        """
        if not canvas_ids:
            return {}

        from sqlalchemy import text  # pylint: disable=C0415

        uuids = []
        id_map = {}
        for cid in canvas_ids:
            parts = cid.split("_", 1)
            if len(parts) == 2:
                uuid_val = parts[1]
                uuids.append(uuid_val)
                id_map[uuid_val] = cid

        if not uuids:
            return {}

        placeholders = ", ".join([f":uuid_{i}" for i in range(len(uuids))])
        query = text(
            f"SELECT uuid, version FROM chat_canvas WHERE uuid IN ({placeholders})"  # noqa: S608
        )
        params = {f"uuid_{i}": uid for i, uid in enumerate(uuids)}

        versions = {}
        with self._db_engine.connect() as conn:
            result = conn.execute(query, params)
            for row in result:
                uuid_val = str(row[0])
                version = row[1] if row[1] is not None else 0
                canvas_id = id_map.get(uuid_val, uuid_val)
                versions[canvas_id] = version

        return versions

    @staticmethod
    def _validate_flag_value(value: str) -> bool:
        """Validate a feature flag value is well-formed.

        Accepts:
          - Legacy format: "1" or "0"
          - JSON format: {"enabled": bool, "rollout_pct": int}
        """
        if value in ("1", "0"):
            return True
        try:
            import json  # pylint: disable=C0415
            data = json.loads(value)
            if not isinstance(data, dict):
                return False
            if "enabled" not in data:
                return False
            if not isinstance(data["enabled"], bool):
                return False
            if "rollout_pct" in data:
                pct = data["rollout_pct"]
                if not isinstance(pct, int) or pct < 0 or pct > 100:
                    return False
            return True
        except (json.JSONDecodeError, TypeError, KeyError):
            return False

    @staticmethod
    def _decode_hash(raw: dict) -> dict:
        """Decode Redis hash bytes to str dict."""
        result = {}
        for k, v in raw.items():
            key = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            val = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            result[key] = val
        return result
