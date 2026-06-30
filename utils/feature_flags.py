"""Feature flags for horizontal scaling rollout.

Provides a lightweight feature flag system backed by environment variables
and Redis. Flags control gradual enablement of scaling features (Redis state
externalization, Socket.IO Redis adapter, Redis Streams event system).

Priority order (highest first):
  1. Environment variable: FF_{FLAG_NAME}=1|true|yes (or 0|false|no)
  2. Per-project Redis key: feature_flags:{project_id}:{flag_name}
  3. Global Redis key: feature_flags:global:{flag_name}
  4. Default: False

Redis key layout (v2 — JSON values with rollout support):
  feature_flags:global:{flag_name}       — JSON: {"enabled": bool, "rollout_pct": int}
  feature_flags:{project_id}:{flag_name} — JSON: {"enabled": bool, "rollout_pct": int}

Legacy plain "1"/"0" string values are still supported for backward compatibility.

Percentage rollout:
  When rollout_pct < 100, the flag is only enabled for a deterministic subset of
  users: hash(user_id) % 100 < rollout_pct. Without a user_id, the flag uses
  the enabled field directly.
"""

import hashlib
import json
import os

from pylon.core.tools import log


KNOWN_FLAGS = (
    "REDIS_STATE_ENABLED",
    "SOCKETIO_REDIS_ENABLED",
    "REDIS_STREAMS_ENABLED",
    "REDIS_STREAMS",
    "SENTINEL_MODE",
    "HPA_ENABLED",
    "EVENT_DEDUP",
)

_TRUE_VALUES = frozenset(("1", "true", "yes"))
_FALSE_VALUES = frozenset(("0", "false", "no"))


def _hash_user_bucket(user_id) -> int:
    """Deterministically map a user_id to a bucket 0-99."""
    digest = hashlib.sha256(str(user_id).encode()).hexdigest()
    return int(digest[:8], 16) % 100


def _parse_flag_value(raw):
    """Parse a Redis flag value (JSON or legacy plain string).

    Returns:
        tuple of (enabled: bool, rollout_pct: int)
    """
    if raw is None:
        return None, None

    decoded = raw if isinstance(raw, str) else raw.decode()

    if decoded in _TRUE_VALUES:
        return True, 100
    if decoded in _FALSE_VALUES:
        return False, 0

    try:
        data = json.loads(decoded)
        enabled = bool(data.get("enabled", False))
        rollout_pct = int(data.get("rollout_pct", 100 if enabled else 0))
        rollout_pct = max(0, min(100, rollout_pct))
        return enabled, rollout_pct
    except (json.JSONDecodeError, TypeError, ValueError):
        return False, 0


class FeatureFlags:
    """Redis-backed feature flag checker for horizontal scaling."""

    def __init__(self, redis_client):
        self._client = redis_client

    def is_enabled(self, flag_name: str, project_id=None, user_id=None) -> bool:
        """Check if a feature flag is enabled.

        Args:
            flag_name: The flag name (e.g. "REDIS_STATE_ENABLED")
            project_id: Optional project ID for per-project override
            user_id: Optional user ID for percentage rollout evaluation

        Returns:
            True if the flag is enabled, False otherwise
        """
        env_val = os.environ.get(f"FF_{flag_name}")
        if env_val is not None:
            return env_val.lower() in _TRUE_VALUES

        if project_id is not None:
            project_key = f"feature_flags:{project_id}:{flag_name}"
            raw = self._client.get(project_key)
            if raw is not None:
                enabled, rollout_pct = _parse_flag_value(raw)
                return self._evaluate_rollout(enabled, rollout_pct, user_id)

        global_key = f"feature_flags:global:{flag_name}"
        raw = self._client.get(global_key)
        if raw is not None:
            enabled, rollout_pct = _parse_flag_value(raw)
            return self._evaluate_rollout(enabled, rollout_pct, user_id)

        return False

    def _evaluate_rollout(self, enabled: bool, rollout_pct: int, user_id=None) -> bool:
        """Evaluate whether a flag is active given rollout percentage."""
        if not enabled:
            return False
        if rollout_pct >= 100:
            return True
        if rollout_pct <= 0:
            return False
        if user_id is None:
            return True
        return _hash_user_bucket(user_id) < rollout_pct

    def set_flag(self, flag_name: str, enabled: bool, project_id=None,
                 rollout_pct: int = 100, ttl: int = None) -> None:
        """Set a feature flag value in Redis.

        Args:
            flag_name: The flag name
            enabled: Whether to enable or disable the flag
            project_id: Optional project ID for per-project scope (None = global)
            rollout_pct: Percentage of users to enable for (0-100, default 100)
            ttl: Optional TTL in seconds (default 30 days; prevents orphaned keys)
        """
        if project_id is not None:
            key = f"feature_flags:{project_id}:{flag_name}"
        else:
            key = f"feature_flags:global:{flag_name}"

        rollout_pct = max(0, min(100, rollout_pct))
        value = json.dumps({"enabled": enabled, "rollout_pct": rollout_pct})
        expire = ttl if ttl is not None else 2592000  # 30 days default
        self._client.set(key, value, ex=expire)
        scope = f"project {project_id}" if project_id else "global"
        log.info(
            "Feature flag %s set to enabled=%s rollout_pct=%d (%s)",
            flag_name, enabled, rollout_pct, scope,
        )

    def delete_flag(self, flag_name: str, project_id=None) -> bool:
        """Remove a feature flag from Redis (reverts to default/env behavior).

        Args:
            flag_name: The flag name
            project_id: Optional project ID (None = global)

        Returns:
            True if the flag existed and was removed
        """
        if project_id is not None:
            key = f"feature_flags:{project_id}:{flag_name}"
        else:
            key = f"feature_flags:global:{flag_name}"

        removed = self._client.delete(key)
        return removed > 0

    def get_all_flags(self, project_id=None, user_id=None) -> dict:
        """Get current state of all known flags.

        Args:
            project_id: Optional project ID for per-project resolution
            user_id: Optional user ID for percentage rollout evaluation

        Returns:
            Dict mapping flag_name -> bool
        """
        return {
            flag: self.is_enabled(flag, project_id, user_id)
            for flag in KNOWN_FLAGS
        }

    def get_flag_details(self, flag_name: str, project_id=None) -> dict:
        """Get full details for a single flag including rollout config.

        Args:
            flag_name: The flag name
            project_id: Optional project ID (None = global)

        Returns:
            Dict with keys: enabled, rollout_pct, source (env/project/global/default)
        """
        env_val = os.environ.get(f"FF_{flag_name}")
        if env_val is not None:
            is_on = env_val.lower() in _TRUE_VALUES
            return {
                "flag_name": flag_name,
                "enabled": is_on,
                "rollout_pct": 100 if is_on else 0,
                "source": "env",
            }

        if project_id is not None:
            project_key = f"feature_flags:{project_id}:{flag_name}"
            raw = self._client.get(project_key)
            if raw is not None:
                enabled, rollout_pct = _parse_flag_value(raw)
                return {
                    "flag_name": flag_name,
                    "enabled": enabled,
                    "rollout_pct": rollout_pct,
                    "source": "project",
                    "project_id": project_id,
                }

        global_key = f"feature_flags:global:{flag_name}"
        raw = self._client.get(global_key)
        if raw is not None:
            enabled, rollout_pct = _parse_flag_value(raw)
            return {
                "flag_name": flag_name,
                "enabled": enabled,
                "rollout_pct": rollout_pct,
                "source": "global",
            }

        return {
            "flag_name": flag_name,
            "enabled": False,
            "rollout_pct": 0,
            "source": "default",
        }

    def list_all_details(self, project_id=None) -> list:
        """Get full details for all known flags.

        Args:
            project_id: Optional project ID for per-project resolution

        Returns:
            List of dicts with flag details
        """
        return [self.get_flag_details(flag, project_id) for flag in KNOWN_FLAGS]

    def list_overrides(self, project_id=None) -> dict:
        """List flags that have explicit Redis overrides (not env/default).

        Args:
            project_id: Optional project ID (None = global overrides only)

        Returns:
            Dict mapping flag_name -> current value for flags with Redis keys
        """
        result = {}
        for flag in KNOWN_FLAGS:
            if project_id is not None:
                key = f"feature_flags:{project_id}:{flag}"
            else:
                key = f"feature_flags:global:{flag}"

            raw = self._client.get(key)
            if raw is not None:
                enabled, _ = _parse_flag_value(raw)
                result[flag] = enabled

        return result
