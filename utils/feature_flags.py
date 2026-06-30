"""Feature flags for horizontal scaling rollout.

Provides a lightweight feature flag system backed by environment variables
and Redis. Flags control gradual enablement of scaling features (Redis state
externalization, Socket.IO Redis adapter, Redis Streams event system).

Priority order (highest first):
  1. Environment variable: FF_{FLAG_NAME}=1|true|yes (or 0|false|no)
  2. Per-project Redis key: feature_flags:{project_id}:{flag_name}
  3. Global Redis key: feature_flags:global:{flag_name}
  4. Default: False

Redis key layout:
  feature_flags:global:{flag_name}       — string: "1" or "0"
  feature_flags:{project_id}:{flag_name} — string: "1" or "0"
"""

import os

from pylon.core.tools import log


KNOWN_FLAGS = (
    "REDIS_STATE_ENABLED",
    "SOCKETIO_REDIS_ENABLED",
    "REDIS_STREAMS_ENABLED",
)

_TRUE_VALUES = frozenset(("1", "true", "yes"))
_FALSE_VALUES = frozenset(("0", "false", "no"))


class FeatureFlags:
    """Redis-backed feature flag checker for horizontal scaling."""

    def __init__(self, redis_client):
        self._client = redis_client

    def is_enabled(self, flag_name: str, project_id=None) -> bool:
        """Check if a feature flag is enabled.

        Args:
            flag_name: The flag name (e.g. "REDIS_STATE_ENABLED")
            project_id: Optional project ID for per-project override

        Returns:
            True if the flag is enabled, False otherwise
        """
        env_val = os.environ.get(f"FF_{flag_name}")
        if env_val is not None:
            return env_val.lower() in _TRUE_VALUES

        if project_id is not None:
            project_key = f"feature_flags:{project_id}:{flag_name}"
            val = self._client.get(project_key)
            if val is not None:
                decoded = val if isinstance(val, str) else val.decode()
                return decoded in _TRUE_VALUES

        global_key = f"feature_flags:global:{flag_name}"
        val = self._client.get(global_key)
        if val is not None:
            decoded = val if isinstance(val, str) else val.decode()
            return decoded in _TRUE_VALUES

        return False

    def set_flag(self, flag_name: str, enabled: bool, project_id=None) -> None:
        """Set a feature flag value in Redis.

        Args:
            flag_name: The flag name
            enabled: Whether to enable or disable the flag
            project_id: Optional project ID for per-project scope (None = global)
        """
        if project_id is not None:
            key = f"feature_flags:{project_id}:{flag_name}"
        else:
            key = f"feature_flags:global:{flag_name}"

        self._client.set(key, "1" if enabled else "0")
        scope = f"project {project_id}" if project_id else "global"
        log.info("Feature flag %s set to %s (%s)", flag_name, enabled, scope)

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

    def get_all_flags(self, project_id=None) -> dict:
        """Get current state of all known flags.

        Args:
            project_id: Optional project ID for per-project resolution

        Returns:
            Dict mapping flag_name -> bool
        """
        return {flag: self.is_enabled(flag, project_id) for flag in KNOWN_FLAGS}

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

            val = self._client.get(key)
            if val is not None:
                decoded = val if isinstance(val, str) else val.decode()
                result[flag] = decoded in _TRUE_VALUES

        return result
