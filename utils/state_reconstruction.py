"""Startup state reconstruction for horizontal scaling.

On pod startup, verifies that externalized Redis state is available and
reports a summary of what was found. If critical registries (toolkit schemas,
index types, prebuilt configs) are missing from Redis, emits event_node
requests to trigger re-population from the indexer.

This module is called during the module's ready() phase, after all event_node
subscriptions are established. It does NOT block startup — missing state will
be populated asynchronously via event_node broadcasts from the indexer.
"""

from pylon.core.tools import log


class StateReconstruction:
    """Checks and reports on externalized state availability at startup."""

    def __init__(self, redis_client, event_node=None):
        """Initialize state reconstruction.

        Args:
            redis_client: Redis client instance (decode_responses=True)
            event_node: Optional event_node for re-requesting missing state.
                       If None, missing state is only logged (not re-requested).
        """
        self._client = redis_client
        self._event_node = event_node

    def run(self) -> dict:
        """Execute state reconstruction checks and return summary.

        Returns:
            Dict with keys: registries, sessions, callbacks, total_keys_found,
            missing_registries, re_requested.
        """
        summary = {
            "registries": {},
            "sessions": {},
            "callbacks": 0,
            "total_keys_found": 0,
            "missing_registries": [],
            "re_requested": [],
        }

        self._check_registries(summary)
        self._check_sessions(summary)
        self._check_callbacks(summary)
        self._log_summary(summary)

        return summary

    def _check_registries(self, summary: dict) -> None:
        """Check if global registries are populated in Redis."""
        registries = {
            "toolkit_schemas": {
                "key": "toolkit_schemas:global",
                "request_event": "application_toolkits_request",
            },
            "index_types": {
                "key": "index_types:global",
                "request_event": "application_file_loaders_request",
            },
            "mcp_prebuilt_configs": {
                "key": "mcp_prebuilt_configs:global",
                "request_event": "application_mcp_prebuilt_config_request",
            },
        }

        for name, info in registries.items():
            try:
                count = self._client.hlen(info["key"])
                if count > 0:
                    summary["registries"][name] = count
                    summary["total_keys_found"] += count
                else:
                    summary["registries"][name] = 0
                    summary["missing_registries"].append(name)
                    self._re_request(name, info["request_event"], summary)
            except Exception as exc:
                log.warning(
                    "State reconstruction: failed to check %s: %s", name, exc
                )
                summary["registries"][name] = -1
                summary["missing_registries"].append(name)

    def _check_sessions(self, summary: dict) -> None:
        """Count active MCP server sessions and ASR sessions in Redis."""
        session_types = {
            "mcp_servers": "mcp_servers:*",
            "asr_sessions": "asr_session:*",
        }

        for name, pattern in session_types.items():
            try:
                count = self._scan_count(pattern)
                summary["sessions"][name] = count
                summary["total_keys_found"] += count
            except Exception as exc:
                log.warning(
                    "State reconstruction: failed to count %s: %s", name, exc
                )
                summary["sessions"][name] = -1

    def _check_callbacks(self, summary: dict) -> None:
        """Count pending task callbacks in Redis."""
        try:
            count = self._scan_count("callback_tasks:*")
            summary["callbacks"] = count
            summary["total_keys_found"] += count
        except Exception as exc:
            log.warning(
                "State reconstruction: failed to count callbacks: %s", exc
            )
            summary["callbacks"] = -1

    def _re_request(self, name: str, event_name: str, summary: dict) -> None:
        """Re-request missing registry data via event_node."""
        if self._event_node is None:
            return
        try:
            self._event_node.emit(event_name, dict())
            summary["re_requested"].append(name)
            log.info(
                "State reconstruction: re-requested %s via %s", name, event_name
            )
        except Exception as exc:
            log.warning(
                "State reconstruction: failed to re-request %s: %s", name, exc
            )

    def _scan_count(self, pattern: str) -> int:
        """Count keys matching a pattern using SCAN (non-blocking)."""
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match=pattern, count=100)
            count += len(keys)
            if cursor == 0:
                break
        return count

    def _log_summary(self, summary: dict) -> None:
        """Log a human-readable reconstruction summary."""
        parts = []

        for name, count in summary["registries"].items():
            if count > 0:
                parts.append(f"{name}={count}")
            elif count == 0:
                parts.append(f"{name}=EMPTY")
            else:
                parts.append(f"{name}=ERROR")

        for name, count in summary["sessions"].items():
            if count >= 0:
                parts.append(f"{name}={count}")

        if summary["callbacks"] >= 0:
            parts.append(f"callbacks={summary['callbacks']}")

        registry_status = "warm" if not summary["missing_registries"] else "cold"

        log.info(
            "State reconstruction complete (%s): %s | total_keys=%d | re_requested=%s",
            registry_status,
            ", ".join(parts),
            summary["total_keys_found"],
            summary["re_requested"] or "none",
        )
