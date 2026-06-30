import json
import time

from pylon.core.tools import log


AUTOSAVE_INTERVAL_SECONDS = 300  # 5 minutes
AUTOSAVE_KEY_PREFIX = "canvas_autosave:"
AUTOSAVE_TTL = 86400  # 24 hours


class CanvasAutosave:
    """Manages periodic auto-save of canvas content with dirty tracking.

    Tracks which canvases have unsaved changes via a Redis hash per canvas.
    The periodic save (every 5 minutes) only persists canvases that have been
    modified since the last save, avoiding unnecessary DB writes.

    Redis state per canvas:
        canvas_autosave:{project_id}_{canvas_uuid} → hash {
            "dirty": "1" or "0",
            "last_saved_at": unix timestamp (float),
            "last_modified_at": unix timestamp (float),
            "version": integer version counter
        }
    """

    def __init__(self, redis_client):
        self._client = redis_client

    def _autosave_key(self, project_id, canvas_uuid):
        return f"{AUTOSAVE_KEY_PREFIX}{project_id}_{canvas_uuid}"

    def mark_dirty(self, project_id, canvas_uuid):
        """Mark a canvas as having unsaved changes.

        Called whenever canvas content is modified (e.g., from edit_canvas SIO event).
        """
        key = self._autosave_key(project_id, canvas_uuid)
        now = time.time()
        pipe = self._client.pipeline(transaction=False)
        pipe.hset(key, mapping={
            "dirty": "1",
            "last_modified_at": str(now),
        })
        pipe.hincrby(key, "version", 1)
        pipe.expire(key, AUTOSAVE_TTL)
        pipe.execute()

    def mark_saved(self, project_id, canvas_uuid):
        """Mark a canvas as saved (clears dirty flag, updates last_saved_at)."""
        key = self._autosave_key(project_id, canvas_uuid)
        now = time.time()
        pipe = self._client.pipeline(transaction=False)
        pipe.hset(key, mapping={
            "dirty": "0",
            "last_saved_at": str(now),
        })
        pipe.expire(key, AUTOSAVE_TTL)
        pipe.execute()

    def is_dirty(self, project_id, canvas_uuid):
        """Check if a canvas has unsaved changes."""
        key = self._autosave_key(project_id, canvas_uuid)
        dirty = self._client.hget(key, "dirty")
        return dirty == "1"

    def get_autosave_info(self, project_id, canvas_uuid):
        """Get auto-save metadata for a canvas.

        Returns:
            dict with keys: dirty (bool), last_saved_at (float or None),
                  last_modified_at (float or None), version (int)
        """
        key = self._autosave_key(project_id, canvas_uuid)
        data = self._client.hgetall(key)
        if not data:
            return {
                "dirty": False,
                "last_saved_at": None,
                "last_modified_at": None,
                "version": 0,
            }
        return {
            "dirty": data.get("dirty") == "1",
            "last_saved_at": float(data["last_saved_at"]) if data.get("last_saved_at") else None,
            "last_modified_at": float(data["last_modified_at"]) if data.get("last_modified_at") else None,
            "version": int(data.get("version", 0)),
        }

    def get_last_saved_at(self, project_id, canvas_uuid):
        """Get the last_saved_at timestamp for a canvas.

        Returns:
            float (unix timestamp) or None if never saved via autosave.
        """
        key = self._autosave_key(project_id, canvas_uuid)
        val = self._client.hget(key, "last_saved_at")
        return float(val) if val else None

    def get_dirty_canvases(self):
        """Get all canvas keys that are currently dirty.

        Scans Redis for autosave keys with dirty=1.

        Returns:
            list of dicts: [{"project_id": str, "canvas_uuid": str}, ...]
        """
        dirty_canvases = []
        cursor = 0
        pattern = f"{AUTOSAVE_KEY_PREFIX}*"
        while True:
            cursor, keys = self._client.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                dirty = self._client.hget(key, "dirty")
                if dirty == "1":
                    suffix = key.removeprefix(AUTOSAVE_KEY_PREFIX)
                    parts = suffix.split("_", 1)
                    if len(parts) == 2:
                        dirty_canvases.append({
                            "project_id": parts[0],
                            "canvas_uuid": parts[1],
                        })
            if cursor == 0:
                break
        return dirty_canvases

    def should_save(self, project_id, canvas_uuid):
        """Check if a canvas should be auto-saved now.

        Returns True if the canvas is dirty AND at least AUTOSAVE_INTERVAL_SECONDS
        have passed since the last save.
        """
        key = self._autosave_key(project_id, canvas_uuid)
        data = self._client.hmget(key, "dirty", "last_saved_at")
        dirty, last_saved_at = data[0], data[1]
        if dirty != "1":
            return False
        if last_saved_at is None:
            return True
        elapsed = time.time() - float(last_saved_at)
        return elapsed >= AUTOSAVE_INTERVAL_SECONDS

    def get_recovery_info(self, project_id, canvas_uuid):
        """Get recovery information for reconnection scenarios.

        Returns information the client needs to decide between server state
        and local state after a disconnect/reconnect.

        Returns:
            dict with keys:
                has_unsaved: bool - whether server has unsaved content in Redis
                server_version: int - server-side version counter
                last_saved_at: float or None - last DB persist timestamp
                last_modified_at: float or None - last edit timestamp
        """
        info = self.get_autosave_info(project_id, canvas_uuid)
        return {
            "has_unsaved": info["dirty"],
            "server_version": info["version"],
            "last_saved_at": info["last_saved_at"],
            "last_modified_at": info["last_modified_at"],
        }

    def delete_state(self, project_id, canvas_uuid):
        """Remove autosave tracking state for a canvas."""
        key = self._autosave_key(project_id, canvas_uuid)
        self._client.delete(key)
