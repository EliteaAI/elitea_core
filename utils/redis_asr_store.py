"""Redis-backed ASR session store for horizontal scaling.

Externalizes ASR session state to Redis so that sessions survive pod
crashes and any replica can serve reconnecting clients.

Redis key layout:
  asr_session:{sid}   — hash: session config + VAD state
  asr_buffer:{sid}    — list: base64-encoded PCM audio chunks (whisper only)
"""

import base64
import time

from pylon.core.tools import log


DEFAULT_TTL = 300  # 5 minutes for abandoned sessions
MAX_BUFFER_CHUNKS = 200  # ~60s at 300ms/chunk


class RedisAsrSessionStore:
    """Manages ASR session state in Redis for horizontal scaling."""

    def __init__(self, redis_client, ttl: int = DEFAULT_TTL):
        self._client = redis_client
        self._ttl = ttl

    def _session_key(self, sid: str) -> str:
        return f"asr_session:{sid}"

    def _buffer_key(self, sid: str) -> str:
        return f"asr_buffer:{sid}"

    def create_session(self, sid: str, session_type: str, config: dict) -> None:
        """Create a new ASR session in Redis.

        Args:
            sid: Socket.IO session ID
            session_type: "whisper" or "realtime"
            config: Dict with project_id, project_llm_key, model_name, language
        """
        session_key = self._session_key(sid)
        mapping = {
            "type": session_type,
            "project_id": str(config.get("project_id", "")),
            "project_llm_key": str(config.get("project_llm_key", "")),
            "model_name": str(config.get("model_name", "")),
            "language": str(config.get("language", "en")),
            "speech_detected": "0",
            "silent_frames": "0",
            "call_in_flight": "0",
            "last_active": str(time.time()),
            "created_at": str(time.time()),
        }
        pipe = self._client.pipeline()
        pipe.hset(session_key, mapping=mapping)
        pipe.expire(session_key, self._ttl)
        pipe.execute()

    def get_session_config(self, sid: str) -> dict:
        """Retrieve session config from Redis. Returns empty dict if not found."""
        session_key = self._session_key(sid)
        data = self._client.hgetall(session_key)
        if not data:
            return {}
        return {
            k if isinstance(k, str) else k.decode(): v if isinstance(v, str) else v.decode()
            for k, v in data.items()
        }

    def session_exists(self, sid: str) -> bool:
        """Check if a session exists in Redis."""
        return self._client.exists(self._session_key(sid)) > 0

    def update_vad_state(self, sid: str, speech_detected: bool, silent_frames: int,
                         call_in_flight: bool) -> None:
        """Update VAD processing state for a whisper session."""
        session_key = self._session_key(sid)
        pipe = self._client.pipeline()
        pipe.hset(session_key, mapping={
            "speech_detected": "1" if speech_detected else "0",
            "silent_frames": str(silent_frames),
            "call_in_flight": "1" if call_in_flight else "0",
            "last_active": str(time.time()),
        })
        pipe.expire(session_key, self._ttl)
        pipe.execute()

    def refresh_activity(self, sid: str) -> None:
        """Touch the session to prevent TTL expiry during active use."""
        session_key = self._session_key(sid)
        pipe = self._client.pipeline()
        pipe.hset(session_key, "last_active", str(time.time()))
        pipe.expire(session_key, self._ttl)
        pipe.execute()
        buffer_key = self._buffer_key(sid)
        if self._client.exists(buffer_key):
            self._client.expire(buffer_key, self._ttl)

    def append_buffer_chunk(self, sid: str, pcm_bytes: bytes) -> None:
        """Append a PCM audio chunk to the session's buffer list.

        Chunks are base64-encoded since the Redis client uses decode_responses=True.
        List is trimmed to MAX_BUFFER_CHUNKS to bound memory usage (~60s of audio).
        """
        buffer_key = self._buffer_key(sid)
        encoded = base64.b64encode(pcm_bytes).decode("ascii")
        pipe = self._client.pipeline()
        pipe.rpush(buffer_key, encoded)
        pipe.ltrim(buffer_key, -MAX_BUFFER_CHUNKS, -1)
        pipe.expire(buffer_key, self._ttl)
        pipe.execute()

    def get_buffer(self, sid: str) -> bytes:
        """Retrieve and concatenate all buffered audio chunks.

        Returns the full PCM buffer as a single bytes object.
        """
        buffer_key = self._buffer_key(sid)
        chunks = self._client.lrange(buffer_key, 0, -1)
        if not chunks:
            return b""
        result = bytearray()
        for chunk in chunks:
            raw = chunk if isinstance(chunk, str) else chunk.decode()
            result.extend(base64.b64decode(raw))
        return bytes(result)

    def get_buffer_size(self, sid: str) -> int:
        """Get number of chunks currently in the buffer."""
        return self._client.llen(self._buffer_key(sid))

    def clear_buffer(self, sid: str) -> None:
        """Clear the audio buffer after a successful flush to indexer."""
        self._client.delete(self._buffer_key(sid))

    def remove_session(self, sid: str) -> bool:
        """Remove a session and its buffer from Redis. Returns True if session existed."""
        session_key = self._session_key(sid)
        buffer_key = self._buffer_key(sid)
        pipe = self._client.pipeline()
        pipe.delete(session_key)
        pipe.delete(buffer_key)
        results = pipe.execute()
        return results[0] > 0

    def recover_session(self, sid: str) -> dict:
        """Attempt to recover a session for a reconnecting client.

        Returns session config dict with 'buffer' key containing recovered PCM bytes,
        or empty dict if no session exists.
        """
        config = self.get_session_config(sid)
        if not config:
            return {}

        recovered = dict(config)
        if config.get("type") == "whisper":
            recovered["buffer"] = self.get_buffer(sid)
            recovered["speech_detected"] = config.get("speech_detected") == "1"
            recovered["silent_frames"] = int(config.get("silent_frames", "0"))
            recovered["call_in_flight"] = config.get("call_in_flight") == "1"
        return recovered

    def get_active_session_count(self) -> int:
        """Count active ASR sessions (uses SCAN to avoid blocking)."""
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match="asr_session:*", count=100)
            count += len(keys)
            if cursor == 0:
                break
        return count

    def evict_stale_sessions(self, timeout_seconds: int = 60) -> list:
        """Evict sessions that haven't been active within timeout_seconds.

        Returns list of evicted SIDs. Note: Redis TTL handles true abandonment;
        this is for proactive cleanup of sessions that stop sending audio.
        """
        evicted = []
        now = time.time()
        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor, match="asr_session:*", count=100)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                last_active = self._client.hget(key_str, "last_active")
                if last_active is None:
                    continue
                last_active_f = float(last_active if isinstance(last_active, str) else last_active.decode())
                if now - last_active_f > timeout_seconds:
                    sid = key_str.split(":", 1)[1]
                    self.remove_session(sid)
                    evicted.append(sid)
                    log.info("ASR: evicted stale Redis session %s (idle > %ds)", sid, timeout_seconds)
            if cursor == 0:
                break
        return evicted
