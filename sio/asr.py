import struct
import threading
import time

from pylon.core.tools import log, web
from tools import auth

from ..utils.sio_utils import SioEvents
from ..utils.redis_asr_store import RedisAsrSessionStore

# Per-sid local sessions (VAD processing state — hot path, needs sub-ms access)
# Redis stores the persistent config; local dict holds threading primitives + buffer
_sessions: dict = {}

# Session limits
_MAX_SESSIONS = 200
_SESSION_TIMEOUT_S = 60

# --- Whisper VAD constants ---
_VAD_SPEECH_THRESHOLD = 500
_VAD_SILENCE_FRAMES = 2
_WHISPER_MIN_BYTES = 4800
_WHISPER_MAX_BUFFER_SECS = 30

# Single global voice events channel (pylon_main → indexer)
_VOICE_EVENTS_CHANNEL = "voice_events"

# Module-level reference to the Redis store (set during plugin init)
_redis_store: RedisAsrSessionStore = None


def init_redis_store(redis_client, ttl: int = 300) -> RedisAsrSessionStore:
    """Initialize the module-level Redis ASR store. Called from module.py during init."""
    global _redis_store
    _redis_store = RedisAsrSessionStore(redis_client, ttl=ttl)
    return _redis_store


def get_redis_store() -> RedisAsrSessionStore:
    """Get the module-level Redis ASR store instance."""
    return _redis_store


def _is_whisper_model(model_name: str) -> bool:
    """True for batch HTTP transcription models (whisper-1, gpt-4o-transcribe, etc.)."""
    lower = model_name.lower() if model_name else ""
    return bool(lower and ("whisper" in lower or "transcribe" in lower))


def _frame_is_speech(pcm_bytes: bytes) -> bool:
    """True if the peak amplitude in this PCM16 frame exceeds the speech threshold."""
    n = len(pcm_bytes) // 2
    if n == 0:
        return False
    samples = struct.unpack_from(f"<{n}h", pcm_bytes)
    return max(abs(s) for s in samples) > _VAD_SPEECH_THRESHOLD


# ---------------------------------------------------------------------------
# Session limit helpers
# ---------------------------------------------------------------------------

def _evict_stale_sessions() -> None:
    """Evict sessions idle for longer than _SESSION_TIMEOUT_S."""
    now = time.monotonic()
    stale = [
        sid for sid, s in list(_sessions.items())
        if now - s.get("last_active", now) > _SESSION_TIMEOUT_S
    ]
    for sid in stale:
        log.info("ASR: evicting stale session %s (idle > %ds)", sid, _SESSION_TIMEOUT_S)
        session = _sessions.pop(sid, None)
        if session is None:
            continue
        if session.get("type") == "whisper":
            _cancel_flush_timer(session)
        else:
            event_node = session.get("event_node")
            if event_node is not None:
                event_node.emit(_VOICE_EVENTS_CHANNEL, {"type": "asr_stop", "sid": sid})
        if _redis_store:
            _redis_store.remove_session(sid)


class SIO:

    @web.sio(SioEvents.asr_start)
    def asr_start(self, sid: str, data: dict) -> None:
        project_id = data.get("project_id")
        model_name = data.get("model_name")
        language = data.get("language") or "en"
        if not auth.is_sio_user_in_project(sid, project_id):
            log.warning("Sid %s is not in project %s", sid, project_id)
            self.context.sio.emit(SioEvents.asr_error, {"error": "Access denied"}, to=sid)
            return

        _evict_stale_sessions()

        if len(_sessions) >= _MAX_SESSIONS:
            log.warning("ASR: session limit reached (%d), rejecting sid %s", _MAX_SESSIONS, sid)
            self.context.sio.emit(
                SioEvents.asr_error,
                {"error": "Voice capacity reached, try again later"},
                to=sid,
            )
            return

        try:
            resolved = self.context.rpc_manager.timeout(10).litellm_resolve_model(
                project_id=project_id, model_name=model_name, section="asr"
            )
            config_project_id = resolved["config_project_id"]
            project_llm_key = resolved["project_llm_key"]
        except Exception:
            log.exception("ASR: failed to resolve model config for project=%s model=%s", project_id, model_name)
            self.context.sio.emit(SioEvents.asr_error, {"error": "Failed to resolve model configuration"}, to=sid)
            return

        session_config = {
            "project_id": config_project_id,
            "project_llm_key": project_llm_key,
            "model_name": model_name,
            "language": language,
        }

        if _is_whisper_model(model_name):
            session_type = "whisper"
            _sessions[sid] = {
                "type": "whisper",
                "last_active": time.monotonic(),
                "event_node": self.event_node,
                "lock": threading.Lock(),
                "buffer": bytearray(),
                "speech_detected": False,
                "silent_frames": 0,
                "flush_timer": None,
                "project_id": config_project_id,
                "project_llm_key": project_llm_key,
                "model_name": model_name,
                "language": language,
                "task_node": self.task_node,
                "call_in_flight": False,
                "pending_buffer": bytearray(),
            }
        else:
            session_type = "realtime"
            _sessions[sid] = {
                "type": "realtime",
                "last_active": time.monotonic(),
                "event_node": self.event_node,
            }

            self.task_node.start_task(
                "indexer_asr_realtime",
                kwargs={
                    "sid": sid,
                    "project_id": config_project_id,
                    "project_llm_key": project_llm_key,
                    "model_name": model_name,
                    "language": language,
                },
                pool="indexer",
                meta={
                    "task_name": "indexer_asr_realtime",
                    "project_id": project_id,
                    "model_name": model_name,
                },
            )

        if _redis_store:
            _redis_store.create_session(sid, session_type, session_config)

    @web.sio(SioEvents.asr_audio_chunk)
    def asr_audio_chunk(self, sid: str, data) -> None:
        session = _sessions.get(sid)
        if not session:
            if _redis_store and _redis_store.session_exists(sid):
                log.info("ASR: session %s found in Redis but not local — attempting recovery", sid)
                _try_recover_session(self, sid)
                session = _sessions.get(sid)
                if not session:
                    return
            else:
                return

        session["last_active"] = time.monotonic()

        if isinstance(data, (bytes, bytearray)):
            pcm_bytes = bytes(data)
        else:
            pcm_bytes = data.get("audio", b"") if isinstance(data, dict) else b""

        if not pcm_bytes:
            return

        if session.get("type") == "whisper":
            _handle_whisper_audio(sid, session, pcm_bytes)
            return

        self.event_node.emit(
            _VOICE_EVENTS_CHANNEL,
            {"type": "asr_audio_input", "sid": sid, "audio": pcm_bytes},
        )

    @web.sio(SioEvents.asr_stop)
    def asr_stop(self, sid: str, data: dict) -> None:
        session = _sessions.get(sid)
        if session and session.get("type") == "whisper":
            _flush_whisper_buffer(sid, session)

        _close_session(self, sid)


# ---------------------------------------------------------------------------
# Session recovery
# ---------------------------------------------------------------------------

def _try_recover_session(sio_handler, sid: str) -> None:
    """Attempt to recover a session from Redis when a client reconnects to a different pod."""
    if not _redis_store:
        return

    recovered = _redis_store.recover_session(sid)
    if not recovered:
        return

    session_type = recovered.get("type")
    log.info("ASR: recovering %s session for sid %s from Redis", session_type, sid)

    if session_type == "whisper":
        recovered_buffer = recovered.get("buffer", b"")
        _sessions[sid] = {
            "type": "whisper",
            "last_active": time.monotonic(),
            "event_node": sio_handler.event_node,
            "lock": threading.Lock(),
            "buffer": bytearray(recovered_buffer),
            "speech_detected": recovered.get("speech_detected", False),
            "silent_frames": recovered.get("silent_frames", 0),
            "flush_timer": None,
            "project_id": recovered.get("project_id", ""),
            "project_llm_key": recovered.get("project_llm_key", ""),
            "model_name": recovered.get("model_name", ""),
            "language": recovered.get("language", "en"),
            "task_node": sio_handler.task_node,
            "call_in_flight": recovered.get("call_in_flight", False),
            "pending_buffer": bytearray(),
        }
    elif session_type == "realtime":
        _sessions[sid] = {
            "type": "realtime",
            "last_active": time.monotonic(),
            "event_node": sio_handler.event_node,
        }


# ---------------------------------------------------------------------------
# Whisper VAD helpers
# ---------------------------------------------------------------------------

def _handle_whisper_audio(sid: str, session: dict, pcm_bytes: bytes) -> None:
    if not pcm_bytes:
        return

    is_speech = _frame_is_speech(pcm_bytes)

    with session["lock"]:
        if is_speech:
            if not session["speech_detected"]:
                session["event_node"].emit("voice_asr_speech_started", {"sid": sid})
            session["buffer"].extend(pcm_bytes)
            session["speech_detected"] = True
            session["silent_frames"] = 0
            _reset_flush_timer(sid, session)
        elif session["speech_detected"]:
            session["buffer"].extend(pcm_bytes)
            session["silent_frames"] += 1
            if session["silent_frames"] >= _VAD_SILENCE_FRAMES:
                _cancel_flush_timer(session)
                _do_flush(sid, session)


def _reset_flush_timer(sid: str, session: dict) -> None:
    """Restart the hard-limit timer (called while holding session lock)."""
    _cancel_flush_timer(session)
    timer = threading.Timer(
        _WHISPER_MAX_BUFFER_SECS,
        _flush_whisper_buffer,
        args=(sid, session),
    )
    timer.daemon = True
    session["flush_timer"] = timer
    timer.start()


def _cancel_flush_timer(session: dict) -> None:
    timer = session.get("flush_timer")
    if timer is not None:
        timer.cancel()
        session["flush_timer"] = None


def _flush_whisper_buffer(sid: str, session: dict) -> None:
    """Entry point called from the timer (no lock held)."""
    with session["lock"]:
        _cancel_flush_timer(session)
        _do_flush(sid, session)


def _do_flush(sid: str, session: dict) -> None:
    """Flush current buffer (lock must be held)."""
    pcm_data = bytes(session["buffer"])
    session["buffer"] = bytearray()
    session["speech_detected"] = False
    session["silent_frames"] = 0

    if len(pcm_data) < _WHISPER_MIN_BYTES:
        session["event_node"].emit("voice_asr_transcript_done", {"sid": sid, "transcript": ""})
        return

    session["event_node"].emit("voice_asr_vad_flush", {"sid": sid})

    if session["call_in_flight"]:
        session["pending_buffer"].extend(pcm_data)
        return

    session["call_in_flight"] = True
    _dispatch_whisper_call(sid, session, pcm_data)

    if _redis_store:
        _redis_store.update_vad_state(
            sid,
            speech_detected=False,
            silent_frames=0,
            call_in_flight=True,
        )
        _redis_store.clear_buffer(sid)


def _dispatch_whisper_call(sid: str, session: dict, pcm_data: bytes) -> None:
    """Submit a Whisper indexer task (call_in_flight must already be True)."""
    session["task_node"].start_task(
        "indexer_asr_whisper",
        kwargs={
            "sid": sid,
            "project_id": session["project_id"],
            "project_llm_key": session["project_llm_key"],
            "model_name": session["model_name"],
            "language": session["language"],
            "audio_bytes": pcm_data,
        },
        pool="indexer",
        meta={
            "task_name": "indexer_asr_whisper",
            "project_id": session["project_id"],
            "model_name": session["model_name"],
        },
    )


def on_whisper_call_done(sid: str) -> None:
    """Called from module.py after voice_asr_transcript_done is forwarded to the SIO client."""
    session = _sessions.get(sid)
    if not session or session.get("type") != "whisper":
        return

    with session["lock"]:
        pending = bytes(session["pending_buffer"])
        session["pending_buffer"] = bytearray()
        if pending:
            _dispatch_whisper_call(sid, session, pending)
        else:
            session["call_in_flight"] = False
            if _redis_store:
                _redis_store.update_vad_state(
                    sid,
                    speech_detected=session["speech_detected"],
                    silent_frames=session["silent_frames"],
                    call_in_flight=False,
                )


def _close_session(sio_handler, sid: str) -> None:
    session = _sessions.pop(sid, None)
    if not session:
        if _redis_store:
            _redis_store.remove_session(sid)
        return
    if session.get("type") == "whisper":
        _cancel_flush_timer(session)
    else:
        sio_handler.event_node.emit(_VOICE_EVENTS_CHANNEL, {"type": "asr_stop", "sid": sid})

    if _redis_store:
        _redis_store.remove_session(sid)
