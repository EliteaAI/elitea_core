import struct
import threading
import time

from pylon.core.tools import log, web
from tools import auth, VaultClient

from ..utils.sio_utils import SioEvents

# Per-sid sessions: { sid: { "type": ..., "last_active": float, ... } }
_sessions: dict = {}

# Session limits
_MAX_SESSIONS = 200
_SESSION_TIMEOUT_S = 60

# --- Whisper VAD constants ---
# Peak amplitude (0–32767) below which a frame is considered silence
_VAD_SPEECH_THRESHOLD = 500
# Consecutive silent frames before flushing the speech buffer
# With 300 ms Whisper chunks: 2 frames = 600 ms silence needed → halves max API call rate
_VAD_SILENCE_FRAMES = 2
# Absolute minimum PCM bytes to bother sending (~0.1 s at 24 kHz, 16-bit)
_WHISPER_MIN_BYTES = 4800
# Hard-limit flush timer (seconds) — guards against very long pauses
_WHISPER_MAX_BUFFER_SECS = 30

# Single global voice events channel (pylon_main → indexer)
_VOICE_EVENTS_CHANNEL = "voice_events"


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
    """Evict sessions idle for longer than _SESSION_TIMEOUT_S. Called lazily at asr_start."""
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

        project_secrets = VaultClient(project_id).get_secrets()
        project_llm_key = project_secrets.get("project_llm_key", "")

        if _is_whisper_model(model_name):
            # Whisper: VAD buffering stays in pylon_main; dispatch an indexer task per flush
            _sessions[sid] = {
                "type": "whisper",
                "last_active": time.monotonic(),
                "event_node": self.event_node,
                "lock": threading.Lock(),
                "buffer": bytearray(),
                "speech_detected": False,
                "silent_frames": 0,
                "flush_timer": None,
                "project_id": project_id,
                "project_llm_key": project_llm_key,
                "model_name": model_name,
                "language": language,
                "task_node": self.task_node,
            }
        else:
            # Realtime: dispatch long-lived indexer task, forward audio via event_node
            _sessions[sid] = {
                "type": "realtime",
                "last_active": time.monotonic(),
                "event_node": self.event_node,
            }

            self.task_node.start_task(
                "indexer_asr_realtime",
                kwargs={
                    "sid": sid,
                    "project_id": project_id,
                    "project_llm_key": project_llm_key,
                    "model_name": model_name,
                    "language": language,
                },
                pool="indexer",
                meta={},
            )

    @web.sio(SioEvents.asr_audio_chunk)
    def asr_audio_chunk(self, sid: str, data) -> None:
        session = _sessions.get(sid)
        if not session:
            return

        session["last_active"] = time.monotonic()

        # Accept raw bytes (binary Socket.IO frame) or dict with "audio" key
        if isinstance(data, (bytes, bytearray)):
            pcm_bytes = bytes(data)
        else:
            pcm_bytes = data.get("audio", b"") if isinstance(data, dict) else b""

        if not pcm_bytes:
            return

        if session.get("type") == "whisper":
            _handle_whisper_audio(sid, session, pcm_bytes)
            return

        # Realtime: forward raw PCM bytes to the indexer task via event_node
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
# Whisper VAD helpers
# ---------------------------------------------------------------------------

def _handle_whisper_audio(sid: str, session: dict, pcm_bytes: bytes) -> None:
    if not pcm_bytes:
        return

    is_speech = _frame_is_speech(pcm_bytes)

    with session["lock"]:
        if is_speech:
            session["buffer"].extend(pcm_bytes)
            session["speech_detected"] = True
            session["silent_frames"] = 0
            _reset_flush_timer(sid, session)
        elif session["speech_detected"]:
            # Include the trailing silent frame for natural audio context
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
    """Flush current buffer as an indexer_asr_whisper task (lock must be held)."""
    pcm_data = bytes(session["buffer"])
    session["buffer"] = bytearray()
    session["speech_detected"] = False
    session["silent_frames"] = 0

    if len(pcm_data) < _WHISPER_MIN_BYTES:
        return

    project_id = session["project_id"]
    project_llm_key = session["project_llm_key"]
    model_name = session["model_name"]
    language = session["language"]
    task_node = session["task_node"]

    task_node.start_task(
        "indexer_asr_whisper",
        kwargs={
            "sid": sid,
            "project_id": project_id,
            "project_llm_key": project_llm_key,
            "model_name": model_name,
            "language": language,
            "audio_bytes": pcm_data,
        },
        pool="indexer",
        meta={},
    )


def _close_session(sio_handler, sid: str) -> None:
    session = _sessions.pop(sid, None)
    if not session:
        return
    if session.get("type") == "whisper":
        _cancel_flush_timer(session)
    else:
        # Signal the indexer task to stop
        sio_handler.event_node.emit(_VOICE_EVENTS_CHANNEL, {"type": "asr_stop", "sid": sid})


