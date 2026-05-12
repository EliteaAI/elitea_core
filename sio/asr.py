import io
import json
import struct
import threading
import wave

from pylon.core.tools import log, web
from tools import auth, rpc_tools

from ..utils.sio_utils import SioEvents

# Per-sid sessions: { sid: { ... } }
_sessions: dict = {}

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


def _is_whisper_model(model_name: str) -> bool:
    return bool(model_name and "whisper" in model_name.lower())


def _frame_is_speech(pcm_bytes: bytes) -> bool:
    """True if the peak amplitude in this PCM16 frame exceeds the speech threshold."""
    n = len(pcm_bytes) // 2
    if n == 0:
        return False
    samples = struct.unpack_from(f"<{n}h", pcm_bytes)
    return max(abs(s) for s in samples) > _VAD_SPEECH_THRESHOLD


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

        try:
            creds = _resolve_asr_credentials(project_id, model_name)

            if _is_whisper_model(model_name):
                _sessions[sid] = {
                    "type": "whisper",
                    "lock": threading.Lock(),
                    "buffer": bytearray(),
                    "speech_detected": False,
                    "silent_frames": 0,
                    "flush_timer": None,
                    "creds": creds,
                    "model_name": model_name,
                    "language": language,
                    "sio_handler": self,
                }
            else:
                ws_url, headers = _build_ws_params(creds, model_name)

                import websocket as ws_lib  # websocket-client

                lock = threading.Lock()
                _sessions[sid] = {
                    "type": "realtime",
                    "ws": None,
                    "lock": lock,
                    "connected": False,
                    "queue": [],
                    "language": language,
                }

                ws = ws_lib.WebSocketApp(
                    ws_url,
                    header=headers,
                    on_open=lambda ws: _on_ws_open(ws, model_name, sid),
                    on_message=lambda ws, msg: _on_realtime_message(self, sid, msg),
                    on_error=lambda ws, err: _on_realtime_error(self, sid, err),
                    on_close=lambda ws, code, reason: _close_session(sid),
                )
                _sessions[sid]["ws"] = ws
                t = threading.Thread(target=ws.run_forever, daemon=True)
                t.start()

        except Exception as e:
            log.error(f"asr_start error for sid={sid}: {e}")
            self.context.sio.emit(SioEvents.asr_error, {"error": str(e)}, to=sid)

    @web.sio(SioEvents.asr_audio_chunk)
    def asr_audio_chunk(self, sid: str, data: dict) -> None:
        session = _sessions.get(sid)
        if not session:
            return
        audio = data.get("audio_base64", "")

        if session.get("type") == "whisper":
            _handle_whisper_audio(sid, session, audio)
            return

        with session["lock"]:
            if not session["connected"]:
                session["queue"].append(audio)
                return
            try:
                session["ws"].send(json.dumps({
                    "type": "input_audio_buffer.append",
                    "audio": audio,
                }))
            except Exception as e:
                log.warning(f"asr_audio_chunk send error for sid={sid}: {e}")

    @web.sio(SioEvents.asr_stop)
    def asr_stop(self, sid: str, data: dict) -> None:
        session = _sessions.get(sid)
        if session and session.get("type") == "whisper":
            _flush_whisper_buffer(sid, session)
        _close_session(sid)


# --- Whisper VAD helpers ---

def _handle_whisper_audio(sid: str, session: dict, audio_base64: str) -> None:
    import base64
    try:
        pcm_bytes = base64.b64decode(audio_base64)
    except Exception:
        return

    is_speech = _frame_is_speech(pcm_bytes)

    with session["lock"]:
        if is_speech:
            session["buffer"].extend(pcm_bytes)
            session["speech_detected"] = True
            session["silent_frames"] = 0
            # (Re)start the hard-limit safety timer
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
    """Flush current buffer to Whisper API (must be called while holding session lock)."""
    pcm_data = bytes(session["buffer"])
    session["buffer"] = bytearray()
    session["speech_detected"] = False
    session["silent_frames"] = 0

    if len(pcm_data) < _WHISPER_MIN_BYTES:
        return

    sio_handler = session["sio_handler"]
    creds = session["creds"]
    model_name = session["model_name"]
    language = session["language"]

    # Run the blocking HTTP call in a worker thread so we don't hold the lock
    t = threading.Thread(
        target=_transcribe_and_emit,
        args=(sio_handler, sid, creds, model_name, language, pcm_data),
        daemon=True,
    )
    t.start()


def _transcribe_and_emit(sio_handler, sid: str, creds: dict, model_name: str, language: str, pcm_data: bytes) -> None:
    import requests as _requests

    try:
        text = _call_whisper(creds, model_name, language, pcm_data)
        if text:
            sio_handler.context.sio.emit(
                SioEvents.asr_transcript_done, {"transcript": text}, to=sid
            )
    except _requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            # Rate-limited — drop this chunk silently so recording stays alive
            log.warning(f"Whisper rate-limited (429) for sid={sid}, dropping chunk")
        else:
            log.error(f"Whisper transcription error for sid={sid}: {e}")
            sio_handler.context.sio.emit(
                SioEvents.asr_error, {"error": str(e)}, to=sid
            )
    except Exception as e:
        log.error(f"Whisper transcription error for sid={sid}: {e}")
        sio_handler.context.sio.emit(
            SioEvents.asr_error, {"error": str(e)}, to=sid
        )


def _pcm16_to_wav(pcm_data: bytes, sample_rate: int = 24000) -> io.BytesIO:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)  # 16-bit PCM
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_data)
    buf.seek(0)
    return buf


def _call_whisper(creds: dict, model_name: str, language: str, pcm_data: bytes) -> str:
    import requests

    api_base: str = creds["api_base"].rstrip("/")
    api_key: str = creds["api_key"]
    api_version: str = creds.get("api_version", "")

    wav_buf = _pcm16_to_wav(pcm_data)

    if api_version:
        # Azure OpenAI
        url = (
            f"{api_base}/openai/deployments/{model_name}"
            f"/audio/transcriptions?api-version={api_version}"
        )
        headers = {"api-key": api_key}
    else:
        base = api_base if api_base else "https://api.openai.com/v1"
        url = f"{base}/audio/transcriptions"
        headers = {"Authorization": f"Bearer {api_key}"}

    response = requests.post(
        url,
        headers=headers,
        files={"file": ("audio.wav", wav_buf, "audio/wav")},
        data={"model": model_name, "language": language},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("text", "")


# --- Realtime WebSocket helpers ---

def _on_ws_open(ws, model_name: str, sid: str) -> None:
    session = _sessions.get(sid)
    language = (session or {}).get("language", "en")
    ws.send(json.dumps({
        "type": "transcription_session.update",
        "session": {
            "input_audio_format": "pcm16",
            "input_audio_transcription": {
                "model": model_name or "gpt-4o-transcribe",
                "language": language,
            },
            "turn_detection": {
                "type": "server_vad",
                "silence_duration_ms": 300,
                "threshold": 0.7,
            },
        },
    }))
    if not session:
        return
    with session["lock"]:
        session["connected"] = True
        for audio in session["queue"]:
            try:
                ws.send(json.dumps({"type": "input_audio_buffer.append", "audio": audio}))
            except Exception:
                pass
        session["queue"] = []


def _on_realtime_message(sio_handler, sid: str, raw: str) -> None:
    try:
        msg = json.loads(raw)
    except Exception:
        return
    event_type = msg.get("type", "")

    if event_type == "error":
        log.error(f"ASR realtime error for sid={sid}: {msg.get('error')}")
        return

    if event_type in ("conversation.item.input_audio_transcription.delta", "response.audio_transcript.delta"):
        delta = msg.get("delta", "")
        if delta:
            sio_handler.context.sio.emit(SioEvents.asr_transcript_delta, {"delta": delta}, to=sid)

    elif event_type in ("conversation.item.input_audio_transcription.completed", "response.audio_transcript.done"):
        transcript = msg.get("transcript", "")
        sio_handler.context.sio.emit(SioEvents.asr_transcript_done, {"transcript": transcript}, to=sid)


def _on_realtime_error(sio_handler, sid: str, error) -> None:
    log.error(f"Realtime WS error for sid={sid}: {error}")
    sio_handler.context.sio.emit(SioEvents.asr_error, {"error": str(error)}, to=sid)
    _close_session(sid)


def _close_session(sid: str) -> None:
    session = _sessions.pop(sid, None)
    if not session:
        return
    if session.get("type") == "whisper":
        _cancel_flush_timer(session)
    else:
        try:
            session["ws"].close()
        except Exception:
            pass


def _resolve_asr_credentials(project_id: int, model_name: str) -> dict:
    """
    Fetch the ASR configuration row via RPC, expand its ai_credentials reference,
    and return a flat dict with api_base, api_key, api_version.
    """
    configs: list[dict] = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_filtered_project(
        project_id=project_id,
        include_shared=True,
        filter_fields={"section": "asr", "status_ok": True},
    )

    config_data = None
    config_project_id = project_id
    for cfg in configs:
        if cfg.get("data", {}).get("name") == model_name:
            config_data = dict(cfg["data"])
            config_project_id = cfg.get("project_id", project_id)
            break

    if config_data is None:
        raise LookupError(
            f"No ASR configuration found for model '{model_name}' in project {project_id}"
        )

    # Expand the ai_credentials reference using the config's owning project so that
    # credentials belonging to a shared public model are found in that project directly,
    # matching the pattern used by image generation and LLM shared model credential lookup.
    ai_creds_ref = config_data.get("ai_credentials")
    if ai_creds_ref:
        expanded = rpc_tools.RpcMixin().rpc.timeout(5).configurations_expand(
            project_id=config_project_id,
            settings=ai_creds_ref,
            user_id=None,
            unsecret=True,
        )
        ai_creds = expanded
    else:
        ai_creds = config_data

    return {
        "api_base": ai_creds.get("api_base") or ai_creds.get("url") or "",
        "api_key": ai_creds.get("api_key") or ai_creds.get("key") or "",
        "api_version": ai_creds.get("api_version") or "",
    }


def _build_ws_params(creds: dict, model_name: str) -> tuple[str, list[str]]:
    api_base: str = creds["api_base"].rstrip("/")
    api_key: str = creds["api_key"]
    api_version: str = creds.get("api_version", "")

    if api_version:
        # Azure AI Foundry endpoint
        deployment = model_name or "gpt-4o-transcribe"
        ws_url = f"{api_base}/openai/realtime?deployment={deployment}&api-version={api_version}&intent=transcription"
        ws_url = ws_url.replace("https://", "wss://").replace("http://", "ws://")
        headers = [f"api-key: {api_key}", "OpenAI-Beta: realtime=v1"]
    else:
        # OpenAI endpoint
        ws_url = (
            f"wss://api.openai.com/v1/realtime"
            f"?intent=transcription&model={model_name or 'gpt-4o-transcribe'}"
        )
        headers = [f"Authorization: Bearer {api_key}", "OpenAI-Beta: realtime=v1"]

    return ws_url, headers
