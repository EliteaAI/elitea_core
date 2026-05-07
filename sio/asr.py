import json
import threading

from pylon.core.tools import log, web
from tools import auth, rpc_tools

from ..utils.sio_utils import SioEvents

# Per-sid sessions: { sid: { "ws": websocket, "lock": threading.Lock() } }
_sessions: dict = {}


class SIO:

    @web.sio(SioEvents.asr_start)
    def asr_start(self, sid: str, data: dict) -> None:
        project_id = data.get("project_id")
        model_name = data.get("model_name")
        model_project_id = data.get("model_project_id") or project_id
        language = data.get("language") or "en"

        if not auth.is_sio_user_in_project(sid, project_id):
            log.warning("Sid %s is not in project %s", sid, project_id)
            self.context.sio.emit(SioEvents.asr_error, {"error": "Access denied"}, to=sid)
            return

        try:
            creds = _resolve_asr_credentials(model_project_id, model_name)
            ws_url, headers = _build_ws_params(creds, model_name)

            import websocket as ws_lib  # websocket-client

            lock = threading.Lock()
            _sessions[sid] = {"ws": None, "lock": lock, "connected": False, "queue": [], "language": language}

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
        _close_session(sid)


# --- Module-level helpers (no self needed) ---

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
    if session:
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
    for cfg in configs:
        if cfg.get("data", {}).get("name") == model_name:
            config_data = dict(cfg["data"])
            break

    if config_data is None:
        raise LookupError(
            f"No ASR configuration found for model '{model_name}' in project {project_id}"
        )

    # Expand the ai_credentials reference (resolves elitea_title reference + unsecrets)
    ai_creds_ref = config_data.get("ai_credentials")
    if ai_creds_ref:
        expanded = rpc_tools.RpcMixin().rpc.timeout(5).configurations_expand(
            project_id=project_id,
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
