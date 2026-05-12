import base64
import threading

from pylon.core.tools import log, web
from tools import auth, rpc_tools

from ..utils.sio_utils import SioEvents

# Per-sid sessions: { sid: { "cancel": threading.Event } }
_sessions: dict = {}


class SIO:

    @web.sio(SioEvents.tts_start)
    def tts_start(self, sid: str, data: dict) -> None:
        project_id = data.get("project_id")
        model_name = data.get("model_name")
        text = data.get("text", "")
        voice = data.get("voice", "alloy")
        speed = float(data.get("speed") or 1.0)

        if not auth.is_sio_user_in_project(sid, project_id):
            log.warning("Sid %s is not in project %s", sid, project_id)
            self.context.sio.emit(SioEvents.tts_error, {"error": "Access denied"}, to=sid)
            return

        if not text:
            self.context.sio.emit(SioEvents.tts_error, {"error": "No text provided"}, to=sid)
            return

        # Cancel any running session for this sid
        _cancel_session(sid)

        cancel_event = threading.Event()
        _sessions[sid] = {"cancel": cancel_event}

        t = threading.Thread(
            target=_stream_tts,
            args=(self, sid, project_id, model_name, text, voice, speed, cancel_event),
            daemon=True,
        )
        t.start()

    @web.sio(SioEvents.tts_stop)
    def tts_stop(self, sid: str, data: dict) -> None:
        _cancel_session(sid)


def _cancel_session(sid: str) -> None:
    session = _sessions.pop(sid, None)
    if session:
        session["cancel"].set()


def _stream_tts(
    sio_handler,
    sid: str,
    project_id: int,
    model_name: str,
    text: str,
    voice: str,
    speed: float,
    cancel_event: threading.Event,
) -> None:
    import requests

    try:
        creds = _resolve_tts_credentials(project_id, model_name)
    except Exception as e:
        log.error(f"tts_start credential error for sid={sid}: {e}")
        sio_handler.context.sio.emit(SioEvents.tts_error, {"error": str(e)}, to=sid)
        _sessions.pop(sid, None)
        return

    api_base: str = creds["api_base"].rstrip("/")
    api_key: str = creds["api_key"]
    api_version: str = creds.get("api_version", "")

    if api_version:
        # Azure OpenAI
        url = (
            f"{api_base}/openai/deployments/{model_name}"
            f"/audio/speech?api-version={api_version}"
        )
        headers = {"api-key": api_key, "Content-Type": "application/json"}
    else:
        base = api_base if api_base else "https://api.openai.com/v1"
        url = f"{base}/audio/speech"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    payload = {
        "model": model_name,
        "input": text,
        "voice": voice,
        "speed": speed,
        "response_format": "pcm",
    }

    try:
        with requests.post(url, headers=headers, json=payload, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=4096):
                if cancel_event.is_set():
                    break
                if chunk:
                    sio_handler.context.sio.emit(
                        SioEvents.tts_audio_chunk,
                        {
                            "audio_base64": base64.b64encode(chunk).decode("ascii"),
                            "sample_rate": 24000,
                        },
                        to=sid,
                    )
    except Exception as e:
        if not cancel_event.is_set():
            log.error(f"TTS streaming error for sid={sid}: {e}")
            sio_handler.context.sio.emit(SioEvents.tts_error, {"error": str(e)}, to=sid)
        _sessions.pop(sid, None)
        return

    if not cancel_event.is_set():
        sio_handler.context.sio.emit(SioEvents.tts_done, {}, to=sid)

    _sessions.pop(sid, None)


def _resolve_tts_credentials(project_id: int, model_name: str) -> dict:
    """
    Fetch the TTS configuration row via RPC, expand its ai_credentials reference,
    and return a flat dict with api_base, api_key, api_version.
    """
    configs: list[dict] = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_filtered_project(
        project_id=project_id,
        include_shared=True,
        filter_fields={"section": "tts", "status_ok": True},
    )

    config_data = None
    for cfg in configs:
        if cfg.get("data", {}).get("name") == model_name:
            config_data = dict(cfg["data"])
            break

    if config_data is None:
        raise LookupError(
            f"No TTS configuration found for model '{model_name}' in project {project_id}"
        )

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
