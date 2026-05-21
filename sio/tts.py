from pylon.core.tools import log, web
from tools import auth

from ..utils.sio_utils import SioEvents

# Single global voice events channel
_VOICE_EVENTS_CHANNEL = "voice_events"


class SIO:

    @web.sio(SioEvents.tts_start)
    def tts_start(self, sid: str, data: dict) -> None:
        project_id = data.get("project_id")
        model_name = data.get("model_name")
        text = data.get("text", "")
        voice = data.get("voice", "alloy")
        speed = float(data.get("speed") or 1.0)
        voice_instructions = data.get("voice_instructions", "")

        if not auth.is_sio_user_in_project(sid, project_id):
            log.warning("Sid %s is not in project %s", sid, project_id)
            self.context.sio.emit(SioEvents.tts_error, {"error": "Access denied"}, to=sid)
            return

        if not text:
            self.context.sio.emit(SioEvents.tts_error, {"error": "No text provided"}, to=sid)
            return

        # Cancel any running TTS session for this sid before starting a new one
        self.event_node.emit(_VOICE_EVENTS_CHANNEL, {"type": "tts_cancel", "sid": sid})

        try:
            resolved = self.context.rpc_manager.timeout(10).litellm_resolve_model(
                project_id=project_id, model_name=model_name, section="tts"
            )
            config_project_id = resolved["config_project_id"]
            project_llm_key = resolved["project_llm_key"]
        except Exception:
            log.exception("TTS: failed to resolve model config for project=%s model=%s", project_id, model_name)
            self.context.sio.emit(SioEvents.tts_error, {"error": "Failed to resolve model configuration"}, to=sid)
            return

        self.task_node.start_task(
            "indexer_tts",
            kwargs={
                "sid": sid,
                "project_id": config_project_id,
                "project_llm_key": project_llm_key,
                "model_name": model_name,
                "text": text,
                "voice": voice,
                "speed": speed,
                "voice_instructions": voice_instructions,
            },
            pool="indexer",
            meta={},
        )

    @web.sio(SioEvents.tts_stop)
    def tts_stop(self, sid: str, data: dict) -> None:
        self.event_node.emit(_VOICE_EVENTS_CHANNEL, {"type": "tts_cancel", "sid": sid})
