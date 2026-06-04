from pylon.core.tools import log, web

from tools import VaultClient, serialize, this

from ..models.pd.chat import ApplicationChatRequest
from ..utils.pipeline_utils import validate_yaml_from_str
from ..utils.predict_utils import generate_predict_payload, PredictPayloadError


YAML_MAX_SIZE = 512 * 1024  # 512KB


class Method:

    @web.method()
    def do_pipeline_run(
        self,
        project_id: int,
        user_id: int,
        payload_in: dict,
        predict_wait=True,
        predict_timeout=float(60 * 60),
        return_chat_history: bool = False,
    ):
        raw_yaml = payload_in.get("yaml")
        if not raw_yaml:
            return {"error": "Field 'yaml' is required"}

        if len(raw_yaml) > YAML_MAX_SIZE:
            return {"error": f"YAML exceeds maximum size of {YAML_MAX_SIZE // 1024}KB"}

        try:
            parsed_yaml = validate_yaml_from_str(raw_yaml)
        except ValueError as e:
            return {"error": str(e)}

        if "entry_point" not in parsed_yaml:
            return {"error": "Pipeline YAML must define an 'entry_point' field"}

        llm_settings = payload_in.get("llm_settings")
        if not llm_settings or not llm_settings.get("model_name"):
            default_model = this.for_module('configurations').module.get_default_model(project_id)
            if not default_model or not default_model.get("model_name"):
                return {"error": "No llm_settings provided and no default model configured for project"}
            if llm_settings:
                default_model.update({k: v for k, v in llm_settings.items() if v is not None})
            llm_settings = default_model

        llm_settings.setdefault("temperature", 0.7)
        llm_settings.setdefault("max_tokens", 4096)

        variables = payload_in.get("variables") or []
        try:
            variables_parsed = [{"name": v["name"], "value": v.get("value", "")} for v in variables]
        except (KeyError, TypeError) as e:
            return {"error": f"Invalid variables format: each variable must have a 'name' field. {e}"}

        version_details = {
            "agent_type": "pipeline",
            "instructions": raw_yaml,
            "llm_settings": llm_settings,
            "tools": payload_in.get("tools", []),
            "variables": variables,
            "meta": {},
            "name": "adhoc_pipeline",
            "status": "published",
        }

        chat_request_data = {
            "project_id": project_id,
            "version_details": version_details,
            "user_input": payload_in.get("user_input", ""),
            "chat_history": payload_in.get("chat_history", []),
            "llm_settings": llm_settings,
            "tools": payload_in.get("tools", []),
            "variables": variables_parsed if variables_parsed else None,
        }

        try:
            parsed = ApplicationChatRequest.model_validate(chat_request_data)
        except Exception as e:
            return {"error": f"Validation error: {e}"}

        try:
            payload = generate_predict_payload(parsed, user_id=user_id, eligible_for_autoapproval=True, return_chat_history=return_chat_history)
        except PredictPayloadError as e:
            return {"error": str(e)}

        vc = VaultClient(project_id)
        payload = vc.unsecret(payload)

        user_context = {
            "user_id": user_id,
            "project_id": project_id,
        }

        task_id = self.task_node.start_task(
            "indexer_agent",
            args=[None, None],
            kwargs=payload,
            pool="agents",
            meta={
                "task_name": "indexer_agent",
                "project_id": project_id,
                "user_context": serialize(user_context),
            }
        )

        if not predict_wait:
            return {
                "message": "Task started",
                "task_id": task_id,
            }

        result = self.task_node.join_task(task_id, timeout=predict_timeout)
        if result is ...:
            self.task_node.stop_task(task_id)
            return {"error": "Timeout"}

        return result
