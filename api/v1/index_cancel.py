import json
import time

from flask import request

from tools import api_tools, auth, config as c, db, log
from ...models.indexer import EmbeddingStore
from ...utils.application_tools import get_toolkit_index_meta, load_and_validate_toolkit_for_index, get_session_for_schema
from ...utils.predict_utils import get_toolkit_config
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.task.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, toolkit_id: int, index_name: str, task_id: str, **kwargs):
        toolkit_config = get_toolkit_config(project_id, auth.current_user()['id'], toolkit_id)
        toolkit_name_id, connection_string, validation_error = load_and_validate_toolkit_for_index(toolkit_config)
        if validation_error:
            return validation_error
        #
        with get_session_for_schema(connection_string, toolkit_name_id) as session:
            meta = get_toolkit_index_meta(session, index_name)
            if meta and meta.cmetadata.get("task_id") == task_id:
                try:
                    if task_id:
                        log.debug(f"Stopping indexer's task {task_id}")
                        self.module.task_node.stop_task(task_id)
                    else:
                        log.debug(f"Indexer's task_id was not provided")
                    meta.cmetadata["state"] = "cancelled"
                    meta.cmetadata["task_id"] = None
                    meta.cmetadata["updated_on"] = time.time()
                    history_raw = meta.cmetadata.pop("history", "[]")
                    try:
                        history = json.loads(history_raw) if history_raw.strip() else []
                        # replace the last history item with updated metadata
                        if history and isinstance(history, list):
                            history[-1] = meta.cmetadata
                        else:
                            history = [meta.cmetadata]
                    except (json.JSONDecodeError, TypeError):
                        log.warning(
                            f"Failed to load index history: {history_raw}. Create new with only current item.")
                        history = [meta.cmetadata]
                    #
                    meta.cmetadata["history"] = json.dumps(history)
                    session.commit()
                    #
                    session.query(EmbeddingStore).filter(
                        EmbeddingStore.cmetadata["collection"].astext == index_name,
                        EmbeddingStore.cmetadata['type'].astext != "index_meta",
                    ).delete(synchronize_session=False)
                    session.commit()
                except Exception as e:
                    return {
                        "ok": False,
                        "error": str(e)
                    }, 400
        return None, 204


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>/<string:index_name>/<string:task_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
