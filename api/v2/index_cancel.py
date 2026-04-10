import json
import time

from tools import api_tools, auth, config as c, log
from ...models.indexer import EmbeddingStore
from ...utils.application_tools import (
    get_toolkit_index_meta,
    load_and_validate_toolkit_for_index,
    get_session_for_schema,
)
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
        # Convert JS 'null' string to Python None
        if task_id == 'null':
            task_id = None

        toolkit_config = get_toolkit_config(project_id, auth.current_user()['id'], toolkit_id)
        toolkit_name_id, connection_string, validation_error = load_and_validate_toolkit_for_index(toolkit_config)
        if validation_error:
            return validation_error
        #
        log.debug(f"Attempting to cancel index {index_name} in toolkit {toolkit_id} (task {task_id})")
        with get_session_for_schema(connection_string, toolkit_name_id) as session:
            meta = get_toolkit_index_meta(session, index_name)
            log.debug(f"Expected task_id to cancel: {task_id}")
            log.debug(f"Actual task_id to cancel: {meta.cmetadata.get('task_id') if meta else 'No meta to get task_id'}")
            if meta and meta.cmetadata.get("task_id") == task_id:
                # Try to stop the task (best-effort)
                if task_id and self.module.task_node is not None:
                    try:
                        log.debug(f"Attempting to stop indexer's task {task_id}")
                        self.module.task_node.stop_task(task_id)
                    except Exception as e:
                        log.warning(f"Failed to stop task {task_id}: {e}. Proceeding with cleanup.")
                log.debug(f"Attempting to update index meta to 'cancelled' state for index {index_name}")
                try:
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
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>/<string:index_name>/<string:task_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
