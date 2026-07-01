from tools import api_tools, auth, config as c, log
from ...utils.application_tools import (
    get_toolkit_index_meta,
    load_and_validate_toolkit_for_index,
    get_session_for_schema,
    cancel_toolkit_index_meta,
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
                    cancel_toolkit_index_meta(
                        connection_string,
                        toolkit_name_id,
                        index_name,
                        expected_task_id=task_id,
                        delete_embeddings=True,
                    )
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
