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
        cancelled = False
        with get_session_for_schema(connection_string, toolkit_name_id) as session:
            meta = get_toolkit_index_meta(session, index_name)
            if meta is None:
                # Row already gone (e.g. deleted from another tab) — nothing to stop,
                # but "could not be stopped" would be the wrong story for the caller.
                log.debug(f"No index_meta to cancel for index_name={index_name}")
                return {"ok": True, "cancelled": False, "reason": "not_found"}, 200
            row_task_id = meta.cmetadata.get("task_id")
            log.debug(f"Expected task_id to cancel: {task_id}")
            log.debug(f"Actual task_id to cancel: {row_task_id}")
            # A null row id is still cancellable (the id can be lost while a run is
            # live); a mismatch means the row belongs to a different run that this
            # possibly-stale request must not touch.
            if row_task_id in (None, task_id):
                # Kill only ids corroborated server-side — by the row, or by the
                # registry, which still names the task after a lost update wiped the
                # row's id. A bare client-held id must not be able to stop tasks.
                corroborated = bool(task_id) and row_task_id == task_id
                if not corroborated and task_id:
                    # Registry keys come from event payloads, these ids from Flask path
                    # converters — normalize so an int/str drift can't silently degrade
                    # corroboration to never-kill.
                    wanted = (str(project_id), str(toolkit_id), str(index_name))
                    with self.module.active_index_tasks_lock:
                        entries = self.module.active_index_tasks.get(str(task_id)) or {}
                        corroborated = any(
                            (str(p), str(t), str(n)) == wanted for (p, t, n) in entries
                        )
                # Try to stop the task (best-effort)
                if corroborated and self.module.task_node is not None:
                    try:
                        log.debug(f"Attempting to stop indexer's task {task_id}")
                        self.module.task_node.stop_task(task_id)
                    except Exception as e:
                        log.warning(f"Failed to stop task {task_id}: {e}. Proceeding with cleanup.")
                elif task_id:
                    log.warning(f"Skipping task stop for index {index_name}: task {task_id} not "
                                f"corroborated by row or registry (task may already be dead)")
                log.debug(f"Attempting to update index meta to 'cancelled' state for index {index_name}")
                try:
                    cancelled = cancel_toolkit_index_meta(
                        connection_string,
                        toolkit_name_id,
                        index_name,
                        expected_task_id=task_id,
                        delete_embeddings=True,
                        require_in_progress=False,
                        session=session,
                    )
                    if not cancelled:
                        log.warning(f"Manual cancel transitioned no row for index {index_name} (task {task_id})")
                except Exception as e:
                    return {
                        "ok": False,
                        "error": str(e)
                    }, 400
        return {"ok": True, "cancelled": bool(cancelled)}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>/<string:index_name>/<string:task_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
