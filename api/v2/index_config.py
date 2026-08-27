from flask import request
from pydantic import ValidationError
from pylon.core.tools import log

from tools import api_tools, auth, config as c, register_openapi, VaultClient

from ...models.pd.index import SaveIndexConfiguration
from ...utils.application_tools import (
    get_session_for_schema,
    get_toolkit_index_meta,
    is_index_stale,
    load_and_validate_toolkit_for_index,
    save_toolkit_index_configuration,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.predict_utils import get_toolkit_config


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Save Index Configuration",
        description="Persist an index configuration without starting an indexing run. "
                    "The saved configuration is used by the next manual or scheduled reindex.",
        request_body=SaveIndexConfiguration,
        tags=["elitea_core/indexes"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.index_meta.edit"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, toolkit_id: int, index_name: str):
        try:
            payload = SaveIndexConfiguration.parse_obj(dict(request.json or {}))
        except ValidationError as e:
            log.error(f"Validation error on index configuration save: {e.errors()}")
            return {"ok": False, "error": f"Validation error on index configuration save: {e.errors()}"}, 400

        toolkit_config = get_toolkit_config(project_id, auth.current_user()['id'], toolkit_id)
        toolkit_name_id, connection_string, validation_error = load_and_validate_toolkit_for_index(toolkit_config)
        if validation_error:
            return validation_error

        try:
            running_error = self._reject_if_running(project_id, connection_string, toolkit_name_id, index_name)
            if running_error:
                return running_error

            configuration = save_toolkit_index_configuration(
                connection_string, toolkit_name_id, index_name, payload.configuration
            )
            if configuration is None:
                return {"ok": False, "error": f"Index '{index_name}' not found"}, 404

            return {"ok": True, "configuration": configuration}, 200
        except Exception as e:
            log.error(f"Error occurred while saving configuration for index '{index_name}': {e}")
            return {"ok": False, "error": "Error occurred while saving index configuration"}, 400

    def _reject_if_running(self, project_id: int, connection_string: str, toolkit_name_id: str, index_name: str):
        """
        A save during a live run would be silently overwritten by that run's own metadata write,
        so the config tab is disabled while indexing. A stale run never writes again, so it must
        not lock the configuration out indefinitely.
        """
        with get_session_for_schema(connection_string, toolkit_name_id) as session:
            meta = get_toolkit_index_meta(session, index_name)
            if not meta:
                return {"ok": False, "error": f"Index '{index_name}' not found"}, 404

            cmetadata = meta.cmetadata or {}
            timeout = int(VaultClient(project_id).get_secrets().get('task_disconnected_timeout_sec', 7200))
            stale = is_index_stale(cmetadata.get('updated_on', 0), cmetadata.get('state', ''), timeout)

            if cmetadata.get('state') == 'in_progress' and not stale:
                return {"ok": False, "error": "Cannot save configuration while indexing is in progress"}, 409

        return None


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>/<string:index_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
