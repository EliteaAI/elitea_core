from flask import request
from pydantic import ValidationError
from pylon.core.tools import log

from tools import api_tools, auth, config as c, register_openapi, VaultClient

from ...models.pd.index import SaveIndexConfiguration
from ...utils.application_tools import (
    IndexRunInProgressError,
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
            # A body that is valid JSON but not an object (a list, a bare string) has to reach the
            # model to be rejected as a validation error rather than blowing up on coercion.
            payload = SaveIndexConfiguration.parse_obj(request.get_json(silent=True) or {})
        except ValidationError as e:
            log.error(f"Validation error on index configuration save: {e.errors()}")
            return {"ok": False, "error": f"Validation error on index configuration save: {e.errors()}"}, 400

        toolkit_config = get_toolkit_config(project_id, auth.current_user()['id'], toolkit_id)
        toolkit_name_id, connection_string, validation_error = load_and_validate_toolkit_for_index(toolkit_config)
        if validation_error:
            return validation_error

        task_disconnected_timeout = int(
            VaultClient(project_id).get_secrets().get('task_disconnected_timeout_sec', 7200)
        )

        try:
            configuration = save_toolkit_index_configuration(
                connection_string, toolkit_name_id, index_name,
                payload.configuration, task_disconnected_timeout,
            )
            if configuration is None:
                return {"ok": False, "error": f"Index '{index_name}' not found"}, 404

            return {"ok": True, "configuration": configuration}, 200
        except IndexRunInProgressError as e:
            return {"ok": False, "error": str(e)}, 409
        except Exception as e:
            log.error(f"Error occurred while saving configuration for index '{index_name}': {e}")
            return {"ok": False, "error": "Error occurred while saving index configuration"}, 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>/<string:index_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
