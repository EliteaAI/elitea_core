import json

from tools import api_tools, auth, config as c, db
from pylon.core.tools import log

from ...utils.constants import PROMPT_LIB_MODE
from ...models.elitea_tools import EliteATool
from ...utils.application_tools import (
    expand_toolkit_settings,
    ValidatorNotSupportedError,
    ConfigurationExpandError,
)


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.tool.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, toolkit_id: int, **kwargs):
        _ = kwargs

        current_user = auth.current_user()
        user_id = current_user.get('id') if current_user else None

        with db.with_project_schema_session(project_id) as session:
            tool = session.query(EliteATool).filter(EliteATool.id == toolkit_id).first()
            if not tool:
                return {'error': f'No such toolkit with id {toolkit_id}'}, 400

            toolkit_type = getattr(tool, 'type', None)
            settings = getattr(tool, 'settings', None) or {}

        try:
            if not isinstance(toolkit_type, str) or not toolkit_type.strip():
                return {"error": f"Toolkit {toolkit_id} has no type"}, 400

            # Expand settings to resolve configuration references (e.g. openapi_configuration)
            # This ensures credentials are properly fetched from vault and passed to the SDK
            try:
                if user_id:
                    settings = expand_toolkit_settings(toolkit_type, settings, project_id, user_id)
            except (ValidatorNotSupportedError, ConfigurationExpandError) as e:
                log.warning(f"Could not expand settings for toolkit {toolkit_id}: {e}")
                # Continue with unexpanded settings - the SDK may still work for some toolkits
            except Exception as e:
                log.warning(f"Error expanding settings for toolkit {toolkit_id}: {e}")
                # Continue with unexpanded settings

            # Call RPC method which properly handles task dispatch to indexer_worker
            task_result = self.module.get_toolkit_available_tools(
                toolkit_type=toolkit_type,
                settings=settings,
            )

            if isinstance(task_result, dict) and task_result.get('error'):
                return {"error": task_result.get('error')}, 400

            # Ensure consistent JSON response
            try:
                json.dumps(task_result)
            except Exception:
                return {"error": "Failed to serialize available tools"}, 500

            return task_result, 200

        except Exception as e:
            log.exception("Error while fetching toolkit available tools")
            return {"error": str(e)}, 500


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
