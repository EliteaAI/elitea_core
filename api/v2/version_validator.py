from pydantic import ValidationError

from ...utils.application_utils import validate_application_version_details
from ...utils.constants import PROMPT_LIB_MODE

from tools import api_tools, auth, config as c


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.version_validator.check"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, application_id: int, version_id: int, **kwargs):
        user_id = auth.current_user().get('id')

        result = {'error': [], 'toolkit_errors': []}
        try:
            validate_application_version_details(project_id, application_id, version_id, user_id)
        except ValidationError as e:
            result['toolkit_errors'] = e.errors(
                include_url=False,
                include_context=False,
                include_input=False
            )
            return result, 400
        except Exception as e:
            result['error'] = str(e)
            return result, 400

        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:application_id>',
        '<int:project_id>/<int:application_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
