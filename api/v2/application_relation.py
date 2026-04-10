from flask import request
from pydantic.v1 import ValidationError

from ...utils.application_tools import application_toolkit_change_relation, ToolkitChangeRelationError
from ...utils.constants import PROMPT_LIB_MODE

from tools import api_tools, auth, config as c

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.application_relation.patch"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def patch(self, project_id: int, application_id: int, version_id: int):
        update_data = dict(request.json)

        if update_data.get('application_id') == application_id:
            return {'error': f"Can not bind same agent to itself: {application_id=}"}, 400

        try:
            result = application_toolkit_change_relation(
                project_id=project_id,
                user_id=auth.current_user().get("id"),
                application_id=application_id,
                version_id=version_id,
                update_data=update_data,
            )
        except ValidationError as e:
            return e.errors(), 400
        except ToolkitChangeRelationError as e:
            return {'error': str(e)}, 400
        except Exception as e:
            log.exception(f"Error while changing relation: {str(e)}")
            return {'error': 'Can not change relation'}, 500

        return result, 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:application_id>',
        '<int:project_id>/<int:application_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
