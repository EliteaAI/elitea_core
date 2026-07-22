from pydantic import ValidationError

from ...utils.application_utils import validate_application_version_details
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.publish_utils import SubAgentTreeError

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

        result = {'error': [], 'toolkit_errors': [], 'connection_errors': []}
        try:
            validate_application_version_details(project_id, application_id, version_id, user_id)
        except SubAgentTreeError as e:
            # Structural sub-agent violation (cycle / leaf-rule, issue #5680). Emit it in the
            # per-tool `toolkit_errors` shape the frontend actually renders (loc[1]=tool id,
            # msg) rather than the generic `error` field the hook ignores — this is what makes
            # a misconfigured agent show a red chip at validate time instead of only hitting a
            # wall at predict time (PR #203 finding #1).
            result['toolkit_errors'].append(e.to_toolkit_error())
            return result, 400
        except ValidationError as e:
            for err in e.errors(include_url=False, include_context=False, include_input=False):
                if err.get('type') == 'connection_error':
                    result['connection_errors'].append(err)
                else:
                    result['toolkit_errors'].append(err)
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
