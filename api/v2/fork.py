import copy
from flask import request

from tools import api_tools, auth, config as c
from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.fork.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        # Work exactly like import_wizard, but set fork parent metadata for agents
        fork_data = copy.copy(request.json)
        author_id = auth.current_user().get("id")
        
        # Set fork parent metadata for all agent versions (this makes is_forked = true)
        for application in fork_data.get('applications', []):
            if application.get('entity') == 'agents':
                for version in application.get('versions', []):
                    # Initialize meta if not present
                    meta = version.get('meta', {})
                    
                    # Set fork parent metadata - this is what makes is_forked = true
                    meta.update({
                        'parent_entity_id': application['id'],
                        'parent_project_id': application['owner_id'],
                        'parent_author_id': version.get('author_id'),
                        'parent_version_id': version.get('id')
                    })
                    
                    version['meta'] = meta

        # Call import_wizard exactly like the import_wizard endpoint does
        result, errors = self.module.import_wizard(
            fork_data.get('applications', []), project_id, author_id
        )

        has_results = any(result[key] for key in result if result[key])
        has_errors = any(errors[key] for key in errors if errors[key])

        if not has_errors and has_results:
            status_code = 201
        elif has_errors and has_results:
            status_code = 207
        else:
            status_code = 400

        return {'result': result, 'errors': errors}, status_code


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
