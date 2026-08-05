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
        # Work exactly like import_wizard, but set fork parent metadata
        fork_data = copy.copy(request.json)
        author_id = auth.current_user().get("id")

        applications = fork_data.get('applications') or []
        entities = applications + (fork_data.get('skills') or [])

        # Set fork parent metadata on each version (this makes is_forked = true)
        for entity in entities:
            if entity.get('entity') in ('agents', 'skills'):
                for version in entity.get('versions', []):
                    meta = version.get('meta') or {}
                    meta.update({
                        'parent_entity_id': entity.get('id'),
                        'parent_project_id': entity.get('owner_id'),
                        'parent_author_id': version.get('author_id'),
                        'parent_version_id': version.get('id')
                    })
                    version['meta'] = meta

        # Re-forking one agent must not multiply its skills, so only a skill the user
        # picked is copied fresh. main_entity is client-derived and may not override.
        main_entity = fork_data.get('main_entity')
        is_agent_fork = bool(applications)
        skills_are_picked = not is_agent_fork and main_entity in (None, 'skills')
        result, errors = self.module.import_wizard(
            entities, project_id, author_id, force_new_skills=skills_are_picked,
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
