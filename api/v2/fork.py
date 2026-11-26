import copy
from flask import request

from tools import api_tools, auth, config as c
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.conversation_starters_utils import import_starters_errors
from ...utils.export_import_utils import ENTITY_IMPORT_MAPPER, _wrap_import_error

from pylon.core.tools import log
from ...utils.folder_access import fork_payload_access_error
from ...utils.permissions import ProjectPermissionChecker


def _starters_rejection(applications: list, skills: list):
    """Reject the whole fork, since import_wizard saves each application alone and would leave a partial fork."""
    messages = [
        import_starters_errors(application.get('name', ''), application.get('versions'))
        for application in applications
    ]
    if not any(messages):
        return None
    not_forked = 'Not forked: another item in this fork failed the conversation starter checks.'
    rejected = {key: [] for key in ENTITY_IMPORT_MAPPER}
    rejected['agents'] = [
        _wrap_import_error(index, message, import_uuid=application.get('import_uuid'))
        for index, (application, own_messages) in enumerate(zip(applications, messages))
        for message in own_messages or [not_forked]
    ]
    rejected['skills'] = [
        _wrap_import_error(index, not_forked, import_uuid=skill.get('import_uuid'))
        for index, skill in enumerate(skills, start=len(applications))
    ]
    return {'result': {key: [] for key in ENTITY_IMPORT_MAPPER}, 'errors': rejected}, 400


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.fork.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        denied, status_code = ProjectPermissionChecker(project_id).check_permissions(
            ["models.applications.export_import.import"]
        )
        if status_code != 200:
            return denied, status_code

        # Work exactly like import_wizard, but set fork parent metadata
        fork_data = copy.copy(request.json)
        author_id = auth.current_user().get("id")

        applications = fork_data.get('applications') or []
        skills = fork_data.get('skills') or []
        entities = applications + skills

        error = fork_payload_access_error(entities)
        if error is not None:
            return error
        rejection = _starters_rejection(applications, skills)
        if rejection is not None:
            return rejection

        # Set fork parent metadata on each version (this makes is_forked = true)
        for entity in entities:
            # A pipeline carried by a published agent has no source entity of its own, so no lineage.
            if entity.get('entity') in ('agents', 'skills') and entity.get('id') is not None:
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
