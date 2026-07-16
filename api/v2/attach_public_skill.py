from traceback import format_exc

from flask import request

from tools import api_tools, config as c, db, auth

from ...models.enums.all import SkillEntityTypes
from ...utils.skill_utils import attach_public_skill_to_agents
from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.fork.post"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        body = request.json or {}
        public_skill_id = body.get('public_skill_id')
        public_version_id = body.get('public_version_id')
        agent_version_ids = body.get('agent_version_ids') or []
        entity_type = body.get('entity_type', 'agent')

        if not public_skill_id or not public_version_id or not agent_version_ids:
            return {
                'error': 'public_skill_id, public_version_id and agent_version_ids are required'
            }, 400

        try:
            entity_type = SkillEntityTypes(entity_type).value
        except ValueError:
            return {'error': f"invalid entity_type '{entity_type}'"}, 400

        try:
            with db.get_session(project_id) as session:
                results = attach_public_skill_to_agents(
                    project_id=project_id,
                    public_skill_id=public_skill_id,
                    public_version_id=public_version_id,
                    agent_version_ids=agent_version_ids,
                    entity_type=entity_type,
                    session=session,
                )
                session.commit()
        except Exception as e:
            log.error(f'attach_public_skill exc\n{format_exc()}')
            return {"error": str(e)}, 400

        # Partial success is carried in the body (per-agent ok flags), not the HTTP status.
        return {'results': results}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
