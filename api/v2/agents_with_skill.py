from traceback import format_exc

from tools import api_tools, config as c, auth

from ...utils.skill_utils import get_agents_with_skill
from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.applications.details"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, skill_id: int, **kwargs):
        try:
            rows = get_agents_with_skill(
                project_id=project_id,
                public_skill_id=skill_id,
            )
        except Exception as e:
            log.error(f'agents_with_skill exc\n{format_exc()}')
            return {"error": str(e)}, 400

        return {
            'total': len(rows),
            'rows': [i.model_dump(mode='json') for i in rows],
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
