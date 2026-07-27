from traceback import format_exc

from flask import request

from tools import api_tools, config as c, db, auth

from ...models.pd.skill import MultiplePublicSkillListModel
from ...utils.skill_utils import list_public_skills_api
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.utils import add_public_project_id

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @add_public_project_id
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.public_applications.list"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, *, project_id: int | None = None, **kwargs):
        # limit=0 is falsy downstream and would drop the LIMIT clause entirely,
        # dumping the whole catalog in one response.
        limit = request.args.get("limit", default=10, type=int)
        if limit <= 0:
            limit = 10
        with db.with_project_schema_session(project_id) as session:
            result = list_public_skills_api(
                project_id=project_id,
                q=request.args.get('query'),
                tags=request.args.get('tags'),
                category=request.args.get('category'),
                my_liked=str(request.args.get('my_liked', '')).lower() in ('true', '1'),
                trend_start_period=request.args.get('trend_start_period'),
                trend_end_period=request.args.get('trend_end_period'),
                limit=limit,
                offset=request.args.get("offset", default=0, type=int),
                sort_by=request.args.get("sort_by", default="created_at"),
                sort_order=request.args.get("sort_order", default='desc'),
                session=session,
            )

        try:
            parsed = MultiplePublicSkillListModel(skills=result['rows'])
            return {
                'total': result['total'],
                'rows': [i.model_dump(mode='json') for i in parsed.skills],
            }, 200
        except Exception as e:
            log.error(f'public skill list exc\n{format_exc()}')
            return {"error": str(e)}, 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
