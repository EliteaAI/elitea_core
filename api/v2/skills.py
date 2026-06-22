from traceback import format_exc

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, serialize, register_openapi

from ...models.pd.skill import (
    SkillCreateModel,
    MultipleSkillListModel,
)
from ...utils.skill_utils import (
    list_skills_api,
    create_skill,
    build_skill_detail,
    SkillError,
)
from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List skills in a project — paginated, filterable by tags, author, and free text",
        description="Returns a paginated list of skills in the project. Supports filtering by tags, author, and free-text search over name and description.",
        parameters=[
            {"name": "query", "in": "query", "schema": {"type": "string"}, "description": "Free-text search over name/description"},
            {"name": "tags", "in": "query", "schema": {"type": "string"}, "description": "Comma-separated tag IDs"},
            {"name": "author_id", "in": "query", "schema": {"type": "integer"}, "description": "Filter by author ID"},
            {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 10}},
            {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}},
            {"name": "sort_by", "in": "query", "schema": {"type": "string", "default": "created_at"}},
            {"name": "sort_order", "in": "query", "schema": {"type": "string", "default": "desc"}},
        ],
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.skills.list"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int | None = None, **kwargs):
        with db.get_session(project_id) as session:
            result = list_skills_api(
                project_id=project_id,
                tags=request.args.get('tags'),
                author_id=request.args.get('author_id', type=int),
                q=request.args.get('query'),
                limit=request.args.get("limit", default=10, type=int),
                offset=request.args.get("offset", default=0, type=int),
                sort_by=request.args.get("sort_by", default="created_at"),
                sort_order=request.args.get("sort_order", default='desc'),
                session=session,
            )
            try:
                parsed = MultipleSkillListModel(skills=result['skills'])
                return {
                    'total': result['total'],
                    'rows': [serialize(i) for i in parsed.skills],
                }, 200
            except Exception as e:
                log.error(f'skill list exc\n{format_exc()}')
                return {"error": str(e)}, 400

    @register_openapi(
        name="Create a new skill with a mandatory initial 'base' version",
        description="Creates a new skill with an initial version. The request must include skill metadata (name, description) and exactly one version definition.",
        request_body=SkillCreateModel,
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.skills.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int | None = None, **kwargs):
        raw = dict(request.json)
        raw["owner_id"] = project_id
        author_id = auth.current_user().get("id")
        raw['project_id'] = project_id
        raw['user_id'] = author_id
        for version in raw.get("versions", []):
            version["author_id"] = author_id
        try:
            skill_data = SkillCreateModel.model_validate(raw)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        with db.get_session(project_id) as session:
            try:
                skill = create_skill(skill_data, session, project_id)
            except SkillError as e:
                return {"error": str(e)}, e.http_status
            session.commit()
            session.refresh(skill)

            return build_skill_detail(skill).model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "",
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
