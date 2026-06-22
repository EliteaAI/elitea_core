from flask import request
from pydantic import BaseModel, ValidationError

from sqlalchemy.orm import joinedload, selectinload
from tools import api_tools, config as c, db, auth, register_openapi

from ...models.skill import Skill, SkillVersion
from ...utils.skill_utils import get_skill_details
from ...utils.constants import PROMPT_LIB_MODE


class DefaultVersionUpdateModel(BaseModel):
    """Request model for updating a skill's default version."""
    version_id: int


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Set the default version for a skill",
        description="Marks the given skill version as the skill's default by storing its id in skill.meta.default_version_id. The version must belong to the skill.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
        ],
        request_body=DefaultVersionUpdateModel,
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def patch(self, project_id: int, skill_id: int, **kwargs):
        try:
            data = DefaultVersionUpdateModel.model_validate(request.json)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        with db.with_project_schema_session(project_id) as session:
            # Verify the version exists and belongs to this skill; load the
            # skill (and its versions) in the same query to avoid a second hit.
            version = session.query(SkillVersion).filter(
                SkillVersion.id == data.version_id,
                SkillVersion.skill_id == skill_id,
            ).options(
                joinedload(SkillVersion.skill).selectinload(Skill.versions)
            ).first()

            if not version:
                return {
                    'error': f'Version {data.version_id} not found or does not belong to skill {skill_id}'
                }, 404

            skill = version.skill
            if not skill.meta:
                skill.meta = {}
            skill.meta['default_version_id'] = data.version_id

            session.commit()

        result = get_skill_details(project_id=project_id, skill_id=skill_id)
        if not result.get('data'):
            return {"error": "Skill not found"}, 404
        return result['data'], 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>",
            "<int:project_id>/<int:skill_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
