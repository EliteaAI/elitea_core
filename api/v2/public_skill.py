from sqlalchemy.orm import joinedload

from tools import api_tools, config as c, db, auth

from ...models.skill import Skill, SkillVersion
from ...models.pd.skill import SkillDetailModel
from ...models.pd.skill_version import SkillVersionDetailModel
from ...models.enums.all import PublishStatus
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.utils import add_public_project_id


class PromptLibAPI(api_tools.APIModeHandler):
    @add_public_project_id
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.public_application.details"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, skill_id: int, version_name: str = None, *args, **kwargs):
        with db.with_project_schema_session(project_id) as session:
            filters = [
                SkillVersion.skill_id == skill_id,
                SkillVersion.status == PublishStatus.published,
            ]
            if version_name:
                filters.append(SkillVersion.name == version_name)

            skill_version = (
                session.query(SkillVersion)
                .filter(*filters)
                .options(
                    joinedload(SkillVersion.skill).selectinload(Skill.versions),
                )
                .order_by(SkillVersion.created_at.desc())
                .first()
            )

            if not skill_version:
                return {
                    'error': f"No skill found with id '{skill_id}' or no published version"
                }, 404

            result = SkillDetailModel.model_validate(skill_version.skill)
            # Public catalog must only expose published versions (mirrors the
            # applications public detail, which filters versions the same way).
            result.versions = [
                v for v in result.versions if v.status == PublishStatus.published
            ]
            # Version details carry the instructions the catalog modal renders inline.
            result.version_details = SkillVersionDetailModel.model_validate(skill_version)
            # Version meta carries source-project lineage ids stamped at publish;
            # the public payload only needs presentation keys (icon_meta).
            if result.version_details.meta:
                result.version_details.meta = {
                    k: v for k, v in result.version_details.meta.items()
                    if not k.startswith(('parent_', 'source_'))
                }
            result.get_likes(project_id)
            result.check_is_liked(project_id)

        return result.model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:skill_id>',
        '<int:skill_id>/<string:version_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
