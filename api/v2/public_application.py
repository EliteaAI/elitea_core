import json

from flask import request
from pylon.core.tools import log
from sqlalchemy.orm import joinedload, selectinload
from tools import api_tools, config as c, db, auth
from ...models.all import ApplicationVersion, Application
from ...models.pd.application import PublishedApplicationDetailModel
from ...models.pd.version import ApplicationVersionDetailModel

from ...models.enums.all import PublishStatus
from ...utils.application_utils import build_skill_mappings_list
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.secrets import check_secret_header
from ...utils.skill_utils import apply_runtime_skills
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
    def get(self, project_id: int, application_id: int, version_name: str = None, *args, **kwargs):
        # Any viewer can read this endpoint, and published skill twins are hidden
        # from the catalog on purpose. The query parameter alone is therefore not
        # enough to release skill bodies.
        serve_runtime_skills = False
        if request.args.get('runtime') == 'true':
            serve_runtime_skills = check_secret_header(
                request.headers.get('X-SECRET'), project_id=project_id,
            )
            if not serve_runtime_skills:
                log.warning(
                    "Withholding runtime skills for public application %s: X-SECRET did "
                    "not validate. A published agent's sub-agents will run without their "
                    "skills — check secrets_header_value on the calling project.",
                    application_id,
                )
        with db.with_project_schema_session(project_id) as session:
            filters = [
                ApplicationVersion.application_id == application_id,
                ApplicationVersion.status.in_([
                    PublishStatus.published, PublishStatus.embedded,
                ]),
            ]
            if version_name:
                filters.append(ApplicationVersion.name == version_name)

            load_options = [
                joinedload(ApplicationVersion.application).options(joinedload(Application.versions)),
            ]
            if serve_runtime_skills:
                load_options.append(selectinload(ApplicationVersion.skill_mappings))
            query = (
                session.query(ApplicationVersion)
                .filter(*filters)
                .options(*load_options)
                .order_by(ApplicationVersion.created_at.desc())
            )
            application_version = query.first()

            if not application_version:
                return {
                    'error': f'No application found with id \'{application_id}\' or no public version'
                }, 400

            result = PublishedApplicationDetailModel.from_orm(application_version.application)
            result.version_details = ApplicationVersionDetailModel.from_orm(application_version)
            result.get_likes(project_id)
            result.check_is_liked(project_id)

            if result.version_details and result.version_details.tools:
                for tool in result.version_details.tools:
                    tool.set_agent_type(project_id)
                    tool.fix_name(project_id)
                    tool.set_agent_meta_and_fields(project_id)
                    tool.set_online(project_id)

            result_dict = result.model_dump(mode='json')
            # The catalog view and the fork base read this same payload. Baking it
            # for every caller would remove the ~ token from every fork.
            if serve_runtime_skills:
                version_details = result_dict['version_details']
                version_details['skills'] = build_skill_mappings_list(
                    application_version.skill_mappings
                )
                apply_runtime_skills(version_details)

        for tool in result_dict.get('version_details', {}).get('tools', []):
            tool['project_id'] = project_id
        return result_dict, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:application_id>',
        '<int:application_id>/<string:version_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
