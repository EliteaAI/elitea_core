import json

from sqlalchemy.orm import joinedload
from tools import api_tools, config as c, db, auth
from ...models.all import ApplicationVersion, Application
from ...models.pd.application import PublishedApplicationDetailModel
from ...models.pd.version import ApplicationVersionDetailModel

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
    def get(self, project_id: int, application_id: int, version_name: str = None, *args, **kwargs):
        with db.with_project_schema_session(project_id) as session:
            filters = [
                ApplicationVersion.application_id == application_id,
                ApplicationVersion.status == PublishStatus.published
            ]
            if version_name:
                filters.append(ApplicationVersion.name == version_name)

            query = (
                session.query(ApplicationVersion)
                .filter(*filters)
                .options(
                    joinedload(ApplicationVersion.application).options(joinedload(Application.versions)),
                )
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

        return result.model_dump(mode='json'), 200


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:application_id>',
        '<int:application_id>/<string:version_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
