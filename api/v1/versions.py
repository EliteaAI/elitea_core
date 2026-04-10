import json
from flask import request

from tools import api_tools, auth, config as c, db
from sqlalchemy.orm import joinedload, selectinload
from pydantic import ValidationError


from ...models.pd.version import (
    ApplicationVersionDetailModel,
    ApplicationVersionListModel,
    ApplicationVersionCreateModel
)
from ...models.all import Application, ApplicationVersion
from ...utils.create_utils import create_version
from ...utils.constants import PROMPT_LIB_MODE


class ProjectAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.versions.get"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, application_id: int, **kwargs):
        with db.with_project_schema_session(project_id) as session:
            application = session.query(Application).options(
                selectinload(Application.versions)
            ).get(application_id)
            if not application:
                return {
                    "ok": False,
                    "error": f"Application with id '{application_id}' doesn't exist"
                }, 400
            
            return [ApplicationVersionListModel.from_orm(version).model_dump(mode='json') for version in application.versions]
            
    @auth.decorators.check_api({
        "permissions": ["models.applications.versions.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, application_id: int, **kwargs):
        payload = request.get_json()
        with db.get_session(project_id) as session:
            application = session.query(Application).get(application_id)
            if not application:
                return {
                    "ok": False,
                    "error": f"Application with id '{application_id}' doesn't exist"
                }
            try:
                payload['user_id'] = payload['author_id'] = auth.current_user().get("id")
                payload['project_id'] = project_id

                version_data = ApplicationVersionCreateModel.model_validate(payload)
            except ValidationError as e:
                return e.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                ), 400
            version = create_version(version_data, application, session)
            # session.add(version)
            session.commit()

            # Explicitly load relationships since they are now lazy
            version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == version.id
            ).options(
                selectinload(ApplicationVersion.tools),
                selectinload(ApplicationVersion.tool_mappings),
                selectinload(ApplicationVersion.variables)
            ).first()

            result = ApplicationVersionDetailModel.from_orm(version)
        return result.model_dump(mode='json'), 201


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:application_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: ProjectAPI,
    }
