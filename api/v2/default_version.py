from flask import request
from pydantic import BaseModel, ValidationError

from sqlalchemy.orm import selectinload, joinedload
from tools import api_tools, config as c, db, auth

from ...models.all import Application, ApplicationVersion
from ...models.pd.application import ApplicationDetailModel
from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


class DefaultVersionUpdateModel(BaseModel):
    """Request model for updating default version"""
    version_id: int


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.version.update"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def patch(self, project_id: int, application_id: int, **kwargs):
        """Set the default version for an application.
        
        Args:
            project_id: Project ID
            application_id: Application ID
            
        Request body:
            {
                "version_id": int  # ID of the version to set as default
            }
            
        Returns:
            Updated ApplicationDetailModel with new default version set in meta
        """
        try:
            data = DefaultVersionUpdateModel.model_validate(request.json)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        with db.with_project_schema_session(project_id) as session:
            # Verify the version exists and belongs to this application
            # Load application in same query to avoid second DB query
            version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == data.version_id,
                ApplicationVersion.application_id == application_id
            ).options(
                joinedload(ApplicationVersion.application).selectinload(Application.versions)
            ).first()

            if not version:
                return {
                    'error': f'Version {data.version_id} not found or does not belong to application {application_id}'
                }, 404

            # Access the application (already loaded via joinedload)
            application = version.application

            # Update the default_version_id in meta
            if not application.meta:
                application.meta = {}
            application.meta['default_version_id'] = data.version_id
            
            session.commit()

            # Return updated application details
            result = ApplicationDetailModel.from_orm(application)
            return result.model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "<int:project_id>",
            "<int:project_id>/<int:application_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
