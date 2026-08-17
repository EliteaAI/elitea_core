import logging

from flask import request
from pydantic import ValidationError

from tools import api_tools, auth, config as c, db, register_openapi

from ...models.all import Application, ApplicationVersion
from ...models.folder import ApplicationFolder
from ...models.enums.all import AgentTypes
from ...models.pd.application_folder import MoveApplicationToFolderRequest
from ...utils.constants import PROMPT_LIB_MODE

log = logging.getLogger(__name__)


def get_application_agent_type(session, application_id: int) -> str | None:
    """Get the agent_type from the base version of an application."""
    base_version = session.query(ApplicationVersion).filter(
        ApplicationVersion.application_id == application_id,
        ApplicationVersion.name == 'base'
    ).first()
    return base_version.agent_type if base_version else None


class PromptLibAPI(api_tools.APIModeHandler):

    @register_openapi(
        name="Move Application to Folder",
        description="Move an agent or pipeline to a folder, or remove it from its current folder",
        mcp_description="""
        USE to move an agent/pipeline into a folder, or remove it from a folder.

        Examples:
        1. Move to folder: PUT ...?application_id=7 with body { 'folder_id': 5 }
        2. Remove from folder: PUT ...?application_id=7 with body { 'folder_id': null }

        Validation:
        - Folder must exist and belong to the current user
        - Folder's agent_type must match the application's type (openai vs pipeline)
        """,
        tags=["elitea_core/applications"],
        mcp_tool=True,
        request_body=MoveApplicationToFolderRequest,
        parameters=[
            {"name": "application_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Application ID to move."},
        ],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.application.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def put(self, project_id: int, application_id: int, **kwargs):
        raw = dict(request.json)
        user_id = auth.current_user().get("id")

        try:
            parsed = MoveApplicationToFolderRequest.model_validate(raw)
        except ValidationError as e:
            return e.errors(), 400

        with db.get_session(project_id) as session:
            # Get the application
            application = session.query(Application).filter(
                Application.id == application_id
            ).first()
            if not application:
                return {"error": "Application not found"}, 404

            # If folder_id is None, remove from folder
            if parsed.folder_id is None:
                application.folder_id = None
                session.commit()
                return {"message": "Application removed from folder", "folder_id": None}, 200

            # Get the target folder
            folder = session.query(ApplicationFolder).filter(
                ApplicationFolder.id == parsed.folder_id
            ).first()
            if not folder:
                return {"error": "Folder not found"}, 404

            # Verify folder ownership
            if folder.owner_id != user_id:
                return {"error": "You don't have permission to move applications to this folder"}, 403

            # Verify agent_type match
            app_agent_type = get_application_agent_type(session, application_id)
            if app_agent_type is None:
                return {"error": "Could not determine application type"}, 400

            # Map agent_type to folder type
            # Classic agents use various types like 'openai', 'chat', etc. - all map to folder type 'openai'
            # Pipelines use 'pipeline' type
            folder_expected_type = 'pipeline' if app_agent_type == AgentTypes.pipeline.value else 'openai'

            if folder.agent_type != folder_expected_type:
                return {
                    "error": f"Folder type mismatch. Application is '{folder_expected_type}' but folder is '{folder.agent_type}'"
                }, 400

            # Move application to folder
            application.folder_id = parsed.folder_id
            session.commit()

            log.info(f"Moved application {application_id} to folder {parsed.folder_id}")
            return {
                "message": "Application moved to folder",
                "folder_id": parsed.folder_id,
                "application_id": application_id
            }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:application_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
