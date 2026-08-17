import logging

from flask import request
from pydantic import ValidationError

from tools import api_tools, auth, db, config as c, register_openapi
from tools import serialize

from ...models.all import Application
from ...models.folder import ApplicationFolder
from ...models.pd.application_folder import (
    ApplicationFolderCreate,
    ApplicationFolderDetails,
)
from ...utils.constants import PROMPT_LIB_MODE

log = logging.getLogger(__name__)


class PromptLibAPI(api_tools.APIModeHandler):
    """Handles collection-level operations: list folders, create folder."""

    @register_openapi(
        name="List Application Folders",
        description="List application folders for agents or pipelines with optional nested applications",
        mcp_description="""
        USE to render the agent/pipeline sidebar with folder structure, or to list folders for a specific type.

        Mode selection guide:
        - Agents sidebar: GET .../application-folders/prompt_lib/42?agent_type=openai
        - Pipelines sidebar: GET .../application-folders/prompt_lib/42?agent_type=pipeline
        - With nested apps: GET ...?agent_type=openai&include_applications=true

        Examples:
        1. List agent folders: GET .../application-folders/prompt_lib/42?agent_type=openai
        2. List pipeline folders: GET .../application-folders/prompt_lib/42?agent_type=pipeline
        3. Search folders: GET ...?agent_type=openai&query=sprint
        """,
        tags=["elitea_core/applications"],
        mcp_tool=True,
        parameters=[
            {"name": "agent_type", "in": "query", "required": True, "schema": {"type": "string", "enum": ["openai", "pipeline"]}, "description": "Filter by agent type: 'openai' for agents, 'pipeline' for pipelines."},
            {"name": "query", "in": "query", "required": False, "schema": {"type": "string"}, "description": "Search query."},
            {"name": "include_applications", "in": "query", "required": False, "schema": {"type": "boolean"}, "description": "Include nested applications in response."},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Max applications per folder (when include_applications=true)."},
        ],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):  # pylint: disable=unused-argument
        with db.get_session(project_id) as session:
            user_id = auth.current_user().get("id")
            agent_type = request.args.get('agent_type')

            if not agent_type:
                return {"error": "agent_type is required (openai or pipeline)"}, 400

            if agent_type not in ['openai', 'pipeline']:
                return {"error": "agent_type must be 'openai' or 'pipeline'"}, 400

            # List folders
            q = request.args.get('query')
            include_applications = request.args.get('include_applications', 'false').lower() == 'true'
            limit = request.args.get('limit', default=10, type=int)

            folder_query = session.query(ApplicationFolder).filter(
                ApplicationFolder.owner_id == user_id,
                ApplicationFolder.agent_type == agent_type,
            )

            if q:
                folder_query = folder_query.filter(
                    ApplicationFolder.name.ilike(f'%{q}%')
                )

            folders = folder_query.order_by(
                ApplicationFolder.name.asc()
            ).all()

            folder_data = []
            for folder in folders:
                folder_item = serialize(ApplicationFolderDetails.model_validate(folder))

                # Get application count
                app_query = session.query(Application).filter(
                    Application.folder_id == folder.id
                )
                folder_item['applications_count'] = app_query.count()

                if include_applications:
                    # Get applications with their base version to determine type
                    applications = app_query.limit(limit).all()
                    folder_item['applications'] = [
                        {
                            'id': app.id,
                            'uuid': str(app.uuid),
                            'name': app.name,
                            'description': app.description,
                            'icon': app.icon,
                            'folder_id': app.folder_id,
                            'created_at': app.created_at.isoformat() if app.created_at else None,
                        }
                        for app in applications
                    ]
                    folder_item['total'] = folder_item['applications_count']

                folder_data.append(folder_item)

            return {
                'total': len(folders),
                'folders': folder_data,
            }, 200

    @register_openapi(
        name="Create Application Folder",
        description="Create a new folder to organize agents or pipelines",
        mcp_description="""
        USE to create a new folder for organizing agents or pipelines.

        DO NOT USE to move an application into a folder → use the move endpoint.
        DO NOT USE to rename or reorder an existing folder → use update_application_folder.

        Examples:
        1. Create agent folder: { 'name': 'Code Review Agents', 'agent_type': 'openai' }
        2. Create pipeline folder: { 'name': 'CI Pipelines', 'agent_type': 'pipeline' }
        """,
        tags=["elitea_core/applications"],
        mcp_tool=True,
        request_body=ApplicationFolderCreate,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):  # pylint: disable=unused-argument
        raw = dict(request.json)
        user_id = auth.current_user().get("id")
        raw['owner_id'] = user_id

        try:
            parsed = ApplicationFolderCreate.model_validate(raw)
        except ValidationError as e:
            return e.errors(), 400

        if parsed.agent_type not in ['openai', 'pipeline']:
            return {"error": "agent_type must be 'openai' or 'pipeline'"}, 400

        with db.get_session(project_id) as session:
            new_folder = ApplicationFolder(**parsed.model_dump())
            session.add(new_folder)
            session.commit()
            log.info(f"Created application folder {new_folder.id} for user {user_id}")
            return serialize(ApplicationFolderDetails.model_validate(new_folder)), 201


class API(api_tools.APIBase):
    """Collection endpoints: list, create (no folder_id in URL)."""
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
