import logging

from flask import request
from pydantic import ValidationError
from sqlalchemy import func

from tools import api_tools, auth, db, config as c, register_openapi
from tools import serialize

from ...models.all import Application
from ...models.folder import ApplicationFolder
from ...models.pd.application_folder import (
    ApplicationFolderUpdate,
    ApplicationFolderDetails,
)
from ...utils.constants import PROMPT_LIB_MODE

log = logging.getLogger(__name__)


class PromptLibAPI(api_tools.APIModeHandler):
    """Handles single-item operations: get, update, patch, delete folder."""

    @register_openapi(
        name="Get Application Folder",
        description="Get details of a specific folder",
        tags=["elitea_core/applications"],
        parameters=[
            {"name": "folder_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Folder ID."},
            {"name": "agent_type", "in": "query", "required": True, "schema": {"type": "string", "enum": ["openai", "pipeline"]}, "description": "Agent type filter."},
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
    def get(self, project_id: int, folder_id: int, **kwargs):  # pylint: disable=unused-argument
        with db.get_session(project_id) as session:
            folder = session.query(ApplicationFolder).filter(
                ApplicationFolder.id == folder_id
            ).first()
            if not folder:
                return {"error": "Folder not found"}, 404

            # Count applications in folder
            app_count = session.query(func.count(Application.id)).filter(
                Application.folder_id == folder_id
            ).scalar() or 0

            result = serialize(ApplicationFolderDetails.model_validate(folder))
            result['applications_count'] = app_count
            return result, 200

    @register_openapi(
        name="Update Application Folder",
        description="Update a folder's name",
        mcp_description="""
        USE to rename a folder.

        DO NOT USE to delete a folder → use the folder DELETE endpoint.
        DO NOT USE to move applications between folders → use the move endpoint.

        Examples:
        1. Rename: { 'name': 'Q3 Review Agents' }
        """,
        tags=["elitea_core/applications"],
        mcp_tool=True,
        parameters=[
            {"name": "folder_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Folder ID."},
        ],
        request_body=ApplicationFolderUpdate,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def put(self, project_id: int, folder_id: int, **kwargs):  # pylint: disable=unused-argument
        raw = dict(request.json)

        try:
            parsed = ApplicationFolderUpdate.model_validate(raw)
        except ValidationError as e:
            return e.errors(), 400

        with db.get_session(project_id) as session:
            folder = session.query(ApplicationFolder).filter(
                ApplicationFolder.id == folder_id
            ).first()
            if not folder:
                return {"error": "Folder not found"}, 404

            for key, value in parsed.model_dump(exclude_unset=True).items():
                setattr(folder, key, value)

            session.commit()
            return serialize(ApplicationFolderDetails.model_validate(folder)), 200

    @register_openapi(
        name="Patch Application Folder",
        description="Update folder pin status.",
        tags=["elitea_core/applications"],
        parameters=[
            {"name": "folder_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Folder ID."},
        ],
        request_body={
            "required": True,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "required": ["is_pinned"],
                        "properties": {
                            "is_pinned": {
                                "type": "boolean",
                                "description": "Set to true to pin the folder, false to unpin.",
                            }
                        },
                    }
                }
            },
        },
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def patch(self, project_id: int, folder_id: int, **kwargs):  # pylint: disable=unused-argument
        """Update folder pin status."""
        raw = dict(request.json)
        is_pinned_raw = raw.get('is_pinned')

        if is_pinned_raw is None:
            return {"error": "is_pinned is required"}, 400

        if isinstance(is_pinned_raw, int):
            is_pinned = is_pinned_raw != 0
        elif isinstance(is_pinned_raw, str):
            is_pinned = is_pinned_raw.lower() in ('true', '1')
        elif isinstance(is_pinned_raw, bool):
            is_pinned = is_pinned_raw
        else:
            return {"error": "is_pinned must be a boolean value"}, 400

        with db.get_session(project_id) as session:
            folder = session.query(ApplicationFolder).filter(
                ApplicationFolder.id == folder_id
            ).first()

            if not folder:
                return {"error": "Folder not found"}, 404

            meta = dict(folder.meta) if folder.meta else {}
            meta['is_pinned'] = is_pinned
            folder.meta = meta

            session.commit()
            return serialize(ApplicationFolderDetails.model_validate(folder)), 200

    @register_openapi(
        name="Delete Application Folder",
        description="Delete a folder and unassign applications from it.",
        tags=["elitea_core/applications"],
        parameters=[
            {"name": "folder_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Folder ID."},
        ],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.applications.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, folder_id: int, **kwargs):  # pylint: disable=unused-argument
        with db.get_session(project_id) as session:
            folder = session.query(ApplicationFolder).filter(
                ApplicationFolder.id == folder_id
            ).first()
            if not folder:
                return {"error": "Folder not found"}, 404

            # Unassign applications from the folder
            applications = session.query(Application).filter(
                Application.folder_id == folder_id
            ).all()
            for app in applications:
                app.folder_id = None

            session.delete(folder)
            session.commit()
            return {}, 204


class API(api_tools.APIBase):
    """Item endpoints: get, update, patch, delete (folder_id required)."""
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:folder_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
