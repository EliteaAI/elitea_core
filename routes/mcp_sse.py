import threading

import flask
from flask import Response
from pylon.core.tools import web, log

from tools import context, auth, this

from ..models.enums.mappings import ENTITY_TYPE_MAP
from ..utils.mcp_handler import CommunicationsHandler
from ..utils.mcp_protocol import validate_session_id, handle_post_request, get_request_method
from ..utils.mcp_config import is_mcp_exposure_enabled

handler = CommunicationsHandler()


def _check_mcp_enabled() -> tuple[dict, int] | None:
    """Check if MCP exposure is enabled. Returns error response or None."""
    if not is_mcp_exposure_enabled():
        return {"error": "MCP exposure is disabled on this deployment"}, 403
    return None


def _check_project_access(project_id: int) -> tuple[dict, int] | None:
    """Check if user has access to project. Returns error response or None."""
    user_id = auth.current_user()["id"]
    user_project_ids = [
        project["id"] for project in context.rpc_manager.call.list_user_projects(user_id)
    ]
    if project_id not in user_project_ids:
        return {"error": "No project access"}, 403
    return None


def _handle_mcp_request(
        project_id: int, resource_type: str = None, resource_id: int = None
) -> Response:
    """
    Common handler for MCP requests (both scoped and global).

    Args:
        project_id: Project ID
        resource_type: Optional resource type ('toolkit', 'application', etc.)
        resource_id: Optional resource ID (version_id for applications, id for toolkits)
    """
    # Check if MCP exposure is enabled
    mcp_error = _check_mcp_enabled()
    if mcp_error:
        return mcp_error

    # Check access
    access_error = _check_project_access(project_id)
    if access_error:
        return access_error

    # We do not support SSE stream for GET requests here
    if flask.request.method == "GET":
        return {"error": "The server does not offer an SSE stream at this endpoint"}, 405

    # Check request type and settings
    request_method = get_request_method(flask.request)
    if request_method == "tools/call" and \
            this.descriptor.config.get("sse_tool_calls", False):
        # SSE mode
        log.debug("Starting tool call in SSE mode")
        stream, error_response, session = handler.create_session_and_stream(
            project_id, return_session=True, one_time=True,
            resource_type=resource_type, resource_id=resource_id
        )
        if error_response:
            return error_response

        def _request_executor(g_dict, *__args, **__kwargs):
            flask.g.__dict__.update(g_dict)
            return handle_post_request(*__args, **__kwargs)

        thread = threading.Thread(
            target=flask.copy_current_request_context(_request_executor),
            args=[flask.g.__dict__, flask.request.data, session],
            daemon=True,
        )
        thread.start()
        return Response(stream, mimetype='text/event-stream')
    else:
        # HTTP mode
        session, error_response = handler.create_http_session(
            project_id, resource_type=resource_type, resource_id=resource_id
        )
        if error_response:
            return error_response
        return handle_post_request(flask.request, session)


class Route:
    @web.route('/<int:project_id>/mcp', methods=['GET', 'POST'])
    def client_streamable_http(self, project_id: int) -> Response:
        """Global MCP endpoint - returns all available tools"""
        return _handle_mcp_request(project_id)

    @web.route('/<int:project_id>/mcp/<string:entity>/<int:entity_version_id>', methods=['GET', 'POST'])
    def client_parametrized_streamable_http(self, project_id: int, entity: str, entity_version_id: int) -> Response:
        """
        Resource-scoped MCP endpoint - returns tools for specific resource.

        :param project_id: Project ID
        :param entity: Entity name (e.g., 'toolkit', 'application', 'agent', 'pipeline')
        :param entity_version_id: ID of the entity (version_id for applications, id for toolkits)
        :return: MCP response object
        """
        # Map entity name to internal resource type
        resource_type = ENTITY_TYPE_MAP.get(entity.lower())
        if not resource_type:
            return {"error": f"Unknown entity type: {entity}. Valid types: {list(ENTITY_TYPE_MAP.keys())}"}, 400

        return _handle_mcp_request(project_id, resource_type=resource_type, resource_id=entity_version_id)

    @web.route('/<int:project_id>/sse', methods=['GET'])
    def client_connected(self, project_id: int) -> Response:
        # Check if MCP exposure is enabled
        mcp_error = _check_mcp_enabled()
        if mcp_error:
            return mcp_error

        # Simple auth first - check project_id is in user projects
        user_id = auth.current_user()["id"]
        user_project_ids = [
            project["id"] for project in context.rpc_manager.call.list_user_projects(user_id)
        ]
        #
        if project_id not in user_project_ids:
            return {"error": "No project access"}, 403
        #
        stream, error_response = handler.create_session_and_stream(project_id)
        #
        if error_response:
            return error_response
        #
        return Response(stream, mimetype='text/event-stream')

    @web.route('/<int:project_id>/messages', methods=['POST'])
    def client_message_received(self, project_id: int) -> Response:
        # Check if MCP exposure is enabled
        mcp_error = _check_mcp_enabled()
        if mcp_error:
            return mcp_error

        sid, error_response = validate_session_id(flask.request)
        if error_response:
            return error_response
        #
        session = handler.get_session(sid)
        if not session:
            return {'error': f"There is no active session for session_id '{sid}'"}, 404
        #
        return handle_post_request(flask.request, session)
