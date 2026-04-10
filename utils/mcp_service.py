import re
import traceback
import json

from mcp import types
from pylon.core.tools import log
from sqlalchemy.orm import joinedload
from tools import db, auth, this, openapi_registry

from ..models.all import Application, ApplicationVersion
from ..utils.application_tools import toolkits_listing
from ..utils.application_utils import list_applications_api
from ..utils.toolkits_utils import get_toolkit_schemas
from .mcp_session import SseSession


class McpApiToolExecutor:
    """Handles execution of MCP API tools via direct Flask WSGI calls."""

    @staticmethod
    def execute(api_tool: dict, arguments: dict) -> dict:
        """
        Execute an API tool by calling Flask WSGI app directly.

        Args:
            api_tool: API tool metadata from openapi_registry with path, method, parameters
            arguments: Tool arguments from MCP client

        Returns:
            dict with 'result' or 'error' key
        """
        try:
            method = api_tool.get("method", "get").upper()
            path = api_tool.get("path", "")
            parameters = api_tool.get("parameters", [])

            path_params, query_params, body_params = McpApiToolExecutor._parse_arguments(
                arguments, parameters
            )
            url_path = McpApiToolExecutor._build_url_path(path, path_params)

            log.debug(f"Calling API tool: {method} {url_path} query={query_params} body={body_params}")

            environ = McpApiToolExecutor._build_wsgi_environ(method, url_path, query_params, body_params)

            status_code, body_data = McpApiToolExecutor._execute_wsgi_request(environ)
            log.debug(f"API tool response status: {status_code}")

            return McpApiToolExecutor._parse_response(status_code, body_data)

        except Exception as exc:
            log.error(f"Error calling API tool {api_tool.get('label')}: {exc}")
            log.error(traceback.format_exc())
            return {"error": f"Failed to call API: {str(exc)}"}

    @staticmethod
    def _parse_arguments(arguments: dict, parameters: list) -> tuple[dict, dict, dict]:
        """Separate arguments into path, query, and body parameters."""
        path_params = {}
        query_params = {}
        body_params = {}

        for param in parameters:
            param_name = param.get("name")
            param_in = param.get("in")

            if param_name in arguments:
                if param_in == "path":
                    path_params[param_name] = arguments[param_name]
                elif param_in == "query":
                    query_params[param_name] = arguments[param_name]

        for key, value in arguments.items():
            if key not in path_params and key not in query_params:
                body_params[key] = value

        return path_params, query_params, body_params

    @staticmethod
    def _build_url_path(path_template: str, path_params: dict) -> str:
        """Replace path parameter placeholders with actual values."""
        url_path = path_template
        for param_name, param_value in path_params.items():
            url_path = url_path.replace(f"{{{param_name}}}", str(param_value))
        return url_path

    @staticmethod
    def _build_wsgi_environ(method: str, url_path: str, query_params: dict, body_params: dict) -> dict:
        """
        Build minimal WSGI environ dict for direct Flask request.

        Only includes fields actually needed for internal API calls.
        """
        from io import BytesIO
        from urllib.parse import urlencode
        from flask import g as flask_g, request as flask_request

        environ = {
            'REQUEST_METHOD': method,
            'PATH_INFO': url_path,
            'QUERY_STRING': urlencode(query_params) if query_params else '',
            'wsgi.version': (1, 0),
            'wsgi.url_scheme': 'http',
            'wsgi.input': BytesIO(b''),
            'wsgi.errors': BytesIO(),
            'SERVER_NAME': 'internal',
            'SERVER_PORT': '0',
            'SERVER_PROTOCOL': 'HTTP/1.1',
        }

        McpApiToolExecutor._add_auth_headers(environ, flask_g, flask_request)
        McpApiToolExecutor._add_request_body(environ, method, body_params)

        return environ

    @staticmethod
    def _add_auth_headers(environ: dict, flask_g, flask_request) -> None:
        """
        Add authentication headers to WSGI environ.

        Forwards all available auth mechanisms to ensure internal request
        passes through auth middleware correctly.
        """
        if hasattr(flask_g, 'auth'):
            auth_obj = flask_g.auth
            environ['HTTP_X_AUTH_TYPE'] = getattr(auth_obj, 'type', 'public')
            environ['HTTP_X_AUTH_ID'] = str(getattr(auth_obj, 'id', '-'))
            environ['HTTP_X_AUTH_REFERENCE'] = getattr(auth_obj, 'reference', '-')
            log.debug(f"Forwarding auth: type={environ['HTTP_X_AUTH_TYPE']}, id={environ['HTTP_X_AUTH_ID']}")

        if flask_request and flask_request.headers.get('Authorization'):
            environ['HTTP_AUTHORIZATION'] = flask_request.headers['Authorization']

        if flask_request and flask_request.cookies:
            cookie_header = '; '.join([f"{k}={v}" for k, v in flask_request.cookies.items()])
            environ['HTTP_COOKIE'] = cookie_header
            log.debug(f"Forwarding {len(flask_request.cookies)} cookies for session auth")

    @staticmethod
    def _add_request_body(environ: dict, method: str, body_params: dict) -> None:
        """Add request body to WSGI environ for POST/PUT/PATCH requests."""
        from io import BytesIO

        if method in ("POST", "PUT", "PATCH") and body_params:
            body_json = json.dumps(body_params)
            body_bytes = body_json.encode('utf-8')
            environ['CONTENT_TYPE'] = 'application/json'
            environ['CONTENT_LENGTH'] = str(len(body_bytes))
            environ['wsgi.input'] = BytesIO(body_bytes)
        else:
            environ['CONTENT_LENGTH'] = '0'

    @staticmethod
    def _execute_wsgi_request(environ: dict) -> tuple[int, str]:
        """Execute WSGI request and return status code and body."""
        api_app = this.module.context.app_router.map.get("/api/")
        if not api_app:
            raise RuntimeError("API app not found in context")

        response_status = [None]
        response_body = []

        def start_response(status, _headers, _exc_info=None):
            response_status[0] = status
            return response_body.append

        app_iter = api_app(environ, start_response)
        try:
            for data in app_iter:
                response_body.append(data)
        finally:
            if hasattr(app_iter, 'close'):
                app_iter.close()

        status_code = int(response_status[0].split()[0]) if response_status[0] else 500
        body_data = b''.join(response_body).decode('utf-8')

        return status_code, body_data

    @staticmethod
    def _parse_response(status_code: int, body_data: str) -> dict:
        """Parse API response body and return result or error dict."""
        if 200 <= status_code < 300:
            try:
                result_data = json.loads(body_data) if body_data else {}
            except json.JSONDecodeError:
                result_data = body_data
            return {"result": result_data}
        else:
            try:
                error_data = json.loads(body_data) if body_data else {}
                error_msg = error_data.get("error") if isinstance(error_data, dict) else str(error_data)
            except json.JSONDecodeError:
                error_msg = body_data
            return {"error": error_msg or f"API call failed with status {status_code}"}


class McpService:
    def __init__(self, session: SseSession):
        self.session = session

    def handle_request(self, request: types.ClientRequest):
        match request.root:
            case types.PingRequest():
                self.session.dispatch_empty_response(request.root.id)
            case types.InitializeRequest():
                self.__handle_initialize_request(request.root)
            case types.CallToolRequest():
                self.__handle_call_tool_request(request.root)
            case types.ListToolsRequest():
                self.__handle_list_tools_request(request.root)
            case types.SetLevelRequest():
                self.__handle_logging_set_level_request(request.root)
            case _:
                _response_unsupported(self.session, request.root)

    def handle_notification(self, notification: types.JSONRPCNotification):
        pass

    def __handle_initialize_request(self, request: types.InitializeRequest):
        initialize_result = _build_initialize_result(
            request.id,
            resource_type=self.session.resource_type,
            resource_id=self.session.resource_id
        )
        self.session.dispatch_message(initialize_result.model_dump_json())

    def __handle_call_tool_request(self, request: types.CallToolRequest):
        response_content = ""
        try:
            if version_id := self.__get_ver_id_by_agent_name(request.params.name):
                log.debug("Starting agent call (version = %s)", version_id)
                #
                result: dict = this.module.do_predict(
                    project_id=self.session.project_id,
                    user_id=auth.current_user()["id"],
                    version_id=version_id,
                    payload_in={"user_input": request.params.arguments["task"], "chat_history": []},
                    raw=None,
                    webhook_signature=None
                )
                #
                if "error" not in result or result["error"] is None:
                    chat_history = result.get("chat_history", [])
                    if chat_history and isinstance(chat_history, list) and "content" in chat_history[-1]:
                        response_content = chat_history[-1]["content"]
                    else:
                        response_content = ""
            elif toolkit := self.__get_toolkit_by_name(request.params.name):
                tk_name = _build_agent_identifier(toolkit.get('name', ''))
                tool_name = re.sub(rf"^{re.escape(tk_name)}_+", "", request.params.name)
                tool_params = request.params.arguments
                #
                result = this.module.do_runtool(
                    project_id=self.session.project_id,
                    user_id=auth.current_user()["id"],
                    toolkit_id=toolkit['id'],
                    tool_name=tool_name,
                    tool_params=tool_params,
                    webhook_signature=None
                )
                #
                if "error" not in result:
                    response_content = json.dumps(result["result"])
            elif api_tool := self.__get_api_tool_by_name(request.params.name):
                log.debug("Starting API tool call: %s", request.params.name)
                result = McpApiToolExecutor.execute(api_tool, request.params.arguments)
                if "error" not in result:
                    response_content = json.dumps(result["result"])
            else:
                result = {"error": f"Version matching failure for {request.params.name}"}
        except Exception as exc:
            log.info(f"Error in do_predict: {exc}")
            log.error("Exception stack trace:")
            log.error(traceback.format_exc())
            result = {"error": f"Prediction failure for {request.params.name}"}

        log.info("Tool result: %s", result)
        #
        if "error" in result and result["error"] is not None:
            response_content = result["error"]
        #
        call_tool_result = _jrpc_server_response(request.id, types.CallToolResult(
            content=[types.TextContent(type="text", text=response_content)]
        ))
        #
        self.session.dispatch_message(call_tool_result.model_dump_json())

    def __handle_list_tools_request(self, request: types.ListToolsRequest):
        log.info("Processing request: %s", request.model_dump())
        #
        # Check if this is a resource-scoped request
        if self.session.resource_type and self.session.resource_id:
            tools = self.__get_scoped_tools()
        else:
            tools = self.__get_all_tools()
        #
        list_tools_result = _jrpc_server_response(request.id, types.ListToolsResult(
            tools=tools,
            nextCursor=None,
        ))
        #
        # exclude 'nextCursor' from serialization if it is None
        list_tools_result_json = list_tools_result.model_dump_json(
            exclude={"result": {"nextCursor"}} \
                if list_tools_result.result["nextCursor"] is None else None
        )
        self.session.dispatch_message(list_tools_result_json)

    def __get_scoped_tools(self) -> list[types.Tool]:
        """Get tools for a specific resource (toolkit or application)"""
        tools = []

        if self.session.resource_type == "toolkit":
            tools = self.__get_toolkit_tools(self.session.resource_id)
        elif self.session.resource_type == "application":
            tools = self.__get_application_tools(self.session.resource_id)

        return tools

    def __get_toolkit_tools(self, toolkit_id: int) -> list[types.Tool]:
        """Get tools for a specific toolkit"""
        tools = []
        toolkits = toolkits_listing(project_id=self.session.project_id, query=None, limit=None)["rows"]
        toolkit = next((tk for tk in toolkits if tk.get("id") == toolkit_id), None)

        if not toolkit:
            log.warning(f"Toolkit {toolkit_id} not found")
            return tools

        if not toolkit.get("meta", {}).get("mcp_options", {}).get("available_by_mcp", False):
            log.warning(f"Toolkit {toolkit_id} is not available via MCP")
            return tools

        tk_schemas = get_toolkit_schemas(project_id=self.session.project_id, user_id=auth.current_user()["id"])
        tk_type = toolkit.get("type")
        tk_name = toolkit.get("name")
        tk_description = toolkit.get("description", "")
        selected_tools = toolkit.get('settings', {}).get('selected_tools', [])
        tk_schema = tk_schemas.get(tk_type, {})

        for tool in selected_tools:
            tools.append(
                types.Tool(
                    name=_build_agent_identifier(f"{tk_name}_{tool}"),
                    description=f"Tool '{tool}' from toolkit type '{tk_type}'. Toolkit description: {tk_description}",
                    inputSchema=tk_schema.get('properties', {}).get('selected_tools', {}).get('args_schemas', {}).get(tool, {})
                )
            )

        return tools

    def __get_application_tools(self, version_id: int) -> list[types.Tool]:
        """Get tools for a specific application version"""
        tools = []

        # Get the application by version_id
        with db.with_project_schema_session(self.session.project_id) as session:
            version = session.query(ApplicationVersion).options(
                joinedload(ApplicationVersion.application)
            ).filter(ApplicationVersion.id == version_id).first()

            if not version:
                log.warning(f"Application version {version_id} not found")
                return tools

            app = version.application
            tool_name = _build_agent_identifier(app.name)

            tools.append(
                types.Tool(
                    name=tool_name,
                    description=getattr(app, "description", ""),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "task": {
                                "type": "string",
                                "description": "Task or message for agent"
                            }
                        },
                        "required": ["task"]
                    }
                )
            )

        return tools

    def __get_all_tools(self) -> list[types.Tool]:
        """Get all tools (legacy behavior)"""
        tools = []
        #
        # Expand toolkits to export as mcp server tools
        toolkits = [
            toolkit
            for toolkit in toolkits_listing(project_id=self.session.project_id, query=None, limit=None)["rows"]
            if toolkit.get("meta", {}).get("mcp_options", {}).get("available_by_mcp", False)
        ]
        tk_schemas = get_toolkit_schemas(project_id=self.session.project_id, user_id=auth.current_user()["id"]) if toolkits else {}
        #
        for toolkit in toolkits:
            tk_type = toolkit.get("type")
            tk_name = toolkit.get("name")
            tk_description = toolkit.get("description", "")
            selected_tools = toolkit.get('settings', {}).get('selected_tools', [])
            tk_schema = tk_schemas.get(tk_type, {})
            #
            for tool in selected_tools:
                tools.append(
                    types.Tool(
                        name=_build_agent_identifier(f"{tk_name}_{tool}"),
                        description=f"Tool '{tool}' from toolkit type '{tk_type}'. Toolkit description: {tk_description}",
                        inputSchema=tk_schema.get('properties', {}).get('selected_tools', {}).get('args_schemas',
                                                                                                  {}).get(tool, {})
                    )
                )
        #
        # Expand agents(applications) to export as mcp server tools
        known_tool_names = set()
        #
        if self.session.tags:
            with db.get_session(self.session.project_id) as session:
                some_result = list_applications_api(
                    project_id=self.session.project_id,
                    limit=None,
                    session=session,
                    tags=self.session.tags,
                )
            #
            for app in some_result["applications"]:
                tool_name = _build_agent_identifier(app.name)
                #
                if tool_name in known_tool_names:
                    log.warning(
                        "Skipping agent with colliding name: %s -> %s",
                        app.name, tool_name,
                    )
                    continue
                #
                known_tool_names.add(tool_name)
                #
                tools.append(
                    types.Tool(
                        name=tool_name,
                        description=getattr(app, "description", ""),
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "task": {
                                    "type": "string",
                                    "description": "Task or message for agent"
                                }
                            },
                            "required": ["task"]
                        }
                    )
                )

        api_tools = openapi_registry.get_mcp_api_tools()
        for api_tool in api_tools:
            tool_name = _build_agent_identifier(api_tool.get("value", api_tool.get("label", "")))

            if tool_name in known_tool_names:
                log.warning(
                    "Skipping API tool with colliding name: %s -> %s",
                    api_tool.get("label"), tool_name,
                )
                continue

            known_tool_names.add(tool_name)

            tools.append(
                types.Tool(
                    name=tool_name,
                    description=api_tool.get("description", ""),
                    inputSchema=api_tool.get("args_schema", {})
                )
            )

        return tools

    def __handle_logging_set_level_request(self, request: types.SetLevelRequest):
        set_level_empty_result_json = _jrpc_server_response(request.id, types.EmptyResult()).model_dump_json()
        self.session.dispatch_message(set_level_empty_result_json)

    def __get_ver_id_by_agent_name(self, agent_name: str) -> int | None:
        with db.get_session(self.session.project_id) as session:
            some_result = list_applications_api(
                project_id=self.session.project_id,
                limit=None,
                session=session,
                tags=self.session.tags,
            )

        for app in some_result.get("applications", []):
            if _build_agent_identifier(app.name) == agent_name:
                with db.with_project_schema_session(self.session.project_id) as session_2:
                    application = session_2.query(Application).options(
                        joinedload(Application.versions).joinedload(ApplicationVersion.tags)
                    ).get(app.id)

                    return application.versions[0].id

        return None

    def __get_toolkit_by_name(self, toolkit_name: str) -> dict | None:
        toolkits = toolkits_listing(project_id=self.session.project_id, query=None, limit=None)
        #
        for toolkit in toolkits["rows"]:
            tk_name = toolkit.get("name")
            selected_tools = toolkit.get('settings', {}).get('selected_tools', [])
            #
            for tool in selected_tools:
                if _build_agent_identifier(f"{tk_name}_{tool}") == toolkit_name:
                    return toolkit
        #
        return None

    def __get_api_tool_by_name(self, tool_name: str) -> dict | None:
        """Find an API tool by its MCP tool name."""
        api_tools = openapi_registry.get_mcp_api_tools()
        #
        for api_tool in api_tools:
            api_tool_name = _build_agent_identifier(api_tool.get("value", api_tool.get("label", "")))
            if api_tool_name == tool_name:
                return api_tool
        #
        return None


def _build_initialize_result(id: str, resource_type: str = None, resource_id: int = None) -> types.JSONRPCResponse:
    # Build server name based on scope
    if resource_type and resource_id:
        server_name = f"ELITEA-{resource_type.upper()}-{resource_id}"
        instructions = f"ELITEA {resource_type.title()} (ID: {resource_id})"
    else:
        server_name = "ELITEA MCP SERVER"
        instructions = "ELITEA"

    return _jrpc_server_response(id, types.InitializeResult(
        protocolVersion="2024-11-05",
        capabilities=types.ServerCapabilities(
            logging={},
            resources=types.ResourcesCapability(subscribe=False, listChanged=False),
            tools=types.ToolsCapability(listChanged=True),
            experimental={},
            prompts=types.PromptsCapability(listChanged=False),
        ),
        serverInfo=types.Implementation(name=server_name, version="0.1.0"),
        instructions=instructions,
    ))


def _jrpc_server_response(id: str, inner_server_result) -> types.JSONRPCResponse:
    result_dict = types.ServerResult(inner_server_result).model_dump(by_alias=True)
    if "_meta" in result_dict and result_dict["_meta"] is None:
        # _meta is expeted to be an object but not None/null
        del result_dict["_meta"]
    return types.JSONRPCResponse(jsonrpc="2.0", id=id, result=result_dict)


def _response_unsupported(session: SseSession, request):
    session.dispatch_error_response(request.id, types.METHOD_NOT_FOUND, f"'{request.method}' request is currently not supported.")


def _build_agent_identifier(name):
    return re.sub(r'[^A-Za-z0-9_\-]', "_", name)
