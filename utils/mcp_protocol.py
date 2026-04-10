import uuid
import json

import flask
from mcp import types
from pydantic import ValidationError
from pylon.core.tools import log

from .mcp_session import SseSession
from ..utils.mcp_service import McpService


def validate_session_id(request: flask.Request) -> tuple[uuid.UUID | None, tuple[dict, int] | None]:
    client_session_id = request.args.get('session_id')
    if not client_session_id:
        return None, ({'error': 'Missing session_id'}, 400)

    try:
        client_session_id = uuid.UUID(client_session_id)
    except ValueError:
        return None, ({'error': f"Invalid session_id format for '{client_session_id}'"}, 400)

    return client_session_id, None


def handle_post_request(request, session: SseSession) -> tuple[dict, int]:
    if isinstance(request, flask.Request):
        log.info(f"Received request: {request.data.decode('utf-8')}")
        log.info(f"With args: {request.args}")
    else:
        log.info(f"Received request: {request}")

    jrpc_message, error_response = _validate_json_rpc_message(request)
    if error_response:
        return error_response

    return _process_jrpc_message(jrpc_message, session)


def get_request_method(request):
    try:
        jrpc_message_raw = request.data.decode("utf-8")
        parsed_json = json.loads(jrpc_message_raw)
        return parsed_json.get("method", None)
    except:
        return None


def _validate_json_rpc_message(request) -> tuple[
    types.JSONRPCMessage | None, tuple[dict, int] | None]:
    try:
        if isinstance(request, flask.Request):
            request_data_raw = request.data
        else:
            request_data_raw = request
        #
        jrpc_message_raw = request_data_raw.decode('utf-8')
        jrpc_message = types.JSONRPCMessage.model_validate_json(jrpc_message_raw)
        return jrpc_message, None
    except ValidationError as exc:
        return None, ({'error': 'Invalid JSON-RPC message format', 'details': str(exc)}, 400)


def _process_jrpc_message(jrpc_message: types.JSONRPCMessage, session: SseSession) -> tuple[dict, int]:
    try:
        service = McpService(session)

        match jrpc_message.root:
            case types.JSONRPCRequest():
                try:
                    client_request = types.ClientRequest.model_validate_json(jrpc_message.model_dump_json())
                    service.handle_request(client_request)
                    #
                    if session.jrpc_response is not None:
                        return session.jrpc_response, 200
                    #
                    return {}, 200
                except ValidationError as exc:
                    log.info(f"Validation error: {exc}")
                    return {'error': 'Invalid request format', 'details': str(exc)}, 400
            case types.JSONRPCNotification():
                service.handle_notification(jrpc_message.root)
                return {}, 200
            case types.JSONRPCResponse():
                log.info("Handling JSON-RPC Response")
                return {}, 200
            case types.JSONRPCError():
                log.info("Handling JSON-RPC Error")
                return {}, 200
            case _:
                return {'error': 'Unsupported JSON-RPC message type'}, 400
    except Exception as exc:
        log.debug(f"Error processing request: {jrpc_message}")
        log.exception("Exception occurred")
        return {'error': 'Internal server error', 'details': str(exc)}, 500
