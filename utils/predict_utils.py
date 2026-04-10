import flask
import json
from datetime import datetime
from pylon.core.tools import log
from tools import VaultClient, db, serialize, this
from tools import auth, rpc_tools, VaultClient, serialize, context
from typing import Optional, Union

from .llm_settings import get_default_max_tokens
from ..models.elitea_tools import EliteATool
from ..models.pd.chat import ApplicationChatRequest, LLMChatRequest
from ..models.pd.tool import ToolDetails
from ..utils.application_tools import expand_toolkit_settings


class PredictPayloadError(Exception):
    """Custom exception for errors in generating predict payload."""
    pass


def get_system_user_token(project_id: int, name: str = 'api', create_if_not_exists: bool = True) -> Optional[str]:
    system_user = rpc_tools.RpcMixin().rpc.timeout(
        2
    ).admin_get_project_system_user(project_id)
    token_list = auth.list_tokens(system_user['id'])
    for i in token_list:
        if i['name'] == name:
            return auth.encode_token(i['id'])
    if create_if_not_exists:
        token_id = auth.add_token(system_user['id'], 'api')
        return auth.encode_token(token_id)
    return


def get_user_token(user_id: int) -> Optional[str]:
    token_list = auth.list_tokens(user_id)
    if user_id is None:
        return
    for i in token_list:
        expires = i.get('expires')
        if not expires or expires > datetime.now():
            return auth.encode_token(i['id'])


def get_system_token(project_id: int) -> Optional[str]:
    """
    Used for the cases you don't need toolkit, configuration expanding, etc.
    Only system access to simple predict LLM models.
    Example: summarization LLM call (no tools, no configurations)
    :param project_id
    :return: system user token
    """
    system_token: str = get_system_user_token(
        project_id=project_id,
        name='api',
        create_if_not_exists=True
    )
    return system_token


def get_predict_token_and_session(project_id: int, user_id: int, sid: str = None) -> tuple[Optional[str], Optional[str]]:
    ''' Returns user token OR project system user token + user auth session '''
    auth_session = None
    token: str = get_user_token(user_id)
    if not token:
        sid = '' if sid is None else sid
        _context = auth.sio_users.get(sid)
        if _context and _context.type == 'user' and _context.user.get('id') == user_id:
            auth_session = _context.reference
        else:
            raise PredictPayloadError("User token not found. Please create user_token")
        token: str = get_system_user_token(
            project_id=project_id,
            name='api',
            create_if_not_exists=True
        )
    return token, auth_session


def get_predict_base_url(project_id: int) -> str:
    from tools import this, constants as c  # pylint: disable=E0401,C0415

    base_url = this.descriptor.config.get("base_url", c.APP_HOST)
    if base_url in ('http://localhost', 'http://127.0.0.1'):
        base_url = 'http://pylon_main:8080'

    return base_url


def load_context_settings_from_conversation(project_id: int, conversation_id: str) -> Optional[dict]:
    """
    Load context_settings from conversation.meta.context_strategy.

    Args:
        project_id: Project ID for database session
        conversation_id: Conversation UUID

    Returns:
        context_strategy dict or None if not found
    """
    if not conversation_id:
        return None

    try:
        from ..models.conversation import Conversation
        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.uuid == str(conversation_id)
            ).first()

            if conversation and conversation.meta:
                return conversation.meta.get('context_strategy')
    except Exception as e:
        log.debug(f"Could not load context_settings from conversation {conversation_id}: {e}")

    return None


def generate_predict_payload(
    parsed: Union[LLMChatRequest, ApplicationChatRequest],
    user_id: int, sid: str = None, is_system_user: bool = False,
    eligible_for_autoapproval: bool = False
) -> dict:
    """
    :param parsed: payload
    :param user_id: User ID
    :param sid: Socket.IO session id
    :param is_system_user: WARN if True, you can't do tool or configuration expand (used in summarization)
    :return:
    """
    vault_client = VaultClient(parsed.project_id)

    if is_system_user:
        token = get_system_token(parsed.project_id)
        auth_session = None
    else:
        token, auth_session = get_predict_token_and_session(parsed.project_id, user_id, sid)

    base_url = get_predict_base_url(parsed.project_id)

    # Get model configuration
    llm_project_id = getattr(parsed.llm_settings, 'model_project_id', None) or parsed.project_id
    llm_model_configuration = rpc_tools.RpcMixin().rpc.call.configurations_get_configuration_model(
        llm_project_id, parsed.llm_settings.model_name
    )

    # Build model parameters, excluding incompatible ones based on model capabilities
    supports_reasoning = llm_model_configuration.get('supports_reasoning', False)
    model_parameters = {}

    for param in ["max_tokens", "temperature", "reasoning_effort", "model_project_id"]:
        param_value = getattr(parsed.llm_settings, param, None)
        if param_value is None:
            continue
        # Skip temperature for reasoning models, skip reasoning_effort for non-reasoning models
        if (param == "temperature" and supports_reasoning) or (param == "reasoning_effort" and not supports_reasoning):
            continue

        if param == "max_tokens" and param_value == -1:
            param_value = get_default_max_tokens(supports_reasoning)

        model_parameters[param] = param_value

    chat_history = [i.dict() for i in parsed.chat_history]
    user_input = parsed.user_input or 'continue'

    supports_vision = llm_model_configuration.get('supports_vision', True)

    # try:
    #     from tools import worker_client  # pylint: disable=E0401,C0415
    #     chat_history = worker_client.limit_tokens(
    #         data=chat_history,
    #         token_limit=None, # todo: problem with token_limit, need to cut chat history on litellm side
    #         max_tokens=parsed.llm_settings.max_tokens,
    #     )
    #
    # except:
    #     from pylon.core.tools import log
    #     log.exception('limit_tokens')

    all_secrets = vault_client.get_all_secrets()

    #
    payload = {
        "llm": {
            "kwargs": {
                "base_url": base_url,
                "model": parsed.llm_settings.model_name,
                "api_key": token,
                "project_id": parsed.project_id,
                **model_parameters,
                #
                "stream": True,  # hardcoded
                "api_extra_headers": {
                    'X-SECRET': all_secrets.get('secrets_header_value', 'secret'),
                    'X-USERSESSION': auth_session if auth_session else '-',
                }
            }
        },
        "chat_history": chat_history,
        "user_input": user_input,
        "thread_id": parsed.thread_id,
        "checkpoint_id": parsed.checkpoint_id,
        'debug': False,
        'tools': parsed.tools,
        'application': {
            'instructions': parsed.instructions,
        },
        'internal_tools': parsed.internal_tools or [],
        'steps_limit': parsed.steps_limit if isinstance(parsed, LLMChatRequest) else None,
        'mcp_tokens': parsed.mcp_tokens or {},
        'ignored_mcp_servers': parsed.ignored_mcp_servers or [],
        'should_continue': parsed.should_continue,
        'hitl_resume': bool(getattr(parsed, 'hitl_resume', False)),
        'hitl_action': getattr(parsed, 'hitl_action', None),
        'hitl_value': getattr(parsed, 'hitl_value', None),
        'is_regenerate': getattr(parsed, 'is_regenerate', False),
        'meta': parsed.meta,
        'conversation_id': getattr(parsed, 'conversation_id', None) or parsed.stream_id,  # For planning toolkit scoping (fallback to stream_id)
        'persona': getattr(parsed, 'persona', 'generic'),  # Default persona for chat
        'context_settings': parsed.context_settings.dict() if parsed.context_settings else {},
        'supports_vision': supports_vision,
    }

    # Auto-approve sensitive actions for API requests when project secret is set.
    # Only honor the explicit server-side argument, never a client-provided model field.
    if eligible_for_autoapproval:
        try:
            secret_value = str(all_secrets.get('sensitive_tools_autoapproval_api', '')).strip().lower()
            if secret_value in ('true', '1', 'yes'):
                payload['auto_approve_sensitive_actions'] = True
                log.debug('[SENSITIVE] Auto-approve sensitive actions enabled for API request (project secret sensitive_tools_autoapproval_api)')
        except Exception as e:
            log.warning(f'[SENSITIVE] Failed to check sensitive_tools_autoapproval_api secret: {e}')

    if isinstance(parsed, ApplicationChatRequest):
        configurations = []
        # if user_id:
        #     configurations = this.context.rpc_manager.timeout(2).configurations_get_filtered_personal(
        #         user_id=user_id,
        #         include_shared=True
        #     )
        # TODO: configurations not needed anymore, remove soon
        payload['llm']['kwargs']['configurations'] = configurations
        #
        payload_variables = None
        if parsed.variables:
            payload_variables = {v.name: v.dict() for v in parsed.variables}
        #
        payload['application'] = {
            "id": parsed.application_id,
            "version_id": parsed.version_id,
            "variables": payload_variables,
            "version_details": parsed.version_details,
        }
        # Merge version_details tools (including nested agents) with request-level tools
        if parsed.version_details and parsed.version_details.get('tools'):
            version_tools = parsed.version_details.get('tools', [])
            request_tools = payload.get('tools') or []
            payload['tools'] = version_tools + request_tools
        if parsed.version_details:
            llm_settings = payload['application']['version_details']['llm_settings']
            llm_settings['model_name'] = parsed.llm_settings.model_name
            llm_settings['model_project_id'] =  parsed.llm_settings.model_project_id
            llm_settings['max_tokens'] =  parsed.llm_settings.max_tokens
            llm_settings['temperature'] =  parsed.llm_settings.temperature
            llm_settings['reasoning_effort'] =  parsed.llm_settings.reasoning_effort

            if supports_reasoning:
                llm_settings['temperature'] = None
            if llm_settings['max_tokens'] == -1:
                llm_settings['max_tokens'] = get_default_max_tokens(supports_reasoning)

    return serialize(payload)


def get_toolkit_config(project_id: int, user_id: int, toolkit_id: int):
    # load toolkit config
    with db.with_project_schema_session(project_id) as session:
        toolkit_config = session.query(EliteATool).filter(
            EliteATool.id == toolkit_id
        ).first()
        if not toolkit_config:
            return {'error': f'No such toolkit with id {toolkit_id}'}
        toolkit_config = ToolDetails.from_orm(toolkit_config)
        toolkit_config.fix_name(project_id)
        toolkit_config.set_online(project_id)
        toolkit_config.set_agent_meta_and_fields(project_id)
        toolkit_config = toolkit_config.model_dump(mode='json')
    # expand toolkit settings
    toolkit_settings_expanded = expand_toolkit_settings(
        toolkit_config["type"], toolkit_config.get('settings', {}), project_id=project_id, user_id=user_id
    )
    toolkit_config['settings'] = toolkit_settings_expanded
    #
    return toolkit_config


def generate_test_tool_payload(project_id: int, user_id: int, toolkit_id: int, tool_name: str, tool_params: dict, sid: str = None) -> dict:
    vault_client = VaultClient(project_id)
    token, auth_session = get_predict_token_and_session(project_id, user_id, sid)
    base_url = get_predict_base_url(project_id)
    llm_settings = this.for_module('configurations').module.get_default_model(project_id)
    #
    # use default model settings
    llm_settings["max_tokens"] = 1024
    llm_settings["temperature"] = 0.7
    #
    payload = {
        "llm": {
            "kwargs": {
                "model": llm_settings.get("model_name"),#llm_settings.model_name,
                **llm_settings,
                #
                "stream": True,  # hardcoded
                "api_extra_headers": {
                    'X-SECRET': vault_client.get_all_secrets().get('secrets_header_value', 'secret'),
                    'X-USERSESSION': auth_session if auth_session else '-',
                }
            }
        },
        "deployment_url": base_url,
        "project_auth_token": token,
        "project_id": project_id,
        "toolkit_config": get_toolkit_config(project_id, user_id, toolkit_id),
        "tool_name": tool_name,
        "tool_params": tool_params,
    }
    #
    return serialize(payload)