_PLUGIN_NAME = "support_assistant"
_DEFAULT_CONFIG = {'enabled': False, 'project_id': None}


def is_support_assistant_available() -> bool:
    try:
        from tools import context
        return _PLUGIN_NAME in context.module_manager.modules
    except Exception:
        return False


def get_support_config() -> dict:
    if not is_support_assistant_available():
        return dict(_DEFAULT_CONFIG)
    try:
        from tools import rpc_tools
        return rpc_tools.RpcMixin().rpc.timeout(3).support_assistant_get_config()
    except Exception:
        return dict(_DEFAULT_CONFIG)


def ensure_support_enrolled(user_id: int) -> dict:
    if not is_support_assistant_available():
        return {'success': False, 'error': 'Support Assistant not enabled'}
    try:
        from tools import rpc_tools
        return rpc_tools.RpcMixin().rpc.timeout(3).support_assistant_ensure_enrolled(user_id)
    except Exception:
        return {'success': False, 'error': 'Support Assistant not available'}

