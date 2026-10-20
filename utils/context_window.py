""" Context window modes (Balanced / Full) and the per-request context budget (#6898) """

import math
import threading
import time
from typing import Optional

BALANCED = 'balanced'
FULL = 'full'
WINDOW_MODES = (BALANCED, FULL)

BALANCED_WINDOW_TOKENS = 272_000
SAFETY_MARGIN_RATIO = 0.01
AUTO_OUTPUT_RESERVE = 1_024
COMPACTION_RATIO = 0.9
DEFAULT_MODEL_WINDOW = 128_000
DEFAULT_MODEL_MAX_OUTPUT = 16_000
MIN_USABLE_INPUT = 2_000

SETTINGS_TYPE = 'context_window_settings'
ENVIRONMENT_SETTINGS_TYPE = 'environment_settings'
MANUAL_TOKENS_FLAG = 'manual_max_context_tokens_enabled'
MANUAL_TOKENS_KEYS = ('manual_max_context_tokens', 'manual_set_by_admin', 'manual_set_by')
MIN_MANUAL_CONTEXT_TOKENS = 1_000
_RPC_TIMEOUT = 3
_CACHE_TTL_SECONDS = 60
_cache_lock = threading.Lock()
_model_cache: dict = {}


def normalize_mode(mode) -> str:
    """Unknown or missing modes (legacy chats with only max_context_tokens) are Balanced."""
    return mode if mode in WINDOW_MODES else BALANCED


def _positive_int(value) -> Optional[int]:
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def resolve_context_budget(
        mode: Optional[str],
        model_window: Optional[int],
        model_max_output: Optional[int] = None,
        llm_max_tokens: Optional[int] = None,
) -> dict:
    """Every budget number the UI shows and the SDK trigger uses, for one request's model."""
    mode = normalize_mode(mode)
    window = _positive_int(model_window)
    window_source = 'model' if window else 'default'
    window = window or DEFAULT_MODEL_WINDOW

    effective_window = min(BALANCED_WINDOW_TOKENS, window) if mode == BALANCED else window
    safety_margin = math.ceil(effective_window * SAFETY_MARGIN_RATIO)
    available_window = effective_window - safety_margin

    custom_max = _positive_int(llm_max_tokens)
    if custom_max:
        max_output = _positive_int(model_max_output) or DEFAULT_MODEL_MAX_OUTPUT
        reserved_output, reserved_output_source = min(custom_max, max_output), 'custom'
    else:
        reserved_output, reserved_output_source = AUTO_OUTPUT_RESERVE, 'auto'

    usable_input = available_window - reserved_output
    if usable_input < MIN_USABLE_INPUT:
        from pylon.core.tools import log  # pylint: disable=E0611,E0401,C0415
        log.warning(
            'Context window %s leaves %s usable input tokens, clamped to %s',
            effective_window, usable_input, MIN_USABLE_INPUT,
        )
        usable_input = MIN_USABLE_INPUT

    return {
        'window_mode': mode,
        'model_window': window,
        'window_source': window_source,
        'effective_window': effective_window,
        'safety_margin': safety_margin,
        'available_window': available_window,
        'reserved_output': reserved_output,
        'reserved_output_source': reserved_output_source,
        'usable_input': usable_input,
        'compaction_threshold': math.floor(usable_input * COMPACTION_RATIO),
    }


def get_project_default_mode(project_id: int) -> str:
    """Admin-set project default for new chats; Balanced when unset or unreadable."""
    from pylon.core.tools import log  # pylint: disable=E0611,E0401,C0415
    from tools import rpc_tools  # pylint: disable=E0401,C0415
    try:
        config = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).configurations_get_first_filtered_project(
            project_id=project_id, filter_fields={'type': SETTINGS_TYPE, 'elitea_title': SETTINGS_TYPE},
        )
    except Exception as e:  # pylint: disable=W0718
        log.warning('Failed to read context window settings for project %s: %s', project_id, e)
        return BALANCED
    return normalize_mode(((config or {}).get('data') or {}).get('default_mode'))


def get_model_limits(project_id: Optional[int], model_name: Optional[str]) -> dict:
    """{context_window, max_output_tokens} of a configured model, or {} when unknown (TTL-cached)."""
    if not project_id or not model_name:
        return {}
    key = (int(project_id), model_name)
    now = time.monotonic()
    with _cache_lock:
        cached = _model_cache.get(key)
        if cached and now - cached[0] < _CACHE_TTL_SECONDS:
            return cached[1]
    from pylon.core.tools import log  # pylint: disable=E0611,E0401,C0415
    from tools import rpc_tools  # pylint: disable=E0401,C0415
    try:
        config = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).configurations_get_configuration_model(
            project_id, model_name,
        ) or {}
    except Exception as e:  # pylint: disable=W0718
        log.warning('Failed to read model limits for %s in project %s: %s', model_name, project_id, e)
        return {}
    limits = {k: config.get(k) for k in ('context_window', 'max_output_tokens') if config.get(k)}
    with _cache_lock:
        _model_cache[key] = (now, limits)
    return limits


def budget_for_model_config(mode: Optional[str], model_config: Optional[dict], llm_max_tokens=None) -> dict:
    model_config = model_config or {}
    return resolve_context_budget(
        mode, model_config.get('context_window'), model_config.get('max_output_tokens'), llm_max_tokens,
    )


def manual_context_tokens_allowed() -> bool:
    """Platform toggle (Administration > environment_settings) for manual summarization triggers."""
    from pylon.core.tools import log  # pylint: disable=E0611,E0401,C0415
    from tools import rpc_tools  # pylint: disable=E0401,C0415
    from .utils import get_public_project_id
    try:
        config = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).configurations_get_first_filtered_project(
            project_id=get_public_project_id(),
            filter_fields={'type': ENVIRONMENT_SETTINGS_TYPE, 'elitea_title': ENVIRONMENT_SETTINGS_TYPE},
        )
    except Exception as e:  # pylint: disable=W0718
        log.warning('Failed to read environment settings: %s', e)
        return False
    return ((config or {}).get('data') or {}).get(MANUAL_TOKENS_FLAG) is True


def is_platform_admin(user_id) -> bool:
    if not user_id:
        return False
    from tools import rpc_tools  # pylint: disable=E0401,C0415
    try:
        roles = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).auth_get_user_roles(user_id, 'administration') or []
    except Exception:  # pylint: disable=W0718
        return False
    return bool({'admin', 'super_admin'} & set(roles))


def manual_context_tokens(strategy: Optional[dict]) -> Optional[int]:
    """Manual trigger stored by a platform admin; the platform toggle is checked by callers."""
    strategy = strategy or {}
    if strategy.get('manual_set_by_admin') is not True:
        return None
    value = _positive_int(strategy.get('manual_max_context_tokens'))
    return value if value and value >= MIN_MANUAL_CONTEXT_TOKENS else None
