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
LEGACY_TRIGGER_KEY = 'legacy_trigger'
# Set only through the context_strategy PUT, which validates them
SERVER_OWNED_STRATEGY_KEYS = MANUAL_TOKENS_KEYS + ('context_window_mode', 'max_context_tokens', LEGACY_TRIGGER_KEY)
_RPC_TIMEOUT = 3
_CACHE_TTL_SECONDS = 60
_cache_lock = threading.Lock()
_model_cache: dict = {}
_value_cache: dict = {}


def _cached(key, load, fallback, ttl=_CACHE_TTL_SECONDS):
    """TTL-cached ``load()``; a failing load returns ``fallback`` and is retried on the next call."""
    now = time.monotonic()
    with _cache_lock:
        cached = _value_cache.get(key)
        if cached and now - cached[0] < ttl:
            return cached[1]
    try:
        value = load()
    except Exception as e:  # pylint: disable=W0718
        from pylon.core.tools import log  # pylint: disable=E0611,E0401,C0415
        log.warning('Failed to load %s: %s', key, e)
        return fallback
    with _cache_lock:
        _value_cache[key] = (now, value)
    return value


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
    """Admin-set project default for new chats; Balanced when unset or unreadable (TTL-cached)."""
    def load():
        from tools import rpc_tools  # pylint: disable=E0401,C0415
        config = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).configurations_get_first_filtered_project(
            project_id=project_id, filter_fields={'type': SETTINGS_TYPE, 'elitea_title': SETTINGS_TYPE},
        )
        return normalize_mode(((config or {}).get('data') or {}).get('default_mode'))
    return _cached(('default_mode', project_id), load, BALANCED)


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
    """Platform toggle (Administration > environment_settings) for manual summarization triggers (TTL-cached)."""
    def load():
        from tools import rpc_tools  # pylint: disable=E0401,C0415
        from .utils import get_public_project_id  # pylint: disable=C0415
        config = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).configurations_get_first_filtered_project(
            project_id=get_public_project_id(),
            filter_fields={'type': ENVIRONMENT_SETTINGS_TYPE, 'elitea_title': ENVIRONMENT_SETTINGS_TYPE},
        )
        return ((config or {}).get('data') or {}).get(MANUAL_TOKENS_FLAG) is True
    return _cached(('manual_tokens_flag',), load, False)


def is_platform_admin(user_id) -> bool:
    """Platform admin role check (TTL-cached per user)."""
    if not user_id:
        return False

    def load():
        from tools import rpc_tools  # pylint: disable=E0401,C0415
        roles = rpc_tools.RpcMixin().rpc.timeout(_RPC_TIMEOUT).auth_get_user_roles(user_id, 'administration') or []
        return bool({'admin', 'super_admin'} & set(roles))
    return _cached(('platform_admin', user_id), load, False)


def manual_context_tokens(strategy: Optional[dict]) -> Optional[int]:
    """Manual trigger stored by a platform admin; the platform toggle is checked by callers."""
    strategy = strategy or {}
    if strategy.get('manual_set_by_admin') is not True:
        return None
    value = _positive_int(strategy.get('manual_max_context_tokens'))
    return value if value and value >= MIN_MANUAL_CONTEXT_TOKENS else None


def manual_change_denied(fields_set, new_value, current_strategy: Optional[dict], user_id,
                         allowed=manual_context_tokens_allowed, is_admin=is_platform_admin) -> bool:
    """A context_strategy PUT that changes the manual trigger needs the platform toggle and a platform admin.

    Resending the stored value is not a change, so the role and toggle are only checked on a real change.
    """
    if 'manual_max_context_tokens' not in fields_set:
        return False
    if new_value == (current_strategy or {}).get('manual_max_context_tokens'):
        return False
    return not allowed() or not is_admin(user_id)


def keep_server_owned_strategy(incoming_strategy, stored_strategy: Optional[dict]):
    """Client-supplied context_strategy with server-owned keys replaced by their stored values (dropped when unset)."""
    if not isinstance(incoming_strategy, dict):
        return incoming_strategy
    stored_strategy = stored_strategy or {}
    for key in SERVER_OWNED_STRATEGY_KEYS:
        incoming_strategy.pop(key, None)
        if key in stored_strategy:
            incoming_strategy[key] = stored_strategy[key]
    return incoming_strategy


def legacy_context_tokens(strategy: Optional[dict]) -> Optional[int]:
    """Deprecated PUT ``max_context_tokens`` (AC22).

    Honored only when that PUT marked it; pre-release chats carrying the old 64000 default follow Balanced.
    """
    strategy = strategy or {}
    if strategy.get(LEGACY_TRIGGER_KEY) is not True:
        return None
    return _positive_int(strategy.get('max_context_tokens'))


def strategy_for_predict(strategy: Optional[dict]) -> Optional[dict]:
    """Stored context_strategy as predict sends it: a marked legacy trigger keeps the legacy path
    (no window mode), anything else runs in its window mode (unknown or missing is Balanced)."""
    if not strategy:
        return strategy
    strategy = dict(strategy)
    if legacy_context_tokens(strategy):
        strategy.pop('context_window_mode', None)
    else:
        strategy['context_window_mode'] = normalize_mode(strategy.get('context_window_mode'))
        strategy.pop('max_context_tokens', None)
    strategy.pop(LEGACY_TRIGGER_KEY, None)
    return strategy


def status_max_tokens(budget: dict, manual: Optional[int] = None, legacy: Optional[int] = None) -> int:
    """Context status ``max_tokens``: a legacy or manual trigger, capped by the usable input, else the window."""
    trigger = manual or legacy
    if trigger:
        return min(trigger, budget.get('usable_input') or budget['available_window'])
    return budget['available_window']


def pick_chat_llm_settings(participants) -> dict:
    """llm_settings of the chat model: the user participant's, else the first participant with a model.

    ``participants`` is an iterable of (entity_name, entity_settings).
    """
    fallback = {}
    for entity_name, entity_settings in participants:
        candidate = (entity_settings or {}).get('llm_settings') or {}
        if not candidate.get('model_name'):
            continue
        if str(entity_name) == 'user':
            return candidate
        fallback = fallback or candidate
    return fallback
