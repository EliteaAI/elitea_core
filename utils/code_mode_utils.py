"""#7580: experimental Code Mode gate; config read live on each call so admin changes apply without a restart."""

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from tools import this

CODE_MODE_TOOL = 'code_mode'

CODE_MODE_LIMITATIONS = [
    {'key': 'swarm', 'text': 'Swarm mode: Code Mode is ignored when the agent runs as a swarm with child agents.'},
    {'key': 'sensitive', 'text': 'Tools that need human approval are not called from a program; '
                                 'the agent calls them directly.'},
    {'key': 'indexer', 'text': 'Indexer tools (index_data, search_index, ...) are not available inside programs.'},
    {'key': 'pipelines', 'text': 'Pipelines: Code Mode applies to agents only.'},
]


def _gate_config() -> dict:
    return this.descriptor.config.get('code_mode_experimental', {}) or {}


def get_code_mode_enabled() -> bool:
    return bool(_gate_config().get('enabled', False))


def get_code_mode_whitelist() -> set:
    raw = _gate_config().get('whitelist_project_ids', []) or []
    return set(
        int(x) for x in raw
        if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit())
    )


def get_code_mode_allowed_models() -> list:
    raw = _gate_config().get('allowed_models', []) or []
    return [str(x).strip() for x in raw if str(x).strip()]


def is_code_mode_allowed(project_id) -> bool:
    if not get_code_mode_enabled():
        return False
    whitelist = get_code_mode_whitelist()
    if not whitelist:
        return True
    try:
        project_id = int(project_id)
    except (TypeError, ValueError):
        log.warning("Non-numeric project_id %r for Code Mode gate", project_id)
        return False
    return project_id in whitelist


def is_code_mode_model_allowed(model_name) -> bool:
    # Empty list = no model restriction
    allowed = get_code_mode_allowed_models()
    return not allowed or (model_name or '') in allowed


def strip_code_mode(internal_tools, project_id, model_name):
    if not internal_tools or CODE_MODE_TOOL not in internal_tools:
        return internal_tools
    if is_code_mode_allowed(project_id) and is_code_mode_model_allowed(model_name):
        return internal_tools
    return [t for t in internal_tools if t != CODE_MODE_TOOL]


def strip_code_mode_from_version(version_details: dict, project_id) -> None:
    meta = version_details.get('meta') or {}
    tools = meta.get('internal_tools')
    model_name = (version_details.get('llm_settings') or {}).get('model_name')
    stripped = strip_code_mode(tools, project_id, model_name)
    if stripped is not tools:
        meta['internal_tools'] = stripped
