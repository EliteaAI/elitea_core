"""Save-time risk scan of a schedule's target version (EL-7011, US-17)."""
import json

from pylon.core.tools import log
from tools import db

from ..models.all import ApplicationVersion
from .application_tools import ConfigurationExpandError, expand_toolkit_settings
from .pipeline_utils import validate_yaml_from_str

CREDENTIAL_ERRORS = {'private_credential_not_found', 'credential_not_found', 'credential_type_mismatch'}
HUMAN_NODE_TYPES = {'hitl', 'printer'}


def _warning(code, message, **extra):
    return {'code': code, 'message': message, **extra}


def _human_steps(version) -> list:
    if version.agent_type == 'pipeline':
        nodes = validate_yaml_from_str(version.instructions).get('nodes') or []
        return [
            _warning('human_input_step', f'Node "{node.get("id")}" waits for a person', step=node.get('id'))
            for node in nodes if isinstance(node, dict) and node.get('type') in HUMAN_NODE_TYPES
        ]
    if 'ask_user' in ((version.meta or {}).get('internal_tools') or []):
        return [_warning('human_input_step', 'The agent can ask the user a question')]
    return []


def _error_types(errors) -> set:
    types = set()
    for error in errors:
        try:
            types.add(json.loads(error['msg']).get('error_type'))
        except (TypeError, ValueError, AttributeError):
            continue
    return types


def _uses_personal_token(tool, expanded: dict) -> bool:
    # Same rule as the UI hook useDelegatedOauthToolkits: a referenced credential is authoritative
    if tool.type == 'mcp' or (tool.meta or {}).get('mcp') or tool.type.startswith('mcp_'):
        return True
    if tool.type not in ('sharepoint', 'openapi'):
        return False
    if (tool.settings or {}).get(f'{tool.type}_configuration'):
        return bool((expanded.get(f'{tool.type}_configuration') or {}).get('oauth_discovery_endpoint'))
    return tool.type == 'openapi' and bool(expanded.get('oauth_discovery_endpoint'))


def _toolkit_warnings(tool, project_id, owner_id) -> list:
    name = tool.name or tool.type
    try:
        expanded = expand_toolkit_settings(tool.type, tool.settings or {}, project_id, owner_id)
    except ConfigurationExpandError as ex:
        if _error_types(ex.errors) & CREDENTIAL_ERRORS:
            return [_warning('missing_credential', f'Toolkit "{name}" has no usable credential for the owner', toolkit=name)]
        raise
    if _uses_personal_token(tool, expanded):
        return [_warning('token_may_expire', f'Toolkit "{name}" signs in with a personal token', toolkit=name)]
    return []


def scan_schedule_risks(project_id: int, version_id: int, owner_id: int) -> list:
    """Warnings for one version run by one owner. Never raises: a failure is one scan_incomplete warning."""
    try:
        with db.get_session(project_id) as session:
            version = session.get(ApplicationVersion, version_id)
            warnings = _human_steps(version)
            for tool in version.tools:
                warnings += _toolkit_warnings(tool, project_id, owner_id)
        return warnings
    except Exception:  # noqa: BLE001 - any failure hides the scan, the save still succeeds
        log.exception('Schedule risk scan failed for version %s', version_id)
        return [_warning('scan_incomplete', 'The version could not be fully checked for risks')]
