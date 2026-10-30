"""Risk review of a schedule's target version (EL-7011, US-17; owner CR 2026-10-21).

`scan_schedule` sorts the findings in three groups. `blocking` stops a save and a run. `sensitive`
needs a confirmation that names the `fingerprint`: a sensitive tool, or a toolkit that runs with the
owner's personal credential. `notices` never stop anything.
"""
import hashlib
import json

from pylon.core.tools import log
from tools import db

from ..models.all import ApplicationVersion
from .application_tools import ConfigurationExpandError, expand_toolkit_settings
from .pipeline_utils import validate_yaml_from_str

CREDENTIAL_ERRORS = {'private_credential_not_found', 'credential_not_found', 'credential_type_mismatch'}
HUMAN_NODE_TYPES = {'hitl', 'printer'}
BLOCKING_CODES = {'personal_signin_required', 'human_input_step', 'missing_credential'}
# The owner can fix these while the run waits, so "Pause and ask me" saves and starts with them
WAIT_FOR_OWNER_CODES = {'personal_signin_required', 'missing_credential'}
WAIT_HINT = 'Sign in or add the credentials before the run; otherwise the run waits for you.'
SENSITIVE_CODE = 'sensitive_action'
PERSONAL_CODE = 'personal_credential'
SENSITIVE_CODES = {SENSITIVE_CODE, PERSONAL_CODE}
TYPE_LABELS = {'github': 'GitHub', 'gitlab': 'GitLab', 'sharepoint': 'SharePoint', 'openapi': 'OpenAPI'}
INCOMPLETE_CODE = 'scan_incomplete'


def _warning(code, message, **extra):
    return {'code': code, 'message': message, **extra}


def _human_steps(version) -> list:
    if version.agent_type == 'pipeline':
        nodes = validate_yaml_from_str(version.instructions).get('nodes') or []
        return [
            _warning('human_input_step', f'Node "{node.get("id")}" waits for a person',
                     step=node.get('id'), step_label=node.get('name') or node.get('id'))
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


def _uses_private_credential(tool) -> bool:
    """True when a credential reference of the toolkit is the owner's personal (private) one."""
    return any(isinstance(value, dict) and 'elitea_title' in value and value.get('private') is True
               for value in (tool.settings or {}).values())


def _personal_credential(tool, name) -> dict:
    """The owner decision 2026-10-21: a personal credential is "warn and confirm", one item per toolkit."""
    label = TYPE_LABELS.get(tool.type, tool.type.replace('_', ' ').title())
    return _warning(PERSONAL_CODE, f'Toolkit "{name}" runs with the owner\'s personal {label} credential',
                    toolkit=name, toolkit_type=tool.type, tool=None, risk_level='needs_approval',
                    badge=PERSONAL_CODE, effect=f"Runs with the owner's personal {label} credential.")


def _sensitive_warnings(tool, name) -> list:
    from .toolkit_security import (  # late: the policy reads the plugin descriptor
        is_tool_sensitive,
        listed_sensitive_tools,
        sensitive_tool_info,
    )
    # An empty selection exposes every tool of the toolkit at run time. The core has no tool list of the
    # toolkit here, so the listed tools of its type stand in: a listed tool is never hidden by an empty list.
    selected = (tool.settings or {}).get('selected_tools') or listed_sensitive_tools(tool.type)
    selected = list(dict.fromkeys(selected))
    return [
        _warning(SENSITIVE_CODE, f'Tool "{tool_name}" of toolkit "{name}" needs approval',
                 toolkit=name, toolkit_type=tool.type, tool=tool_name, risk_level='needs_approval',
                 **sensitive_tool_info(tool.type, tool_name))
        for tool_name in selected if is_tool_sensitive(tool.type, tool_name)
    ]


def _toolkit_warnings(tool, project_id, owner_id) -> list:
    name = tool.name or tool.type
    try:
        expanded = expand_toolkit_settings(tool.type, tool.settings or {}, project_id, owner_id)
    except ConfigurationExpandError as ex:
        if _error_types(ex.errors) & CREDENTIAL_ERRORS:
            return [_warning('missing_credential', f'Toolkit "{name}" has no usable credential for the owner',
                             toolkit=name, toolkit_type=tool.type)]
        raise
    warnings = []
    if _uses_personal_token(tool, expanded):  # an interactive sign-in blocks; no personal item beside it
        warnings.append(_warning('personal_signin_required',
                                 f'Toolkit "{name}" needs a personal sign-in; a scheduled run cannot sign in',
                                 toolkit=name, toolkit_type=tool.type, reason='needs a personal sign-in'))
    elif _uses_private_credential(tool):
        warnings.append(_personal_credential(tool, name))
    return warnings + _sensitive_warnings(tool, name)


def _content_digest(version) -> str:
    """sha256 of what a run executes: the instructions, the variables and each toolkit's raw settings.

    The settings are read before the secret expansion, so a changed credential reference or endpoint
    changes the digest and asks the owner to confirm again.
    """
    content = {
        'instructions': version.instructions,
        'variables': sorted((json.dumps([variable.name, variable.value], sort_keys=True, default=str)
                            for variable in version.variables)),
        'toolkits': sorted(json.dumps([tool.name, tool.type, tool.settings or {}], sort_keys=True, default=str)
                           for tool in version.tools),
    }
    return hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).hexdigest()


def _findings(project_id: int, version_id: int, owner_id: int) -> tuple[list, dict | None, str | None]:
    """Every finding of one version run by one owner, the fix link of its entity and the digest of its content.

    Never raises.
    """
    try:
        with db.get_session(project_id) as session:
            version = session.get(ApplicationVersion, version_id) if version_id else None
            if version is None:
                return [], None, None
            fix = {'entity_type': 'pipeline' if version.agent_type == 'pipeline' else 'agent',
                   'id': version.application_id, 'version_id': version_id}
            warnings = _human_steps(version)
            for tool in version.tools:
                try:
                    warnings += _toolkit_warnings(tool, project_id, owner_id)
                except Exception:  # noqa: BLE001 - one toolkit fails, the other warnings stay
                    log.exception('Schedule risk scan failed for toolkit %s of version %s', tool.name, version_id)
                    warnings.append(_warning(INCOMPLETE_CODE, f'Toolkit "{tool.name or tool.type}" could not be checked for risks',
                                             toolkit=tool.name or tool.type))
            digest = _content_digest(version)
        return warnings, fix, digest
    except Exception:  # noqa: BLE001 - any failure is a notice; the caller decides what it allows
        log.exception('Schedule risk scan failed for version %s', version_id)
        return [_warning(INCOMPLETE_CODE, 'The version could not be fully checked for risks')], None, None


def _fingerprint(version_id: int | None, sensitive: list, digest: str | None) -> str:
    """sha256 of the version id, the sorted (toolkit, tool) pairs and the content digest.

    A personal credential is (toolkit, "*").
    """
    pairs = sorted([item['toolkit'], item['tool'] or '*'] for item in sensitive)
    return hashlib.sha256(json.dumps([version_id, pairs, digest]).encode()).hexdigest()


def scan_schedule(project_id: int, version_id: int | None, owner_id: int, approval_rule: str | None = None) -> dict:
    """The risk review of one version run by one owner: `blocking`, `sensitive`, `notices`, `fingerprint`.

    A blocking finding carries `fix`, the agent or pipeline version to open. With the rule `pause_and_ask`,
    a missing credential or a personal sign-in is a notice with `fix` and a hint, not a block. `sensitive` holds the
    `sensitive_action` tools and the `personal_credential` toolkits (`tool` null). Never raises: a
    failure is a `scan_incomplete` notice.
    """
    findings, fix, digest = _findings(project_id, version_id, owner_id)
    waits = WAIT_FOR_OWNER_CODES if approval_rule == 'pause_and_ask' else set()
    blocking = [{**item, 'fix': fix} for item in findings if item['code'] in BLOCKING_CODES - waits]
    sensitive = [item for item in findings if item['code'] in SENSITIVE_CODES]
    notices = [{**item, 'fix': fix, 'hint': WAIT_HINT} if item['code'] in waits else item
               for item in findings if item['code'] not in BLOCKING_CODES - waits | SENSITIVE_CODES]
    return {'blocking': blocking, 'sensitive': sensitive, 'notices': notices,
            'fingerprint': _fingerprint(version_id, sensitive, digest)}


def scan_complete(review: dict) -> bool:
    """False when a tool could not be checked: then "Run without asking" is refused."""
    return not any(item['code'] == INCOMPLETE_CODE for item in review['notices'])
