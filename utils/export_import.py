from collections import defaultdict
from copy import deepcopy
from io import BytesIO
import json
import re
from typing import List, Dict, Any
import uuid
import zipfile

import yaml

from ..models.all import Application, ApplicationVersion
from ..models.elitea_tools import EliteATool
from sqlalchemy.orm import joinedload, selectinload

from tools import db, rpc_tools, serialize
from pylon.core.tools import log

from ..models.pd.application import (
    ApplicationExportModel,
)
from ..models.pd.export_import import ApplicationForkModel
from ..models.pd.tool import ToolExportDetails, ToolForkDetails


def _export_compound_application_tools(
    res,
    project_id: int,
    user_id: int,
    forked: bool = False,
    data_done: dict = None,
    follow_version_ids: set = None
):
    tool_ids = defaultdict(set)
    toolkit_ref_app_version_ids = set()
    # Track applications already exported but needing additional versions
    extra_version_ids = set()
    for tool in res.get('toolkits', []):
        tool_type_name = tool['type']
        if tool_type_name in ('application',):
            ref_field_name = f'{tool_type_name}_id'
            id_ = tool['settings'][ref_field_name]
            if id_ not in data_done[ref_field_name]:
                tool_ids[tool_type_name].add(id_)
                # Always track child agent version IDs when version filtering is active
                # This ensures we only export the specific version referenced by the parent
                if follow_version_ids is not None and tool_type_name == 'application':
                    child_version_id = tool['settings'].get('application_version_id', None)
                    if child_version_id is not None:
                        toolkit_ref_app_version_ids.add(child_version_id)
            elif follow_version_ids is not None and tool_type_name == 'application':
                # Application already exported at a higher level, but check if this
                # specific version was included. This handles the case where the same
                # agent is used at different hierarchy levels with different versions.
                child_version_id = tool['settings'].get('application_version_id', None)
                if child_version_id is not None and child_version_id not in data_done.get('application_version_id', {}):
                    extra_version_ids.add(child_version_id)
                    tool_ids[tool_type_name].add(id_)

    # recursive export of all found applications-tools
    if tool_ids['application']:
        # If parent has version filtering, pass the collected child version IDs
        # If parent exports all versions (None), child agents also export all versions
        child_follow_version_ids = (toolkit_ref_app_version_ids | extra_version_ids) if follow_version_ids is not None else None
        tools_result = _export_application_main(
                           project_id=project_id,
                           user_id=user_id,
                           application_ids=tool_ids['application'],
                           forked=forked,
                           data_done=data_done,
                           follow_version_ids=child_follow_version_ids
                       )

        if not tools_result['ok']:
            return tools_result

        # Merge results: for applications that were already in res (extra versions case),
        # append the new versions to the existing entry instead of adding a duplicate
        if extra_version_ids:
            existing_apps = {app.get('import_uuid'): app for app in res.get('applications', [])}
            new_apps = []
            for app in tools_result.get('applications', []):
                existing = existing_apps.get(app.get('import_uuid'))
                if existing:
                    existing['versions'].extend(app['versions'])
                else:
                    new_apps.append(app)
            if new_apps:
                res.setdefault('applications', []).extend(new_apps)
            if toolkits := tools_result.get('toolkits'):
                res.setdefault('toolkits', []).extend(toolkits)
        else:
            for entity in ('applications', 'toolkits'):
                if ent_res := tools_result.get(entity):
                    res.setdefault(entity, []).extend(ent_res)

    return res


def _export_application_main(project_id: int, user_id: int, application_ids, forked: bool, data_done: dict, follow_version_ids: set = None):
    with db.get_session(project_id) as session:
        applications = session.query(
            Application
        ).filter(
            Application.id.in_(application_ids),
        ).options(
            selectinload(Application.versions).selectinload(ApplicationVersion.tools),
            selectinload(Application.versions).selectinload(ApplicationVersion.tool_mappings),
            selectinload(Application.versions).selectinload(ApplicationVersion.variables)
        ).all()

        application_db_ids = {app.id for app in applications}
        for application_id in application_ids:
            if application_id not in application_db_ids:
                if data_done:
                    log.error(f'Toolkit error: no application found by {project_id=} {application_id=}')
                else:
                    return {
                        'ok': False,
                        'msg': f'No application found: {project_id=} {application_id=}'
                    }

        result_model = ApplicationExportModel if not forked else ApplicationForkModel

        applications_serialized = []
        for app in applications:
            app_dict = app.to_json()
            for app_version in app.versions:
                # If follow_version_ids is set (not None), filter to only those versions
                # None means export all versions, empty set means export none (error case)
                if follow_version_ids is not None and app_version.id not in follow_version_ids:
                    continue
                app_version_dict = app_version.to_dict()
                app_dict.setdefault('versions', []).append(app_version_dict)
            if not app_dict.get('versions'):
                return {
                    'ok': False,
                    'msg': f'Application {app.id} has no versions to export'
                }
            app_dict['project_id'] = project_id
            app_dict['user_id'] = user_id
            applications_serialized.append(app_dict)
        applications = deepcopy(applications_serialized)

        result_pd = (result_model.model_validate(app) for app in applications)
        result = [r.model_dump(mode='json') for r in result_pd]

        toolkits = []
        for app in applications_serialized:
            for version in app['versions']:
                for tool in version.get('tools', []):
                    tool['project_id'] = project_id
                    tool['user_id'] = user_id
                    result_model = ToolExportDetails if not forked else ToolForkDetails
                    details = result_model.model_validate(tool)
                    if forked:
                        details.owner_id = project_id
                    details.fix_name(project_id)
                    toolkit_dict = details.model_dump()
                    # Extract selected_tools to top-level before filtering
                    # (selected_tools comes from EntityToolMapping via apply_selected_tools_intersection)
                    if 'settings' in toolkit_dict and toolkit_dict['settings'].get('selected_tools'):
                        toolkit_dict['selected_tools'] = toolkit_dict['settings']['selected_tools']
                    # Sanitize and filter settings for export
                    if 'settings' in toolkit_dict:
                        toolkit_dict['settings'] = _filter_internal_keys(
                            _sanitize_mcp_headers(
                                _sanitize_pgvector_configuration(toolkit_dict['settings'])
                            )
                        )
                    toolkits.append(toolkit_dict)

    for app in result:
        data_done['application_id'][app['id']] = app['import_uuid']
        for version in app['versions']:
            if forked:
                version_id = version.get('id')
            else:
                version_id = version.pop('id')
            data_done['application_version_id'][version_id] = version['import_version_uuid']

    try:
        res = _export_compound_application_tools(
            res={
                'ok': True,
                'applications': result,
                'toolkits': toolkits
            },
            project_id=project_id,
            user_id=user_id,
            forked=forked,
            data_done=data_done,
            follow_version_ids=follow_version_ids
        )
    except Exception as ex:
        res = {'ok': False, 'msg': f"Tool error. {ex}"}

    return serialize(res)


def _toolkits_deduplicate_by_import_uuid(toolkits):
    res = []
    import_uuids = set()
    for toolkit in toolkits:
        tuid = toolkit['import_uuid']
        if tuid not in import_uuids:
            res.append(toolkit)
            import_uuids.add(tuid)

    return res


def export_application(project_id: int, user_id: int, application_ids: List[int] = None, forked: bool = False, follow_version_ids: list = None):
    # maps id/version ids to import uuid/import version uuids of already processed entities
    data_done = defaultdict(dict)
    if follow_version_ids is not None:
        follow_version_ids = set(follow_version_ids)
    else:
        follow_version_ids = None
    result = _export_application_main(project_id, user_id, application_ids, forked, data_done, follow_version_ids)

    if not result.get('ok'):
        return result

    for app in result['applications']:
        if app['id'] in application_ids:
            app['original_exported'] = True
        else:
            app['original_exported'] = False

    return _post_export(
        project_id-project_id,
        result=result,
        data_done=data_done,
        forked=forked
    )


def _post_export(project_id: int, result: dict, data_done: dict, forked: bool = False):
    for entities_type in ('applications', 'toolkits'):
        for ent in result.get(entities_type, []):
            if 'original_exported' not in ent:
                ent['original_exported'] = False

    # substitute all ids in toolkits with refs by import_uuid/import_version_uuid
    result['toolkits'] = _toolkits_deduplicate_by_import_uuid(result['toolkits'])
    for tool in result['toolkits']:
        tool_type = tool['type']
        tool_name = tool['name']
        if tool_type in ('application',):
            entity_id = f'{tool_type}_id'
            entity_version_id = f'{tool_type}_version_id'
            try:
                tool['settings']['import_uuid'] = data_done[entity_id][tool['settings'].pop(entity_id)]
                if entity_version_id in tool['settings']:
                    tool['settings']['import_version_uuid'] = data_done[entity_version_id][tool['settings'].pop(entity_version_id)]
            except KeyError:
                return {
                    'ok': False,
                    'msg': (
                        f'Tool error: {tool_name=} '
                        f'references to invalid/missing {tool_type} or version'
                    )
                }

    result['_metadata'] = {'version': 2}

    return result


def _export_toolkits_main(project_id: int, user_id: int, toolkit_ids: List[int], forked: bool,  data_done: dict):
    with db.get_session(project_id) as session:
        toolkits = session.query(
            EliteATool
        ).filter(
            EliteATool.id.in_(toolkit_ids),
        ).all()

        toolkit_db_ids = {app.id for app in toolkits}
        for toolkit_id in toolkit_ids:
            if toolkit_id not in toolkit_db_ids:
                if data_done:
                    log.error(f'Tool error: no toolkit found by {project_id=} {toolkit_id=}')
                else:
                    return {
                        'ok': False,
                        'msg': f'No toolkit found: {project_id=} {toolkit_id=}'
                    }

        result_model = ToolExportDetails if not forked else ToolForkDetails
        toolkits_serialized = []
        for toolkit in toolkits:
            td = toolkit.to_json()
            td['project_id'] = project_id
            td['user_id'] = user_id
            toolkits_serialized.append(td)
        result_pd = [result_model.model_validate(toolkit) for toolkit in toolkits_serialized]
        for r in result_pd:
            if forked:
                r.owner_id = project_id
            r.fix_name(project_id)
        result = []
        for r in result_pd:
            toolkit_dict = r.model_dump(mode='json')
            # Extract selected_tools to top-level before filtering
            if 'settings' in toolkit_dict and toolkit_dict['settings'].get('selected_tools'):
                toolkit_dict['selected_tools'] = toolkit_dict['settings']['selected_tools']
            # Sanitize and filter settings for export
            if 'settings' in toolkit_dict:
                toolkit_dict['settings'] = _filter_internal_keys(
                    _sanitize_mcp_headers(
                        _sanitize_pgvector_configuration(toolkit_dict['settings'])
                    )
                )
            result.append(toolkit_dict)

    try:
        res = _export_compound_application_tools(
            res={
                'ok': True,
                'toolkits': result
            },
            project_id=project_id,
            user_id=user_id,
            forked=forked,
            data_done=data_done
        )
    except Exception as ex:
        res = {'ok': False, 'msg': f"Tool error. {ex}"}

    return res


def export_toolkits(project_id: int, user_id: int, toolkit_ids: List[int] = None, forked: bool = False):
    data_done = defaultdict(dict)
    result = _export_toolkits_main(project_id, user_id, toolkit_ids, forked, data_done)

    if not result.get('ok'):
        return result

    for toolkit in result['toolkits']:
        if toolkit['id'] in toolkit_ids:
            toolkit['original_exported'] = True
        else:
            toolkit['original_exported'] = False

    return _post_export(
        project_id-project_id,
        result=result,
        data_done=data_done,
        forked=forked
    )



def generate_repeatable_uuid(prefix: str, values: dict, suffix: str):
    hash_ = hash((prefix, "".join(sorted(str(values))), suffix))
    return str(uuid.UUID(int=abs(hash_)))


# ============================================================================
# MD Format Export/Import Functions
# ============================================================================

def _slugify(text: str) -> str:
    """Convert text to a safe filename slug."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text[:50]  # Limit length


# Fields to exclude from settings (internal/confusing for users)
# configuration_uuid and configuration_project_id are internal refs to source project's config
# selected_tools is extracted to top-level 'tools' field for cleaner MD format
SETTINGS_EXCLUDE_KEYS = {'configuration_uuid', 'configuration_project_id', 'import_uuid', 'available_tools', 'selected_tools'}


def _sanitize_pgvector_configuration(settings: dict) -> dict:
    """
    Sanitize pgvector_configuration in settings for export.

    pgvector_configuration is project-level and every project has its own.
    When exporting:
    - private should always be False to allow import into any project
    - connection_string must be removed as it contains database credentials
    - Only keep non-sensitive metadata (elitea_title, configuration_type)
    """
    if not isinstance(settings, dict):
        return settings

    result = dict(settings)
    if 'pgvector_configuration' in result and isinstance(result['pgvector_configuration'], dict):
        # Copy and sanitize - only keep safe fields
        original = result['pgvector_configuration']
        result['pgvector_configuration'] = {
            'private': False,  # Always False for export portability
            'elitea_title': original.get('elitea_title', ''),
            'configuration_type': original.get('configuration_type', 'pgvector'),
        }
        # Remove connection_string and any other sensitive fields

    return result


def _sanitize_mcp_headers(settings: dict) -> dict:
    """
    Sanitize MCP headers in settings for export to prevent credential leakage.
    
    Removes or masks sensitive header values that could contain authentication
    tokens, API keys, cookies, or other credentials.
    
    Args:
        settings: The settings dictionary to sanitize
        
    Returns:
        Sanitized settings with sensitive headers masked or removed
    """
    if not isinstance(settings, dict):
        return settings
        
    # Sensitive header patterns (case-insensitive)
    SENSITIVE_HEADER_PATTERNS = {
        'authorization',    # Authorization: Bearer token
        'cookie',           # Cookie: session data
        'x-api-key',        # API key headers
        'api-key',
        'apikey',
        'x-auth-token',     # Auth token headers
        'auth-token',
        'x-access-token',
        'access-token',
    }
    
    # Headers containing these substrings are considered sensitive
    SENSITIVE_SUBSTRINGS = {
        'token',
        'key', 
        'secret',
        'password',
        'auth',
        'credential',
    }
    
    result = dict(settings)
    
    # Check if this is an MCP toolkit with headers
    if 'headers' in result and isinstance(result['headers'], dict):
        sanitized_headers = {}
        
        for header_name, header_value in result['headers'].items():
            if not header_name or not isinstance(header_name, str):
                continue
                
            header_name_lower = header_name.lower()
            is_sensitive = False
            
            # Check exact matches
            if header_name_lower in SENSITIVE_HEADER_PATTERNS:
                is_sensitive = True
            # Check substring matches
            elif any(substring in header_name_lower for substring in SENSITIVE_SUBSTRINGS):
                is_sensitive = True
                
            if is_sensitive:
                # Mask the value instead of removing entirely to preserve structure
                sanitized_headers[header_name] = "***MASKED***"
                log.debug(f"Masked sensitive MCP header during export: {header_name}")
            else:
                sanitized_headers[header_name] = header_value
                
        result['headers'] = sanitized_headers
        
    return result


def _filter_internal_keys(obj: Any, exclude_keys: set = None) -> Any:
    """
    Recursively filter out internal keys from a dictionary or list.

    Args:
        obj: The object to filter (dict, list, or primitive)
        exclude_keys: Set of keys to exclude at any nesting level

    Returns:
        Filtered object with internal keys removed
    """
    if exclude_keys is None:
        exclude_keys = SETTINGS_EXCLUDE_KEYS

    if isinstance(obj, dict):
        return {
            k: _filter_internal_keys(v, exclude_keys)
            for k, v in obj.items()
            if k not in exclude_keys
        }
    elif isinstance(obj, list):
        return [_filter_internal_keys(item, exclude_keys) for item in obj]
    else:
        return obj


def _extract_toolkits_for_md(tools: list, toolkits: list) -> list:
    """
    Extract toolkit configurations for MD export.

    Note: In JSON export, version.tools only contains {import_uuid: ...} references.
    Full toolkit details are in the separate 'toolkits' array.
    We need to look up full details by import_uuid.

    Output format uses cleaner field names:
    - 'toolkit' instead of 'toolkit_name'
    - 'tools' instead of 'selected_tools'
    """
    toolkit_configs = []

    # Build map of import_uuid -> full toolkit details
    toolkit_map = {tk.get('import_uuid'): tk for tk in toolkits if tk.get('import_uuid')}

    for tool in tools:
        # Get import_uuid from the tool reference
        import_uuid = tool.get('import_uuid')

        if not import_uuid:
            continue

        # Look up full toolkit details
        full_toolkit = toolkit_map.get(import_uuid, {})
        tool_type = full_toolkit.get('type')

        # Skip application-type tools (they're exported as separate files)
        if tool_type == 'application':
            continue

        config = {
            'toolkit': full_toolkit.get('name', ''),
            'type': tool_type,
        }

        # Include meta if present (preserves MCP flag and other metadata)
        meta = full_toolkit.get('meta')
        if meta:
            config['meta'] = meta

        # Include settings if present, filtering out internal fields and sanitizing pgvector/MCP
        settings = full_toolkit.get('settings')
        if settings:
            sanitized_settings = _sanitize_mcp_headers(_sanitize_pgvector_configuration(settings))
            filtered_settings = _filter_internal_keys(sanitized_settings)
            if filtered_settings:
                config['settings'] = filtered_settings

        # Include tools (selected_tools) - check both top-level and inside settings
        selected_tools = full_toolkit.get('selected_tools') or (settings.get('selected_tools') if settings else None)
        if selected_tools:
            config['tools'] = selected_tools

        toolkit_configs.append(config)

    return toolkit_configs


def _extract_toolkits_for_md_pipeline(tools: list, toolkits: list) -> list:
    """
    Extract toolkit configurations for pipeline MD export.

    Unlike agents, pipelines need to include application-type tools (nested agents)
    as references so they can be linked during import.

    Output format:
    - Regular toolkits: full config with 'toolkit', 'type', 'settings', 'tools'
    - Application toolkits: reference with 'toolkit', 'type': 'application', 'import_uuid'
    """
    toolkit_configs = []

    # Build map of import_uuid -> full toolkit details
    toolkit_map = {tk.get('import_uuid'): tk for tk in toolkits if tk.get('import_uuid')}

    for tool in tools:
        # Get import_uuid from the tool reference
        import_uuid = tool.get('import_uuid')

        if not import_uuid:
            continue

        # Look up full toolkit details
        full_toolkit = toolkit_map.get(import_uuid, {})
        tool_type = full_toolkit.get('type')

        # Skip application-type tools (they're already in nested_agents section)
        if tool_type == 'application':
            continue

        config = {
            'toolkit': full_toolkit.get('name', ''),
            'type': tool_type,
        }

        # Include meta if present (preserves MCP flag and other metadata)
        meta = full_toolkit.get('meta')
        if meta:
            config['meta'] = meta

        # Include settings if present, filtering out internal fields and sanitizing pgvector/MCP
        settings = full_toolkit.get('settings')
        if settings:
            sanitized_settings = _sanitize_mcp_headers(_sanitize_pgvector_configuration(settings))
            filtered_settings = _filter_internal_keys(sanitized_settings)
            if filtered_settings:
                config['settings'] = filtered_settings

        # Include tools (selected_tools) - check both top-level and inside settings
        selected_tools = full_toolkit.get('selected_tools') or (settings.get('selected_tools') if settings else None)
        if selected_tools:
            config['tools'] = selected_tools

        toolkit_configs.append(config)

    return toolkit_configs


def _extract_nested_agents_pipelines(tools: list, toolkits: list, instructions: str = None,
                                      applications: list = None, version_map: dict = None) -> tuple:
    """
    Extract nested agents and pipelines from application-type tools and pipeline nodes.

    Args:
        tools: List of tool references from version
        toolkits: List of full toolkit configurations
        instructions: Pipeline instructions YAML string (optional, for pipeline nodes)
        applications: List of exported applications (optional, for name lookup)
        version_map: Dict mapping import_version_uuid -> version_name (for version-aware refs)

    Returns:
        Tuple of (nested_agents, nested_pipelines) lists
    """
    nested_agents = []
    nested_pipelines = []
    seen_refs = set()  # Track (name, version) tuples to avoid duplicates

    # Build map of import_uuid -> full toolkit details
    toolkit_map = {tk.get('import_uuid'): tk for tk in toolkits if tk.get('import_uuid')}

    # Build map of application_id -> name from exported applications
    app_id_to_name = {}
    if applications:
        for app in applications:
            app_id = app.get('id')
            app_name = app.get('name')
            if app_id and app_name:
                app_id_to_name[app_id] = app_name

    # Extract from tools array (existing behavior)
    for tool in tools:
        import_uuid = tool.get('import_uuid')
        if not import_uuid:
            continue

        full_toolkit = toolkit_map.get(import_uuid, {})
        tool_type = full_toolkit.get('type')

        # Only process application-type tools (nested agents/pipelines)
        if tool_type != 'application':
            continue

        toolkit_name = full_toolkit.get('name', '')
        if not toolkit_name:
            continue

        # Get version from toolkit settings -> import_version_uuid -> version_name lookup
        import_version_uuid = full_toolkit.get('settings', {}).get('import_version_uuid')
        version_name = version_map.get(import_version_uuid) if version_map and import_version_uuid else None

        # Use (name, version) as unique key to allow same agent with different versions
        ref_key = (toolkit_name, version_name)
        if ref_key not in seen_refs:
            seen_refs.add(ref_key)
            # Create reference entry with toolkit name and version
            entry = {'name': toolkit_name}
            if version_name and version_name != 'base':
                entry['version'] = version_name
            nested_agents.append(entry)

    # Extract from pipeline instructions YAML nodes (for subgraph/pipeline type nodes)
    if instructions:
        try:
            instructions_yaml = yaml.safe_load(instructions)
            if isinstance(instructions_yaml, dict):
                nodes = instructions_yaml.get('nodes', [])
                for node in nodes:
                    node_type = node.get('type', '')
                    # Check for pipeline/subgraph/agent node types that reference other applications
                    if node_type in ('pipeline', 'subgraph', 'agent'):
                        # The 'tool' field contains the name of the referenced agent/pipeline
                        tool_name = node.get('tool', '')
                        # Check if this agent name already exists with any version
                        # (tools array entries have version info, instruction nodes don't)
                        already_seen = tool_name and any(name == tool_name for name, _ in seen_refs)
                        if tool_name and not already_seen:
                            ref_key = (tool_name, None)
                            seen_refs.add(ref_key)
                            entry = {'name': tool_name}
                            nested_agents.append(entry)
        except (yaml.YAMLError, AttributeError, TypeError):
            # If instructions aren't valid YAML, skip node extraction
            pass

    return nested_agents, nested_pipelines


def _build_version_map(applications: list) -> dict:
    """
    Build a map of import_version_uuid -> version_name from exported applications.

    Args:
        applications: List of exported application dicts

    Returns:
        Dict mapping import_version_uuid to version name
    """
    version_map = {}
    for app in applications:
        for ver in app.get('versions', []):
            ivu = ver.get('import_version_uuid')
            vname = ver.get('name')
            if ivu and vname:
                version_map[ivu] = vname
    return version_map


def _application_to_md(app: dict, toolkits: list, applications: list = None, version: dict = None) -> str:
    """
    Convert application dictionary to Markdown format.

    Args:
        app: Application data with versions
        toolkits: List of toolkit configurations
        applications: List of all exported applications (for version_map building)
        version: Specific version to export (if None, exports first version)

    Returns:
        Markdown string with YAML frontmatter
    """
    if not app.get('versions'):
        raise ValueError(f"Application {app.get('name')} has no versions")

    # Use provided version or fall back to first version
    if version is None:
        version = app['versions'][0]

    version_name = version.get('name', 'base')
    raw_agent_type = version.get('agent_type', 'react')
    agent_type = 'agent' if raw_agent_type == 'openai' else raw_agent_type

    # Build frontmatter structure
    frontmatter = {
        'name': app.get('name', ''),
        'description': app.get('description', ''),
    }

    # Include version name if not 'base'
    if version_name and version_name != 'base':
        frontmatter['version'] = version_name

    # LLM Settings
    llm_settings = version.get('llm_settings', {})
    if llm_settings.get('model_name'):
        frontmatter['model'] = llm_settings['model_name']
    if llm_settings.get('temperature') is not None:
        frontmatter['temperature'] = llm_settings['temperature']
    if llm_settings.get('max_tokens'):
        frontmatter['max_tokens'] = llm_settings['max_tokens']
    if llm_settings.get('top_p') is not None:
        frontmatter['top_p'] = llm_settings['top_p']

    # Agent configuration
    frontmatter['agent_type'] = agent_type

    meta = version.get('meta', {})
    if meta.get('step_limit'):
        frontmatter['step_limit'] = meta['step_limit']
    # Export internal tools (pyodide, data_analysis, planner, swarm, etc.)
    if meta.get('internal_tools'):
        frontmatter['internal_tools'] = meta['internal_tools']

    # Add welcome message and conversation starters to markdown export
    welcome_message = version.get('welcome_message', '')
    if welcome_message:
        frontmatter['welcome_message'] = welcome_message

    conversation_starters = version.get('conversation_starters', [])
    if conversation_starters and isinstance(conversation_starters, list) and len(conversation_starters) > 0:
        frontmatter['conversation_starters'] = conversation_starters

    # Build version_map for nested agent version lookups
    version_map = _build_version_map(applications) if applications else {}

    # Extract nested agents/pipelines from tools array AND pipeline instructions nodes
    tools = version.get('tools', [])
    # For pipelines, also extract from instructions YAML nodes (subgraph/pipeline type nodes)
    instructions_raw = version.get('instructions', '') if agent_type == 'pipeline' else None
    nested_agents, nested_pipelines = _extract_nested_agents_pipelines(
        tools, toolkits, instructions_raw, applications, version_map
    )
    if nested_agents:
        frontmatter['nested_agents'] = nested_agents
    if nested_pipelines:
        frontmatter['nested_pipelines'] = nested_pipelines

    # Handle pipelines vs agents
    if agent_type == 'pipeline':
        # For pipelines, parse instructions YAML and move to frontmatter
        instructions_raw = version.get('instructions', '')
        try:
            if instructions_raw:
                instructions_yaml = yaml.safe_load(instructions_raw)
                if isinstance(instructions_yaml, dict):
                    # Filter internal keys from all pipeline fields
                    if instructions_yaml.get('state'):
                        frontmatter['state'] = _filter_internal_keys(instructions_yaml['state'])
                    if instructions_yaml.get('entry_point'):
                        frontmatter['entry_point'] = instructions_yaml['entry_point']
                    # Export interrupt configurations
                    if instructions_yaml.get('interrupt_after'):
                        frontmatter['interrupt_after'] = instructions_yaml['interrupt_after']
                    if instructions_yaml.get('interrupt_before'):
                        frontmatter['interrupt_before'] = instructions_yaml['interrupt_before']
                    if instructions_yaml.get('nodes'):
                        frontmatter['nodes'] = _filter_internal_keys(instructions_yaml['nodes'])
        except yaml.YAMLError:
            # If instructions aren't valid YAML, include as-is in body
            pass

        # Extract toolkits with full configuration (same as agents)
        tools = version.get('tools', [])
        if tools:
            toolkit_list = _extract_toolkits_for_md_pipeline(tools, toolkits)
            if toolkit_list:
                frontmatter['toolkits'] = toolkit_list

        # Export pipeline_settings (visual flow graph: node positions, edges, orientation)
        pipeline_settings = version.get('pipeline_settings')
        if pipeline_settings:
            frontmatter['pipeline_settings'] = pipeline_settings

        # Pipeline has no body - all config is in frontmatter
        body = ''
    else:
        # For agents, instructions become the body
        body = version.get('instructions', '')

        # Extract toolkits with full configuration
        tools = version.get('tools', [])
        if tools:
            toolkit_list = _extract_toolkits_for_md(tools, toolkits)
            if toolkit_list:
                frontmatter['toolkits'] = toolkit_list

    # Add variables if present
    variables = version.get('variables', [])
    if variables:
        frontmatter['variables'] = variables

    # Build final MD content
    yaml_str = yaml.dump(
        frontmatter,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120
    )

    return f"---\n{yaml_str}---\n\n{body}"


def create_zip_archive(files: List[Dict[str, str]]) -> BytesIO:
    """
    Create a ZIP archive from a list of files.

    Args:
        files: List of {'filename': str, 'content': str} dicts

    Returns:
        BytesIO buffer containing the ZIP archive
    """
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_info in files:
            zf.writestr(file_info['filename'], file_info['content'])
    zip_buffer.seek(0)
    return zip_buffer


def export_application_md(
    project_id: int,
    user_id: int,
    application_ids: List[int],
    follow_version_ids: list = None
) -> Dict[str, Any]:
    """
    Export applications as Markdown format.

    Args:
        project_id: Project ID
        user_id: User ID performing the export
        application_ids: List of application IDs to export
        follow_version_ids: Optional list of specific version IDs to export

    Returns:
        Dict with:
        - ok: bool
        - files: List of {filename: str, content: str}
        - has_dependencies: bool (True if ZIP needed)
    """
    # Use existing JSON export logic to get structured data
    json_export = export_application(
        project_id=project_id,
        user_id=user_id,
        application_ids=application_ids,
        forked=False,
        follow_version_ids=follow_version_ids
    )

    if not json_export.get('ok', True):
        return json_export

    md_files = []
    original_app_ids = set(application_ids)
    has_dependencies = False
    all_applications = json_export.get('applications', [])
    all_toolkits = json_export.get('toolkits', [])

    # Convert each application version to MD (one file per version)
    for app in all_applications:
        app_name = app.get('name', '')
        is_original = app.get('id') in original_app_ids or app.get('original_exported', False)
        if not is_original:
            has_dependencies = True

        # Export each version as a separate file
        for version in app.get('versions', []):
            try:
                md_content = _application_to_md(
                    app,
                    all_toolkits,
                    applications=all_applications,
                    version=version
                )

                version_name = version.get('name', 'base')
                agent_type = 'pipeline' if version.get('agent_type') == 'pipeline' else 'agent'

                # Filename includes version for non-base versions
                if version_name == 'base':
                    filename = f"{_slugify(app_name)}.{agent_type}.md"
                else:
                    filename = f"{_slugify(app_name)}.{_slugify(version_name)}.{agent_type}.md"

                md_files.append({
                    'filename': filename,
                    'content': md_content,
                    'is_original': is_original
                })
            except Exception as e:
                log.error(f"Failed to convert application {app_name} version {version.get('name')} to MD: {e}")
                return {
                    'ok': False,
                    'msg': f"Failed to export {app_name} version {version.get('name')}: {str(e)}"
                }

    return {
        'ok': True,
        'files': md_files,
        'has_dependencies': has_dependencies
    }


