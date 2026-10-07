import json
from typing import Optional

from pylon.core.tools import log
from tools import this

from .folder_access import APPLICATION_ENTITY_TYPES, resolve_entities_access

VERSION_DETAILS_PERMISSION = 'models.applications.version.details'
DEFAULT_MAX_NODES = 100
DEFAULT_MAX_BYTES = 2 * 1024 * 1024


def prefetch_key(application_id, version_id) -> str:
    return f"{application_id}:{version_id}"


def expand_version_for_sdk(project_id: int, application_id: int, version_id: int, user_id: int) -> dict:
    """Version details exactly as the SDK receives them from PATCH version; {'error': ...} on failure."""
    # Same-plugin call: go through the module, an RPC would be a pylon_main -> pylon_main round trip
    version_details = this.module.get_application_version_details_expanded(
        project_id=project_id,
        application_id=application_id,
        version_id=version_id,
        user_id=user_id,
    )
    if 'error' in version_details:
        return version_details

    # #5267: MCP tools are runtime-only; scoped to the end user's own project and token
    try:
        from .internal_tools import inject_mcp_toolkits
        agent_internal_tools = (version_details.get('meta') or {}).get('internal_tools', [])
        mcp_tools = inject_mcp_toolkits(
            user_id=user_id,
            current_project_id=project_id,
            internal_tools=agent_internal_tools,
            existing_tools=version_details.get('tools'),
            scope_project_id=project_id,
        )
        if mcp_tools:
            version_details.setdefault('tools', [])
            version_details['tools'].extend(mcp_tools)
    except Exception as e:
        log.warning(f"[#5267] Failed to inject MCP toolkits into version details: {e}")

    try:
        from .internal_tools import dedupe_internal_mcp_tools, resolve_internal_mcp_tools
        dedupe_internal_mcp_tools(version_details.get('tools'))
        resolve_internal_mcp_tools(version_details.get('tools'), user_id, project_id)
    except Exception as e:
        log.warning(f"Failed to resolve internal MCP toolkits in version details: {e}")

    from .skill_utils import apply_runtime_skills
    apply_runtime_skills(version_details)
    return version_details


def _application_children(tools: Optional[list], project_id: int) -> list:
    children = []
    for tool in tools or []:
        if not isinstance(tool, dict) or tool.get('type') != 'application':
            continue
        settings = tool.get('settings') or {}
        app_id, ver_id = settings.get('application_id'), settings.get('application_version_id')
        # Cross-project children go through the SDK's public-app path, not PATCH version
        if not app_id or not ver_id or tool.get('project_id', project_id) != project_id:
            continue
        children.append((int(app_id), int(ver_id)))
    return children


def _application_summaries(project_id: int, app_ids: list) -> dict:
    from tools import db
    from ..models.all import Application
    with db.get_session(project_id) as session:
        rows = session.query(Application.id, Application.name, Application.description).filter(
            Application.id.in_(app_ids)
        ).all()
    return {r.id: {'name': r.name, 'description': r.description} for r in rows}


def _can_read_version_details(project_id: int, user_id: int) -> bool:
    try:
        from tools import auth, config as c
        return VERSION_DETAILS_PERMISSION in auth.get_user_permissions(
            user_id, mode=c.DEFAULT_MODE, project_id=project_id
        )
    except Exception as e:
        log.warning(f"[subagent_prefetch] permission lookup failed, not prefetching: {e}")
        return False


def _denied_application_ids(project_id: int, app_ids: list, user_id: int) -> set:
    # Mirrors PATCH version's require_folder_access(write=True), but for the end user
    levels = resolve_entities_access(project_id, APPLICATION_ENTITY_TYPES, app_ids, user_id)
    return {int(i) for i, level in levels.items() if level in ('no_access', 'read_only')}


def collect_subagent_version_details(
        project_id: int,
        root_tools: Optional[list],
        user_id: int,
        max_nodes: int = DEFAULT_MAX_NODES,
        max_bytes: int = DEFAULT_MAX_BYTES,
) -> dict:
    """{"app_id:version_id": {name, description, version_details}} for reachable same-project sub-agents.
    Best effort: whatever is skipped here, the SDK fetches through PATCH version as before."""
    from .publish_utils import MAX_SUB_AGENT_VALIDATION_DEPTH

    level = _application_children(root_tools, project_id)
    if not level:
        return {}
    # PATCH version (the SDK's per-child fetch) requires this permission; without it the SDK
    # skipped those children, so return nothing and let it fall back rather than widen access
    if not _can_read_version_details(project_id, user_id):
        return {}

    result, seen, total_bytes = {}, set(), 0
    depth = 0
    try:
        while level and depth < MAX_SUB_AGENT_VALIDATION_DEPTH:
            depth += 1
            level = [c for c in dict.fromkeys(level) if c not in seen]
            seen.update(level)
            if not level:
                break
            app_ids = sorted({a for a, _ in level})
            denied = _denied_application_ids(project_id, app_ids, user_id)
            summaries = _application_summaries(project_id, app_ids)
            next_level = []
            for app_id, ver_id in level:
                if len(result) >= max_nodes or app_id in denied or app_id not in summaries:
                    continue
                try:
                    details = expand_version_for_sdk(project_id, app_id, ver_id, user_id)
                except Exception as e:
                    log.warning(f"[subagent_prefetch] skipping {app_id}/{ver_id}: {e}")
                    continue
                if 'error' in details:
                    continue
                entry = {**summaries[app_id], 'version_details': details}
                size = len(json.dumps(entry, default=str))
                if total_bytes + size > max_bytes:
                    log.info(f"[subagent_prefetch] size cap reached at {len(result)} sub-agents")
                    return result
                total_bytes += size
                result[prefetch_key(app_id, ver_id)] = entry
                next_level.extend(_application_children(details.get('tools'), project_id))
            level = next_level
    except Exception as e:
        log.warning(f"[subagent_prefetch] stopped after {len(result)} sub-agents: {e}")
    return result
