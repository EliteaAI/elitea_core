from pydantic import ValidationError
from sqlalchemy import Integer

from ..models.pd.import_wizard import IMPORT_MODEL_ENTITY_MAPPER, DEPRECATED_ENTITIES
from ..utils.export_import_utils import (
    ENTITY_IMPORT_MAPPER, _wrap_import_error,
    _wrap_import_result
)

from tools import rpc_tools, db
from pylon.core.tools import web, log


def _find_existing_toolkit(project_id: int, toolkit_type: str, toolkit_name: str = None, settings: dict = None):
    """
    Find an existing toolkit in the project that matches the given criteria.

    For application-type toolkits: matches by application_id in settings
    For other toolkits: matches by type and name

    Returns the toolkit dict if found, None otherwise.
    """
    from ..models.all import EliteATool
    from ..models.pd.tool import ToolDetails

    with db.get_session(project_id) as session:
        query = session.query(EliteATool).filter(EliteATool.type == toolkit_type)

        if toolkit_type == 'application':
            # For application toolkits, match by application_id in settings
            app_id = settings.get('application_id') if settings else None
            if app_id:
                # Use JSON containment to find matching application_id
                query = query.filter(
                    EliteATool.settings['application_id'].astext.cast(Integer) == app_id
                )
            else:
                return None
        else:
            # For other toolkits, match by name
            if toolkit_name:
                query = query.filter(EliteATool.name == toolkit_name)
            else:
                return None

        existing = query.first()
        if existing:
            return ToolDetails.from_orm(existing).model_dump()
        return None


def _check_configuration_exists(project_id: int, elitea_title: str) -> bool:
    """
    Check if a configuration with the given title exists in the project or shared.

    Returns True if configuration exists, False otherwise.
    """
    try:
        rpc_call = rpc_tools.RpcMixin().rpc.timeout(3)
        # Check in project first
        config = rpc_call.configurations_get_first_filtered_project(
            project_id=project_id,
            filter_fields={'elitea_title': elitea_title}
        )
        if config:
            return True
        # Check in public/shared configurations (project_id=1 is typically public)
        public_configs = rpc_call.configurations_get_filtered_public(
            filter_fields={'elitea_title': elitea_title}
        )
        return bool(public_configs)
    except Exception as e:
        log.warning(f"[IMPORT] Could not check configuration existence for '{elitea_title}': {e}")
        return False


def _merge_selected_tools(existing_tools: list, new_tools: list) -> list:
    """
    Merge selected tools lists, adding any missing tools from new_tools to existing_tools.
    Returns the merged list.
    """
    if not new_tools:
        return existing_tools or []
    if not existing_tools:
        return new_tools

    # Create set of existing tool names for fast lookup
    existing_names = set(existing_tools)

    # Add any new tools that don't exist
    merged = list(existing_tools)
    for tool in new_tools:
        if tool not in existing_names:
            merged.append(tool)
            log.info(f"[IMPORT] Enabled missing tool: {tool}")

    return merged


def _update_toolkit_selected_tools(project_id: int, toolkit_id: int, selected_tools: list):
    """
    Update a toolkit's selected_tools by merging with new tools.
    """
    from ..models.all import EliteATool

    with db.get_session(project_id) as session:
        toolkit = session.query(EliteATool).filter(EliteATool.id == toolkit_id).first()
        if not toolkit:
            return

        # Get current settings
        current_settings = dict(toolkit.settings) if toolkit.settings else {}
        current_selected = current_settings.get('selected_tools', [])

        # Merge tools
        merged = _merge_selected_tools(current_selected, selected_tools)

        # Update if changed
        if merged != current_selected:
            current_settings['selected_tools'] = merged
            toolkit.settings = current_settings
            session.commit()
            log.info(f"[IMPORT] Updated toolkit {toolkit_id} selected_tools: {merged}")


def _create_minimal_toolkit(project_id: int, author_id: int, toolkit_type: str, toolkit_name: str,
                            description: str = '', settings: dict = None, selected_tools: list = None,
                            meta: dict = None) -> dict:
    """
    Create a minimal toolkit directly in the database, bypassing Pydantic validation.

    This is used as a fallback when normal toolkit creation fails due to strict schema
    validation (e.g., missing credentials). The toolkit is created as a placeholder
    that users can configure later.

    Returns dict with 'id' of created toolkit.
    """
    from ..models.all import EliteATool

    with db.get_session(project_id) as session:
        # Create minimal settings with selected_tools
        minimal_settings = settings or {}
        if selected_tools:
            minimal_settings['selected_tools'] = selected_tools

        # Merge provided meta (e.g., mcp flag) with import metadata
        toolkit_meta = dict(meta or {})
        toolkit_meta.update({'import_incomplete': True, 'import_note': 'Created with missing credentials - requires configuration'})

        toolkit = EliteATool(
            type=toolkit_type,
            name=toolkit_name,
            description=description or '',
            settings=minimal_settings,
            author_id=author_id,
            meta=toolkit_meta
        )
        session.add(toolkit)
        session.commit()

        log.info(f"[IMPORT] Created minimal toolkit {toolkit_type}/{toolkit_name} with id {toolkit.id} (missing credentials)")
        return {'id': toolkit.id}


def _sanitize_credential_settings(settings: dict, project_id: int = None) -> dict:
    """
    Sanitize toolkit settings by stripping credential configurations to just reference fields.

    Credential configurations (like jira_configuration, pgvector_configuration) exported from MD
    contain full config values (base_url, connection_string, etc.), but for import we only need
    the reference fields (elitea_title, private) to look up existing credentials in the project.

    If project_id is provided, missing credentials will be removed entirely (set to empty dict)
    to allow toolkit creation with unfilled credentials.

    Fields that don't look like credential configs are passed through unchanged.
    """
    if not settings:
        return settings

    sanitized = {}
    credential_reference_fields = {'elitea_title', 'private'}

    for key, value in settings.items():
        if isinstance(value, dict):
            # Check if this looks like a credential configuration
            # (has elitea_title or configuration_type)
            if 'elitea_title' in value or 'configuration_type' in value:
                elitea_title = value.get('elitea_title')

                # If project_id provided, check if credential exists
                if project_id and elitea_title:
                    if not _check_configuration_exists(project_id, elitea_title):
                        # Credential doesn't exist - remove it entirely
                        log.warning(f"[IMPORT] Credential config '{key}' with title '{elitea_title}' "
                                    f"not found in project {project_id} - creating toolkit with empty credentials")
                        sanitized[key] = {}
                        continue

                # Strip to just reference fields
                sanitized[key] = {
                    k: v for k, v in value.items()
                    if k in credential_reference_fields
                }
                log.info(f"[IMPORT] Stripped credential config '{key}' to reference: {sanitized[key]}")
            else:
                # Not a credential config, pass through
                sanitized[key] = value
        else:
            # Non-dict values pass through unchanged
            sanitized[key] = value

    return sanitized


class RPC:
    @web.rpc('applications_import_wizard', 'import_wizard')
    def import_wizard(self, import_data: dict, project_id: int, author_id: int):
        rpc_call = rpc_tools.RpcMixin().rpc.call

        result, errors = {}, {}

        for key in ENTITY_IMPORT_MAPPER:
            result[key] = []
            errors[key] = []

        # applications, which requires to add toolkit separetly
        # when all initial app/ds/prompts/toolkits are imported
        postponed_applications = []
        # map exported import_uuid/import_version_uuid with real id's of saved entities
        postponed_id_mapper = {}
        # track deprecated toolkit UUIDs to handle missing references gracefully
        deprecated_toolkit_uuids = set()

        # toolkits must be imported after all other entity types
        postponed_toolkits = {}
        # track toolkit UUIDs that failed to create (e.g., due to settings validation)
        failed_toolkit_uuids = set()
        # map application names to their IDs (for name-based matching during MD import)
        application_name_mapper = {}
        # map (app_name, version_name) -> (app_id, version_id) for version-aware matching
        application_version_mapper = {}
        # Collect embedded toolkits from all agents - process AFTER all agents are imported
        # This is critical because embedded toolkits may reference OTHER agents via application_import_uuid
        all_embedded_toolkits = []  # List of (model, embedded_toolkit) tuples
        for item_index, item in enumerate(import_data):
            log.info(f"[IMPORT DEBUG] Processing item {item_index}: entity={item.get('entity')}")
            log.info(f"[IMPORT DEBUG] Item keys: {item.keys()}")
            if 'versions' in item and item['versions']:
                for v_idx, version in enumerate(item['versions']):
                    log.info(f"[IMPORT DEBUG] Version {v_idx}: tools count = {len(version.get('tools', []))}")
                    if version.get('tools'):
                        log.info(f"[IMPORT DEBUG] First tool: {version['tools'][0] if version['tools'] else 'N/A'}")
            has_postponed_toolkits = False
            entity = item['entity']
            if entity in DEPRECATED_ENTITIES:
                log.warning(f'Entity {entity} is deprecated and will not be imported')
                continue

            entity_model = IMPORT_MODEL_ENTITY_MAPPER.get(entity)
            if entity_model:
                try:
                    model = entity_model.parse_obj(item)
                except ValidationError as e:
                    errors[entity].append(_wrap_import_error(item_index, f'Validation error: {e}'))
                    continue
            else:
                if entity not in ENTITY_IMPORT_MAPPER:
                    errors[entity] = []
                errors[entity].append(_wrap_import_error(item_index, f'No such entity "{entity}" in import entity mapper'))
                continue

            if entity == 'toolkits':
                if model.import_data.type in DEPRECATED_ENTITIES:
                    log.warning(f'Toolkit {model.import_data.type} is deprecated and will not be imported')
                    deprecated_toolkit_uuids.add(model.import_data.import_uuid)
                    errors['toolkits'].append(_wrap_import_error(item_index, f'Toolkit {model.import_data.type} is deprecated and was not imported'))
                    continue
                postponed_toolkits[item_index] = model.import_data
                continue

            model_data = model.dict()

            rpc_func = ENTITY_IMPORT_MAPPER.get(entity)
            if rpc_func:
                r = e = None
                try:
                    r, e = getattr(rpc_call, rpc_func)(
                        model_data, project_id, author_id
                    )
                except Exception as ex:
                    log.error(ex)
                    e = ["Import function has been failed"]
                if r:
                    if entity == 'agents':
                        has_postponed_toolkits = model.has_postponed_toolkits()
                        has_embedded_toolkits = model.has_embedded_toolkits()
                        log.info(f"[IMPORT DEBUG] Agent imported: has_postponed_toolkits={has_postponed_toolkits}, has_embedded_toolkits={has_embedded_toolkits}")
                        log.info(f"[IMPORT DEBUG] Model versions count: {len(model.versions)}")
                        for v in model.versions:
                            log.info(f"[IMPORT DEBUG] Version '{v.name}': postponed_tools={len(v.postponed_tools)}, embedded_toolkits={len(v.embedded_toolkits)}")

                        # Collect embedded toolkits for deferred processing (after ALL agents are imported)
                        # This is critical because embedded toolkits may reference OTHER agents via application_import_uuid
                        if has_embedded_toolkits:
                            for embedded_toolkit in model.get_all_embedded_toolkits():
                                all_embedded_toolkits.append(embedded_toolkit)
                                log.info(f"[IMPORT] Deferred embedded toolkit {embedded_toolkit.type}/{embedded_toolkit.name} (uuid={embedded_toolkit.import_uuid}, app_name={embedded_toolkit.application_name})")

                        if has_postponed_toolkits or has_embedded_toolkits:
                            # result will be appended later when all toolkits will be added to apps
                            postponed_applications.append((item_index, model))
                    if not (entity == 'agents' and (model.has_postponed_toolkits() or model.has_embedded_toolkits())):
                        result[entity].append(_wrap_import_result(item_index, r))
                    postponed_id_mapper.update(model.map_postponed_ids(imported_entity=r))
                    # Track application names for name-based matching (used in MD import)
                    if entity == 'agents' and r.get('name'):
                        app_name = r['name']
                        app_id = r['id']
                        application_name_mapper[app_name] = app_id
                        # Also map each version for version-aware matching
                        for ver in r.get('versions', []):
                            ver_name = ver.get('name', 'base')
                            ver_id = ver.get('id')
                            if ver_id:
                                application_version_mapper[(app_name, ver_name)] = (app_id, ver_id)
                                log.info(f"[IMPORT] Mapped agent '{app_name}' version '{ver_name}' to app_id={app_id}, version_id={ver_id}")

                for er in e:
                    errors[entity].append(_wrap_import_error(item_index, er))

        # Process all embedded toolkits AFTER all agents are imported
        # Now postponed_id_mapper contains all agent UUIDs -> database IDs
        # and application_name_mapper contains all agent names -> database IDs
        # Deduplicate embedded toolkits by import_uuid (same toolkit may be referenced by multiple agents)
        seen_import_uuids = set()
        deduplicated_embedded_toolkits = []
        for toolkit in all_embedded_toolkits:
            if toolkit.import_uuid not in seen_import_uuids:
                seen_import_uuids.add(toolkit.import_uuid)
                deduplicated_embedded_toolkits.append(toolkit)
            else:
                log.info(f"[IMPORT] Skipping duplicate embedded toolkit with import_uuid={toolkit.import_uuid}")
                # Add to errors for user feedback
                errors['toolkits'].append(_wrap_import_error(item_index, f'Duplicate embedded toolkit "{toolkit.name or toolkit.type}" was skipped (already exists)'))

        log.info(f"[IMPORT] Processing {len(deduplicated_embedded_toolkits)} embedded toolkits (deferred, {len(all_embedded_toolkits) - len(deduplicated_embedded_toolkits)} duplicates removed)")
        log.info(f"[IMPORT] postponed_id_mapper has {len(postponed_id_mapper)} entries")
        log.info(f"[IMPORT] application_name_mapper has {len(application_name_mapper)} entries: {list(application_name_mapper.keys())}")
        log.info(f"[IMPORT] application_version_mapper has {len(application_version_mapper)} entries")
        for embedded_toolkit in deduplicated_embedded_toolkits:
            try:
                # Handle application-type toolkits (nested agents) specially
                if embedded_toolkit.type == 'application':
                    application_id = None
                    app_version_id = None
                    agent_name = embedded_toolkit.name or embedded_toolkit.toolkit_name or embedded_toolkit.application_name
                    agent_version = embedded_toolkit.application_version  # Get target version from embedded toolkit

                    log.info(f"[IMPORT] Looking for nested agent: name={agent_name}, version={agent_version}, "
                             f"toolkit_name={embedded_toolkit.toolkit_name}, application_name={embedded_toolkit.application_name}")

                    # Priority 1: Version-specific matching (name + version)
                    # This ensures we link to the correct version when the same agent is used with different versions
                    version_key = (agent_name, agent_version) if agent_name and agent_version else None
                    if version_key and version_key in application_version_mapper:
                        application_id, app_version_id = application_version_mapper[version_key]
                        log.info(f"[IMPORT] Matched nested agent '{agent_name}' version '{agent_version}' "
                                 f"to app_id={application_id}, version_id={app_version_id}")
                    # Priority 2: Try base version if no version specified
                    elif agent_name and (agent_name, 'base') in application_version_mapper:
                        application_id, app_version_id = application_version_mapper[(agent_name, 'base')]
                        log.info(f"[IMPORT] Matched nested agent '{agent_name}' to base version "
                                 f"app_id={application_id}, version_id={app_version_id}")
                    # Priority 3: Fall back to name-only matching and lookup version
                    elif agent_name and agent_name in application_name_mapper:
                        application_id = application_name_mapper[agent_name]
                        # Get version details from the imported application
                        app_details = rpc_call.applications_get_application_by_id(
                            project_id=project_id,
                            application_id=application_id,
                        )
                        if agent_version:
                            # Find specific version by name
                            app_version_id = next(
                                (v['id'] for v in app_details.get('versions', []) if v.get('name') == agent_version),
                                None
                            )
                            if app_version_id:
                                log.info(f"[IMPORT] Matched nested agent '{agent_name}' version '{agent_version}' "
                                         f"by lookup to app_id={application_id}, version_id={app_version_id}")
                            else:
                                # Version not found, fall back to base or latest
                                app_version_id = next(
                                    (v['id'] for v in app_details.get('versions', []) if v.get('name') == 'base'),
                                    app_details['versions'][-1]['id'] if app_details.get('versions') else None
                                )
                                log.warning(f"[IMPORT] Version '{agent_version}' not found for agent '{agent_name}', "
                                           f"using fallback version_id={app_version_id}")
                        else:
                            # No version specified - use base or latest
                            app_version_id = next(
                                (v['id'] for v in app_details.get('versions', []) if v.get('name') == 'base'),
                                app_details['versions'][-1]['id'] if app_details.get('versions') else None
                            )
                            log.info(f"[IMPORT] Matched nested agent '{agent_name}' by name to app_id={application_id}, "
                                     f"using base/latest version_id={app_version_id}")

                    if application_id and app_version_id:
                        # Check if application toolkit already exists for this agent+version combo
                        existing_toolkit = _find_existing_toolkit(
                            project_id=project_id,
                            toolkit_type='application',
                            settings={'application_id': application_id}
                        )

                        if existing_toolkit:
                            # Check if existing toolkit points to the correct version
                            existing_version_id = existing_toolkit.get('settings', {}).get('application_version_id')
                            if existing_version_id == app_version_id:
                                # Use existing toolkit - same version
                                postponed_id_mapper[embedded_toolkit.import_uuid] = existing_toolkit['id']
                                log.info(f"[IMPORT] Reusing existing application toolkit {existing_toolkit['id']} "
                                         f"for agent {application_id} version {app_version_id}")
                            else:
                                # Different version - create new toolkit
                                toolkit_payload = {
                                    'type': 'application',
                                    'name': embedded_toolkit.name or embedded_toolkit.toolkit_name or 'Agent',
                                    'description': embedded_toolkit.description or '',
                                    'settings': {
                                        'application_id': application_id,
                                        'application_version_id': app_version_id,
                                    },
                                    'selected_tools': [],
                                    'meta': {},
                                }
                                toolkit_result = rpc_call.applications_import_toolkit(
                                    payload=toolkit_payload,
                                    project_id=project_id,
                                    author_id=author_id
                                )
                                postponed_id_mapper[embedded_toolkit.import_uuid] = toolkit_result['id']
                                log.info(f"[IMPORT] Created new application toolkit for agent {application_id} "
                                         f"version {app_version_id} (different from existing version {existing_version_id})")
                        else:
                            # Create new toolkit
                            toolkit_payload = {
                                'type': 'application',
                                'name': embedded_toolkit.name or embedded_toolkit.toolkit_name or 'Agent',
                                'description': embedded_toolkit.description or '',
                                'settings': {
                                    'application_id': application_id,
                                    'application_version_id': app_version_id,
                                },
                                'selected_tools': [],
                                'meta': {},
                            }
                            toolkit_result = rpc_call.applications_import_toolkit(
                                payload=toolkit_payload,
                                project_id=project_id,
                                author_id=author_id
                            )
                            postponed_id_mapper[embedded_toolkit.import_uuid] = toolkit_result['id']
                            log.info(f"[IMPORT] Created application toolkit for agent {application_id} "
                                     f"version {app_version_id} with toolkit_id={toolkit_result['id']}")
                    else:
                        log.warning(f"Application toolkit '{embedded_toolkit.name}' could not find matching agent. "
                                    f"Tried: name={agent_name}, version={agent_version}. "
                                    f"Available agents: {list(application_name_mapper.keys())}. "
                                    f"Available versions: {list(application_version_mapper.keys())}")
                        failed_toolkit_uuids.add(embedded_toolkit.import_uuid)
                    continue

                # For non-application toolkits, check if one with same type and name exists
                toolkit_name = embedded_toolkit.name or embedded_toolkit.toolkit_name or embedded_toolkit.type
                existing_toolkit = _find_existing_toolkit(
                    project_id=project_id,
                    toolkit_type=embedded_toolkit.type,
                    toolkit_name=toolkit_name
                )

                if existing_toolkit:
                    # Use existing toolkit and enable any missing tools
                    toolkit_id = existing_toolkit['id']
                    postponed_id_mapper[embedded_toolkit.import_uuid] = toolkit_id
                    log.info(f"[IMPORT] Reusing existing toolkit {toolkit_id} ({embedded_toolkit.type}/{toolkit_name})")
                    # Report to user that embedded toolkit was skipped because it already exists, include toolkit ID
                    errors['toolkits'].append(_wrap_import_error(item_index, f'Embedded toolkit "{toolkit_name}" already exists in project and was reused (existing toolkit ID: {toolkit_id}) instead of creating a new one'))

                    # Enable missing tools if any
                    if embedded_toolkit.selected_tools:
                        _update_toolkit_selected_tools(
                            project_id=project_id,
                            toolkit_id=toolkit_id,
                            selected_tools=embedded_toolkit.selected_tools
                        )
                else:
                    # Create new toolkit
                    # Sanitize settings: strip credential configs to just {elitea_title, private}
                    # Pass project_id to remove missing credentials entirely (allow creation with empty creds)
                    sanitized_settings = _sanitize_credential_settings(embedded_toolkit.settings, project_id=project_id)
                    toolkit_payload = {
                        'type': embedded_toolkit.type,
                        'name': toolkit_name,
                        'description': embedded_toolkit.description or '',
                        'settings': sanitized_settings,
                        'selected_tools': embedded_toolkit.selected_tools or [],
                        'meta': embedded_toolkit.meta or {},
                    }
                    toolkit_result = rpc_call.applications_import_toolkit(
                        payload=toolkit_payload,
                        project_id=project_id,
                        author_id=author_id
                    )
                    # Map the embedded toolkit's import_uuid to the created toolkit's id
                    postponed_id_mapper[embedded_toolkit.import_uuid] = toolkit_result['id']
                    log.info(f"[IMPORT] Created embedded toolkit {embedded_toolkit.type} with id {toolkit_result['id']}")

                    # Update selected_tools for newly created toolkit (not preserved during creation)
                    if embedded_toolkit.selected_tools:
                        _update_toolkit_selected_tools(
                            project_id=project_id,
                            toolkit_id=toolkit_result['id'],
                            selected_tools=embedded_toolkit.selected_tools
                        )
            except Exception as ex:
                log.warning(f"[IMPORT] Normal toolkit creation failed for {embedded_toolkit.type}: {ex}")
                # Try to create a minimal toolkit as fallback (bypassing Pydantic validation)
                try:
                    toolkit_name = embedded_toolkit.name or embedded_toolkit.toolkit_name or embedded_toolkit.type
                    minimal_result = _create_minimal_toolkit(
                        project_id=project_id,
                        author_id=author_id,
                        toolkit_type=embedded_toolkit.type,
                        toolkit_name=toolkit_name,
                        description=embedded_toolkit.description or '',
                        settings=_sanitize_credential_settings(embedded_toolkit.settings, project_id=project_id),
                        selected_tools=embedded_toolkit.selected_tools or [],
                        meta=embedded_toolkit.meta or {}
                    )
                    postponed_id_mapper[embedded_toolkit.import_uuid] = minimal_result['id']
                    log.info(f"[IMPORT] Created minimal toolkit {embedded_toolkit.type} (id={minimal_result['id']}) as fallback - requires configuration")
                except Exception as fallback_ex:
                    log.error(f"[IMPORT] Fallback toolkit creation also failed for {embedded_toolkit.type}: {fallback_ex}")
                    failed_toolkit_uuids.add(embedded_toolkit.import_uuid)
                    log.warning(f"[IMPORT] Toolkit {embedded_toolkit.type} will be skipped entirely")
                    # Add to errors for user feedback
                    errors['toolkits'].append(_wrap_import_error(item_index, f'Embedded toolkit "{embedded_toolkit.name or embedded_toolkit.type}" failed to create and was skipped: {str(fallback_ex)}'))

        # import all toolkits
        for item_index, toolkit in postponed_toolkits.items():
            try:
                # Check if toolkit already exists
                toolkit_type = toolkit.type
                toolkit_name = toolkit.name or toolkit_type

                # For application toolkits, check by application_id
                if toolkit_type == 'application':
                    resolved_payload = toolkit.dict_import_uuid_resolved(postponed_id_mapper)
                    app_id = resolved_payload.get('settings', {}).get('application_id')
                    existing_toolkit = _find_existing_toolkit(
                        project_id=project_id,
                        toolkit_type='application',
                        settings={'application_id': app_id}
                    ) if app_id else None
                else:
                    # For other toolkits, check by type and name
                    existing_toolkit = _find_existing_toolkit(
                        project_id=project_id,
                        toolkit_type=toolkit_type,
                        toolkit_name=toolkit_name
                    )

                if existing_toolkit:
                    # Use existing toolkit
                    r = existing_toolkit
                    log.info(f"[IMPORT] Reusing existing toolkit {r['id']} ({toolkit_type}/{toolkit_name})")
                    # Report to user that toolkit was skipped because it already exists, include toolkit ID
                    errors['toolkits'].append(_wrap_import_error(item_index, f'Toolkit "{toolkit_name}" already exists in project and was reused (existing toolkit ID: {r["id"]}) instead of creating a new one'))

                    # Enable missing tools if any
                    selected_tools = toolkit.settings.get('selected_tools', []) if hasattr(toolkit.settings, 'get') else getattr(toolkit.settings, 'selected_tools', [])
                    if selected_tools:
                        _update_toolkit_selected_tools(
                            project_id=project_id,
                            toolkit_id=r['id'],
                            selected_tools=selected_tools
                        )
                else:
                    # Create new toolkit
                    r = rpc_call.applications_import_toolkit(
                        payload=toolkit.dict_import_uuid_resolved(postponed_id_mapper),
                        project_id=project_id,
                        author_id=author_id
                    )

                    # Update selected_tools for newly created toolkit (not preserved during creation)
                    selected_tools = toolkit.settings.get('selected_tools', []) if hasattr(toolkit.settings, 'get') else getattr(toolkit.settings, 'selected_tools', [])
                    if selected_tools:
                        _update_toolkit_selected_tools(
                            project_id=project_id,
                            toolkit_id=r['id'],
                            selected_tools=selected_tools
                        )

                result['toolkits'].append(_wrap_import_result(item_index, r))
                postponed_id_mapper.update(toolkit.map_postponed_ids(imported_entity=r))
            except Exception as ex:
                errors['toolkits'].append(_wrap_import_error(item_index, str(ex)))

        # link all toolkits with application versions, which are toolkit-incomplete
        # Track created links to avoid duplicate attempts (same toolkit may be referenced multiple times)
        created_links = set()  # Set of (toolkit_id, version_id) tuples
        application_ids_to_get_details = set()
        for item_index, postponed_application in postponed_applications:
            import_uuid = postponed_application.import_uuid
            try:
                application_id = postponed_id_mapper[import_uuid]
                application_ids_to_get_details.add((item_index, application_id))
            except KeyError:
                e = f"Agent with {import_uuid=} has not been imported, can not bind toolkits with it"
                errors['agents'].append(_wrap_import_error(item_index, e))
                continue

            for version in postponed_application.versions:
                import_version_uuid = version.import_version_uuid
                try:
                    application_version_id = postponed_id_mapper[import_version_uuid]
                except KeyError:
                    e = f"Agent version with {import_uuid=} {import_version_uuid=} has not been imported, can not bind toolkits with it"
                    errors['agents'].append(_wrap_import_error(item_index, e))
                    continue

                for postponed_toolkit_mapping in version.postponed_tools:
                    payload = {
                        "entity_version_id": application_version_id,
                        "entity_id": application_id,
                        "entity_type": "agent",
                        "has_relation": True
                    }
                    toolkit_import_uuid = postponed_toolkit_mapping.import_uuid
                    try:
                        toolkit_id = postponed_id_mapper[toolkit_import_uuid]
                    except KeyError:
                        # Check if this is a deprecated toolkit that was intentionally filtered out
                        if toolkit_import_uuid in deprecated_toolkit_uuids:
                            log.warning(f"Agent version with {import_uuid=} {import_version_uuid=} references deprecated toolkit {toolkit_import_uuid=} which was not imported")
                            continue
                        # Check if this toolkit failed to create (e.g., settings validation error)
                        elif toolkit_import_uuid in failed_toolkit_uuids:
                            log.warning(f"Agent version with {import_uuid=} {import_version_uuid=} references toolkit {toolkit_import_uuid=} which failed to create - skipping link")
                            continue
                        else:
                            # This is a genuine error - toolkit should have been imported but wasn't
                            e = f"Agent version with {import_uuid=} {import_version_uuid=} can not be bound with {toolkit_import_uuid=} cause the later was not imported"
                            errors['agents'].append(_wrap_import_error(item_index, e))
                            continue
                    # Skip if this link was already created (same toolkit may be in postponed_tools multiple times)
                    link_key = (toolkit_id, application_version_id)
                    if link_key in created_links:
                        log.info(f"[IMPORT] Skipping duplicate link: toolkit_id={toolkit_id}, version_id={application_version_id}")
                        continue
                    created_links.add(link_key)

                    try:
                        rpc_call.applications_toolkit_link(
                            project_id=project_id,
                            toolkit_id=toolkit_id,
                            payload=payload,
                        )
                    except Exception as ex:
                        log.error(ex)
                        e = f"Can not bind {toolkit_id=} with {application_id=} {application_version_id=}"
                        errors['agents'].append(_wrap_import_error(item_index, e))

        # Re-read details for correct result for all applications whith posponed tools
        for item_index, application_id in application_ids_to_get_details:
            try:
                r = rpc_call.applications_get_application_by_id(
                    project_id=project_id,
                    application_id=application_id,
                )
                result['agents'].append(_wrap_import_result(item_index, r))
            except Exception as ex:
                log.error(ex)
                e = f"Can not get detail for {application_id=}"
                errors['agents'].append(_wrap_import_error(item_index, e))

        return result, errors
