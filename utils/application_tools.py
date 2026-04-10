import json
import time
from copy import deepcopy
from typing import Optional, List
from uuid import uuid4

from pydantic import ValidationError
from sqlalchemy import asc, create_engine, desc, func, String, text, Integer, Boolean
from sqlalchemy.orm import Session
from tools import auth, db, this, serialize, context

from ..models.all import EliteATool, EntityToolMapping, ApplicationVersion
from ..models.indexer import EmbeddingStore
from ..models.enums.all import ToolEntityTypes
from ..models.enums.all import InitiatorType

RPC_CALL_TIMEOUT = 3

from pylon.core.tools import log


class ValidatorNotSupportedError(RuntimeError):
    pass


class ToolkitSchemaError(RuntimeError):
    pass


class ToolkitChangeRelationError(Exception):
    pass


class ConfigurationExpandError(Exception):
    def __init__(self, errors):
        super().__init__()
        self.errors = errors


# TODO: move to sdk?
# TODO: or just rewrite
def find_suggested_toolkit_name_field(toolkit_type: str, for_configuration=False) -> Optional[str]:
    tk = find_toolkit_schema_by_type(toolkit_type)
    if tk is None:
        return

    for k, v in tk.get('properties', {}).items():
        if v.get('toolkit_name'):
            if isinstance(v['toolkit_name'], bool):
                if not for_configuration:
                    return k
                # else:
                #     if v.get('configuration_title'):
                #         return 'configuration_title'
                #     else:
                #         return

    return


# TODO: move to sdk?
def find_suggested_toolkit_max_length(toolkit_type: str) -> Optional[str]:
    tk = find_toolkit_schema_by_type(toolkit_type)
    if tk is None:
        return

    for k, v in tk.get('properties', {}).items():
        if v.get('max_toolkit_length'):
            return v.get('max_toolkit_length')


def find_toolkit_schema_by_type(toolkit_type: str) -> Optional[dict]:
    toolkits = this.module.toolkit_schemas
    try:
        return toolkits[toolkit_type]
    except KeyError:
        pass


def find_toolkit_schema_by_type_everywhere(toolkit_type: str, project_id: int, user_id: int):
    from .toolkits_utils import get_toolkit_schemas

    external = False
    tk = find_toolkit_schema_by_type(toolkit_type)
    if tk is None:
        # try finding in mcp/provider_hub schemas
        tk = get_toolkit_schemas(project_id, user_id).get(toolkit_type)
        if tk:
            external = True
    return tk, external


def _expand_toolkit_settings(credential_settings: dict, project_id: int, user_id: int):
    if not credential_settings:
        return credential_settings

    required_fields = {"elitea_title", "private"}
    if set(credential_settings.keys()) != required_fields:
        raise ValueError(f"Toolkit credential settings must contain only fields: {required_fields}")

    credential_settings_expanded = context.rpc_manager.timeout(RPC_CALL_TIMEOUT).configurations_expand(
        project_id=project_id,
        settings=credential_settings,
        user_id=user_id,
        unsecret=True
    )
    return credential_settings_expanded


def _expand_toolkit_reference(toolkit_id: int, project_id: int, user_id: int):
    """Fetch toolkit by ID and return expanded configuration with nested settings expansion"""
    from ..models.pd.tool import ToolDetails

    session = db.get_project_schema_session(project_id)
    try:
        elitea_toolkit = session.query(EliteATool).filter(
            EliteATool.id == toolkit_id
        ).first()

        if not elitea_toolkit:
            raise ToolkitChangeRelationError(f"No such toolkit with id {toolkit_id}")

        toolkit_details = ToolDetails.from_orm(elitea_toolkit)

        # Recursively expand the toolkit's own settings if it has configuration fields
        expanded_settings = toolkit_details.settings
        if expanded_settings and toolkit_details.type:
            try:
                expanded_settings = expand_toolkit_settings(
                    toolkit_details.type,
                    expanded_settings,
                    project_id,
                    user_id
                )
            except (ValidatorNotSupportedError, ToolkitSchemaError):
                # If toolkit schema not found or expansion fails, use original settings
                pass

        # Return toolkit as dict with settings expanded
        return {
            "id": toolkit_details.id,
            "toolkit_name": toolkit_details.toolkit_name,
            "type": toolkit_details.type,
            "settings": expanded_settings,
            "author_id": toolkit_details.author_id,
            "created_at": toolkit_details.created_at.isoformat() if toolkit_details.created_at else None
        }
    finally:
        session.close()


def expand_toolkit_settings(type_: str, settings: dict, project_id: int, user_id: int):
    tk , _ = find_toolkit_schema_by_type_everywhere(type_, project_id, user_id)
    if tk is None:
        raise ValidatorNotSupportedError(f"Toolkit schema not found for type: {type_}")

    to_be_expanded_configuration_fieldnames = []
    to_be_expanded_toolkit_fieldnames = []
    provider_hub_secret_fieldnames = []

    for k, v in tk.get('properties', {}).items():
        if v.get('configuration_types') or v.get('configuration_sections'):
            to_be_expanded_configuration_fieldnames.append(k)
        elif v.get('toolkit_types'):
            to_be_expanded_toolkit_fieldnames.append(k)
        elif v.get('secret') is True and k.startswith('toolkit_configuration_'):
            provider_hub_secret_fieldnames.append(k)

    settings = deepcopy(settings)
    errors = []

    # expand configurations (credentials)
    for to_be_expanded_fieldname in to_be_expanded_configuration_fieldnames:
        try:
            settings[to_be_expanded_fieldname] = _expand_toolkit_settings(
                settings.get(to_be_expanded_fieldname),  # credential configuration might be Optional
                project_id,
                user_id
            )
        except Exception as ex:
            errors.append({
                'loc': (to_be_expanded_fieldname, ),
                'msg': str(ex)
            })

    # expand toolkit references
    for to_be_expanded_fieldname in to_be_expanded_toolkit_fieldnames:
        try:
            toolkit_id = settings.get(to_be_expanded_fieldname)
            if toolkit_id:  # toolkit reference might be Optional
                settings[to_be_expanded_fieldname] = _expand_toolkit_reference(
                    toolkit_id,
                    project_id,
                    user_id
                )
        except Exception as ex:
            errors.append({
                'loc': (to_be_expanded_fieldname, ),
                'msg': str(ex)
            })

    # Resolve vault refs for provider-hub secret fields (read direction only).
    # {{secret.xxx}} refs stored in DB are un-vaulted here so the SDK receives
    # plain-text values. Plain-text values (not yet migrated) are left as-is.
    if provider_hub_secret_fieldnames:
        from tools import VaultClient  # pylint: disable=C0415
        try:
            vault_client = VaultClient(project_id)
            vault_secrets = vault_client.get_all_secrets()
            for fieldname in provider_hub_secret_fieldnames:
                current_val = settings.get(fieldname)
                if current_val and isinstance(current_val, str):
                    settings[fieldname] = vault_client.unsecret(current_val, secrets=vault_secrets)
        except Exception as ex:
            log.warning("expand_toolkit_settings: failed to resolve vault refs for project %s: %s", project_id, ex)

    if errors:
        raise ConfigurationExpandError(errors)
    return settings


def wrap_provider_hub_secret_fields(type_: str, settings: dict, project_id: int) -> None:
    """At save time: store Provider Hub 'secret':true field values in Vault.

    Mutates settings dict in-place, replacing plain-text values with
    {{secret.xxx}} vault reference strings directly. This avoids Pydantic v2's
    SecretStr JSON serializer returning '**********' when model_dump(mode='json')
    is called later.

    Idempotent: skips fields whose value is already a {{secret.xxx}} vault ref.
    """
    from tools import SecretString, VaultClient  # pylint: disable=C0415
    try:
        tk, _ = find_toolkit_schema_by_type_everywhere(type_, project_id, None)
    except Exception:
        return
    if tk is None:
        return

    secret_fieldnames = [
        k for k, v in tk.get('properties', {}).items()
        if v.get('secret') is True and k.startswith('toolkit_configuration_')
    ]
    if not secret_fieldnames:
        return

    vault_client = None
    for fieldname in secret_fieldnames:
        current_val = settings.get(fieldname)
        if (
            current_val
            and isinstance(current_val, str)
            and not SecretString._secret_pattern.match(current_val)
        ):
            if vault_client is None:
                vault_client = VaultClient(project=project_id)
            s = SecretString(current_val)
            s.vault_client = vault_client
            settings[fieldname] = s.store_secret()


def toolkits_listing(
    project_id: int,
    query: str,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = "created_at",
    sort_order: str = 'desc',
    toolkit_type: Optional[List[str]] = None,
    filter_mcp: Optional[bool] = False,
    filter_application: Optional[bool] = None,
    author_id: Optional[int] = None,
    search_artifact: Optional[str] = None,
):
    from ..models.all import EliteATool
    from ..models.pd.tool import ToolDetails
    from ..utils.authors import get_authors_data
    from tools import rpc_tools

    with db.get_session(project_id) as session:
        # Import pin utility from social plugin
        add_pins_with_priority = rpc_tools.RpcMixin().rpc.timeout(2).social_add_pins_with_priority()
        extra_columns = []

        q = session.query(EliteATool)

        q = q.filter(EliteATool.type != 'application')

        if search_artifact:
            q = q.filter(EliteATool.name.ilike(f"%{search_artifact}%"))
        elif query:
            q = q.filter(
                (EliteATool.name.ilike(f"%{query}%")) |
                (EliteATool.description.ilike(f"%{query}%"))
                # Search only by name + description EL-2653
                # (EliteATool.type.ilike(f"%{query}%")) |
                # (func.cast(EliteATool.settings, String).ilike(f"%{query}%"))
            )
        if toolkit_type:
            q = q.filter(EliteATool.type.in_(toolkit_type))

        if author_id:
            q = q.filter(EliteATool.author_id == author_id)

        if filter_mcp:
            # Filter for MCP toolkits: either meta['mcp'] is True OR type is 'mcp'
            q = q.filter(
                (EliteATool.meta['mcp'].astext.cast(Boolean) == True) |
                (EliteATool.type == 'mcp')
            )
        else:
            # Filter out MCP toolkits: meta['mcp'] must be False/None AND type must not be 'mcp'
            q = q.filter(
                (EliteATool.meta['mcp'].astext.cast(Boolean) == False) |
                (EliteATool.meta['mcp'].astext.is_(None))
            ).filter(
                EliteATool.type != 'mcp'
            )

        if filter_application is True:
            # Filter for application toolkits: meta['application'] is True
            log.info(f"Filtering FOR applications (application=true)")
            q = q.filter(
                EliteATool.meta['application'].astext.cast(Boolean) == True
            )
        elif filter_application is False:
            # Filter out application toolkits: meta['application'] must be False/None
            log.info(f"Filtering OUT applications (showing non-applications)")
            q = q.filter(
                (EliteATool.meta['application'].astext.cast(Boolean) == False) |
                (EliteATool.meta['application'].astext.is_(None))
            )
        # else: filter_application is None, don't filter by application status

        # Add pin status (project-wide) - always included
        q, new_columns = add_pins_with_priority(
            original_query=q,
            project_id=project_id,
            entity=EliteATool
        )
        extra_columns.extend(new_columns)

        sort_by_author = sort_by in ("author", "authors")

        # Apply sorting: pinned items always first (by updated_at DESC), then regular sorting
        if sort_by != "name" and not sort_by_author:
            if hasattr(EliteATool, sort_by):
                sort_fn = desc if sort_order.lower() == "desc" else asc
                # Sort by: 1) Pinned first (DESC), 2) Pin updated_at (DESC), 3) User's choice, 4) ID
                q = q.order_by(
                    desc(q.column_descriptions[-2]['expr']),  # is_pinned column
                    desc(q.column_descriptions[-1]['expr']),  # pin_updated_at column (DESC)
                    sort_fn(getattr(EliteATool, sort_by))
                )
        else:
            # For name/author sorting, we'll handle it after fetching all results
            q = q.order_by(
                desc(q.column_descriptions[-2]['expr']),  # is_pinned column
                desc(q.column_descriptions[-1]['expr']),  # pin_updated_at column (DESC)
            )

        total_count = q.count()

        if sort_by != "name" and not sort_by_author:
            tools = q.offset(offset).limit(limit).all()
        else:
            tools = q.all()

        result = []

        log.debug(f"Total tools: {total_count}")

        # Pre-fetch all authors in a single batch call to avoid N+1 queries
        def extract_toolkit(item):
            try:
                toolkit, *_ = item
                return toolkit
            except (TypeError, ValueError):
                return item

        author_ids = list(set(
            extract_toolkit(t).author_id
            for t in tools
            if extract_toolkit(t).author_id
        ))
        authors_data = get_authors_data(author_ids) if author_ids else []
        authors_map = {a['id']: a for a in authors_data}
        log.debug(f"[N+1 FIX] Pre-fetched {len(authors_map)} authors for {len(author_ids)} unique IDs")

        # Pre-fetch MCP schemas once to avoid N+1 queries in set_online()
        mcp_schemas = None
        try:
            current_user = auth.current_user()
            user_id = current_user['id']
            from ..utils.toolkits_utils import get_mcp_schemas
            mcp_schemas = get_mcp_schemas(project_id, user_id)
            log.debug(f"[N+1 FIX] Pre-fetched {len(mcp_schemas)} MCP schemas")
        except Exception as e:
            log.warning(f"[N+1 FIX] Failed to pre-fetch MCP schemas: {e}")

        # Extract toolkit objects and set pin attributes
        for i in tools:
            try:
                toolkit, *extra_data = i
                for k, v in zip(extra_columns, extra_data):
                    setattr(toolkit, k, v)
            except (TypeError, ValueError):
                toolkit = i

            # Pass pre-fetched authors via validation context
            toolkit_detail = ToolDetails.model_validate(
                toolkit,
                from_attributes=True,
                context={'authors_map': authors_map}
            )
            toolkit_detail.fix_name(project_id)
            toolkit_detail.set_agent_type(project_id)
            toolkit_detail.set_online(project_id, mcp_schemas=mcp_schemas)
            toolkit_detail.set_agent_meta_and_fields(project_id)

            # Set pin status from attributes
            toolkit_detail.is_pinned = getattr(toolkit, 'is_pinned', False)

            result.append(toolkit_detail)

        if sort_by == "name":
            reverse_name = sort_order.lower() == "desc"
            name_key = lambda x: (x.name or "").lower()
            # Pinned items first, then sort by name within each group
            pinned = [r for r in result if r.is_pinned]
            non_pinned = [r for r in result if not r.is_pinned]
            pinned.sort(key=name_key, reverse=reverse_name)
            non_pinned.sort(key=name_key, reverse=reverse_name)
            result = pinned + non_pinned
            result = result[offset:offset + limit]
        elif sort_by_author:
            reverse_author = sort_order.lower() == "desc"
            # Build author name lookup from the already pre-fetched authors_map
            author_name_map = {}
            for aid, adata in authors_map.items():
                display = (
                    adata.get('name')
                    or adata.get('email')
                    or str(adata.get('id', ''))
                )
                author_name_map[aid] = display.lower()

            def _author_sort_key(item):
                aid = getattr(item.author, 'id', None) if item.author else None
                return author_name_map.get(aid, '')
            # Pinned items first, then sort by author name
            pinned = [r for r in result if r.is_pinned]
            non_pinned = [r for r in result if not r.is_pinned]
            pinned.sort(key=_author_sort_key, reverse=reverse_author)
            non_pinned.sort(key=_author_sort_key, reverse=reverse_author)
            result = pinned + non_pinned
            result = result[offset:offset + limit]

        return {"rows": [serialize(i) for i in result], "total": total_count}


def toolkit_change_relation(
    project_id: int,
    toolkit_id: int,
    relation_data: dict,
    session=None
):
    """ Add or delete toolkit to the application version """
    from ..models.pd.tool import ToolUpdateRelationModel

    relation_data = ToolUpdateRelationModel.parse_obj(relation_data)

    session_is_created = False
    if session is None:
        session = db.get_project_schema_session(project_id)
        session_is_created = True

    try:
        elitea_toolkit = session.query(EliteATool).filter(
            EliteATool.id == toolkit_id
        ).first()
        if not elitea_toolkit:
            raise ToolkitChangeRelationError(f"No such toolkit with id {toolkit_id}")

        elitea_tool_mapping = session.query(EntityToolMapping).filter(
            EntityToolMapping.tool_id == elitea_toolkit.id,
            EntityToolMapping.entity_version_id == relation_data.entity_version_id,
            EntityToolMapping.entity_id == relation_data.entity_id,
            EntityToolMapping.entity_type == relation_data.entity_type
        ).first()

        if elitea_tool_mapping and relation_data.has_relation:
            # Relation already exists - update selected_tools if provided
            if relation_data.selected_tools is not None:
                elitea_tool_mapping.selected_tools = relation_data.selected_tools
                session.flush()
                return {'has_relation': True, 'tool_id': toolkit_id, 'updated': True}
            else:
                raise ToolkitChangeRelationError(
                    f"Already exists relation with toolkit id {elitea_toolkit.id}"
                )
        elif not elitea_tool_mapping and not relation_data.has_relation:
            raise ToolkitChangeRelationError(
                f"Already removed relation with toolkit id {elitea_toolkit.id}"
            )
        else:
            if relation_data.has_relation:
                application_tool_to_application = EntityToolMapping(
                    tool_id=toolkit_id,
                    entity_version_id=relation_data.entity_version_id,
                    entity_id=relation_data.entity_id,
                    entity_type=relation_data.entity_type,
                    selected_tools=relation_data.selected_tools  # Set selected_tools from request
                )
                session.add(application_tool_to_application)
                session.flush()
            else:
                session.query(EntityToolMapping).filter(
                    EntityToolMapping.id == elitea_tool_mapping.id,
                ).delete()
                session.flush()

        return {'has_relation': relation_data.has_relation, 'tool_id': toolkit_id}
    finally:
        if session_is_created:
            session.commit()
            session.close()


def application_toolkit_change_relation(
    project_id: int,
    user_id: int,
    application_id: int,
    version_id: int,
    update_data: dict,
    session=None
):
    """ Add or delete agent/pipeline to the application version as a toolkit """
    from ..models.pd.application import ApplicationRelationModel

    update_data = ApplicationRelationModel.parse_obj(update_data)

    session_is_created = False
    if session is None:
        session = db.get_project_schema_session(project_id)
        session_is_created = True

    try:
        child_application_version = session.query(ApplicationVersion).filter(
            ApplicationVersion.id == version_id,
            ApplicationVersion.application_id == application_id
        ).first()

        # Allow removal even if child version doesn't exist (orphaned reference cleanup)
        if not child_application_version and update_data.has_relation:
            raise ToolkitChangeRelationError(
                f'Child application[{application_id}] version[{version_id}] not found'
            )

        # If removing a reference to a non-existent version, find and clean up the orphaned toolkit
        if not child_application_version and not update_data.has_relation:
            log.info(f"Cleaning up orphaned reference to application[{application_id}] version[{version_id}]")
            orphaned_toolkit = session.query(EliteATool).where(
                EliteATool.type == 'application',
                EliteATool.settings['application_id'].astext.cast(Integer) == application_id,
                EliteATool.settings['application_version_id'].astext.cast(Integer) == version_id
            ).first()

            if orphaned_toolkit:
                # Remove the mapping for the parent application version
                parent_application_version = session.query(ApplicationVersion).filter(
                    ApplicationVersion.id == update_data.version_id,
                    ApplicationVersion.application_id == update_data.application_id
                ).first()
                if parent_application_version:
                    session.query(EntityToolMapping).filter(
                        EntityToolMapping.tool_id == orphaned_toolkit.id,
                        EntityToolMapping.entity_version_id == parent_application_version.id,
                        EntityToolMapping.entity_id == parent_application_version.application_id
                    ).delete()
                    session.flush()
                    return {'ok': True, 'orphaned_cleanup': True}
            return {'ok': True, 'orphaned_cleanup': True}

        parent_application_version = session.query(ApplicationVersion).filter(
            ApplicationVersion.id == update_data.version_id,
            ApplicationVersion.application_id == update_data.application_id
        ).first()
        if not parent_application_version:
            raise ToolkitChangeRelationError(
                f'Application[{update_data.application_id}] version[{update_data.version.id}] not found'
            )

        # When REMOVING a relation (has_relation=False), find the toolkit that is actually
        # linked to this parent via EntityToolMapping. This ensures we remove the correct mapping
        # even if there are multiple toolkits for the same child application.
        if not update_data.has_relation:
            application_toolkit = session.query(EliteATool).join(
                EntityToolMapping,
                EliteATool.id == EntityToolMapping.tool_id
            ).where(
                EliteATool.type == 'application',
                EliteATool.settings['application_id'].astext.cast(Integer) == application_id,
                EntityToolMapping.entity_id == parent_application_version.application_id,
                EntityToolMapping.entity_version_id == parent_application_version.id,
                EntityToolMapping.entity_type == ToolEntityTypes.agent,
            ).first()

            if not application_toolkit:
                # No toolkit linked to this parent - relation already removed or never existed
                return {'ok': True, 'already_removed': True}
        else:
            # When ADDING a relation (has_relation=True), first check if this parent already
            # has a relation to this child application (any version). This prevents duplicates
            # that can occur after version deletion with replacement.
            existing_relation = session.query(EliteATool).join(
                EntityToolMapping,
                EliteATool.id == EntityToolMapping.tool_id
            ).where(
                EliteATool.type == 'application',
                EliteATool.settings['application_id'].astext.cast(Integer) == application_id,
                EntityToolMapping.entity_id == parent_application_version.application_id,
                EntityToolMapping.entity_version_id == parent_application_version.id,
                EntityToolMapping.entity_type == ToolEntityTypes.agent,
            ).first()

            if existing_relation:
                # Parent already has a relation to this child app
                existing_version_id = existing_relation.settings.get('application_version_id')
                if existing_version_id == version_id:
                    # Same version - relation already exists
                    raise ToolkitChangeRelationError(
                        f"Already exists relation with toolkit id {existing_relation.id}"
                    )
                else:
                    # Different version - update the existing toolkit's version
                    log.info(f"Updating existing relation toolkit {existing_relation.id} "
                             f"from version {existing_version_id} to {version_id}")
                    new_settings = dict(existing_relation.settings)
                    new_settings['application_version_id'] = version_id
                    existing_relation.settings = new_settings
                    existing_relation.name = child_application_version.name
                    session.flush()
                    return {'has_relation': True, 'tool_id': existing_relation.id, 'updated_version': True}

            # No existing relation - find or create a toolkit for this (app_id, version_id)
            application_toolkit = session.query(EliteATool).where(
                EliteATool.type == 'application',
                EliteATool.settings['application_id'].astext.cast(Integer) == application_id,
                EliteATool.settings['application_version_id'].astext.cast(Integer) == version_id
            ).first()

            if not application_toolkit:
                # Create new toolkit for this specific application + version combination
                application_toolkit = EliteATool(
                    name=child_application_version.name,
                    type='application',
                    author_id=user_id,
                    settings={
                        'application_id': application_id,
                        'application_version_id': version_id
                    },
                )
                session.add(application_toolkit)
                session.flush()

        toolkit_relation_data = {
            'entity_id': parent_application_version.application_id,
            'entity_version_id': parent_application_version.id,
            'entity_type': ToolEntityTypes.agent,
            'has_relation': update_data.has_relation
        }

        result = toolkit_change_relation(
            project_id=project_id,
            toolkit_id=application_toolkit.id,
            relation_data=toolkit_relation_data,
            session=session
        )
        session.flush()
        return result
    finally:
        if session_is_created:
            session.commit()
            session.close()


def _transform_deprecated_tools_errors(validation_errors: list[dict]) -> list[dict]:
    """Collapses per-item Pydantic literal_error entries for selected_tools into a single human-readable error."""
    deprecated_tools = [
        err.get('input')
        for err in validation_errors
        if err.get('type') == 'literal_error' and 'selected_tools' in err.get('loc', ())
    ]

    result = []
    if deprecated_tools:
        invalid_names = ', '.join(f"'{t}'" for t in deprecated_tools if t is not None)
        result.append({
            'type': 'value_error',
            'loc': ('settings', 'selected_tools'),
            'input': deprecated_tools,
            'ctx': {'error': (
                f"the following tools are no longer available: {invalid_names}. "
                "Please remove them to continue."
            )},
        })

    for err in validation_errors:
        if err.get('type') == 'literal_error' and 'selected_tools' in err.get('loc', ()):
            continue
        result.append({
            'type': 'value_error',
            'loc': ('settings', *err.get('loc', [])),
            'input': err.get('input'),
            'ctx': {'error': err.get('msg', '')},
        })

    return result


def raise_validation_error_if_any(validation_errors: list[dict] | str, model):
    """Re-raises validation errors as Pydantic ValidationError, with improved messaging for deprecated selected_tools."""
    if isinstance(validation_errors, list):
        raise ValidationError.from_exception_data(
            model.__name__,
            _transform_deprecated_tools_errors(validation_errors),
        )
    else:
        raise ValueError(str(validation_errors))

def load_and_validate_toolkit_for_index(toolkit_config):
    toolkit_id = toolkit_config.get('id')
    if not toolkit_id:
        return None, None, ({"ok": False, "error": f"Toolkit id is missing for toolkit {toolkit_id}"}, 400)
    #
    pgvector_configuration = toolkit_config.get('settings', {}).get('pgvector_configuration')
    if not pgvector_configuration:
        return None, None, ({"ok": False, "error": f"PGVector configuration is missing for toolkit {toolkit_id}"}, 400)
    connection_string = pgvector_configuration.get('connection_string')
    if not connection_string:
        return None, None, ({"ok": False, "error": f"Connection string is missing in PGVector configuration for toolkit {toolkit_id}"}, 400)
    #
    return str(toolkit_id), connection_string, None

def validate_toolkit_for_index(toolkit_config):
    toolkit_schema, connection_string, error = load_and_validate_toolkit_for_index(toolkit_config)
    #
    if error:
        raise ValueError(error[0]['error'])
    #
    return toolkit_schema, connection_string


def get_toolkit_index_meta(session: Session, index_name: str):
    return session.query(EmbeddingStore).filter(
        EmbeddingStore.cmetadata['type'].astext == "index_meta",
        func.jsonb_extract_path_text(EmbeddingStore.cmetadata, 'collection') == index_name
    ).first()


def reset_or_create_toolkit_index_meta(connection_string: str, toolkit_name_id: str, index_name: str, default: dict):
    with get_session_for_schema(connection_string, toolkit_name_id) as session:
        meta = get_toolkit_index_meta(session, index_name)
        if meta:
            history_raw = meta.cmetadata.get("history", "[]")
            try:
                history = json.loads(history_raw) if history_raw.strip() else []
            except (json.JSONDecodeError, TypeError):
                log.warning(f"Failed to load index history: {history_raw}. Setting to empty list.")
                history = []
            #
            # Update current meta with new data and put the same item to history
            history.append(default)
            meta.cmetadata = {**default, "history": json.dumps(history)}
        else:
            # create new index meta with history containing only current initial state
            meta = EmbeddingStore(
                id=uuid4(),
                document=f"index_meta_{index_name}",
                cmetadata={**default, "history": json.dumps([default])}
            )
            session.add(meta)
        # save all changes
        session.commit()


def update_toolkit_index_meta_history_with_failed_state(connection_string: str, toolkit_id, index_name: str, error: str):
    with get_session_for_schema(connection_string, str(toolkit_id)) as session:
        meta = get_toolkit_index_meta(session, index_name)
        if meta:
            current_metadata = meta.cmetadata.copy()
            current_metadata['state'] = 'failed'
            current_metadata['updated_on'] = time.time()
            current_metadata['error'] = error
            history_raw = current_metadata.pop("history", "[]")
            try:
                history = json.loads(history_raw) if history_raw.strip() else []
            except (json.JSONDecodeError, TypeError):
                log.warning(f"Failed to load index history: {history_raw}. Setting to empty list.")
                history = []
            #
            # Update current meta with new data and put the same item to history
            history.append(current_metadata)# add item with no history
            current_metadata['history'] = json.dumps(history)
            meta.cmetadata = current_metadata
            session.commit()
            log.debug(f"Updated failed state for index_name={index_name} and add to history")
        else:
            log.warning(f"No metadata found for index_name={index_name}, cannot update failed state and history")


def update_toolkit_index_meta_failed_state(connection_string: str, toolkit_name_id: str, index_name: str, error: str):
    """
    Update only the current state of index metadata to failed without touching history.

    This is used when an error occurs after SDK has already updated metadata,
    to avoid duplicate history entries while still preserving the latest error.

    Args:
        connection_string: Database connection string
        toolkit_name_id: Toolkit schema name
        index_name: Name of the index
        error: Error message to store
    """
    with get_session_for_schema(connection_string, toolkit_name_id) as session:
        meta = get_toolkit_index_meta(session, index_name)
        if meta:
            # Update only state, updated_on, and error - preserve history as-is
            current_metadata = meta.cmetadata.copy()
            current_metadata['state'] = 'failed'
            current_metadata['updated_on'] = time.time()
            current_metadata['error'] = error

            # Check if the last history entry is the initial in_progress state with same created_on
            # If so, replace it with the failed state instead of keeping both
            history_raw = current_metadata.get("history", "[]")
            try:
                history = json.loads(history_raw) if history_raw.strip() else []
            except (json.JSONDecodeError, TypeError):
                log.warning(f"Failed to load index history: {history_raw}. Setting to empty list.")
                history = []

            if history:
                last_entry = history[-1]
                # Check if last entry is in_progress with same created_on (initial state from start_index_task)
                if (last_entry.get('state') == 'in_progress' and
                    last_entry.get('created_on') == current_metadata.get('created_on')):
                    # Remove the last in_progress entry and append current failed state instead
                    history.pop()
                    meta_copy = current_metadata.copy()
                    meta_copy.pop('history', None)
                    history.append(meta_copy)
                    current_metadata['history'] = json.dumps(history)
                    log.debug(
                        f"Replaced in_progress history entry with failed state for index_name={index_name}"
                    )

            meta.cmetadata = current_metadata
            session.commit()
            log.debug(f"Updated failed state for index_name={index_name} without touching history")
        else:
            log.warning(f"No metadata found for index_name={index_name}, cannot update failed state")


def ensure_pgvector_schema_and_tables(connection_string: str, schema: str, vector_dimension: int = None):
    import sqlalchemy
    from sqlalchemy import create_engine, text, Column, String, ForeignKey, Index
    from sqlalchemy.dialects.postgresql import UUID, JSONB, JSON
    from sqlalchemy.orm import declarative_base, relationship, Session
    from sqlalchemy.schema import CreateSchema
    from pgvector.sqlalchemy import Vector

    engine = create_engine(connection_string)
    with engine.begin() as conn:
        conn.execute(CreateSchema(schema, if_not_exists=True))

    Base = declarative_base()

    class CollectionStore(Base):
        __tablename__ = "langchain_pg_collection"
        __table_args__ = {"schema": schema}

        uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
        name = Column(String, nullable=False, unique=True)
        cmetadata = Column(JSON)

        embeddings = relationship(
            "EmbeddingStore",
            back_populates="collection",
            passive_deletes=True,
        )

    class EmbeddingStore(Base):
        __tablename__ = "langchain_pg_embedding"
        __table_args__ = {"schema": schema}

        id = Column(String, primary_key=True)
        collection_id = Column(
            UUID(as_uuid=True),
            ForeignKey(f"{schema}.langchain_pg_collection.uuid", ondelete="CASCADE"),
        )
        collection = relationship(CollectionStore, back_populates="embeddings")
        embedding = Column(Vector(vector_dimension))
        document = Column(String, nullable=True)
        cmetadata = Column(JSONB, nullable=True)

        __table_args__ = (
            Index(
                "ix_cmetadata_gin",
                "cmetadata",
                postgresql_using="gin",
                postgresql_ops={"cmetadata": "jsonb_path_ops"},
            ),
            {"schema": schema},
        )

    Base.metadata.create_all(engine)


def get_session_for_schema(connection_string: str, schema: str):
    ensure_pgvector_schema_and_tables(connection_string, schema)

    engine = create_engine(connection_string)
    session = Session(engine)
    session.execute(text(f'SET search_path TO "{schema}"'))
    return session


def start_index_task(task_node, data, sio_event, initiator=InitiatorType.user):
    """
    Start an index task with proper metadata

    Args:
        task_node: The task node to start the task on
        data: Task data including toolkit_config, project_id, etc.
        sio_event: Socket.IO event name
        initiator: Initiator type ('user', 'llm', 'schedule'). If None, will be inferred.
    """

    toolkit_config = data.get('toolkit_config', {})
    project_id = data.get('project_id')
    chat_project_id = data.get('chat_project_id')
    tool_name = data.get('tool_name')
    tool_params = data.get('tool_params', '{}')
    if isinstance(tool_params, str):
        tool_params = json.loads(tool_params)

    #
    task_kwargs = deepcopy(data)
    stream_id = task_kwargs.pop('stream_id', None)
    message_id = task_kwargs.pop('message_id', None)
    question_id = task_kwargs.pop('question_id', None)
    task_kwargs['tool_params'] = tool_params
    #
    task_id = task_node.start_task(
        "indexer_test_toolkit_tool",
        args=[stream_id, message_id],
        kwargs=task_kwargs,
        pool="agents",
        meta={
            "task_name": "indexer_test_toolkit_tool",
            "project_id": project_id,
            "chat_project_id": chat_project_id,
            "message_id": message_id,
            "question_id": question_id,
            "sio_event": sio_event,
            "initiator": str(initiator),
            "toolkit_config": toolkit_config,
            "tool_name": tool_name,
            "tool_params": tool_params,
            "user_id": task_kwargs.get('user_id', ''),
            "deployment_url": task_kwargs.get('deployment_url', ''),
            "project_auth_token": task_kwargs.get('project_auth_token', ''),
            "user_context": {
                "user_id": task_kwargs.get("user_id", None),
                "project_id": project_id,
            },
        },
    )
    #
    # Save index_meta data for index_data tool
    index_name = tool_params.get('index_name')
    toolkit_name_id, connection_string = validate_toolkit_for_index(toolkit_config)
    created_on = time.time()
    cmetadata = {
        "collection": index_name,
        "type": "index_meta",
        "indexed": 0,
        "updated": 0,
        "state": "in_progress",
        "index_configuration": tool_params,
        "created_on": created_on,
        "updated_on": created_on,
        "task_id": task_id,
        "conversation_id": data.get('conversation_id', None),
        "toolkit_id": int(toolkit_name_id),
    }
    reset_or_create_toolkit_index_meta(connection_string, toolkit_name_id, index_name, cmetadata)
    #
    return task_id


def handle_index_data_failure(ctx, event_data: dict):
    """
    Handle index_data tool failure by updating metadata with failed status.

    This function is called when index_data fails (either before SDK or after SDK has updated metadata).
    It uses a lightweight update that doesn't touch history to avoid duplicates.

    Args:
        ctx: Pylon context
        event_data: Event data containing task_id, index_name, error, toolkit_config, etc.
    """
    try:
        task_id = event_data.get('task_id')
        index_name = event_data.get('index_name')
        error = event_data.get('error')
        toolkit_config = event_data.get('toolkit_config', {})

        if not index_name:
            log.error(f"Missing required fields in index_data failure event: {event_data}")
            return

        log.debug(f"Handling index_data failure for task_id={task_id}, index_name={index_name}")

        # Validate toolkit for index to get connection string and toolkit_name_id
        toolkit_name_id, connection_string = validate_toolkit_for_index(toolkit_config)

        # Update only the failed state without touching history
        update_toolkit_index_meta_failed_state(connection_string, toolkit_name_id, index_name, error)
        log.debug(f"Updated failed state for index_name={index_name}, task_id={task_id}")

    except Exception as e:
        log.exception(f"Failed to handle index_data failure event: {e}")


def ensure_index_data_has_task_id(ctx, event_data: dict):
    """
    Ensure task_id is set in cmetadata for an index.

    Simple function that only updates task_id if it's missing (None or not set).

    Args:
        ctx: Pylon context
        event_data: Event data containing task_id, index_name, toolkit_config
    """
    try:
        task_id = event_data.get('task_id')
        index_name = event_data.get('index_name')
        toolkit_config = event_data.get('toolkit_config', {})
        created_at = event_data.get('created_at')

        if not task_id or not index_name:
            log.error(f"Missing task_id or index_name in event: {event_data}")
            return

        # Get connection details
        toolkit_name_id, connection_string = validate_toolkit_for_index(toolkit_config)

        # Get session and update if needed
        with get_session_for_schema(connection_string, toolkit_name_id) as session:
            meta = get_toolkit_index_meta(session, index_name)
            if not meta:
                log.warning(f"No metadata found for index_name={index_name}")
                return

            # Read current metadata
            current_metadata = meta.cmetadata.copy()

            # Only update if task_id is None AND created_at matches (stronger condition)
            if current_metadata.get('task_id') is None and current_metadata.get('created_on') == created_at:
                current_metadata['task_id'] = task_id
                current_metadata['updated_on'] = event_data.get('updated_on')
                meta.cmetadata = current_metadata
                session.commit()
                log.debug(f"Set task_id={task_id} for index_name={index_name}")
            else:
                log.debug(f"Skipping task_id update for index_name={index_name}: "
                          f"task_id={current_metadata.get('task_id')}, "
                          f"created_on={current_metadata.get('created_on')}, event_created_at={created_at}")

    except Exception as e:
        log.exception(f"Failed to ensure task_id for index: {e}")


def is_index_stale(updated_on: float, index_data_state: str, task_disconnected_timeout: int) -> bool:
    """
    Determine if an index task is stale (hasn't been updated within the timeout period).

    Returns:
    - stale=False: Task finished OR task is in_progress and was recently updated
    - stale=True: Task is in_progress but hasn't been updated for too long

    Args:
        updated_on: Timestamp when the task was last updated (Unix timestamp)
        index_data_state: Current state of the index (e.g., "in_progress", "completed", "failed")
        task_disconnected_timeout: Timeout in seconds after which task is considered stale

    Returns:
        bool: True if task is stale, False otherwise
    """""
    # If state is not "in_progress", the task is finished - not stale
    if index_data_state != "in_progress":
        return False

    # Check if task hasn't been updated for too long
    current_time = time.time()
    time_since_update = current_time - updated_on

    return time_since_update > task_disconnected_timeout


def clean_up_schedule_in_toolkit(project_id: int, toolkit_id: int, index_name: str):
    try:
        log.debug(f"Starting clean_up_schedule_in_toolkit: project_id={project_id}, toolkit_id={toolkit_id}, index_name={index_name}")
        if index_name:
            with db.get_session(project_id) as project_session:
                toolkit = project_session.query(EliteATool).filter(
                    EliteATool.id == toolkit_id
                ).first()
                if not toolkit:
                    log.error(f"Toolkit {toolkit_id} not found")
                    return {"ok": False, "error": f"Toolkit {toolkit_id} not found ({project_id=}, {index_name=})"}, 404

                meta = toolkit.meta or {}
                indexes_meta = meta.get("indexes_meta", {})

                # Remove the entire index_meta_id entry (all users)
                if index_name in indexes_meta:
                    from sqlalchemy.orm.attributes import flag_modified
                    log.debug(f"Removing index '{index_name}' from toolkit {toolkit_id} (project_id={project_id})")
                    indexes_meta.pop(index_name)
                    toolkit.meta["indexes_meta"] = indexes_meta
                    flag_modified(toolkit, "meta")
                    project_session.commit()
                    log.debug(f"Index '{index_name}' successfully removed and committed for toolkit {toolkit_id} (project_id={project_id})")
                else:
                    log.debug(f"Index '{index_name}' not found in toolkit {toolkit_id} (project_id={project_id})")
                return {"ok": True}, 200
    except Exception as e:
        log.error(f"Error during index deletion {e}")
        return {"ok": False, "error": f"Error during index deletion (Toolkit {toolkit_id}{project_id=}, {index_name=}) {e}"}, 400
