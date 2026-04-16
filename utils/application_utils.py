from json import loads
from datetime import datetime
from typing import List, NamedTuple, Optional, Tuple, Literal, Generator
from sqlalchemy import func, desc, or_, asc, distinct
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.exc import IntegrityError
from pydantic import ValidationError

from tools import context, db, serialize, store_secrets, rpc_tools, auth
from pylon.core.tools import log

from .utils import get_public_project_id
from ..models.enums.all import AgentTypes
from ..models.all import Application, ApplicationVersion, ApplicationVariable, ApplicationVersionTagAssociation
from ..models.elitea_tools import EliteATool, EntityToolMapping
from ..models.enums.events import ApplicationEvents
from ..models.pd.application import (
    ApplicationDetailModel,
    ApplicationVersionDetailModel,
    ApplicationDetailLikesModel,
    ApplicationUpdateModel
)
from ..models.pd.version import ApplicationVersionDetailToolValidatedModel
from ..models.all import Tag
from ..models.enums.all import ToolEntityTypes
from ..utils.like_utils import add_likes, add_trending_likes, add_my_liked, get_like_model
from ..models.pd.tool import ToolValidatedDetails


class _AuthorSortRow(NamedTuple):
    """Lightweight row used to sort applications by author with pin priority."""
    app_id: int
    author_id: int
    is_pinned: bool
    pin_updated_at: Optional[datetime]


def apply_selected_tools_intersection(tools, tool_mappings):
    """
    Apply selected_tools intersection from tool_mappings to tools.

    Args:
        tools: List of tool objects/dicts with settings.selected_tools
        tool_mappings: List of EntityToolMapping objects with tool_id and selected_tools

    Returns:
        None (modifies tools in place)
    """
    # Create mapping dict from tool_mappings
    mapping_dict = {m.tool_id: m.selected_tools for m in tool_mappings if m.selected_tools}

    for tool in tools:
        # Handle both dict and object access patterns
        if isinstance(tool, dict):
            settings = tool.get('settings', {})
            tool_id = tool.get('id')
        else:
            settings = tool.settings if hasattr(tool, 'settings') else {}
            tool_id = tool.id if hasattr(tool, 'id') else None

        if not settings or not tool_id:
            continue

        tool_selected_from_mapping = mapping_dict.get(tool_id)
        tool_selected_from_settings = settings.get('selected_tools')

        if tool_selected_from_settings:
            # Always store original as available_tools
            settings['available_tools'] = tool_selected_from_settings

            # Apply intersection if mapping exists
            if tool_selected_from_mapping:
                settings['selected_tools'] = list(
                    set(tool_selected_from_settings) & set(tool_selected_from_mapping)
                )
        elif tool_selected_from_mapping:
            # No tools in toolkit settings, but mapping has selected tools
            # Use mapping directly (for toolkits that don't pre-define available tools)
            settings['selected_tools'] = tool_selected_from_mapping


class ApplicationVersionNonFoundError(Exception):
    def __init__(self, application_id: int, version_id: int):
        super().__init__(f"Application with id {application_id} and version {version_id} not found")
        self.application_id = application_id
        self.version_id = version_id


class ToolkitConnectionError(Exception):
    """Raised when toolkit configuration connection check fails.
    Carries structured connection_errors so the API can return them
    separately from settings_errors, preserving the existing API contract.
    """
    def __init__(self, connection_errors: list):
        super().__init__("Toolkit connection check failed")
        self.connection_errors = connection_errors


class ApplicationToolExpandedError(Exception):
    pass


class VersionMismatchError(Exception):
    pass


class VersionNotUpdatableError(Exception):
    pass


def applications_update_version(version_data, session) -> dict:
    if 'id' in version_data.model_fields and version_data.id:
        version: ApplicationVersion = session.query(ApplicationVersion).filter(
            ApplicationVersion.id == version_data.id
        ).first()
    else:
        version: ApplicationVersion = session.query(ApplicationVersion).filter(
            ApplicationVersion.application_id == version_data.application_id,
            ApplicationVersion.name == version_data.name,
        ).first()
        version_data.id = version.id

    if not version:
        return {'updated': False, 'msg': f'Application version with id {version_data.id} not found'}

    if version.name == 'base' and version_data.name and version_data.name != 'base':
        raise VersionNotUpdatableError(
            'You cannot change the name of the base version'
        )

    if version.status in ('published', 'embedded'):
        raise VersionNotUpdatableError(
            f'Version id {version_data.id} is {version.status} and can not be updated'
        )

    if version_data.variables is not None:
        variable_names = [var.name for var in version_data.variables]
        session.query(ApplicationVariable).filter(
            ~ApplicationVariable.name.in_(variable_names),
            ApplicationVariable.application_version == version
        ).delete()

        for var in version_data.variables:
            application_var = session.query(ApplicationVariable).where(
                ApplicationVariable.application_version == version,
                ApplicationVariable.name == var.name
            ).first()
            if not application_var:
                application_var = ApplicationVariable(
                    **var.model_dump(exclude_unset=True)
                )
                application_var.application_version = version
                session.add(application_var)
            else:
                session.query(ApplicationVariable).where(
                    ApplicationVariable.name == var.name,
                    ApplicationVariable.application_version == version,
                ).update(
                    var.model_dump(exclude_none=True)
                )
                session.commit()

    for key, value in version_data.model_dump(exclude={'tags', 'variables', 'tools'}).items():
        setattr(version, key, value)

    try:
        version.tags.clear()
        if version_data.tags:
            existing_tags = session.query(Tag).filter(
                Tag.name.in_({i.name for i in version_data.tags})
            ).all()
            existing_tags_map = {i.name: i for i in existing_tags}
            for tag in version_data.tags:
                application_tag = existing_tags_map.get(tag.name, Tag(**tag.model_dump()))
                version.tags.append(application_tag)
            session.add(version)
        session.commit()
        result = ApplicationVersionDetailModel.from_orm(version)
        project_id = version_data.project_id
        for tool in result.tools:
            tool.fix_name(project_id=project_id)
            tool.set_agent_type(project_id=project_id)
            tool.set_agent_meta_and_fields(project_id=project_id)
    except IntegrityError as e:
        log.error(e)
        return {'updated': False, 'msg': 'Values you passed violates unique constraint'}

    return {'updated': True, 'data': loads(result.model_dump_json())}


def application_update(project_id: int, application_id: int, update_data: ApplicationUpdateModel, session) -> dict:
    application_only_data = update_data.model_dump(exclude={'version'}, exclude_none=True)
    store_secrets(application_only_data, project_id)
    application_only_data = serialize(application_only_data)

    session.query(Application).filter(Application.id == application_id).update(
        application_only_data
    )
    if update_data.version:
        version = session.query(ApplicationVersion).filter(
            ApplicationVersion.id == update_data.version.id,
        ).first()
        if not version:
            raise ApplicationVersionNonFoundError(
                application_id=application_id,
                version_id=update_data.version.id
            )
        if version.application_id != application_id:
            raise VersionMismatchError(
                f'Version id {update_data.version.id} mismatch with versions of application with id {application_id}')
#        for k, v in update_data.version.dict(
#            exclude={'tags', 'tools', 'variables', 'name'}, exclude_none=True
#        ).items():
#            setattr(version, k, v)
        applications_update_version(update_data.version, session)
    session.commit()

    context.event_manager.fire_event(
        ApplicationEvents.application_updated,
        {
            "id": application_id,
            "owner_id": project_id,
            "data": application_only_data
        }
    )

    result = get_application_details(project_id, application_id)
    return result['data']


def set_columns_as_attrs(q_result, extra_columns: list) -> Generator:
    for i in q_result:
        try:
            entity, *extra_data = i
            for k, v in zip(extra_columns, extra_data):
                setattr(entity, k, v)
        except TypeError:
            entity = i
        yield entity


def list_applications(
        project_id: int,
        limit: int | None = 10, offset: int | None = 0,
        sort_by: str = 'created_at',
        sort_order: Literal['asc', 'desc'] = 'desc',
        filters: Optional[list] = None,
        with_likes: bool = True,
        my_liked: bool = False,
        trend_period: Optional[Tuple[datetime, datetime]] = None,
        session=None
) -> Tuple[int, list]:
    if my_liked and not with_likes:
        my_liked = False

    if filters is None:
        filters = []

    # OPTIMIZATION: Get count using a simple query WITHOUT likes/pins subqueries
    # This is much faster than counting the complex query with all joins
    count_query = session.query(func.count(Application.id))
    if filters:
        count_query = count_query.filter(*filters)
    # Handle trend_period filter (requires join to Like table)
    if trend_period:
        Like = rpc_tools.RpcMixin().rpc.timeout(2).social_get_like_model()
        trend_filter_subquery = (
            session.query(Like.entity_id)
            .filter(
                Like.entity == Application.likes_entity_name,
                Like.project_id == project_id,
                Like.created_at.between(*trend_period),
            )
            .distinct()
            .subquery()
        )
        count_query = count_query.filter(Application.id.in_(trend_filter_subquery.select()))
    # Handle my_liked filter
    if my_liked:
        Like = rpc_tools.RpcMixin().rpc.timeout(2).social_get_like_model()
        user_id = auth.current_user().get('id')
        my_liked_subquery = (
            session.query(Like.entity_id)
            .filter(
                Like.entity == Application.likes_entity_name,
                Like.project_id == project_id,
                Like.user_id == user_id,
            )
            .distinct()
            .subquery()
        )
        count_query = count_query.filter(Application.id.in_(my_liked_subquery.select()))

    total = count_query.scalar() or 0
    log.debug(f"[PERF] Applications count query returned {total} (optimized)")

    # Import pin utility from social plugin
    add_pins_with_priority = rpc_tools.RpcMixin().rpc.timeout(2).social_add_pins_with_priority()

    sort_by_likes = sort_by == "likes"
    sort_by_author = sort_by in ("author", "authors")

    # For sort_by_author: run a lean query to get (app_id, author_id) for ALL matching
    # records, resolve names via a single auth batch call, sort globally, slice to the
    # requested page, then restrict the full hydration query to just those IDs.
    # This avoids a full-table scan + version/tag load on every paginated request.
    author_page_id_order: dict[int, int] = {}
    if sort_by_author and total > 0:
        from .authors import get_authors_data

        # Lean subquery: grab author_id from each application's base version only.
        base_version_sq = (
            session.query(
                ApplicationVersion.application_id.label('app_id'),
                ApplicationVersion.author_id.label('v_author_id'),
            )
            .filter(ApplicationVersion.name == 'base')
            .subquery()
        )

        author_query = (
            session.query(
                Application.id,
                func.coalesce(base_version_sq.c.v_author_id, 0).label('author_id'),
            )
            .outerjoin(base_version_sq, Application.id == base_version_sq.c.app_id)
        )
        if filters:
            author_query = author_query.filter(*filters)

        # Attach pin columns so pinned items can be sorted first.
        author_query, _ = add_pins_with_priority(
            original_query=author_query,
            project_id=project_id,
            entity=Application,
        )

        # Pin columns added by add_pins_with_priority use coalesce() so named
        # attribute access on the raw Row isn't reliable — extract by index once
        # into a NamedTuple for safe, readable access downstream.
        author_sort_rows: list[_AuthorSortRow] = [
            _AuthorSortRow(app_id=row[0], author_id=row[1], is_pinned=row[2], pin_updated_at=row[3])
            for row in author_query.all()
        ]

        # Batch-resolve author display names in a single call.
        unique_author_ids = {r.author_id for r in author_sort_rows if r.author_id}
        author_name_map: dict[int, str] = {}
        if unique_author_ids:
            try:
                authors_data = get_authors_data(list(unique_author_ids))
                for author in authors_data:
                    display = (
                        author.get('name')
                        or author.get('email')
                        or str(author.get('id', ''))
                    )
                    author_name_map[author['id']] = display.lower()
            except Exception as e:  # noqa: BLE001
                log.warning(f"[sort_by=author] Failed to resolve author names: {e}")

        # Pinned items always first (sorted by pin_updated_at DESC among themselves),
        # then all non-pinned items sorted by author name.
        pinned_rows = [r for r in author_sort_rows if r.is_pinned]
        non_pinned_rows = [r for r in author_sort_rows if not r.is_pinned]

        reverse_author = sort_order.lower() == 'desc'
        pinned_rows.sort(
            key=lambda r: author_name_map.get(r.author_id, ''),
            reverse=reverse_author,
        )
        non_pinned_rows.sort(
            key=lambda r: author_name_map.get(r.author_id, ''),
            reverse=reverse_author,
        )

        sorted_rows = pinned_rows + non_pinned_rows
        _start = offset or 0
        _end = _start + limit if limit else len(sorted_rows)
        page_ids = [r.app_id for r in sorted_rows[_start:_end]]

        if not page_ids:
            return total, []

        # Record page position so we can restore the author sort order after hydration.
        author_page_id_order = {app_id: i for i, app_id in enumerate(page_ids)}

        # Override query params: restrict to page IDs, no DB pagination, safe sort_by.
        filters = filters + [Application.id.in_(page_ids)]
        sort_by = 'id'
        sort_by_author = False
        limit = None
        offset = None

    extra_columns = []

    # Step 1: Query applications without eager loading versions/tags
    query = session.query(Application)

    # OPTIMIZATION: Pre-fetch Like model once to avoid repeated RPC calls
    like_model = get_like_model() if with_likes else None

    if with_likes:
        query, new_columns = add_likes(
            original_query=query,
            project_id=project_id,
            sort_by_likes=sort_by_likes,
            sort_order=sort_order,
            entity=Application,
            like_model=like_model
        )
        extra_columns.extend(new_columns)

    if trend_period and with_likes:
        query, new_columns = add_trending_likes(
            original_query=query,
            project_id=project_id,
            trend_period=trend_period,
            filter_results=True,
            entity=Application,
            like_model=like_model
        )
        extra_columns.extend(new_columns)

    # Only add my_liked subquery for Agent Studio (when with_likes=True)
    if with_likes:
        query, new_columns = add_my_liked(
            original_query=query,
            project_id=project_id,
            filter_results=my_liked,
            entity=Application,
            like_model=like_model
        )
        extra_columns.extend(new_columns)

    # Add pin status (project-wide) - always included
    query, new_columns = add_pins_with_priority(
        original_query=query,
        project_id=project_id,
        entity=Application
    )
    extra_columns.extend(new_columns)

    if filters:
        query = query.filter(*filters)

    # Apply sorting: pinned items always first (by updated_at DESC), then regular sorting for unpinned
    # The query now has columns: Application, ..., is_pinned, pin_updated_at
    # We need to reference these by position in the SELECT
    if not sort_by_likes:
        if sort_by != 'id':
            sort_fn_primary = asc if sort_order.lower() == "asc" else desc
            sort_fn_secondary = asc
            # Sort by: 1) Pinned first (DESC), 2) Pin updated_at (DESC - most recent first), 3) User's choice, 4) ID
            query = query.order_by(
                desc(query.column_descriptions[-2]['expr']),  # is_pinned column
                desc(query.column_descriptions[-1]['expr']),  # pin_updated_at column (DESC)
                sort_fn_primary(getattr(Application, sort_by)),
                sort_fn_secondary(Application.id)
            )
        else:
            sort_fn = asc if sort_order.lower() == "asc" else desc
            query = query.order_by(
                desc(query.column_descriptions[-2]['expr']),  # is_pinned column
                desc(query.column_descriptions[-1]['expr']),  # pin_updated_at column (DESC)
                sort_fn(Application.id)
            )

    # Apply limit and offset for pagination
    if limit:
        query = query.limit(limit)
    if offset:
        query = query.offset(offset)

    q_result = query.all()

    # Step 2: Extract applications and their IDs
    applications_with_attrs = list(set_columns_as_attrs(q_result, extra_columns))

    if not applications_with_attrs:
        return total, []

    # Extract application IDs
    application_ids = [app.id for app in applications_with_attrs]

    # Step 3: Load versions for these applications in a separate query
    versions_query = (
        session.query(ApplicationVersion)
        .filter(ApplicationVersion.application_id.in_(application_ids))
        .order_by(ApplicationVersion.application_id, ApplicationVersion.created_at.desc())
    )
    all_versions = versions_query.all()

    # Step 4: Build a mapping of application_id -> versions
    versions_by_app_id = {}
    version_ids = []
    for version in all_versions:
        if version.application_id not in versions_by_app_id:
            versions_by_app_id[version.application_id] = []
        versions_by_app_id[version.application_id].append(version)
        version_ids.append(version.id)

    # Step 5: Load tags for these versions in a separate query
    if version_ids:
        # Query the association table to get version_id -> tag relationships
        tags_query = (
            session.query(
                ApplicationVersionTagAssociation.c.version_id,
                Tag
            )
            .join(Tag, Tag.id == ApplicationVersionTagAssociation.c.tag_id)
            .filter(ApplicationVersionTagAssociation.c.version_id.in_(version_ids))
        )
        tag_associations = tags_query.all()

        # Build a mapping of version_id -> tags
        tags_by_version_id = {}
        for version_id, tag in tag_associations:
            if version_id not in tags_by_version_id:
                tags_by_version_id[version_id] = []
            tags_by_version_id[version_id].append(tag)

        # Step 6: Assign tags to versions
        for version in all_versions:
            version.tags = tags_by_version_id.get(version.id, [])

    # Step 7: Assign versions to applications
    for app in applications_with_attrs:
        app.versions = versions_by_app_id.get(app.id, [])

    # Re-order results to match the author sort order determined in the lean query phase.
    if author_page_id_order:
        applications_with_attrs.sort(
            key=lambda app: author_page_id_order.get(app.id, len(author_page_id_order))
        )

    return total, applications_with_attrs


def get_application_details(project_id: int, application_id: int,
                            version_name: str = None, first_existing_version: bool = False, skip_like_details: bool = False) -> dict:
    from ..utils.authors import get_authors_data

    with db.get_session(project_id) as session:
        # When version_name is None, use default version resolution
        if version_name is None:
            # Get application and use its default version (default_version_id → 'base' fallback)
            application = session.query(Application).filter(
                Application.id == application_id
            ).options(
                selectinload(Application.versions)
            ).first()
            
            if application:
                default_version = application.get_default_version()
                if default_version:
                    application_version = session.query(ApplicationVersion).filter(
                        ApplicationVersion.id == default_version.id
                    ).options(
                        joinedload(ApplicationVersion.application),
                        selectinload(ApplicationVersion.tools),
                        selectinload(ApplicationVersion.tool_mappings),
                        selectinload(ApplicationVersion.variables)
                    ).first()
                else:
                    application_version = None
            else:
                application_version = None
        else:
            # version_name is provided, search for that specific version
            if version_name == 'latest':
                log.warning(f"Using deprecated version_name='latest' for application {application_id}, consider using default version (version_name=None) instead")
            
            application_version = session.query(ApplicationVersion).filter(
                ApplicationVersion.application_id == application_id,
                ApplicationVersion.name == version_name
            ).options(
                joinedload(ApplicationVersion.application),
                selectinload(ApplicationVersion.tools),
                selectinload(ApplicationVersion.tool_mappings),
                selectinload(ApplicationVersion.variables)
            ).first()

        # Fallback to first existing version if needed
        if not application_version and (version_name is None or first_existing_version):
            application_version = (
                session.query(ApplicationVersion)
                .filter(ApplicationVersion.application_id == application_id)
                .options(
                    joinedload(ApplicationVersion.application),
                    selectinload(ApplicationVersion.tools),
                    selectinload(ApplicationVersion.tool_mappings),
                    selectinload(ApplicationVersion.variables)
                )
                .order_by(ApplicationVersion.created_at.desc())
            ).first()

        if not application_version:
            # this should raise an error
            return {
                'ok': False,
                'msg': f'No application found with id \'{application_id}\' or no version \'{version_name}\''
            }

        # OPTIMIZATION: Pre-fetch version author data in a single batch call
        # Application has no author_id - only version does. Tool authors not needed.
        authors_map = {}
        if application_version.author_id:
            authors_data = get_authors_data([application_version.author_id])
            authors_map = {a['id']: a for a in authors_data}
            log.debug(f"[PERF] Application details: pre-fetched {len(authors_map)} authors (version only)")

        # Pass authors_map via validation context to avoid repeated RPC calls
        validation_context = {'authors_map': authors_map}

        result = ApplicationDetailModel.model_validate(
            application_version.application,
            from_attributes=True,
            context=validation_context
        )
        result.version_details = ApplicationVersionDetailModel.model_validate(
            application_version,
            from_attributes=True,
            context=validation_context
        )
        result.check_is_pinned(project_id)
        try:
            ai_project_id = get_public_project_id()
        except Exception:
            ai_project_id = 1
        if project_id == ai_project_id and not skip_like_details:
            likes_result = ApplicationDetailLikesModel.model_validate(
                result,
                from_attributes=True,
                context=validation_context
            )
            likes_result.get_likes(project_id)
            likes_result.check_is_liked(project_id)
            result = likes_result

        # Pre-fetch MCP schemas once to avoid N+1 queries in set_online()
        mcp_schemas = None
        if result.version_details.tools:
            try:
                current_user = auth.current_user()
                user_id = current_user['id']
                from ..utils.toolkits_utils import get_mcp_schemas
                mcp_schemas = get_mcp_schemas(project_id, user_id)
                log.debug(f"[PERF] Application details: pre-fetched {len(mcp_schemas)} MCP schemas for {len(result.version_details.tools)} tools")
            except Exception as e:
                log.warning(f"[PERF] Failed to pre-fetch MCP schemas: {e}")

        for i in result.version_details.tools:
            i.fix_name(project_id)
            i.set_agent_type(project_id)
            i.set_online(project_id, mcp_schemas=mcp_schemas)
            i.set_agent_meta_and_fields(project_id)

    return {'ok': True, 'data': result.model_dump(mode='json')}


def application_ids_to_names(session, application_id: int, version_id: int) -> Tuple[str, str]:
    row = (
        session.query(Application.name, ApplicationVersion.name)
        .outerjoin(
            ApplicationVersion,
            (ApplicationVersion.application_id == Application.id) & (ApplicationVersion.id == version_id),
        )
        .filter(Application.id == application_id)
        .one_or_none()
    )

    if row is None:
        return ("Unknown", "Unknown")

    application_name, version_name = row
    return application_name or "Unknown", version_name or "Unknown"


def validate_toolkit_details(
    project_id: int,
    toolkit_id: int,
    user_id: int,
    mcp_tokens: dict = None,
    check_connection: bool = False,
    session=None
):
    session_created = False
    if not session:
        session = db.get_project_schema_session(project_id)
        session_created = True
    try:
        toolkit = session.query(EliteATool).filter(
            EliteATool.id == toolkit_id
        ).first()
        if not toolkit:
            raise RuntimeError(f'No such toolkit with id {toolkit_id}')
        toolkit_dict = toolkit.to_json()

        if toolkit_dict.get('type') == 'application':
            _validate_toolkit_type_application(
                session=session,
                tool=toolkit_dict,
                project_id=project_id,
                user_id=user_id
            )
        else:
            toolkit_dict['project_id'] = project_id
            toolkit_dict['user_id'] = user_id
            # Pass check_connection + mcp_tokens via context so ToolValidatedDetails
            # can run the connection check on already-expanded settings — no second
            # expand_toolkit_settings call needed.
            validation_context = {
                'check_connection': check_connection,
                'mcp_tokens': mcp_tokens or {},
            }
            try:
                ToolValidatedDetails.model_validate(toolkit_dict, context=validation_context)
            except ValidationError as e:
                # Intercept the sentinel error raised by the check_connection validator
                # and re-raise it as ToolkitConnectionError so the API can return
                # connection_errors separately from settings_errors.
                for err in e.errors():
                    if (
                        err.get('type') == 'value_error'
                        and isinstance(err.get('ctx', {}).get('error'), dict)
                        and ToolValidatedDetails.CONNECTION_ERROR_SENTINEL
                            in err['ctx']['error']
                    ):
                        raise ToolkitConnectionError(
                            err['ctx']['error'][ToolValidatedDetails.CONNECTION_ERROR_SENTINEL]
                        )
                raise
    finally:
        if session_created:
            session.close()

    return True


def _find_configurations_in_settings(settings: dict) -> list[dict]:
    """
    Recursively find all expanded configurations in settings.
    A configuration is identified by having 'configuration_type' field.

    Returns list of dicts with configuration info:
    [{'type': 'jira', 'title': 'my-jira', 'project_id': 1, 'data': {...}}, ...]
    """
    configurations = []

    def _traverse(obj, parent_key=None):
        if isinstance(obj, dict):
            # Check if this dict is an expanded configuration
            if 'configuration_type' in obj:
                configurations.append({
                    'type': obj['configuration_type'],
                    'title': obj.get('elitea_title', parent_key or 'unknown'),
                    'project_id': obj.get('configuration_project_id'),
                    'data': obj
                })
            # Continue traversing nested dicts
            for key, value in obj.items():
                _traverse(value, parent_key=key)
        elif isinstance(obj, list):
            for item in obj:
                _traverse(item, parent_key)

    _traverse(settings)
    return configurations


def _inject_oauth_tokens(config_data: dict, mcp_tokens: dict) -> dict:
    """
    Inject OAuth access_token into configuration data for OAuth-enabled configurations.

    For configurations like SharePoint that support delegated OAuth flow,
    the access_token needs to be provided from the browser session.

    The mcp_tokens dict is keyed by:
    - "<credential_uuid>:<oauth_discovery_endpoint>" for credential-specific OAuth tokens
      (highest priority — prevents cross-credential token sharing when two credentials share
      the same oauth_discovery_endpoint / tenant)
    - Server URL (canonicalized) for remote MCPs and legacy OAuth configurations
    - Toolkit type (e.g., 'mcp_github') for pre-built MCPs

    We match tokens to configurations using:
    - First: credential-specific key "<configuration_uuid>:<oauth_discovery_endpoint>"
    - Then: url fields (site_url, url, base_url, endpoint, oauth_discovery_endpoint)
    """
    if not mcp_tokens or not config_data:
        return config_data

    # Already has an access token - don't overwrite
    if config_data.get('access_token'):
        return config_data

    # Try to find a matching token
    config_data = config_data.copy()

    # --- Credential-specific key lookup (highest priority) ---
    # When the frontend stores tokens per-credential it uses the key:
    #   "<configuration_uuid>:<oauth_discovery_endpoint>"
    # This prevents two credentials that share the same oauth_discovery_endpoint
    # (same Azure AD tenant) from sharing each other's tokens.
    configuration_uuid = config_data.get('configuration_uuid')
    oauth_discovery_endpoint = config_data.get('oauth_discovery_endpoint')
    if configuration_uuid and oauth_discovery_endpoint:
        credential_specific_key = f"{configuration_uuid}:{oauth_discovery_endpoint}"
        if credential_specific_key in mcp_tokens:
            token_info = mcp_tokens[credential_specific_key]
            if isinstance(token_info, dict) and token_info.get('access_token'):
                config_data['access_token'] = token_info['access_token']
                log.debug(f"Injected access_token via credential-specific key={credential_specific_key}")
                return config_data
            elif isinstance(token_info, str):
                config_data['access_token'] = token_info
                log.debug(f"Injected access_token (legacy str) via credential-specific key={credential_specific_key}")
                return config_data

    # --- URL-based lookup (fallback for remote MCPs and legacy flows) ---
    # Potential URL fields to match against
    url_fields = ['site_url', 'url', 'base_url', 'endpoint', 'oauth_discovery_endpoint']
    for field in url_fields:
        url_value = config_data.get(field)
        if url_value:
            # Try exact match first
            if url_value in mcp_tokens:
                token_info = mcp_tokens[url_value]
                if isinstance(token_info, dict) and token_info.get('access_token'):
                    config_data['access_token'] = token_info['access_token']
                    log.debug(f"Injected access_token for {field}={url_value}")
                    return config_data
                elif isinstance(token_info, str):
                    config_data['access_token'] = token_info
                    log.debug(f"Injected access_token (legacy format) for {field}={url_value}")
                    return config_data

            # Try canonicalized URL match
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url_value)
                canonical = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
                if canonical in mcp_tokens:
                    token_info = mcp_tokens[canonical]
                    if isinstance(token_info, dict) and token_info.get('access_token'):
                        config_data['access_token'] = token_info['access_token']
                        log.debug(f"Injected access_token (canonical match) for {field}={url_value}")
                        return config_data
            except Exception:
                pass

    return config_data


def _check_configurations_connection_from_expanded_settings(
    expanded_settings: dict,
    mcp_tokens: dict = None,
) -> list[dict] | None:
    """
    Run connection checks against already-expanded toolkit settings.
    Called by:
    - ToolValidatedDetails.check_connection model_validator (reuses self.settings,
      avoiding a second expand_toolkit_settings call)
    """
    mcp_tokens = mcp_tokens or {}

    # Find all configurations in expanded settings
    configurations = _find_configurations_in_settings(expanded_settings)
    if not configurations:
        return None

    # Import CONFIG_TYPE_REGISTRY to check if configuration supports check_connection
    try:
        from plugins.configurations.models.pd.registry import CONFIG_TYPE_REGISTRY
    except ImportError:
        log.warning("Could not import CONFIG_TYPE_REGISTRY, skipping connection checks")
        return None

    # Check connection for each configuration
    errors = []
    for config in configurations:
        config_type = config['type']
        config_title = config['title']
        config_data = config['data']

        # Skip configurations that don't support check_connection
        registry_item = CONFIG_TYPE_REGISTRY.get(config_type)
        if not registry_item:
            log.debug(f"Configuration type {config_type} not found in registry, skipping check_connection")
            continue

        # Check if this configuration type has check_connection support
        has_check_connection = bool(registry_item.check_connection_func) or (
            registry_item.model and
            hasattr(registry_item.model, "check_connection") and
            callable(getattr(registry_item.model, "check_connection"))
        )
        if not has_check_connection:
            log.debug(f"Configuration type {config_type} does not support check_connection, skipping")
            continue

        # Inject OAuth tokens for configurations that need them
        config_data = _inject_oauth_tokens(config_data, mcp_tokens)
        log.info(f"{config_data=} for connection check of {config_type}/{config_title}")
        try:
            # Call check_connection via RPC to indexer
            result = context.rpc_manager.timeout(30).applications_configuration_check_connection(
                type_=config_type,
                settings=config_data
            )

            if result:
                # check_connection returns None for success, str/dict for failure
                if isinstance(result, dict):
                    errors.append({
                        'configuration_title': config_title,
                        'configuration_type': config_type,
                        'message': result.get('error', result.get('message', str(result))),
                        'requires_authorization': result.get('requires_authorization', False),
                        'auth_metadata': result.get('auth_metadata')
                    })
                else:
                    errors.append({
                        'configuration_title': config_title,
                        'configuration_type': config_type,
                        'message': str(result),
                        'requires_authorization': False
                    })
        except Exception as ex:
            log.warning(f"Connection check failed for {config_type}/{config_title}: {ex}")
            errors.append({
                'configuration_title': config_title,
                'configuration_type': config_type,
                'message': str(ex),
                'requires_authorization': False
            })

    return errors if errors else None



def _validate_toolkit_type_application(
    session,
    tool: dict,
    project_id: int,
    user_id: int,
    _visited: set = None
) -> bool:
    """
    Validate application (agent/pipeline) toolkit recursively.

    Args:
        session: Database session
        tool: Application toolkit dict with settings.application_id and settings.application_version_id
        project_id: Project ID
        user_id: User ID
        _visited: Set of (application_id, version_id) tuples for cycle detection (should not be None when called)

    Returns:
        True if validation passes

    Raises:
        ValueError: If application or any of its nested toolkits is misconfigured
    """
    application_id = tool.get('settings', {}).get('application_id')
    application_version_id = tool.get('settings', {}).get('application_version_id')

    if not application_id or not application_version_id:
        raise ValueError("Application toolkit missing application_id or application_version_id")

    # _visited should always be initialized by validate_application_version_details
    # Keep backward compatibility just in case
    if _visited is None:
        _visited = set()

    agent_name, version_name = application_ids_to_names(session, application_id, application_version_id)
    try:
        # Circular check is now done inside validate_application_version_details
        validate_application_version_details(project_id, application_id, application_version_id, user_id, _visited=_visited)
    except Exception as ex:
        raise ValueError(
            f"Application misconfiguration error for {agent_name=} {version_name=}: {ex}"
        ) from None

    return True


def validate_application_version_details(
    project_id: int,
    application_id: int,
    version_id: int,
    user_id: int,
    _visited: set = None
) -> bool:
    # Initialize _visited set at the top level to share across all sibling toolkits
    if _visited is None:
        _visited = set()

    # Skip if already validated in this chain (prevents circular references and duplicate work)
    key = (application_id, version_id)
    if key in _visited:
        return True
    _visited.add(key)

    with db.get_session(project_id) as session:
        application_version = session.query(ApplicationVersion).filter(
            ApplicationVersion.id == version_id,
            ApplicationVersion.application_id == application_id
        ).options(
            selectinload(ApplicationVersion.tools),
            selectinload(ApplicationVersion.tool_mappings),
            selectinload(ApplicationVersion.variables)
        ).first()
        if not application_version:
            raise ApplicationVersionNonFoundError(application_id, version_id)

        application_version_dict = application_version.to_dict()
        application_version_dict['project_id'] = project_id
        application_version_dict['user_id'] = user_id
        version_toolkits = application_version_dict.pop('tools', [])

        try:
            # validate version without toolkits, because need to distinguish between version and toolkit errors
            ApplicationVersionDetailToolValidatedModel.model_validate(application_version_dict)
        except ValidationError as e:
            raise RuntimeError(f"Application version data validation error: {e}") from e

        application_toolkit_errors = []
        for tool in version_toolkits:
            # validate application toolkits which can expand recursively
            if tool.get('type') == 'application':
                try:
                    log.debug(f"Validating application toolkit: {tool.get('id')}")
                    _validate_toolkit_type_application(
                        session=session,
                        tool=tool,
                        project_id=project_id,
                        user_id=user_id,
                        _visited=_visited
                    )
                except ValueError as ex:
                    application_toolkit_errors.append({
                        'type': 'value_error',
                        'loc': ('tools', tool['id'], '__root__'),
                        'input': tool,
                        'ctx': {'error': str(ex)},
                    })
            else:
                log.debug(f"Validating regular toolkit: {tool.get('id')}")
                tool['project_id'] = project_id
                tool['user_id'] = user_id
                try:
                    ToolValidatedDetails.model_validate(tool)
                except ValidationError as e:
                    # re-wrap with new location
                    for err in e.errors():
                        application_toolkit_errors.append({
                            'type': err['type'],
                            'loc': ('tools', tool['id'], '__root__'),
                            'input': err.get('input'),
                            'ctx': err.get('ctx', {'error': err.get('msg', 'Validation error')}),
                        })
                except Exception as ex:
                    application_toolkit_errors.append({
                        'type': 'value_error',
                        'loc': ('tools', tool['id'], '__root__'),
                        'input': tool,
                        'ctx': {'error': str(ex)},
                    })
        if application_toolkit_errors:
            raise ValidationError.from_exception_data(
                'ApplicationVersionDetailToolValidatedModel',
                application_toolkit_errors,
            )

    return True


def get_application_by_tags(project_id, tags: List[int]):
    session = db.get_project_schema_session(project_id)
    try:
        return (
            session.query(Application.id)
            .join(Application.versions)
            .join(ApplicationVersion.tags)
            .filter(Tag.id.in_(tags))
            .group_by(Application.id)
            .having(
                func.count(distinct(Tag.id)) == len(tags)
            )
        ).all()
    finally:
        session.close()


def list_applications_api(
        project_id: int,
        tags: str | list | None = None,
        author_id: int | None = None,
        statuses: str | list | None = None,
        q: str | None = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: str = 'created_at',
        sort_order: Literal['asc', 'desc'] = 'desc',
        my_liked: bool = False,
        trend_start_period: str | None = None,
        trend_end_period: str | None = None,
        with_likes: bool = True,
        collection: Optional[dict[str, int]] = None,
        agents_type: str = 'all',
        without_tags: bool = False,
        session=None,
) -> dict:
    # OPTIMIZATION: Only include likes subqueries for Agent Studio (public library)
    # For regular projects, likes are not displayed - skip expensive subqueries
    try:
        ai_project_id = get_public_project_id()
        is_agent_studio = (project_id == ai_project_id)
        if not is_agent_studio and with_likes:
            with_likes = False
            log.debug(f"[PERF] Skipping likes subqueries for non-Agent Studio project {project_id}")
    except Exception as e:
        log.warning(f"[PERF] Failed to check ai_project_id: {e}")

    filters = []

    if author_id:
        filters.append(Application.versions.any(ApplicationVersion.author_id == author_id))

    if tags:
        if isinstance(tags, str):
            tags = [int(tag) for tag in tags.split(',')]
        for tag_id in tags:
            filters.append(
                Application.versions.any(
                    ApplicationVersion.tags.any(Tag.id == tag_id)
                )
            )

    if without_tags:
        filters.append(
            ~Application.versions.any(
                ApplicationVersion.tags.any()
            )
        )

    if statuses:
        if isinstance(statuses, str):
            statuses = statuses.split(',')
        filters.append(Application.versions.any(ApplicationVersion.status.in_(statuses)))

    # Search parameters
    if q:
        filters.append(
            or_(
                Application.name.ilike(f"%{q}%"),
                Application.description.ilike(f"%{q}%")
            )
        )

    if collection and collection.get('id') and collection.get('owner_id'):
        collection_value = {
            "id": collection['id'],
            "owner_id": collection['owner_id']
        }
        filters.append(Application.collections.contains([collection_value]))

    if agents_type:
        agents_type = agents_type.strip().lower()
        pipeline_only_query = Application.versions.any(
            ApplicationVersion.agent_type == AgentTypes.pipeline.value
        )
        if agents_type == 'classic':
            filters.append(~pipeline_only_query)
        elif agents_type == 'pipeline':
            filters.append(pipeline_only_query)
        else:
            pass

    trend_period = None
    if trend_start_period:
        if isinstance(trend_start_period, str):
            trend_start_period = datetime.strptime(trend_start_period, "%Y-%m-%dT%H:%M:%S")
        if not trend_end_period:
            trend_end_period = datetime.utcnow()
        if isinstance(trend_end_period, str):
            trend_end_period = datetime.strptime(trend_end_period, "%Y-%m-%dT%H:%M:%S")
        trend_period = (trend_start_period, trend_end_period)

    # list applications
    total, applications = list_applications(
        project_id=project_id,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
        my_liked=my_liked,
        trend_period=trend_period,
        with_likes=with_likes,
        filters=filters,
        session=session,
    )
    # if search_data:
    #     fire_searched_event(project_id, search_data)

    return {
        'total': total,
        'applications': applications,
    }


def validate_and_resolve_llm_settings(
    project_id: int,
    llm_settings: Optional[dict],
    application_id: Optional[int] = None,
    version_id: Optional[int] = None,
) -> Optional[dict]:
    """Check if llm_settings.model_name is available; fall back to the project's default LLM if not."""
    try:
        available = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_available_models(
            project_id=project_id, section='llm', include_shared=True
        )

        if llm_settings and llm_settings.get('model_name'):
            model_name = llm_settings['model_name']
            model_project_id = llm_settings.get('model_project_id') or project_id

            if (model_project_id, model_name) in available:
                return llm_settings

        default = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_default_model(
            project_id=project_id, section='llm', include_shared=True
        )
        default_model_name = default.get('model_name') if default else None
        default_model_project_id = default.get('model_project_id') if default else None

        if not default_model_name:
            log.error(
                f"No default LLM model configured for project {project_id}. "
                f"Returning original (unavailable) llm_settings."
            )
            return llm_settings

        resolved = dict(llm_settings) if llm_settings else {}
        resolved['model_name'] = default_model_name
        resolved['model_project_id'] = default_model_project_id

        # Reuse already-fetched available dict to get default model capabilities
        # — avoids an extra RPC call.
        default_model_config = available.get((default_model_project_id, default_model_name), {})
        supports_reasoning = bool(default_model_config.get('supports_reasoning', False))

        if supports_reasoning:
            # Reasoning models ignore temperature; promote to reasoning_effort if not already set.
            resolved['temperature'] = None
            if not resolved.get('reasoning_effort'):
                resolved['reasoning_effort'] = 'medium'
        else:
            # Non-reasoning models ignore reasoning_effort.
            resolved['reasoning_effort'] = None
            if resolved.get('temperature') is None:
                resolved['temperature'] = 0.7

        ctx = f"application_id={application_id} version_id={version_id} " if (application_id or version_id) else ""
        log.warning(
            f"LLM model fallback applied {ctx}(project_id={project_id}). "
            f"Before: {llm_settings!r}. After: {resolved!r}"
        )
        return resolved

    except Exception as exc:
        log.warning(
            f"Failed to validate LLM model availability for project {project_id}: {exc}. "
            f"Returning original llm_settings."
        )
        return llm_settings


def get_application_version_details_expanded(
    project_id: int,
    application_id: int,
    version_id: int,
    user_id: int,
    session=None,
    **kwargs
):

    session_created = False
    if not session:
        session = db.get_project_schema_session(project_id)
        session_created = True
    try:
        application_version = session.query(ApplicationVersion).filter(
            ApplicationVersion.id == version_id,
            ApplicationVersion.application_id == application_id
        ).options(
            selectinload(ApplicationVersion.tools),
            selectinload(ApplicationVersion.tool_mappings),
            selectinload(ApplicationVersion.variables)
        ).first()
        if not application_version:
            raise ApplicationVersionNonFoundError(application_id, version_id)

        application_version_dict = application_version.to_dict()
        application_version_dict['project_id'] = project_id
        application_version_dict['user_id'] = user_id
        version_details = ApplicationVersionDetailToolValidatedModel.model_validate(application_version_dict)
        for tool in version_details.tools:
            tool.set_agent_type(project_id)
            tool.set_online(project_id)
            tool.set_agent_meta_and_fields(project_id)

        result = version_details.model_dump(mode='json', exclude={'author_id'})

        if result.get('llm_settings'):
            result['llm_settings'] = validate_and_resolve_llm_settings(
                project_id, result['llm_settings'],
                application_id=application_id, version_id=version_id
            )

        log.debug(f"{result=}")
        return result
    finally:
        if session_created:
            session.close()


def check_if_usable_attachment_toolkit(
    project_id: int, attachment_toolkit_id: int | None, application_id: int, version_id: int, session=None
) -> bool:
    if attachment_toolkit_id is None:
        return True

    session_created = False
    if not session:
        session = db.get_project_schema_session(project_id)
        session_created = True
    try:
        with db.get_session(project_id) as session:
            toolkit = (
                session.query(EliteATool)
                .filter(EliteATool.id == attachment_toolkit_id)
                .first()
            )
            if not toolkit:
                raise ValueError(f"Toolkit with ID {attachment_toolkit_id} not found")

            attachment_toolkit_type = getattr(toolkit, 'type', None)
            if attachment_toolkit_type != 'artifact':
                raise ValueError(
                    f"Attachment toolkit must be of type 'artifact', got '{attachment_toolkit_type}'"
                )

            version_id = version_id
            application_id = application_id
            if version_id is not None:
                mapping_query = session.query(EntityToolMapping).filter(
                    EntityToolMapping.tool_id == attachment_toolkit_id,
                    EntityToolMapping.entity_id == application_id,
                    EntityToolMapping.entity_version_id == version_id,
                    EntityToolMapping.entity_type == ToolEntityTypes.agent,
                )
                mapping_exists = session.query(mapping_query.exists()).scalar()
                if not mapping_exists:
                    raise ValueError(
                        f"attachment_toolkit_id {attachment_toolkit_id} is not mapped to application version {version_id}"
                    )
    finally:
        if session_created:
            session.close()
    return True
