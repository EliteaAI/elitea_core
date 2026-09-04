import functools
from queue import Empty
from typing import List, Optional, Union

from sqlalchemy import and_, exists

from tools import rpc_tools
from pylon.core.tools import log


def get_restricted_folder_ids(
        project_id: int,
        entity_types: Union[str, List[str]],
        user_id: Optional[int] = None,
) -> list:
    """Folders the caller has no access to.

    An absent `social` plugin means the folder feature is not installed, so an empty
    list is correct. Any other failure is *not* swallowed: returning [] there would
    silently expose restricted entities, so the error propagates to the caller.
    """
    try:
        return rpc_tools.RpcMixin().rpc.timeout(3).social_get_restricted_folder_ids(
            project_id=project_id,
            entity_type=entity_types,
            user_id=user_id,
        ) or []
    except Empty:
        log.debug("social_get_restricted_folder_ids unavailable, no folder filtering applied")
        return []


def folder_exclusion_clause(
        project_id: int,
        entity_types: Union[str, List[str]],
        id_column,
        user_id: Optional[int] = None,
):
    """SQL predicate hiding entities that live in the caller's no-access folders (#6524).

    Returns None when nothing is restricted, so the hot path adds no subquery at all.
    Must be applied to the listing query *before* count/offset/limit, otherwise the
    total and the page size are computed over rows the user cannot see.
    """
    restricted = get_restricted_folder_ids(project_id, entity_types, user_id)
    if not restricted:
        return None
    #
    try:
        FolderItem = rpc_tools.RpcMixin().rpc.timeout(2).social_get_folder_item_model()
    except Empty:
        log.debug("social_get_folder_item_model unavailable, no folder filtering applied")
        return None
    #
    types = [entity_types] if isinstance(entity_types, str) else list(entity_types)
    return ~exists().where(and_(
        FolderItem.entity.in_(types),
        FolderItem.entity_id == id_column,
        FolderItem.folder_id.in_(restricted),
    ))


# Entity types as stored in social_folder_items. Applications and toolkits are each
# split into two folder types, and a caller holding only a numeric id cannot tell them
# apart without an extra query — ids are unique per table, so passing both is exact.
APPLICATION_ENTITY_TYPES = ['agent', 'pipeline']
TOOLKIT_ENTITY_TYPES = ['toolkit', 'mcp']

NO_ACCESS_ERROR = 'Not found'
READ_ONLY_ERROR = 'You have read-only access to this folder'


def resolve_entity_access(
        project_id: int,
        entity_type: Union[str, List[str]],
        entity_id: int,
        user_id: Optional[int] = None,
) -> str:
    """Effective folder access for one entity: 'full' | 'read_only' | 'no_access'.

    Absent `social` plugin means no folders exist, so 'full'. Every other failure
    propagates — answering 'full' on a broken lookup would hand out the entity.
    """
    try:
        return rpc_tools.RpcMixin().rpc.timeout(3).social_resolve_entity_access(
            project_id=project_id,
            entity_type=entity_type,
            entity_id=entity_id,
            user_id=user_id,
        ) or 'full'
    except Empty:
        log.debug("social_resolve_entity_access unavailable, folder access not enforced")
        return 'full'


def entity_access_error(
        project_id: int,
        entity_type: Union[str, List[str]],
        entity_id: int,
        write: bool = False,
        user_id: Optional[int] = None,
):
    """None when the operation is allowed, else the `(payload, status)` to return.

    `no_access` answers 404 with the generic not-found body so a restricted entity is
    indistinguishable from a nonexistent one (no existence enumeration).
    """
    level = resolve_entity_access(project_id, entity_type, entity_id, user_id)
    if level == 'no_access':
        return {'ok': False, 'error': NO_ACCESS_ERROR}, 404
    if write and level == 'read_only':
        return {'ok': False, 'error': READ_ONLY_ERROR}, 403
    return None


def resolve_entities_access(
        project_id: int,
        entity_type: Union[str, List[str]],
        entity_ids: List[int],
        user_id: Optional[int] = None,
) -> dict:
    """{entity_id: level} for restricted entities only; absent ids are unrestricted."""
    if not entity_ids:
        return {}
    try:
        return rpc_tools.RpcMixin().rpc.timeout(3).social_resolve_entities_access_bulk(
            project_id=project_id,
            entity_type=entity_type,
            entity_ids=entity_ids,
            user_id=user_id,
        ) or {}
    except Empty:
        log.debug("social_resolve_entities_access_bulk unavailable, folder access not enforced")
        return {}


def application_id_from_version(project_id: int, version_id: int) -> Optional[int]:
    """Parent application of a version, for endpoints keyed by version id only."""
    if not version_id:
        return None
    from tools import db  # pylint: disable=C0415
    from ..models.all import ApplicationVersion  # pylint: disable=C0415
    with db.get_session(project_id) as session:
        return session.query(ApplicationVersion.application_id).filter(
            ApplicationVersion.id == version_id
        ).scalar()


def require_folder_access(
        entity_types: Union[str, List[str]],
        id_param: str,
        write: bool = False,
        via_version: bool = False,
):
    """Enforce folder-level access on the entity addressed by the request path (#6524).

    Place below `@auth.decorators.check_api` so RBAC runs first: folder exceptions can
    only ever narrow role-based access, never widen it. `via_version` resolves the
    parent application when the route is keyed by version id.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            project_id = kwargs.get('project_id')
            entity_id = kwargs.get(id_param)
            if project_id and entity_id:
                if via_version:
                    entity_id = application_id_from_version(project_id, entity_id)
                if entity_id:
                    error = entity_access_error(
                        project_id, entity_types, entity_id, write=write
                    )
                    if error is not None:
                        return error
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


def entities_access_error(
        project_id: int,
        entity_type: Union[str, List[str]],
        entity_ids: List[int],
        write: bool = False,
        user_id: Optional[int] = None,
):
    """None when every id is allowed, else the `(payload, status)` to return.

    Used by the endpoints that take a list of ids (export, fork) where a per-request
    decorator cannot see the ids. All-or-nothing on purpose: a partial export would leak
    which of the requested ids exist.
    """
    levels = resolve_entities_access(project_id, entity_type, entity_ids, user_id)
    if not levels:
        return None
    if any(level == 'no_access' for level in levels.values()):
        return {'ok': False, 'error': NO_ACCESS_ERROR}, 404
    if write and any(level == 'read_only' for level in levels.values()):
        return {'ok': False, 'error': READ_ONLY_ERROR}, 403
    return None


# Payload `entity` values as sent by the fork/import wizard, mapped to folder item types.
FORK_ENTITY_TYPES = {
    'agents': APPLICATION_ENTITY_TYPES,
    'applications': APPLICATION_ENTITY_TYPES,
    'skills': ['skill'],
    'toolkits': TOOLKIT_ENTITY_TYPES,
}


def fork_payload_access_error(items: list, default_kind: Optional[str] = None):
    """Folder access check on the *source* entities of a fork/import payload.

    Each item carries its own `owner_id` (the source project), so restrictions are
    resolved there and not in the target project: a user who cannot see a folder in the
    source project must not be able to fork its contents out of it.
    """
    grouped = {}
    for item in items or []:
        try:
            source_project = int(item.get('owner_id') or 0)
            entity_id = int(item.get('id') or 0)
        except (TypeError, ValueError):
            continue
        if not source_project or not entity_id:
            continue
        types = FORK_ENTITY_TYPES.get(item.get('entity') or default_kind)
        if not types:
            continue
        grouped.setdefault((source_project, tuple(types)), []).append(entity_id)
    #
    for (source_project, types), ids in grouped.items():
        error = entities_access_error(source_project, list(types), ids)
        if error is not None:
            return error
    return None
