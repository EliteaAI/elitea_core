"""Registry CRUD + per-project projection for platform-wide eval dimensions (§16.1).

Two stores are kept in sync:

* the **registry** — ``centry.eval_platform_dimension``, one row per platform dimension.
  Source of truth; what the admin console reads and writes.
* the **projection** — an ordinary ``p_<id>.eval_dimension`` row with ``tier='platform'``,
  correlated to the registry by ``uuid``, in the projects that actually use the dimension.

The projection exists because ``eval_binding.dimension_id`` is an FK into the tenant schema.
Mirroring registry rows into each project means every existing binding, snapshot, judge and
scoring path keeps working with no changes at all.

Projection is **lazy**: ``materialize`` copies a registry row into a single project the moment
that project attaches the dimension. Admin writes touch the registry only. Pushing a later
definition edit out to the projects already holding a copy is an explicit act —
``resync_dimension`` / ``resync_all``, which update in place and never insert.

Projected rows are written as ORM rows directly rather than through
``evaluation_library_utils.create_dimension`` / ``update_dimension`` — those deliberately
refuse platform-tier writes, and that guard must stay in force for the project API.

Deletion is soft only: ``eval_binding.dimension_id`` cascades on delete, so dropping a
projected row would silently delete bindings across every project. ``set_active(False)``
hides the dimension from the picker while bindings and run history survive.
"""

from typing import List, Optional

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from sqlalchemy.exc import IntegrityError

from tools import context, db  # pylint: disable=E0401

from ..models.eval_platform_dimension import EvalPlatformDimension
from ..models.evaluation import EvalDimension, EvalTier
from ..models.pd.eval_platform_dimension import (
    EvalPlatformDimensionCreateModel,
    EvalPlatformDimensionUpdateModel,
)
from .evaluation_library_utils import EvalLibraryError

_PROJECTED_FIELDS = (
    'name', 'description', 'allowed_engines', 'scale_type', 'scale_min', 'scale_max',
    'polarity', 'default_weight', 'default_target', 'default_target_operator',
)


class EvalPlatformDimensionNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, identifier):
        super().__init__(f'Platform eval dimension "{identifier}" not found')
        self.identifier = identifier


class EvalPlatformDimensionNameConflictError(EvalLibraryError):
    http_status = 409

    def __init__(self, name: str):
        super().__init__(f'A platform eval dimension named "{name}" already exists')
        self.name = name


class EvalPlatformDimensionInactiveError(EvalLibraryError):
    http_status = 409

    def __init__(self, name: str):
        super().__init__(f'Platform eval dimension "{name}" is deactivated and cannot be attached')
        self.name = name


# ----------------------------------------------------------------------------
# Registry (shared schema)
# ----------------------------------------------------------------------------

def list_registry(active_only: bool = False) -> List[EvalPlatformDimension]:
    with db.with_project_schema_session(None) as session:
        query = session.query(EvalPlatformDimension)
        if active_only:
            query = query.filter(EvalPlatformDimension.is_active.is_(True))
        rows = query.order_by(EvalPlatformDimension.name.asc()).all()
        session.expunge_all()
        return rows


def get_registry(dimension_uuid: str) -> Optional[EvalPlatformDimension]:
    with db.with_project_schema_session(None) as session:
        row = (
            session.query(EvalPlatformDimension)
            .filter(EvalPlatformDimension.uuid == dimension_uuid)
            .first()
        )
        if row is not None:
            session.expunge(row)
        return row


def create_registry(
    data: EvalPlatformDimensionCreateModel,
    owner_id: Optional[int] = None,
) -> EvalPlatformDimension:
    with db.with_project_schema_session(None) as session:
        row = EvalPlatformDimension(owner_id=owner_id, **data.model_dump())
        session.add(row)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            raise EvalPlatformDimensionNameConflictError(data.name)
        session.refresh(row)
        session.expunge(row)
        return row


def update_registry(
    dimension_uuid: str,
    data: EvalPlatformDimensionUpdateModel,
) -> EvalPlatformDimension:
    with db.with_project_schema_session(None) as session:
        row = (
            session.query(EvalPlatformDimension)
            .filter(EvalPlatformDimension.uuid == dimension_uuid)
            .first()
        )
        if row is None:
            raise EvalPlatformDimensionNotFoundError(dimension_uuid)
        for key, value in data.model_dump(exclude_unset=True).items():
            setattr(row, key, value)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            raise EvalPlatformDimensionNameConflictError(row.name)
        session.refresh(row)
        session.expunge(row)
        return row


def set_active(dimension_uuid: str, is_active: bool) -> EvalPlatformDimension:
    return update_registry(
        dimension_uuid,
        EvalPlatformDimensionUpdateModel.model_construct(is_active=is_active),
    )


# ----------------------------------------------------------------------------
# Projection (per-project schemas)
# ----------------------------------------------------------------------------

def project_to(
    project_id: int,
    entries: List[EvalPlatformDimension],
    insert_missing: bool = True,
) -> dict:
    """Idempotently upsert ``entries`` into ``p_<project_id>.eval_dimension``.

    Matching is on ``uuid``, never name, so renaming a registry entry updates the projected
    row in place instead of inserting a duplicate.

    With ``insert_missing=False`` the call is update-only: a project that never attached the
    dimension is left alone rather than having a copy conjured into it. That is what the
    resync paths want — they propagate edits, they do not distribute the catalog.
    """
    inserted = updated = skipped = 0
    with db.get_session(project_id) as session:
        existing = {
            str(row.uuid): row
            for row in session.query(EvalDimension)
            .filter(EvalDimension.tier == EvalTier.platform)
            .all()
        }
        for entry in entries:
            values = {field: getattr(entry, field) for field in _PROJECTED_FIELDS}
            values['meta'] = dict(entry.meta or {})
            row = existing.get(str(entry.uuid))
            if row is None and not insert_missing:
                skipped += 1
            elif row is None:
                session.add(EvalDimension(
                    uuid=entry.uuid,
                    tier=EvalTier.platform,
                    # eval_dimension.owner_id is NOT NULL; the registry allows no owner.
                    owner_id=entry.owner_id or 0,
                    **values,
                ))
                inserted += 1
            else:
                for key, value in values.items():
                    setattr(row, key, value)
                updated += 1
        session.commit()
    return {
        'project_id': project_id, 'inserted': inserted,
        'updated': updated, 'skipped': skipped,
    }


def local_dimension_id(project_id: int, dimension_uuid: str) -> Optional[int]:
    """The id of ``dimension_uuid``'s projected row in this project, or None if not attached."""
    with db.get_session(project_id) as session:
        row = (
            session.query(EvalDimension)
            .filter(
                EvalDimension.tier == EvalTier.platform,
                EvalDimension.uuid == dimension_uuid,
            )
            .first()
        )
        return row.id if row is not None else None


def materialize(project_id: int, dimension_uuid: str) -> EvalDimension:
    """Copy a registry entry into ``p_<project_id>`` and return the **local** row.

    The attach path: a project only ever gets a projected row because someone picked the
    dimension out of the catalog. Idempotent — attaching twice reuses the existing row, which
    is what keeps the caller's binding pointing at a stable id.
    """
    entry = get_registry(dimension_uuid)
    if entry is None:
        raise EvalPlatformDimensionNotFoundError(dimension_uuid)
    if not entry.is_active:
        raise EvalPlatformDimensionInactiveError(entry.name)

    project_to(project_id, [entry])

    with db.get_session(project_id) as session:
        row = (
            session.query(EvalDimension)
            .filter(
                EvalDimension.tier == EvalTier.platform,
                EvalDimension.uuid == entry.uuid,
            )
            .first()
        )
        session.expunge(row)
        return row


def _active_project_ids() -> List[int]:
    projects = context.rpc_manager.timeout(120).project_list(
        filter_={'create_success': True}
    )
    return [int(project['id']) for project in projects]


def _holds_any(project_id: int, uuids: List[str]) -> bool:
    """Whether this project carries a projected row for any of ``uuids``.

    A read-only probe so the resync only opens a write transaction on the projects it will
    actually change — most projects attach none of the catalog, and a resync that commits in
    every schema is what pushes the request past its timeout.
    """
    wanted = set(uuids)
    with db.get_session(project_id) as session:
        rows = (
            session.query(EvalDimension)
            .filter(EvalDimension.tier == EvalTier.platform)
            .all()
        )
        return any(str(row.uuid) in wanted for row in rows)


def _resync(entries: List[EvalPlatformDimension]) -> dict:
    """Push ``entries`` into every project that already holds a copy. Never inserts.

    A failing project is collected, not raised — one broken schema must not stop the rest of
    the platform from picking up the edit.
    """
    synced, failures = [], []
    uuids = [str(entry.uuid) for entry in entries]
    if not uuids:
        return {'synced': [], 'synced_projects': 0, 'failures': []}
    for project_id in _active_project_ids():
        try:
            if not _holds_any(project_id, uuids):
                continue
            result = project_to(project_id, entries, insert_missing=False)
        except Exception as exc:  # pylint: disable=broad-except
            log.exception('Failed to sync platform eval dimensions into project %s', project_id)
            failures.append({'project_id': project_id, 'error': str(exc)})
            continue
        if result['updated']:
            synced.append(result)
    return {'synced': synced, 'synced_projects': len(synced), 'failures': failures}


def resync_dimension(dimension_uuid: str) -> dict:
    """Push one registry entry's current definition into the projects using it."""
    entry = get_registry(dimension_uuid)
    if entry is None:
        raise EvalPlatformDimensionNotFoundError(dimension_uuid)
    return _resync([entry])


def resync_all() -> dict:
    """Push the whole registry into the projects using each entry. The bulk equivalent."""
    return _resync(list_registry())
