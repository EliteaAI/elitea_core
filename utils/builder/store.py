"""Changeset store on ``p_<id>.builder_changesets`` (#7383), same interface as ``service.MemoryStore``."""
from sqlalchemy.orm.attributes import flag_modified
from tools import db

from ...models.builder_changeset import BuilderChangeset

_COLUMNS = ('id', 'user_id', 'entity_type', 'op', 'destructive', 'confirmed_by', 'applied', 'fingerprint')


def _row_dict(row: BuilderChangeset, project_id: int) -> dict:
    return {**(row.payload or {}), **{k: getattr(row, k) for k in _COLUMNS},
            'created_at': row.created_ts, 'project_id': project_id}


class DbStore:
    def __init__(self, project_id: int):
        self.project_id = project_id

    def put(self, row: dict):
        payload = {k: v for k, v in row.items() if k not in _COLUMNS + ('created_at', 'project_id')}
        with db.get_session(self.project_id) as session:
            session.add(BuilderChangeset(
                id=row['id'], user_id=row['user_id'], entity_type=row['entity_type'], op=row['op'],
                destructive=bool(row['destructive']), payload=payload, created_ts=row['created_at'],
                confirmed_by=row.get('confirmed_by'), applied=bool(row.get('applied')),
                fingerprint=row.get('fingerprint'),
            ))
            session.commit()

    def get(self, changeset_id):
        with db.get_session(self.project_id) as session:
            row = session.get(BuilderChangeset, changeset_id)
            return _row_dict(row, self.project_id) if row else None

    def update(self, changeset_id, **fields):
        with db.get_session(self.project_id) as session:
            row = session.get(BuilderChangeset, changeset_id)
            if row is None:
                return
            for key, value in fields.items():
                if key in _COLUMNS:
                    setattr(row, key, value)
                else:
                    row.payload = {**(row.payload or {}), key: value}
                    flag_modified(row, 'payload')
            session.commit()

    def find_pending(self, user_id, fingerprint, not_before):
        with db.get_session(self.project_id) as session:
            row = session.query(BuilderChangeset).filter(
                BuilderChangeset.user_id == user_id,
                BuilderChangeset.fingerprint == fingerprint,
                BuilderChangeset.applied.is_(False),
                BuilderChangeset.created_ts >= not_before,
            ).order_by(BuilderChangeset.created_ts.desc()).first()
            return _row_dict(row, self.project_id) if row else None

    def claim_apply(self, changeset_id) -> bool:
        """Mark applied only if it wasn't: two concurrent applies can't both run (AC-20)."""
        with db.get_session(self.project_id) as session:
            updated = session.query(BuilderChangeset).filter(
                BuilderChangeset.id == changeset_id, BuilderChangeset.applied.is_(False),
            ).update({'applied': True}, synchronize_session=False)
            session.commit()
            return updated == 1
