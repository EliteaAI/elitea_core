import uuid
from datetime import datetime
from typing import List, Optional

from tools import db_tools, db, config as c
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, DateTime, func, ForeignKey, Text, Table, Column, UniqueConstraint, Index, text
from sqlalchemy.ext.mutable import MutableDict

from .enums.all import PublishStatus


SKILL_TABLE_NAME = 'skills'
SKILL_VERSION_TABLE_NAME = 'skill_versions'
ENTITY_SKILL_MAPPING_TABLE_NAME = 'entity_skill_mapping'


SkillVersionTagAssociation = Table(
    'skill_version_tag_association',
    db.Base.metadata,
    Column(
        'version_id',
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{SKILL_VERSION_TABLE_NAME}.id', ondelete='CASCADE'),
        primary_key=True
    ),
    Column(
        'tag_id',
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.tags.id', ondelete='CASCADE'),
        primary_key=True
    ),
    schema=c.POSTGRES_TENANT_SCHEMA
)


class Skill(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = SKILL_TABLE_NAME
    __table_args__ = (
        Index(
            'uq_skills_shared_owner', 'shared_owner_id', 'shared_id',
            unique=True, postgresql_where=text('shared_owner_id IS NOT NULL'),
        ),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    # Entity name used by the social plugin's pin subquery
    # (mirrors Application.pins_entity_name = 'application').
    pins_entity_name: str = 'skill'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(String(2304), nullable=False)
    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)

    shared_owner_id: Mapped[int] = mapped_column(Integer, nullable=True)
    shared_id: Mapped[int] = mapped_column(Integer, nullable=True)

    versions: Mapped[List['SkillVersion']] = relationship(
        back_populates='skill',
        lazy=True,
        cascade='all, delete',
        passive_deletes=True
    )

    def get_default_version(self) -> Optional['SkillVersion']:
        """Get the default version of the skill.

        First checks meta.default_version_id, then falls back to 'base' version.
        Returns None if neither is found.
        """
        default_version_id = self.meta.get('default_version_id') if self.meta else None
        if default_version_id:
            try:
                return next(v for v in self.versions if v.id == default_version_id)
            except StopIteration:
                pass
        # Fall back to 'base' version
        try:
            return next(v for v in self.versions if v.name == 'base')
        except StopIteration:
            return None


class SkillVersion(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = SKILL_VERSION_TABLE_NAME
    __table_args__ = (
        UniqueConstraint('skill_id', 'name', name='_skill_version_name_uc'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    skill_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{SKILL_TABLE_NAME}.id', ondelete='CASCADE'),
        index=True
    )
    skill: Mapped['Skill'] = relationship(back_populates='versions', lazy=True)

    name: Mapped[str] = mapped_column(String(128), nullable=False, default='base')
    instructions: Mapped[str] = mapped_column(Text, nullable=False)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)

    # Tags (follows ApplicationVersion pattern)
    tags: Mapped[List['Tag']] = relationship(
        secondary=SkillVersionTagAssociation,
        backref='skill_versions',
        lazy='select'
    )

    status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        server_default=PublishStatus.draft.value,
        default=PublishStatus.draft.value,
        index=True
    )


class EntitySkillMapping(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = ENTITY_SKILL_MAPPING_TABLE_NAME
    __table_args__ = (
        UniqueConstraint('entity_version_id', 'skill_id', 'entity_type', name='_entity_skill_unique'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity_version_id: Mapped[int] = mapped_column(Integer, index=True)
    entity_type: Mapped[str] = mapped_column(String(50), index=True)  # 'agent', 'pipeline', etc.
    skill_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{SKILL_TABLE_NAME}.id', ondelete='CASCADE')
    )
    skill_version_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{SKILL_VERSION_TABLE_NAME}.id')
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    skill: Mapped['Skill'] = relationship('Skill', lazy='joined')
    skill_version: Mapped['SkillVersion'] = relationship('SkillVersion', lazy='joined')
