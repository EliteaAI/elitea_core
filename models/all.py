import uuid
from datetime import datetime
from typing import List, Optional

from tools import db_tools, db, config as c
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, DateTime, func, ForeignKey, JSON, Table, Column, UniqueConstraint, MetaData
from sqlalchemy.ext.mutable import MutableDict

from .enums.all import ToolTypes, AgentTypes, PublishStatus, ToolEntityTypes
from ..models.elitea_tools import EliteATool, EntityToolMapping


# Merged from promptlib_shared.models.all
class AbstractLikesMixin:
    @property
    def likes_entity_name(self):
        raise NotImplementedError


class Tag(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'tags'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    data: Mapped[dict] = mapped_column(JSON, nullable=True)


class Collection(db_tools.AbstractBaseMixin, db.Base, AbstractLikesMixin):
    __tablename__ = "prompt_collections"
    __table_args__ = (
        UniqueConstraint('shared_owner_id', 'shared_id', name='_collection_shared_origin'),
        {"schema": c.POSTGRES_TENANT_SCHEMA},
    )
    likes_entity_name: str = 'collection'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=True)
    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False)
    prompts: Mapped[dict] = mapped_column(JSONB, nullable=True)
    datasources: Mapped[dict] = mapped_column(JSONB, nullable=True)
    applications: Mapped[dict] = mapped_column(JSONB, nullable=True)
    status: Mapped[PublishStatus] = mapped_column(String, nullable=False, default=PublishStatus.draft)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    # reference fields to origin
    shared_owner_id: Mapped[int] = mapped_column(Integer, nullable=True)
    shared_id: Mapped[int] = mapped_column(Integer, nullable=True)
    meta: Mapped[dict] = mapped_column(JSON, default=dict)


class Application(db_tools.AbstractBaseMixin, db.Base, AbstractLikesMixin):
    __tablename__ = 'applications'
    __table_args__ = (
        UniqueConstraint('shared_owner_id', 'shared_id', name='application_shared_origin'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    likes_entity_name: str = 'application'
    pins_entity_name: str = 'application'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(String(2304), nullable=True)
    icon: Mapped[str] = mapped_column(String, nullable=True)

    versions: Mapped[List['ApplicationVersion']] = relationship(back_populates='application', lazy=True,
                                                                cascade='all, delete')
    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    shared_owner_id: Mapped[int] = mapped_column(Integer, nullable=True)
    shared_id: Mapped[int] = mapped_column(Integer, nullable=True)
    collections: Mapped[list] = mapped_column(JSONB, nullable=True, default=list)

    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)

    webhook_secret: Mapped[str] = mapped_column(String, nullable=True)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)

    def get_default_version(self) -> Optional['ApplicationVersion']:
        """Get the default version of the application.
        
        First checks meta.default_version_id, then falls back to 'base' version.
        Returns None if neither is found.
        """
        # Try to get version by default_version_id from meta
        default_version_id = self.meta.get('default_version_id') if self.meta else None
        if default_version_id:
            try:
                return next(version for version in self.versions if version.id == default_version_id)
            except StopIteration:
                # default_version_id is invalid, fall through to base version
                pass
        
        # Fall back to base version
        try:
            return next(version for version in self.versions if version.name == 'base')
        except StopIteration:
            return None

    def get_latest_version(self) -> Optional['ApplicationVersion']:
        """Deprecated: Use get_default_version() instead.
        
        Maintained for backward compatibility.
        """
        return self.get_default_version()


class ApplicationVersion(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'application_versions'
    __table_args__ = (
        UniqueConstraint('shared_owner_id', 'shared_id', name='application_version_shared_origin'),
        UniqueConstraint('application_id', 'name', name='_application_version_name_uc'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    application_id: Mapped[int] = mapped_column(ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{Application.__tablename__}.id'
    ), index=True)
    application: Mapped['Application'] = relationship(back_populates='versions', lazy=True)

    name: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[PublishStatus] = mapped_column(String, nullable=False, default=PublishStatus.draft, index=True)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False)

    tags: Mapped[List[Tag]] = relationship(secondary=lambda: ApplicationVersionTagAssociation,
                                           backref='application_versions', lazy='joined')
    uuid: Mapped[str] = mapped_column(UUID(as_uuid=True), unique=True, default=uuid.uuid4)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    shared_owner_id: Mapped[int] = mapped_column(Integer, nullable=True)
    shared_id: Mapped[int] = mapped_column(Integer, nullable=True)

    llm_settings: Mapped[dict] = mapped_column(JSON, default=dict)
    instructions: Mapped[str] = mapped_column(String, nullable=True)
    variables: Mapped[List['ApplicationVariable']] = relationship(back_populates='application_version', lazy=True,
                                                                  cascade='all, delete-orphan')
    conversation_starters: Mapped[dict] = mapped_column(JSON, default=list)
    welcome_message: Mapped[str] = mapped_column(String, default='')
    tools: Mapped[List['EliteATool']] = relationship(
        back_populates='application_versions',
        secondary=EntityToolMapping.__table__,
        primaryjoin=f'and_(ApplicationVersion.id == EntityToolMapping.entity_version_id, '
                    f'EntityToolMapping.entity_type == "{ToolEntityTypes.agent}")',
        secondaryjoin=f'and_(EliteATool.id == EntityToolMapping.tool_id, '
                      f'EntityToolMapping.entity_type == "{ToolEntityTypes.agent}")',
        lazy=True
    )
    tool_mappings: Mapped[List['EntityToolMapping']] = relationship(
        'EntityToolMapping',
        primaryjoin='and_(ApplicationVersion.id == foreign(EntityToolMapping.entity_version_id), '
                    'EntityToolMapping.entity_type == "agent")',
        lazy=True,
        viewonly=True,
        overlaps='tools'
    )
    agent_type: Mapped[str] = mapped_column(String, nullable=False, default=AgentTypes.openai.value)
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    pipeline_settings: Mapped[dict] = mapped_column(JSONB, default=dict)

    def to_dict(self):
        from ..utils.application_utils import apply_selected_tools_intersection
        
        app_version_dict = self.to_json()
        app_version_dict['tools'] = []
        app_version_dict['variables'] = []
        app_version_dict['tags'] = []
        
        for toolkit in self.tools:
            tool_dict = toolkit.to_json()
            app_version_dict['tools'].append(tool_dict)
        
        # Apply selected_tools intersection using pre-loaded tool_mappings
        apply_selected_tools_intersection(app_version_dict['tools'], self.tool_mappings)
        
        for variable in self.variables:
            app_version_dict['variables'].append(variable.to_json())
        for tag in self.tags:
            app_version_dict['tags'].append(tag.to_json())

        return app_version_dict


class ApplicationVariable(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'application_variables'
    __table_args__ = (
        UniqueConstraint('application_version_id', 'name', name='_application_version_variable_name_uc'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    application_version_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{ApplicationVersion.__tablename__}.id'))
    application_version: Mapped['ApplicationVersion'] = relationship(back_populates='variables', lazy=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[str] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())


ApplicationVersionTagAssociation = Table(
    'application_version_tag_association',
    db.Base.metadata,
    Column('version_id', ForeignKey(
        f'{c.POSTGRES_TENANT_SCHEMA}.{ApplicationVersion.__tablename__}.id'
    ), primary_key=True),
    Column('tag_id', ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{Tag.__tablename__}.id'), primary_key=True),
    schema=c.POSTGRES_TENANT_SCHEMA
)


# Merged from chat.models.all
CONVERSATION_TABLE_NAME = 'chat_conversations'


class SelectedConversations(db.Base):
    __tablename__ = 'chat_selected_conversations'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    conversation_id: Mapped[int] = mapped_column(
        ForeignKey(
            f'{c.POSTGRES_TENANT_SCHEMA}.{CONVERSATION_TABLE_NAME}.id',
            ondelete='CASCADE'
        ),
        nullable=False
    )
