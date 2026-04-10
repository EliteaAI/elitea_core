import json
import os
import shutil
from collections import defaultdict
from datetime import datetime
from typing import List, Optional
try:
    from enum import StrEnum
except ImportError:
    from enum import Enum
    class StrEnum(str, Enum):
        pass

from pydantic import BaseModel
from pylon.core.tools import log, web
from tools import auth, db, serialize, db_tools, config as c, rpc_tools, this
from plugins.admin.tasks.logs import make_logger

from ..models.all import Collection
from ..models.all import AbstractLikesMixin, Tag
from ..models.all import Application, ApplicationVersion, ApplicationVariable, ApplicationVersionTagAssociation
from ..models.enums.all import PublishStatus

from sqlalchemy import Integer, String, DateTime, func, ForeignKey, JSON, Table, Column, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict


class PromptVersionType(StrEnum):
    chat = 'chat'
    structured = 'structured'
    freeform = 'freeform'


class MessageRoles(StrEnum):
    system = 'system'
    user = 'user'
    assistant = 'assistant'


class CollectionPatchOperations(StrEnum):
    add = 'add'
    remove = 'remove'


class Prompt(db_tools.AbstractBaseMixin, db.Base, AbstractLikesMixin):
    __tablename__ = 'prompts'
    __table_args__ = (
        UniqueConstraint('shared_owner_id', 'shared_id', name='_shared_origin'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )
    likes_entity_name: str = 'prompt'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=True)
    versions: Mapped[List['PromptVersion']] = relationship(back_populates='prompt', lazy=True,
                                                           cascade='all, delete',
                                                           order_by='PromptVersion.created_at.desc()')
    owner_id: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    shared_owner_id: Mapped[int] = mapped_column(Integer, nullable=True)
    shared_id: Mapped[int] = mapped_column(Integer, nullable=True)
    collections: Mapped[list] = mapped_column(JSONB, nullable=True, default=list)
    new_agent_id: Mapped[int] = mapped_column(Integer, nullable=True)

    def get_latest_version(self):
        return next(version for version in self.versions if version.name == 'latest')


class PromptVersion(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'prompt_versions'
    __table_args__ = (
        UniqueConstraint('shared_owner_id', 'shared_id', name='_version_shared_origin'),
        UniqueConstraint('prompt_id', 'name', name='_prompt_name_uc'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    prompt_id: Mapped[int] = mapped_column(ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.prompts.id'), index=True)
    prompt: Mapped['Prompt'] = relationship(back_populates='versions', lazy=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    commit_message: Mapped[str] = mapped_column(String, nullable=True)
    type: Mapped[PromptVersionType] = mapped_column(String(64), nullable=False, default=PromptVersionType.chat)
    status: Mapped[PublishStatus] = mapped_column(String, nullable=False, default=PublishStatus.draft, index=True)
    context: Mapped[str] = mapped_column(String, nullable=True)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False)
    variables: Mapped[List['PromptVariable']] = relationship(back_populates='prompt_version', lazy=True,
                                                             cascade='all, delete-orphan')
    messages: Mapped[List['PromptMessage']] = relationship(back_populates='prompt_version', lazy=True,
                                                           cascade='all, delete-orphan', order_by='PromptMessage.id')
    tags: Mapped[List[Tag]] = relationship(secondary=lambda: PromptVersionTagAssociation,
                                           backref='prompt_version', lazy='joined')
    model_settings: Mapped[dict] = mapped_column(JSON, nullable=True)
    embedding_settings: Mapped[dict] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    # reference fields to origin
    shared_owner_id: Mapped[int] = mapped_column(Integer, nullable=True)
    shared_id: Mapped[int] = mapped_column(Integer, nullable=True)
    conversation_starters: Mapped[dict] = mapped_column(JSON, default=list)
    welcome_message: Mapped[str] = mapped_column(String, default='')
    meta: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSONB), default=dict)
    new_agent_version_id: Mapped[int] = mapped_column(Integer, nullable=True)


class PromptVariable(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'prompt_variables'
    __table_args__ = (
        UniqueConstraint('prompt_version_id', 'name', name='_prompt_version_variable_name_uc'),
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    prompt_version_id: Mapped[int] = mapped_column(ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.prompt_versions.id'))
    prompt_version: Mapped['PromptVersion'] = relationship(back_populates='variables', lazy=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[str] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())


class PromptMessage(db_tools.AbstractBaseMixin, db.Base):
    __tablename__ = 'prompt_messages'
    __table_args__ = ({'schema': c.POSTGRES_TENANT_SCHEMA},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    prompt_version_id: Mapped[int] = mapped_column(ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.prompt_versions.id'))
    prompt_version: Mapped['PromptVersion'] = relationship(back_populates='messages', lazy=True)
    role: Mapped[MessageRoles] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=True)
    content: Mapped[str] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())
    custom_content: Mapped[dict] = mapped_column(JSON, nullable=True)
    # todo: add order


PromptVersionTagAssociation = Table(
    'prompt_version_tag_association',
    db.Base.metadata,
    Column('version_id', ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.prompt_versions.id'), index=True),
    Column('tag_id', ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{Tag.__tablename__}.id'), index=True),
    schema=c.POSTGRES_TENANT_SCHEMA
)


class EliminatePromptPayload(BaseModel):
    project_ids: Optional[List[int]] = []
    rollback: Optional[bool] = False
    prompt_icon_path: Optional[str] = '/data/static/prompt_icon'


def prompt_2_agent_migration(*args, payload: dict = None, **kwargs) -> dict:
    """Migrate legacy prompts to agents for all or specific projects. No params required (runs on all projects by default)."""
    with make_logger() as log:
        log.info(f"Starting prompt to agent migration")
        if payload is None:
            eliminate_prompt_payload = EliminatePromptPayload.model_construct()
        else:
            eliminate_prompt_payload = EliminatePromptPayload.model_validate(payload)

        log.info(f"Payload: {eliminate_prompt_payload}")

        rpc = rpc_tools.RpcMixin().rpc.call

        def get_all_project_ids():
            return [
                i['id'] for i in rpc.project_list(
                    filter_={'create_success': True}
                )
            ]

        project_ids = eliminate_prompt_payload.project_ids or get_all_project_ids()
        project_ids.sort()
        new_agent_ids = defaultdict(list)
        errors = list()

        application_model = Application
        application_version_model = ApplicationVersion
        application_variable_model = ApplicationVariable
        application_tag_model = ApplicationVersionTagAssociation

        for pid in project_ids:
            with db.get_session(pid) as session:
                if eliminate_prompt_payload.rollback:
                    delete_application_query = session.query(Prompt.new_agent_id)
                    delete_application_ids = session.scalars(delete_application_query).all()
                    if delete_application_ids:
                        for application_id in delete_application_ids:
                            rpc.applications_delete_application(pid, application_id)

                        session.query(PromptVersion).filter(
                            PromptVersion.prompt_id.in_(session.query(Prompt.id).filter(
                                Prompt.new_agent_id.in_(delete_application_ids)
                            ).subquery())
                        ).update(
                            {"new_agent_version_id": None},
                        )
                        session.query(Prompt).filter(
                            Prompt.new_agent_id.in_(delete_application_ids)
                        ).update(
                            {"new_agent_id": None},
                        )
                        session.commit()
                    continue
                try:
                    # TODO comment and run migration
                    session.execute(
                        text(f'ALTER TABLE p_{pid}.prompts ADD COLUMN IF NOT EXISTS new_agent_id INTEGER')
                    )
                    session.execute(
                        text(f'ALTER TABLE p_{pid}.prompt_versions ADD COLUMN IF NOT EXISTS new_agent_version_id INTEGER')
                    )
                    session.commit()
                    prompt_query = session.query(Prompt)
                    collection_query = session.query(Collection)
                    application_tags = []
                    for prompt in prompt_query.yield_per(100):
                        if prompt.new_agent_id:
                            continue
                        application = application_model(
                            name=prompt.name,
                            description=prompt.description,
                            created_at=prompt.created_at,
                            shared_id=prompt.shared_id,
                            owner_id=prompt.owner_id,
                            shared_owner_id=prompt.shared_owner_id,
                            collections=prompt.collections,
                        )
                        session.add(application)
                        for prompt_version in prompt.versions:
                            llm_settings = prompt_version.model_settings
                            model_settings = llm_settings.pop('model')
                            llm_settings['model_name'] = model_settings.get('model_name')
                            llm_settings['integration_uid'] = model_settings.get('integration_uid')
                            new_meta = prompt_version.meta or {}
                            if "icon_meta" in new_meta and new_meta["icon_meta"]:
                                new_meta["icon_meta"]["url"] = new_meta["icon_meta"]["url"].replace(
                                    "prompt_lib/prompt_icon", "applications/application_icon"
                                )
                            application_version = application_version_model(
                                name=prompt_version.name,
                                author_id=prompt_version.author_id,
                                status=prompt_version.status,
                                created_at=prompt_version.created_at,
                                shared_id=prompt_version.shared_id,
                                shared_owner_id=prompt_version.shared_owner_id,
                                conversation_starters=prompt_version.conversation_starters,
                                welcome_message=prompt_version.welcome_message,
                                instructions=prompt_version.context,
                                llm_settings=llm_settings,
                                meta=new_meta,
                                application=application,
                            )
                            application_variables = [
                                application_variable_model(
                                    application_version=application_version,
                                    application_version_id=application_version.id,
                                    name=prompt_var.name,
                                    value=prompt_var.value,
                                    created_at=prompt_var.created_at,
                                    updated_at=prompt_var.updated_at,
                                ) for prompt_var in prompt_version.variables
                            ]
                            application_version.variables = application_variables
                            session.add(application_version)
                            session.flush()
                            application_tags.extend([
                                {'version_id': application_version.id, 'tag_id': prompt_tag.id}
                                for prompt_tag in prompt_version.tags
                            ])
                            session.execute(
                                text(f"""
                                        UPDATE p_{pid}.prompt_versions
                                        SET new_agent_version_id = :new_agent_version_id
                                        WHERE id = :prompt_version_id
                                    """),
                                {
                                    'new_agent_version_id': application_version.id,
                                    'prompt_version_id': prompt_version.id
                                }
                            )
                            # First query: Update the `chat_participant_mapping` table
                            # DO NOT CHANGE ORDER OF QUERIES
                            session.execute(
                                text(f"""
                                    UPDATE p_{pid}.chat_participant_mapping
                                    SET entity_settings = jsonb_build_object(
                                        'icon_meta', entity_settings->'icon_meta',
                                        'variables', entity_settings->'variables',
                                        'version_id', {application_version.id},
                                        'llm_settings', jsonb_build_object(
                                            'top_k', entity_settings->'model_settings'->'top_k',
                                            'top_p', entity_settings->'model_settings'->'top_p',
                                            'max_tokens', entity_settings->'model_settings'->'max_tokens',
                                            'model_name', entity_settings->'model_settings'->'model'->>'model_name',
                                            'temperature', entity_settings->'model_settings'->'temperature',
                                            'integration_uid', entity_settings->'model_settings'->'model'->>'integration_uid'
                                        ),
                                        'chat_history_template', COALESCE(entity_settings->>'chat_history_template', 'all')
                                    )
                                    WHERE participant_id IN (
                                        SELECT id
                                        FROM p_{pid}.chat_participants
                                        WHERE (entity_meta->>'id')::int = {prompt.id} AND entity_name = 'prompt'
                                    );
                                """)
                            )
                            # Second query: Update the `chat_participants` table
                            session.execute(
                                text(f"""
                                            UPDATE p_{pid}.chat_participants
                                            SET entity_name = 'application',
                                                entity_meta = jsonb_set(entity_meta, '{{id}}', '{json.dumps(application.id)}'::jsonb)
                                            WHERE (entity_meta->>'id')::int = {prompt.id} AND entity_name = 'prompt';
                                        """)
                            )
                            # Update elitea_tools, change `type` column and modify the `settings` JSONB field
                            session.execute(
                                text(f"""
                                    UPDATE p_{pid}.elitea_tools
                                    SET type = 'application',
                                        settings = (
                                            settings
                                            || jsonb_build_object(
                                                'application_version_id', '{str(application_version.id)}'::jsonb,
                                                'application_id', '{str(application.id)}'::jsonb,
                                                'selected_tools', '[]'::jsonb
                                            )
                                        )
                                        - 'prompt_version_id'
                                        - 'prompt_id'
                                    WHERE type = 'prompt'
                                      AND settings->>'prompt_version_id' = '{str(prompt_version.id)}';
                                """)
                            )

                        session.query(Prompt).filter(Prompt.id == prompt.id).update(
                            {"new_agent_id": application.id}
                        )
                        new_agent_ids[pid].append(application.id)

                    # migrate collections
                    for collection in collection_query.yield_per(100):
                        if collection.prompts:
                            new_application_items = []
                            for prompt_entry in collection.prompts:
                                prompt_id = prompt_entry.get("id")
                                if prompt_id:
                                    result = session.query(Prompt.new_agent_id).filter(Prompt.id == prompt_id).first()

                                    if int(result.new_agent_id) not in new_agent_ids[pid]:
                                        continue

                                    # Check if the result exists and contains a valid new_agent_id
                                    if result and result.new_agent_id:
                                        new_application_items.append({
                                            "id": result.new_agent_id,
                                            "owner_id": prompt_entry.get("owner_id")
                                        })

                            new_application_items.extend(list(collection.applications))
                            collection.applications = new_application_items
                            session.add(collection)

                    if application_tags:
                        session.execute(application_tag_model.insert().values(application_tags))
                    session.commit()
                except Exception as e:
                    session.rollback()
                    log.error(f'Project ID {pid}, error: {str(e)}')
                    errors.append({'project_id': pid, 'error': str(e)})

        for pid in project_ids:
            with db.get_session(pid) as session:
                try:
                    # Migrate forks
                    prompt_version_query = session.query(PromptVersion)
                    for prompt_version in prompt_version_query.all():
                        # Check if the prompt version has fork metadata in its meta field
                        if prompt_version.meta and "parent_project_id" in prompt_version.meta:
                            parent_project_id = prompt_version.meta["parent_project_id"]
                            parent_entity_id = prompt_version.meta["parent_entity_id"]
                            parent_entity_version_id = prompt_version.meta["parent_entity_version_id"]
                            parent_author_id = prompt_version.meta["parent_author_id"]

                            # Fetch the parent application's new_agent_id and new_agent_version_id from the parent project
                            parent_new_agent_id = None
                            parent_new_agent_version_id = None
                            with db.get_session(parent_project_id) as parent_session:
                                # Fetch the parent prompt's new_agent_id
                                parent_result = parent_session.query(Prompt.new_agent_id).filter(
                                    Prompt.id == parent_entity_id
                                ).first()
                                log.debug(f'{parent_result=}')

                                # Fetch the parent prompt version's new_agent_version_id
                                parent_version_result = parent_session.query(PromptVersion.new_agent_version_id).filter(
                                    PromptVersion.id == parent_entity_version_id
                                ).first()
                                log.debug(f'{parent_version_result=}')

                                if parent_result and parent_result.new_agent_id:
                                    parent_new_agent_id = parent_result.new_agent_id
                                    log.debug(f'{parent_new_agent_id=}')

                                if parent_version_result and parent_version_result.new_agent_version_id:
                                    parent_new_agent_version_id = parent_version_result.new_agent_version_id
                                    log.debug(f'{parent_new_agent_version_id=}')

                            # If the parent application exists, update the meta field of the current application version
                            if parent_new_agent_id and parent_new_agent_version_id:
                                # Fetch the existing application version for the current prompt version
                                new_agent_version_id_q = session.execute(
                                    text(f"""
                                        SELECT new_agent_version_id
                                        FROM p_{pid}.prompt_versions
                                        WHERE id = :prompt_version_id
                                    """),
                                    {'prompt_version_id': prompt_version.id}
                                ).fetchone()
                                current_result = parent_session.execute(
                                    text(f"""
                                        SELECT id, meta
                                        FROM p_{pid}.application_versions
                                        WHERE id = :current_agent_version_id
                                    """),
                                    {'current_agent_version_id': new_agent_version_id_q.new_agent_version_id}
                                ).fetchone()

                                log.debug(f'{current_result=}')

                                if current_result:
                                    current_agent_version_id = current_result.id
                                    current_meta = current_result.meta or {}

                                    # Update the meta field with parent application version information
                                    new_meta = {
                                        "parent_entity_id": parent_new_agent_id,
                                        "parent_entity_version_id": parent_new_agent_version_id,
                                        "parent_author_id": parent_author_id,
                                        "parent_project_id": parent_project_id,
                                    }
                                    current_meta.update(new_meta)
                                    log.debug(f'{new_meta=}')

                                    session.execute(
                                        text(f"""
                                                        UPDATE p_{pid}.application_versions
                                                        SET meta = :meta
                                                        WHERE id = :current_agent_version_id
                                                    """),
                                        {
                                            'meta': json.dumps(current_meta),
                                            'current_agent_version_id': current_agent_version_id
                                        }
                                    )
                    session.commit()
                except Exception as e:
                    session.rollback()
                    log.error(f'Project ID {pid}, error: {str(e)}')
                    errors.append({'project_id': pid, 'error': str(e)})

        for project_id in project_ids:
            prompt_path = eliminate_prompt_payload.prompt_icon_path
            log.debug(f"prompt_path: {prompt_path}")
            application_path = prompt_path.replace('prompt_icon', 'application_icon')
            prompt_project_path = os.path.join(prompt_path, str(project_id))
            application_project_path = os.path.join(application_path, str(project_id))

            if not os.path.isdir(prompt_project_path):
                continue

            if not os.path.exists(application_project_path):
                os.makedirs(application_project_path)
                log.debug(f"Created project folder in application_icon: {application_project_path}")

            for file_name in os.listdir(prompt_project_path):
                prompt_file_path = os.path.join(prompt_project_path, file_name)
                application_file_path = os.path.join(application_project_path, file_name)

                if not os.path.isfile(prompt_file_path):
                    continue

                if os.path.exists(application_file_path):
                    log.debug(f"File already exists, skipping: {application_file_path}")
                    continue

                shutil.copy2(prompt_file_path, application_file_path)
                log.debug(f"Copied {prompt_file_path} to {application_file_path}")

        log.info(f"Finished: {serialize({'new_agent_ids': new_agent_ids, 'errors': errors})}")

        return serialize({'new_agent_ids': new_agent_ids, 'errors': errors})