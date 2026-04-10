from datetime import datetime
from collections import defaultdict
from typing import List, Optional

from flask import request
from pydantic import BaseModel, ValidationError
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, DateTime, func, ForeignKey

from tools import api_tools, auth, db, serialize, db_tools, config as c

from ...models.enums.all import ToolTypes, EntityTypes
from ...models.all import ApplicationVersion
from ...models.elitea_tools import EliteATool, EntityToolMapping

from pylon.core.tools import log


class ApplicationTool(db_tools.AbstractBaseMixin, db.Base):
    """
    Deprecated (left for the migration "merge_tools" API)
    """
    __tablename__ = 'application_tools'
    __table_args__ = (
        {'schema': c.POSTGRES_TENANT_SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    application_version_id: Mapped[int] = mapped_column(
        ForeignKey(f'{c.POSTGRES_TENANT_SCHEMA}.{ApplicationVersion.__tablename__}.id'))
    application_version: Mapped['ApplicationVersion'] = relationship(lazy=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=True, onupdate=func.now())

    type: Mapped[ToolTypes] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(String(1024), nullable=True)
    settings: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)


class MergeToolsPayload(BaseModel):
    project_ids: Optional[List[int]] = None
    flush: bool = False


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.merge_tools.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self):
        payload = dict(request.json)

        try:
            merge_tools_data = MergeToolsPayload.parse_obj(payload)
        except ValidationError as e:
            return e.errors(), 400

        def get_all_project_ids():
            return [
                i['id'] for i in self.module.context.rpc_manager.call.project_list(
                    filter_={'create_success': True}
                )
            ]

        project_ids = merge_tools_data.project_ids or get_all_project_ids()
        project_ids.sort()
        results = list()
        errors = list()

        for pid in project_ids:
            with db.get_session(pid) as session:
                if merge_tools_data.flush:
                    session.query(EntityToolMapping).delete()
                    session.query(EliteATool).delete()
                try:
                    query = session.query(ApplicationTool).order_by(ApplicationTool.created_at)
                    seen_tools = defaultdict(list)
                    inserted_tools = []
                    inserted_mappings = []

                    for tool in query.yield_per(100):
                        settings_hash = ''.join(sorted(str(tool.settings)))
                        settings_hash = hash(settings_hash)
                        identifier = (tool.name, tool.description, tool.type, settings_hash)
                        seen_tools[identifier].append(tool)

                    for tools in seen_tools.values():
                        if tools:
                            first_tool = tools[0]
                            new_tool = EliteATool(
                                name=first_tool.name,
                                description=first_tool.description,
                                settings=first_tool.settings,
                                type=first_tool.type,
                                created_at=first_tool.created_at,
                                # updated_at=first_tool.updated_at,
                                author_id=first_tool.application_version.author_id,
                            )
                            session.add(new_tool)
                            session.flush()
                            inserted_tools.append(new_tool)

                            pending_mappings = set()
                            for tool in tools:
                                pending_mappings.add((
                                    tool.application_version.application_id,
                                    tool.application_version_id,
                                ))
                            for i in pending_mappings:
                                entity_id, entity_version_id = i
                                new_mapping = EntityToolMapping(
                                    tool_id=new_tool.id,
                                    entity_id=entity_id,
                                    entity_version_id=entity_version_id,
                                    entity_type=EntityTypes.agent.value,
                                )
                                inserted_mappings.append(new_mapping)
                                session.add(new_mapping)

                    session.commit()
                    results.extend(inserted_tools)
                    results.extend(inserted_mappings)
                except Exception as e:
                    session.rollback()
                    log.error(f'Project ID {pid}, error: {str(e)}')
                    errors.append({'project_id': pid, 'error': str(e)})

        return {'results': serialize(results), 'errors': serialize(errors)}, 201


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '',
    ])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI,
    }
