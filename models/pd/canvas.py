from uuid import UUID
from typing import Optional, List, Literal
from datetime import datetime

import redis
from pylon.core.tools import log
from tools import this

from pydantic import BaseModel, ConfigDict, computed_field, model_validator

from .. import config as c
from ..enums.all import CanvasTypes, ParticipantTypes
from ...utils.canvas_utils import get_canvas_authors_key
from ...utils.participant_utils import get_entity_details


class CanvasItemVersionBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    canvas_content: str
    code_language: Optional[str] = None
    created_at: datetime


class CanvasItemVersionCreate(BaseModel):
    canvas_content: str
    code_language: Optional[str] = None


class CanvasItemVersionDetail(CanvasItemVersionBase):
    pass


class CanvasItemBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: UUID | str
    name: str
    canvas_type: CanvasTypes
    created_at: datetime
    updated_at: Optional[datetime] = None
    meta: dict


class CanvasItemDetail(CanvasItemBase):
    latest_version: Optional[CanvasItemVersionDetail] = None
    item_type: Literal['canvas_message']

    @computed_field
    def editors(self) -> list:
        try:
            from flask import request
            if project_id := request.view_args.get('project_id'):
                client = this.module.get_redis_client()
                canvas_authors_key: str = get_canvas_authors_key(project_id, self.uuid)
                redis_author_values = client.smembers(canvas_authors_key)

                canvas_author_details: list[dict] = []
                try:
                    for author_id in redis_author_values:
                        canvas_author_details.append(
                            get_entity_details(
                                entity_name=ParticipantTypes.user,
                                entity_meta={'id': int(author_id)}
                            )
                        )
                except ValueError:
                    log.error(f"Failed to convert value to int: {author_id}")
                return canvas_author_details
        except RuntimeError:
            ...
        return []


class CanvasItemEditPayload(BaseModel):
    name: Optional[str] = None
    # canvas_type: Optional[CanvasTypes] = None
    code_language: Optional[str] = None


class CanvasItemCreatePayload(BaseModel):
    message_group_id: int
    message_item_id: int
    name: str
    canvas_type: CanvasTypes
    meta: Optional[dict] = {}
    canvas_content_starts_at: int
    canvas_content_ends_at: int
    code_language: Optional[str] = None

    @model_validator(mode="after")
    def check_canvas_content_range(self):
        if self.canvas_content_starts_at > self.canvas_content_ends_at:
            raise ValueError("canvas_content_starts_at must be <= canvas_content_ends_at")
        return self
