from typing import Literal

from pydantic import BaseModel, ConfigDict


class TextMessageItemBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    content: str


class TextMessageItemDetail(TextMessageItemBase):
    id: int
    item_type: Literal['text_message']
