from typing import List

from pydantic import BaseModel, ConfigDict


class ApplicationSearchModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str


class MultipleApplicationSearchModel(BaseModel):
    items: List[ApplicationSearchModel]
