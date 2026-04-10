from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict


class FolderBase(BaseModel):
    name: str
    meta: Optional[Dict] = Field(default_factory=dict)


class FolderCreate(FolderBase):
    owner_id: int
    position: Optional[int] = None


class FolderUpdate(BaseModel):
    name: Optional[str] = None
    meta: Optional[Dict] = Field(default_factory=dict)
    position: Optional[int] = None
    neighbor_above_id: Optional[int] = None  # ID of folder above drop target (for rebalancing)
    neighbor_below_id: Optional[int] = None  # ID of folder below drop target (for rebalancing)


class FolderDetails(FolderBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    owner_id: int
    position: Optional[int] = None


class FolderList(FolderDetails):
    pass