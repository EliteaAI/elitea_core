from typing import List

from pydantic import BaseModel
from .tag import TagListModel


class MultipleTagListModel(BaseModel):
    items: List[TagListModel]