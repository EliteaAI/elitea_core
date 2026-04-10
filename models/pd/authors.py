from datetime import datetime
from typing import Optional

from .collection_base import AuthorBaseModel


class TrendingAuthorModel(AuthorBaseModel):
    last_login: Optional[datetime]
    likes: int = 0


class AuthorDetailModel(AuthorBaseModel):
    id: Optional[int] = None
    title: Optional[str]
    description: Optional[str]
    public_prompts: int = 0
    total_prompts: int = 0
    public_collections: int = 0
    total_collections: int = 0
    rewards: int = 0
