from queue import Empty
from typing import List, Optional

from sqlalchemy import func

from tools import db, rpc_tools
from ..models.all import Application, ApplicationVersion
from ..models.pd.authors import TrendingAuthorModel
from ..utils.authors import get_authors_data


def get_trending_authors(project_id: int, limit: int = 5, entity_name: str = 'application') -> List[dict]:

    try:
        Like = rpc_tools.RpcMixin().rpc.timeout(2).social_get_like_model()
    except Empty:
        return []

    with db.with_project_schema_session(project_id) as session:

        # Likes subquery
        likes_subquery = Like.query.filter(
            Like.project_id == project_id,
            Like.entity == entity_name
        ).subquery()

        # Subquery
        application_likes_subq = (
            session.query(Application.id, func.count(likes_subquery.c.user_id).label('likes'))
            .outerjoin(likes_subquery, likes_subquery.c.entity_id == Application.id)
            .group_by(Application.id)
            .subquery()
        )

        # Main query
        sq_result = (
            session.query(
                ApplicationVersion.application_id,
                ApplicationVersion.author_id,
                application_likes_subq.c.likes
            )
            .outerjoin(
                application_likes_subq, application_likes_subq.c.id == ApplicationVersion.application_id
            )
            .group_by(
                ApplicationVersion.application_id,
                ApplicationVersion.author_id,
                application_likes_subq.c.likes
            )
            .subquery()
        )

        result = (
            session.query(sq_result.c.author_id, func.sum(sq_result.c.likes))
            .group_by(sq_result.c.author_id)
            .order_by(func.sum(sq_result.c.likes).desc())
            .limit(limit)
            .all()
        )

        authors = get_authors_data([row[0] for row in result])

        trending_authors = []
        for row in result:
            for author in authors:
                if author['id'] == row[0]:
                    author_data = TrendingAuthorModel(**author)
                    author_data.likes = int(row[1])
                    trending_authors.append(author_data)
                    break

    return trending_authors


def empty_str_to_none(value: Optional[str]) -> Optional[str]:
    if value is not None and value == '':
        return None
    return value

def field_is_not_empty(value: Optional[str]) -> bool:
    return bool(empty_str_to_none(value))


def deep_update(mapping: dict, *updating_mappings: dict) -> dict:
    """Deep update a dictionary with one or more dictionaries."""
    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for key, value in updating_mapping.items():
            if key in updated_mapping and isinstance(updated_mapping[key], dict) and isinstance(value, dict):
                updated_mapping[key] = deep_update(updated_mapping[key], value)
            else:
                updated_mapping[key] = value
    return updated_mapping
