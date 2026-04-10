from datetime import datetime
from typing import Tuple, List, Optional, Any

from flask_sqlalchemy.query import Query
from sqlalchemy import Subquery, func, desc, asc

from tools import rpc_tools, db, auth
from pylon.core.tools import log


def get_like_model():
    """Helper to fetch Like model via RPC - call once and reuse."""
    return rpc_tools.RpcMixin().rpc.timeout(2).social_get_like_model()


def add_my_liked(
        original_query,
        project_id: int,
        entity: db.Base,
        filter_results: bool = False,
        like_model: Any = None,
) -> Tuple[Query, List[str]]:
    """

    :param original_query:
    :param project_id:
    :param entity:
    :param filter_results: will filter results with only liked by user
    :param like_model: pre-fetched Like model to avoid repeated RPC calls
    :return:
    """
    Like = like_model if like_model is not None else get_like_model()

    user_likes_subquery: Subquery = (
        db.session.query(
            Like.entity_id,
            func.coalesce(func.bool_or(
                Like.user_id == auth.current_user().get('id')), False
            ).label('user_liked')
        )
        .filter(
            Like.entity == entity.likes_entity_name,
            Like.project_id == project_id,
            # Like.user_id == auth.current_user().get('id')
        )
        .group_by(Like.entity_id)
        .subquery()
    )
    mutated_query = (
        original_query
        .outerjoin(user_likes_subquery, user_likes_subquery.c.entity_id == entity.id)
        .add_columns(user_likes_subquery.c.user_liked)
    )
    if filter_results:
        mutated_query = mutated_query.filter(user_likes_subquery.c.user_liked == True)
    return mutated_query, ['is_liked']


# .add_columns(func.coalesce(
#     func.bool_or(likes_subquery.c.user_id == user_id),
#     False
# ).label('is_liked'))


def add_likes(
        original_query,
        project_id: int,
        entity: db.Base,
        sort_by_likes: bool = False,
        sort_order: str = 'desc',
        like_model: Any = None,
) -> Tuple[Query, List[str]]:
    Like = like_model if like_model is not None else get_like_model()

    likes_subquery: Subquery = (
        db.session.query(
            Like.entity_id,
            func.count(Like.id).label('likes_count')
        )
        .filter(
            Like.entity == entity.likes_entity_name,
            Like.project_id == project_id
        )
        .group_by(Like.entity_id)
        .subquery()
    )
    # if my_liked:
    #     likes_subquery.filter(
    #         Like.user_id == auth.current_user().get("id")
    #     )

    mutated_query = (
        original_query
        .outerjoin(likes_subquery, likes_subquery.c.entity_id == entity.id)
        .add_columns(func.coalesce(likes_subquery.c.likes_count, 0))
    )
    if sort_by_likes:
        sort_fn = desc if sort_order != "asc" else asc
        mutated_query = mutated_query.order_by(sort_fn(func.coalesce(likes_subquery.c.likes_count, 0)))

    return mutated_query, ['likes']


def add_trending_likes(
        original_query,
        project_id: int,
        entity: db.Base,
        trend_period: Optional[Tuple[datetime, datetime]],
        filter_results: bool = False,
        like_model: Any = None,
) -> Tuple[Query, List[str]]:
    """

    :param original_query:
    :param project_id:
    :param entity:
    :param trend_period:
    :param filter_results: if true will filter results with trending likes > 0
    :param like_model: pre-fetched Like model to avoid repeated RPC calls
    :return:
    """
    Like = like_model if like_model is not None else get_like_model()

    trend_subquery = (
        db.session.query(
            Like.entity_id,
            func.count(Like.id).label('trend_likes_count')
        )
        .filter(
            Like.entity == entity.likes_entity_name,
            Like.project_id == project_id,
            Like.created_at.between(*trend_period),
        )
        .group_by(Like.entity_id)
        .subquery()
    )
    # if my_liked:
    #     trend_subquery.filter(
    #         Like.user_id == auth.current_user().get("id")
    #     )

    # Modify the original query to include the subquery for likes count within the specified period
    mutated_query = (
        original_query
        .outerjoin(trend_subquery, trend_subquery.c.entity_id == entity.id)
        .add_columns(trend_subquery.c.trend_likes_count)
    )
    if filter_results:
        mutated_query = mutated_query.filter(trend_subquery.c.trend_likes_count > 0)

    return mutated_query, ['trending_likes']
