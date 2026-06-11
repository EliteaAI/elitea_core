from queue import Empty
from typing import List

from tools import auth, rpc_tools

from ..models.pd.collection_base import AuthorDetailModel


def get_authors_data(author_ids: List[int]) -> List[dict]:
    try:
        users_data: list = auth.list_users(user_ids=author_ids)
    except RuntimeError:
        return []
    try:
        social_data: list = rpc_tools.RpcMixin().rpc.timeout(2).social_get_users(author_ids)
    except (Empty, KeyError):
        social_data = []

    social_by_user = {s['user_id']: s for s in social_data}
    for user in users_data:
        social_user = social_by_user.get(user['id'])
        if social_user:
            user['avatar'] = social_user.get('avatar')

    return users_data


def get_author_data(author_id: int) -> dict:
    try:
        author_data = auth.get_user(user_id=author_id)
    except RuntimeError:
        return {}
    try:
        social_data = rpc_tools.RpcMixin().rpc.timeout(2).social_get_user(author_data['id'])
    except (Empty, KeyError):
        social_data = {}
    social_data.update(author_data)
    return AuthorDetailModel(**social_data).model_dump()

