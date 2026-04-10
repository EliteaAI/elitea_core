import copy
import json
from collections import defaultdict
from datetime import datetime
from functools import partial
from itertools import chain
from typing import Dict, List, Optional, Union

from flask import request
from pydantic import ValidationError
from sqlalchemy import and_, desc, or_
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import exists
from werkzeug.datastructures import MultiDict

from pylon.core.tools import log
from tools import VaultClient, db, rpc_tools

from .collection_registry import (
    ENTITY_REG,
    get_entity_info_by_name,
)
from .like_utils import add_likes, add_my_liked, add_trending_likes, get_like_model
from .utils import set_columns_as_attrs
from .utils import get_public_project_id
from .authors import get_author_data, get_authors_data
from ..models.all import Collection
from ..models.enums.all import CollectionPatchOperations
from ..models.pd.collections import (
    CollectionDetailModel,
    CollectionItem,
    CollectionListModel,
    CollectionModel,
    CollectionPatchModel,
    CollectionShortDetailModel,
    CollectionPrivateTwinModel,
    PublishedCollectionDetailModel,
)
from ..models.enums.all import PublishStatus
from ..models.enums.all import AgentTypes
from ..models.pd.collection_base import TagBaseModel
from ..models.pd.entity import EntityListModel, PublishedEntityListModel
from ..utils.exceptions import (
    EntityAlreadyInCollectionError,
    EntityDoesntExist,
    EntityInaccessableError,
    EntityNotInCollectionError,
)
from ..utils.utils import add_public_project_id, get_entities_by_tags


def check_addability(owner_id: int, user_id: int):
    membership_check = rpc_tools.RpcMixin().rpc.call.admin_check_user_in_project
    try:
        ai_project_id = get_public_project_id()
    except Exception:
        ai_project_id = None
    return (
            ai_project_id and ai_project_id == int(owner_id)
    ) or membership_check(owner_id, user_id)


def get_entities_for_collection(
        entity_type,
        entity_version_type,
        collection_entities: List[Dict[int, int]],
        only_public: bool = False,
        extra_filter_params: Optional[Dict[str, str]] = None) -> list:
    Entity = entity_type
    EntityVersion = entity_version_type

    entities = []
    filters = []
    offset = request.args.get("offset", 0, type=int)
    limit = request.args.get("limit", 0, type=int)
    trend_period = request.args.get("trending_period")
    my_liked = request.args.get('my_liked', False)

    if author_id := request.args.get('author_id'):
        filters.append(Entity.versions.any(EntityVersion.author_id == author_id))

    if statuses := request.args.get('statuses'):
        statuses = statuses.split(',')
        filters.append(Entity.versions.any(EntityVersion.status.in_(statuses)))

    # filter applications by type: agent or pipeline
    agent_type_param = extra_filter_params and extra_filter_params.get("agent_type")
    if agent_type_param:
        pipeline_only_query = Entity.versions.any(
            EntityVersion.agent_type == AgentTypes.pipeline.value)

        if agent_type_param == 'agent':
            filters.append(~pipeline_only_query)
        elif agent_type_param == 'pipeline':
            filters.append(pipeline_only_query)

    # Search parameters
    if search := request.args.get('query'):
        filters.append(
            or_(
                Entity.name.ilike(f"%{search}%"),
                Entity.description.ilike(f"%{search}%")
            )
        )

    if only_public:
        filters.append(
            Entity.versions.any(EntityVersion.status == PublishStatus.published)
        )

    grouped_entities = group_by_project_id(collection_entities)
    for project_id, ids in grouped_entities.items():
        with db.with_project_schema_session(project_id) as session:

            if tags := request.args.get('tags'):
                # Filtering parameters
                if isinstance(tags, str):
                    tags = [int(tag) for tag in tags.split(',')]
                entities_subq = get_entities_by_tags(project_id, tags, Entity, EntityVersion)
                filters.append(Entity.id.in_(entities_subq))

            entity_query = session.query(Entity).filter(Entity.id.in_(ids), *filters)
            extra_columns = []

            if only_public:
                # OPTIMIZATION: Pre-fetch Like model once to avoid repeated RPC calls
                like_model = get_like_model()

                entity_query, new_columns = add_likes(
                    original_query=entity_query,
                    project_id=project_id,
                    entity=Entity,
                    like_model=like_model
                )
                extra_columns.extend(new_columns)

                if trend_period:
                    entity_query, new_columns = add_trending_likes(
                        original_query=entity_query,
                        project_id=project_id,
                        entity=Entity,
                        trend_period=trend_period,
                        filter_results=True,
                        like_model=like_model
                    )
                    extra_columns.extend(new_columns)

                entity_query, new_columns = add_my_liked(
                    original_query=entity_query,
                    project_id=project_id,
                    entity=Entity,
                    filter_results=my_liked,
                    like_model=like_model
                )
                extra_columns.extend(new_columns)

            q_result = entity_query.all()
            project_entities = list(set_columns_as_attrs(q_result, extra_columns))

            # user_map
            author_ids = set()
            for entity in project_entities:
                # entity = entity[0] if only_public else entity
                for version in entity.versions:
                    author_ids.add(version.author_id)
            users = get_authors_data(list(author_ids))
            user_map = {user['id']: user for user in users}

            for entity in project_entities:
                if only_public:
                    entity = PublishedEntityListModel.from_orm(entity)
                else:
                    entity = EntityListModel.from_orm(entity)
                entity.set_authors(user_map)
                entities.append(entity.model_dump(mode='json'))

    if limit:
        entities = entities[offset:limit + offset]
    return entities


def get_filter_collection_by_tags_condition(project_id: int, tags: List[int], session=None):
    entity_filters = []
    for entity_info in ENTITY_REG:
        entity_filters.extend(
            get_filter_collection_by_entity_tags_condition(
                project_id=project_id,
                tags=tags,
                entity_name=entity_info.entity_name,
                session=session
            )
        )

    return or_(*entity_filters)


def get_filter_collection_by_entity_tags_condition(project_id: int, tags: List[int], entity_name, session=None):
    session_created = False
    if session is None:
        session = db.get_project_schema_session(project_id)
        session_created = True

    entity_info = get_entity_info_by_name(entity_name)

    entity_filters = []
    kwargs = {
        'entity_type': entity_info.get_entity_type(),
        'entity_version_type': entity_info.get_entity_version_type()
    }
    kwargs['session'] = session
    kwargs['subquery'] = False

    entity_ids = get_entities_by_tags(project_id, tags, **kwargs)

    if not entity_ids:
        return entity_filters

    for id_ in entity_ids:
        entity_value = {
            "owner_id": project_id,
            "id": id_
        }
        entity_filters.append(entity_info.get_entities_field(Collection).contains([entity_value]))

    if session_created:
        session.close()

    return entity_filters


def list_collections(
        project_id: int,
        args: MultiDict[str, str] | dict | None = None,
        with_likes: bool = True,
        my_liked: bool = False
):
    if my_liked:
        my_liked = with_likes

    if args is None:
        args = dict()

    if type(args) == dict:
        # Pagination parameters
        limit = args.get("limit", 10)
        offset = args.get("offset", 0)

        # Sorting parameters
        sort_by = args.get("sort_by", "id")
        sort_order = args.get("sort_order", "desc")

        # Search query filters
        search = args.get('query')

        # trend period
        trend_start_period = args.get('trend_start_period')
        trend_end_period = args.get('trend_end_period')
    else:
        # Pagination parameters
        limit = args.get("limit", default=10, type=int)
        offset = args.get("offset", default=0, type=int)

        # Sorting parameters
        sort_by = args.get("sort_by", default="id")
        sort_order = args.get("sort_order", default="desc")

        # Search query filters
        search = args.get("query")

        # trend period
        trend_start_period = args.get('trend_start_period')
        trend_end_period = args.get('trend_end_period')

    trend_period = None
    if trend_start_period:
        trend_end_period = datetime.now() if not trend_end_period \
            else datetime.strptime(trend_end_period, "%Y-%m-%dT%H:%M:%S")
        trend_start_period = datetime.strptime(trend_start_period, "%Y-%m-%dT%H:%M:%S")
        trend_period = (trend_start_period, trend_end_period)

    # filtering
    filters = []
    if author_id := args.get('author_id'):
        filters.append(Collection.author_id == author_id)

    if statuses := args.get('statuses'):
        if isinstance(statuses, str):
            statuses = statuses.split(',')
        filters.append(Collection.status.in_(statuses))

    with db.with_project_schema_session(project_id) as session:
        if tags := args.get('tags'):
            # tag filtering
            if isinstance(tags, str):
                tags = [int(tag) for tag in tags.split(',')]
            condition = get_filter_collection_by_tags_condition(project_id, tags)
            filters.append(condition)

        query = session.query(Collection)
        extra_columns = []
        sort_by_likes = sort_by == "likes"

        # OPTIMIZATION: Pre-fetch Like model once to avoid repeated RPC calls
        like_model = get_like_model() if with_likes else None

        if with_likes:
            query, new_columns = add_likes(
                original_query=query,
                project_id=project_id,
                sort_by_likes=sort_by_likes,
                sort_order=sort_order,
                entity=Collection,
                like_model=like_model
            )
            extra_columns.extend(new_columns)
        if trend_period:
            query, new_columns = add_trending_likes(
                original_query=query,
                project_id=project_id,
                entity=Collection,
                trend_period=trend_period,
                filter_results=True,
                like_model=like_model
            )
            extra_columns.extend(new_columns)
        # if my_liked:
        query, new_columns = add_my_liked(
            original_query=query,
            project_id=project_id,
            entity=Collection,
            filter_results=my_liked,
            like_model=like_model
        )
        extra_columns.extend(new_columns)

        if search:
            query = query.filter(
                or_(
                    Collection.name.ilike(f"%{search}%"),
                    Collection.description.ilike(f"%{search}%")
                )
            )

        if filters:
            query = query.filter(*filters)

        # Apply sorting
        if not trend_period and not sort_by_likes:
            if sort_order.lower() == "asc":
                query = query.order_by(getattr(Collection, sort_by, sort_by))
            else:
                query = query.order_by(desc(getattr(Collection, sort_by, sort_by)))

        total = query.count()

        # Apply limit and offset for pagination
        query = query.limit(limit).offset(offset)

        q_result: Union[List[tuple[Collection, int, int]], List[Collection]] = query.all()

        # if with_likes:
        #     collections_with_likes = []
        #     for collection, likes, is_liked, *other in collections:
        #         collection.likes = likes
        #         collection.is_liked = is_liked
        #         if other:
        #             collection.trending_likes = other[0]
        #         collections_with_likes.append(collection)
        #     collections = collections_with_likes

    return total, list(set_columns_as_attrs(q_result, extra_columns))


def delete_collection(project_id: int, collection_id: int):
    with db.with_project_schema_session(project_id) as session:
        if collection := session.query(Collection).get(collection_id):
            collection_data = collection.to_json()
            session.delete(collection)
            session.commit()
            fire_collection_deleted_event(collection_data)
            return True
    return False


def fire_collection_deleted_event(collection_data: dict):
    rpc_tools.EventManagerMixin().event_manager.fire_event(
        'promptlib_shared_collection_deleted', collection_data
    )


def fire_collection_unpublished(collection_data):
    rpc_tools.EventManagerMixin().event_manager.fire_event(
        "promptlib_shared_collection_unpublished", {
            "private_id": collection_data['shared_id'],
            "private_owner_id": collection_data['shared_owner_id']
        }
    )


def update_collection(project_id: int, collection_id: int, data: dict):
    with db.with_project_schema_session(project_id) as session:
        if collection := session.query(Collection).get(collection_id):
            for field, value in data.items():
                if hasattr(collection, field):
                    setattr(collection, field, value)
            session.commit()

            return get_detail_collection(collection)
        return None


def get_collection(project_id: int, collection_id: int, only_public: bool = False):
    with db.with_project_schema_session(project_id) as session:
        if collection := session.query(Collection).get(collection_id):
            return get_detail_collection(collection, only_public)
        return None


def get_detail_collection(collection: Collection, only_public: bool = False):
    data = collection.to_json()
    project_id = data['owner_id']
    collection_items = [
        (e, data.pop(e.entities_name, [])) for e in ENTITY_REG
    ]
    collection_model = PublishedCollectionDetailModel if only_public \
        else CollectionDetailModel

    collection_data = collection_model(**data)
    collection_data.author = get_author_data(collection.author_id)

    if only_public:
        collection_data.get_likes(project_id)
        collection_data.check_is_liked(project_id)

    result = json.loads(collection_data.json(exclude={"author_id"}))

    for entity_info, entities in collection_items:
        if not entities:
            continue

        kwargs = {
            'entity_type': entity_info.get_entity_type(),
            'entity_version_type': entity_info.get_entity_version_type()
        }
        kwargs['collection_entities'] = entities
        kwargs['only_public'] = only_public

        if entity_info.entities_name == 'applications':
            kwargs['extra_filter_params'] = {"agent_type": "pipeline"}
            pipeline_entity_rows = get_entities_for_collection(**kwargs)
            result["pipelines"] = {
                "total": len(pipeline_entity_rows),
                "rows": pipeline_entity_rows
            }

            # agents - all applications except pipelines
            kwargs['extra_filter_params'] = {"agent_type": "agent"}
            agent_entity_rows = get_entities_for_collection(**kwargs)
            result[entity_info.entities_name] = {
                "total": len(agent_entity_rows),
                "rows": agent_entity_rows
            }
        else:
            entity_rows = get_entities_for_collection(**kwargs)
            result[entity_info.entities_name] = {
                "total": len(entities), "rows": entity_rows
            }

    return result


def create_collection(project_id: int, data: CollectionModel | dict, fire_event: bool = True) -> Collection:
    if isinstance(data, dict):
        collection: CollectionModel = CollectionModel.parse_obj(data)
    else:
        collection: CollectionModel = data

    for entities in chain(e.get_entities_field(collection) for e in ENTITY_REG):
        for entity in entities:
            if not check_addability(entity.owner_id, collection.author_id):
                raise EntityInaccessableError(
                    f"User doesn't have access to project '{entity.owner_id}'"
                )

    with db.with_project_schema_session(project_id) as session:
        c = Collection(**collection.dict())
        session.add(c)
        session.commit()
        if fire_event:
            rpc_tools.EventManagerMixin().event_manager.fire_event(
                'promptlib_shared_collection_added', c.to_json()
            )
        return c


def add_entity_to_collection(collection, entities_name, entity_data: CollectionItem, return_data=True):
    entities_list: List = copy.deepcopy(getattr(collection, entities_name))
    entities_list.append(entity_data.model_dump(mode='json'))
    setattr(collection, entities_name, entities_list)
    if return_data:
        return get_detail_collection(collection)


def remove_entity_from_collection(collection, entities_name, entity_data: CollectionItem, return_data=True):
    entities_list = [
        copy.deepcopy(entity) for entity in getattr(collection, entities_name)
        if not (int(entity['owner_id']) == entity_data.owner_id and
                int(entity['id']) == entity_data.id)
    ]
    setattr(collection, entities_name, entities_list)
    if return_data:
        return get_detail_collection(collection)


def remove_entity_from_collections(collection_ids: list, entities_name, entity_data: dict, session):
    for collection in session.query(Collection).filter(Collection.id.in_(collection_ids)).all():
        new_data = [
            copy.deepcopy(entity) for entity in getattr(collection, entities_name)
            if entity['owner_id'] != entity_data['owner_id'] or entity['id'] != entity_data['id']
        ]
        setattr(collection, entities_name, new_data)


def delete_entity_from_collections(entity_name, collection_ids: list, entity_data: dict, session):
    entities_field_name = get_entity_info_by_name(entity_name).entities_name
    remove_entity_from_collections(collection_ids, entities_field_name, entity_data, session)


def fire_patch_collection_event(collection_data, operartion, entity_data, entity_name):
    if operartion == CollectionPatchOperations.add:
        removed_entities = tuple()
        added_entities = [(entity_data['owner_id'], entity_data['id'])]
    else:
        added_entities = tuple()
        removed_entities = [(entity_data['owner_id'], entity_data['id'])]

    rpc_tools.EventManagerMixin().event_manager.fire_event(
        'promptlib_shared_collection_updated', {
            "removed_entities": removed_entities,
            "added_entities": added_entities,
            "entity_name": entity_name,
            "collection_data": {
                "owner_id": collection_data['owner_id'],
                "id": collection_data['id']
            }
        }
    )


# when passthrough_mode is used, we do not return any results and skip NotIn/Already in errors
def patch_collection_with_entities(data_in: CollectionPatchModel, passthrough_mode=False):
    new_private_data, new_public_data = get_entity_private_public_counterpart(data_in)

    return_data = not passthrough_mode
    sessions = []
    event_callers = []
    results = []
    try:
        for data in (new_private_data, new_public_data):
            if data:
                session = db.get_project_schema_session(data.project_id)
                sessions.append(session)
                try:
                    result, event_caller = do_patch_collection(data, session, return_data)
                except (EntityAlreadyInCollectionError, EntityNotInCollectionError):
                    if passthrough_mode:
                        continue
                    raise
                if result:
                    results.append(result)
                if event_caller:
                    event_callers.append(event_caller)
    except Exception as ex:
        for session in sessions:
            session.rollback()
        raise
    else:
        for session in sessions:
            session.commit()
    finally:
        for session in sessions:
            session.close()

    for event_caller in event_callers:
        event_caller()

    if passthrough_mode:
        return

    for r in results:
        if data_in.project_id == r['owner_id'] and data_in.collection_id == r['id']:
            return r
    else:
        raise RuntimeError("Empty result on collection operation")


def get_entity_private_public_counterpart(data: CollectionPatchModel):
    public_project_id = get_public_project_id()
    for entity_info in ENTITY_REG:
        if entity_data := entity_info.get_entity_field(data):
            Entity = entity_info.get_entity_type()
            break

    # case 1: private element -> private collection
    if public_project_id not in (data.project_id, entity_data.owner_id):
        public_entity = get_entity_public_twin(
            public_project_id=public_project_id,
            entity_type=Entity,
            private_entity_id=entity_data.id,
            private_entity_owner_id=entity_data.owner_id
        )
        public_collection = get_collection_public_twin(
            private_project_id=data.project_id,
            private_collection_id=data.collection_id,
            public_project_id=public_project_id
        )
        # case 1.1: private element with public twin -> private collection with public twin
        if public_entity and public_collection:
            new_public_data = data.copy(
                update={
                    "project_id": public_collection.owner_id,
                    "collection_id": public_collection.id,
                    entity_info.entity_name: CollectionItem.from_orm(public_entity)
                }
            )
            return data, new_public_data
        # case 1.2: private element without public twin -> private collection with public twin
        # case 1.3: private element without public twin -> private collection without public twin
        # case 1.4: private element with public twin -> private collection without public twin
        else:
            return data, None
    # case 2: private element -> public collection
    elif public_project_id != entity_data.owner_id and public_project_id == data.project_id:
        public_entity = get_entity_public_twin(
            public_project_id=public_project_id,
            entity_type=Entity,
            private_entity_id=entity_data.id,
            private_entity_owner_id=entity_data.owner_id
        )
        private_collection = get_collection_private_twin(
            public_project_id=public_project_id,
            public_collection_id=data.collection_id,
            public_collection_owner_id=data.project_id
        )

        # case 2.1: private element without public twin -> public collection with private twin
        # case 2.2: private element with public twin -> public collection with private twin
        if private_collection:
            raise RuntimeError(
                f"Operation error: use private collection instead of public"
            )
        # case 2.3: private element without public twin -> public collection without private twin
        # case 2.4: private element with public twin -> public collection without private twin
        elif private_collection is None:
            raise RuntimeError(
                "Operation error: private element within public collection"
            )
    # case 3: public element -> private collection
    elif public_project_id == entity_data.owner_id and public_project_id != data.project_id:
        private_entity = get_entity_private_twin(
            public_project_id=public_project_id,
            entity_type=Entity,
            public_entity_id=entity_data.id,
            public_entity_owner_id=entity_data.owner_id
        )
        public_collection = get_collection_public_twin(
            private_project_id=data.project_id,
            private_collection_id=data.collection_id,
            public_project_id=public_project_id
        )
        # case 3.1: public element without private twin -> private collection with public twin
        if private_entity is None and public_collection:
            new_public_data = data.copy(
                update={
                    "project_id": public_collection.owner_id,
                    "collection_id": public_collection.id
                }
            )
            return data, new_public_data
        # case 3.2: public element with private twin -> private collection with public twin
        elif private_entity and public_collection:
            new_private_data = data.copy(
                update={
                    entity_info.entity_name: CollectionItem.from_orm(private_entity)
                }
            )
            new_public_data = data.copy(
                update={
                    "project_id": public_collection.owner_id,
                    "collection_id": public_collection.id
                }
            )
            return new_private_data, new_public_data
        # case 3.3: public element without private twin -> private collection without public twin
        elif private_entity is None and public_collection is None:
            return data, None
        # case 3.4: public element with private twin -> private collection without public twin
        elif private_entity and public_collection is None:
            new_private_data = data.copy(
                update={
                    entity_info.entity_name: CollectionItem.from_orm(private_entity)
                }
            )
            return new_private_data, None
    # case 4: public element -> public collection
    # case 4.1: public element without private twin -> public collection with private twin
    # case 4.2: public element with private twin -> public collection with private twin
    # case 4.3: public element without private twin -> public collection without private twin
    # case 4.4: public element with private twin -> public collection without private twin
    elif public_project_id == entity_data.owner_id and public_project_id == data.project_id:
        raise RuntimeError("Operation error: public element on public collection")


def get_entity_private_twin(public_project_id, entity_type, public_entity_id, public_entity_owner_id):
    with db.with_project_schema_session(public_project_id) as session:
        entity = session.query(entity_type).filter_by(
            owner_id=public_entity_owner_id,
            id=public_entity_id
        ).first()
        if entity:
            try:
                return CollectionPrivateTwinModel.from_orm(entity)
            except ValidationError:
                pass


def get_entity_public_twin(public_project_id, entity_type, private_entity_id, private_entity_owner_id):
    with db.with_project_schema_session(public_project_id) as session:
        entity = session.query(entity_type).filter_by(
            shared_owner_id=private_entity_owner_id,
            shared_id=private_entity_id
        ).first()
        return entity


def get_collection_private_twin(public_project_id, public_collection_id, public_collection_owner_id):
    with db.with_project_schema_session(public_project_id) as session:
        collection = session.query(Collection).filter_by(
            owner_id=public_collection_owner_id,
            id=public_collection_id
        ).first()
        if collection:
            try:
                return CollectionPrivateTwinModel.from_orm(collection)
            except ValidationError:
                pass


def get_collection_public_twin(private_project_id, private_collection_id, public_project_id):
    with db.with_project_schema_session(public_project_id) as session:
        collection = session.query(Collection).filter_by(
            shared_owner_id=private_project_id,
            shared_id=private_collection_id
        ).first()
        return collection


def check_addability_for_entity(
        project_id,
        collection_id,
        entity_name,
        entity_id,
        entity_owner_id
):
    entity_data_in = {
        entity_name: {
            "id": entity_id,
            'owner_id': entity_owner_id
        }
    }
    data_in = CollectionPatchModel(
        project_id=project_id,
        collection_id=collection_id,
        operation=CollectionPatchOperations.add,
        **entity_data_in
    )
    try:
        new_private_data, new_public_data = get_entity_private_public_counterpart(data_in)
    except Exception as ex:
        log.debug((
            f"{entity_name=} {entity_id=} {entity_owner_id=} not addable "
            f"to {collection_id=} of {project_id=}: {ex}"
        ))
        return False
    for data in (new_private_data, new_public_data):
        if data:
            for entity_info in ENTITY_REG:
                if entity_data := entity_info.get_entity_field(data):
                    break
            else:
                raise RuntimeError("empty input in check addability collection items")
            with db.get_project_schema_session(data.project_id) as session:
                if collection := session.query(Collection).get(data.collection_id):
                    if not check_addability(entity_data.owner_id, collection.author_id):
                        return False
                else:
                    return False
    return True


def do_patch_collection(data: CollectionPatchModel, session, return_data):
    op_map = {
        CollectionPatchOperations.add: add_entity_to_collection,
        CollectionPatchOperations.remove: remove_entity_from_collection
    }

    for entity_info in ENTITY_REG:
        if entity_data := entity_info.get_entity_field(data):
            Entity = entity_info.get_entity_type()
            break
    else:
        raise RuntimeError("empty input patched collection items, nothing to patch")

    with db.with_project_schema_session(entity_data.owner_id) as entity_session:
        entity = entity_session.query(Entity).filter_by(id=entity_data.id).first()
        if not entity:
            raise EntityDoesntExist(
                f"{entity_info.entity_name} '{entity_data.id}' in project '{entity_data.owner_id}' doesn't exist"
            )

    if collection := session.query(Collection).get(data.collection_id):
        if not check_addability(entity_data.owner_id, collection.author_id):
            raise EntityInaccessableError(
                f"User doesn't have access to project '{entity_data.owner_id}'"
            )

        entity_in_collection = get_include_entity_flag(
            entity_name=entity_info.entity_name,
            collection=CollectionListModel.from_orm(collection),
            entity_id=entity.id,
            entity_owner_id=entity.owner_id,
        )
        if data.operation == CollectionPatchOperations.add and entity_in_collection:
            raise EntityAlreadyInCollectionError('Already in collection')
        if data.operation == CollectionPatchOperations.remove and not entity_in_collection:
            raise EntityNotInCollectionError('Not in collection')

        result = op_map[data.operation](collection, entity_info.entities_name, entity_data, return_data)
        collection_data = collection.to_json()
        session.flush()

        event_caller = partial(
            fire_patch_collection_event,
            collection_data,
            data.operation,
            entity_data.dict(),
            entity_info.entity_name
        )
        return result, event_caller
    else:
        raise RuntimeError(f"Collection with id={data.collection_id} does not exist in project {data.project_id}")

    return None


def get_collection_tags(collection) -> list:
    tags = dict()

    for ent in ENTITY_REG:
        entities = ent.get_entities_field(collection)
        if not entities:
            continue
        Entity = ent.get_entity_type()
        EntityVersion = ent.get_entity_version_type()

        entities_data = defaultdict(list)
        for entity_data in entities:
            entities_data[entity_data['owner_id']].append(entity_data['id'])

        for project_id, entity_ids in entities_data.items():
            with db.with_project_schema_session(project_id) as session:
                for entity_id in entity_ids:
                    query = (
                        session.query(Entity)
                        .options(
                            joinedload(Entity.versions).joinedload(EntityVersion.tags)
                        )
                    )
                    entity = query.get(entity_id)

                    if not entity:
                        continue

                    for version in entity.versions:
                        for tag in version.tags:
                            tags[tag.name] = TagBaseModel.from_orm(tag)

    return list(tags.values())


def fire_public_collection_status_change_event(shared_owner_id, shared_id, status):
    rpc_tools.EventManagerMixin().event_manager.fire_event(
        'public_collection_status_change', {
            'private_project_id': shared_owner_id,
            'private_collection_id': shared_id,
            'status': status
        })


class CollectionPublishing:
    def __init__(self, project_id: int, collection_id: int):
        self._project_id = project_id
        self._collection_id = collection_id
        self._public_id = get_public_project_id()

    def check_already_published(self):
        with db.with_project_schema_session(self._public_id) as session:
            exist_query = (
                session.query(exists().where(and_(
                    Collection.shared_id == self._collection_id,
                    Collection.shared_owner_id == self._project_id,
                )))
            ).scalar()
            return exist_query

    def get_public_entities_of_collection(
            self,
            private_entity_ids: List[dict],
            entity_type,
            entity_version_type
    ):
        with db.with_project_schema_session(self._public_id) as session:
            public_entities = filter(lambda x: x['owner_id'] == self._public_id, private_entity_ids)
            private_entities = filter(lambda x: x['owner_id'] != self._public_id, private_entity_ids)

            entity_ids = session.query(entity_type.id).filter(
                or_(
                    *[
                        and_(
                            entity_type.shared_id == data['id'],
                            entity_type.shared_owner_id == data['owner_id']
                        ) for data in private_entities
                    ],
                    *[
                        and_(
                            entity_type.id == data['id'],
                        ) for data in public_entities
                    ]
                ),
                entity_type.versions.any(entity_version_type.status == PublishStatus.published)
            ).all()

        result = [{"id": entity_id[0], "owner_id": self._public_id} for entity_id in entity_ids]
        return result

    @staticmethod
    def set_status(project_id, collection_id, status: PublishStatus, session=None, return_updated=True):
        session_created = False
        if session is None:
            session_created = True
            session = db.get_project_schema_session(project_id)
        try:
            collection = session.query(Collection).get(collection_id)
            if not collection:
                raise RuntimeError(f"Collecton with id {collection_id} not found in project {project_id}")
            collection.status = status
            session.commit()
        finally:
            if session_created:
                session.close()

        if return_updated:
            return collection

    @staticmethod
    def approve(project_id, collection_id):
        # changing the status of collection
        with db.with_project_schema_session(project_id) as session:
            collection = CollectionPublishing.set_status(
                project_id, collection_id, PublishStatus.published, session
            )
            #
            fire_public_collection_status_change_event(
                collection.shared_owner_id,
                collection.shared_id,
                PublishStatus.published
            )
            collection_model = CollectionShortDetailModel.from_orm(collection)
        return {"ok": True, "result": collection_model.model_dump(mode='json')}

    @staticmethod
    def reject(project_id, collection_id):
        # changing the status of collection
        with db.with_project_schema_session(project_id) as session:
            collection = CollectionPublishing.set_status(
                project_id, collection_id, PublishStatus.rejected, session
            )
            #
            fire_public_collection_status_change_event(
                collection.shared_owner_id,
                collection.shared_id,
                PublishStatus.rejected
            )
            collection_model = CollectionShortDetailModel.from_orm(collection)
        return {"ok": True, "result": collection_model.model_dump(mode='json')}

    def set_statuses(self, public_collection_data: Collection, status: PublishStatus):
        # set public collection
        collection_id = public_collection_data.id
        self.set_status(self._public_id, collection_id, status)

        # set private collection
        self.set_status(self._project_id, self._collection_id, status)

    def publish(self):
        if self.check_already_published():
            return {
                "ok": False,
                "error": "Already published"
            }

        collection_data = None
        with db.with_project_schema_session(self._project_id) as session:
            collection = session.query(Collection).get(self._collection_id)
            if not collection:
                return {
                    "ok": False,
                    "error": "Collection is not found",
                    "error_code": 404
                }

            collection_data = collection.to_json()
            collection_data['shared_id'] = collection_data.pop('id')
            collection_data['shared_owner_id'] = collection_data.pop('owner_id')
            collection_data['owner_id'] = self._public_id

            collection_is_empty = True
            for ent in ENTITY_REG:
                entities = ent.get_entities_field(collection)
                if not entities:
                    continue
                entity_ids = self.get_public_entities_of_collection(
                    entities,
                    ent.get_entity_type(),
                    ent.get_entity_version_type()
                )
                collection_data.pop(ent.entities_name)
                if entity_ids:
                    collection_is_empty = False
                    collection_data[ent.entities_name] = entity_ids

            if collection_is_empty:
                raise Exception("Collection doesn't contain public entities")

        new_collection = create_collection(self._public_id, collection_data)
        self.set_statuses(new_collection, PublishStatus.on_moderation)
        new_collection.status = PublishStatus.on_moderation
        result = get_detail_collection(new_collection)
        return {
            "ok": True,
            "new_collection": result
        }


def unpublish(current_user_id, project_id, collection_id):
    public_id = get_public_project_id()
    with db.with_project_schema_session(public_id) as session:
        collection = session.query(Collection).filter_by(
            shared_id=collection_id,
            shared_owner_id=project_id
        ).first()

        if not collection:
            return {
                "ok": False,
                "error": f"Public collection with id '{collection_id}'",
                "error_code": 404,
            }

        if int(collection.author_id) != int(current_user_id):
            return {
                "ok": False,
                "error": "Current user is not author of the collection",
                "error_code": 403
            }

        # TODO: why checking draft ?
        if collection.status == PublishStatus.draft:
            return {"ok": False, "error": "Collection is not public yet"}

        collection_data = collection.to_json()
        session.delete(collection)
        session.commit()
        fire_collection_deleted_event(collection_data)
        fire_collection_unpublished(collection_data)
        return {"ok": True, "msg": "Successfully unpublished"}


def group_by_project_id(data, data_type='dict'):
    items = defaultdict(set)
    group_field = "owner_id" if not data_type == "tuple" else 0
    data_field = "id" if not data_type == "tuple" else 1
    for entity in data:
        items[entity[group_field]].add(entity[data_field])
    return items


@add_public_project_id
def get_include_entity_flag(
        entity_name: str, collection: CollectionListModel,
        entity_id: int, entity_owner_id: int, *,
        project_id: int
) -> bool:
    entity_info = get_entity_info_by_name(entity_name)
    Entity = entity_info.get_entity_type()
    entities = entity_info.get_entities_field(collection)

    public_entities = []
    for p in entities:
        p_owner_id = int(p['owner_id'])
        if p_owner_id == entity_owner_id and \
                int(p['id']) == entity_id:
            return True
        elif p_owner_id == project_id and \
                entity_owner_id != project_id:
            # if entity is public but target collection owner is not public
            public_entities.append(p)
    else:
        if public_entities:
            with db.with_project_schema_session(project_id) as session:
                q = session.query(Entity).filter(
                    Entity.id.in_([p['id'] for p in public_entities]),
                    Entity.shared_owner_id == entity_owner_id,
                    Entity.shared_id == entity_id,
                )
                return bool(q.first())
    return False


def deep_merge_collection_export_results(d1, d2):
    if not d1:
        return copy.deepcopy(d2)

    res = copy.deepcopy(d1)
    auxiliary_data = {}  # for the _metadata in export

    for key in d2.keys():
        if key.startswith("_"):
            auxiliary_data[key] = d2[key]
            continue
        already_exported = [x['import_uuid'] for x in d1.get(key, [])]
        for entity in d2[key]:
            if entity['import_uuid'] not in already_exported:
                res.setdefault(key, []).append(entity)
            elif entity['original_exported']:
                # when we merge same entity from collection and from agent-deps
                # original_exported flag must be always True in such case
                for saved_entity in d1[key]:
                    if saved_entity['import_uuid'] == entity['import_uuid']:
                        saved_entity['original_exported'] = True
                        break
    if auxiliary_data:
        res.update(auxiliary_data)
    return res
