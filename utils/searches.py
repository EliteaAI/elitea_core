import json
from typing import List

from flask import request
from sqlalchemy.orm import joinedload
from sqlalchemy import or_, and_, not_

from ..utils.exceptions import NotFound
from ..models.all import Tag
from ..utils.utils import get_entities_by_tags
from ..models.all import Collection
from ..models.pd.misc import MultipleTagListModel
from ..models.pd.collections import MultipleCollectionSearchModel
from .collections import get_filter_collection_by_entity_tags_condition

from tools import db, api_tools
from pylon.core.tools import log


def get_search_options(project_id, Model, PDModel, joinedload_, args_prefix, filters=None):
    query = request.args.get('query', '')
    search_query = f"%{query}%"

    # filter_fields = ('name', 'title')
    # conditions = []
    # for field in filter_fields:
    #     if hasattr(Model, field):
    #         conditions.append(
    #             getattr(Model, field).ilike(search_query)
    #         )
    # or_(*conditions)

    filter_ = and_(getattr(Model, 'name').ilike(search_query), *filters)
    args_data = get_args(args_prefix)
    total, res = api_tools.get(
        project_id=project_id,
        args=args_data,
        data_model=Model,
        custom_filter=filter_,
        joinedload_=joinedload_,
        is_project_schema=True
    )
    parsed = PDModel(items=res)
    return {
        "total": total,
        "rows": [prompt.model_dump(mode='json') for prompt in parsed.items]
    }


def get_search_options_one_entity(
    project_id,
    entity_name,
    Model,
    ModelVersion,
    MultipleSearchModel,
    ModelVersionTagAssociation,
    args_prefix=None
):
    result = {}
    entities = set(request.args.getlist('entities[]'))
    tags = tuple(set(int(tag) for tag in request.args.getlist('tags[]')))
    statuses = request.args.getlist('statuses[]')
    author_id = request.args.get("author_id", type=int)
    if args_prefix is None:
        args_prefix = entity_name

    meta_data = {
        entity_name: {
            "Model": Model,
            "PDModel": MultipleSearchModel,
            "joinedload_": [Model.versions],
            "args_prefix": args_prefix,
            "filters": [],
        },
        "collection": {
            "Model": Collection,
            "PDModel": MultipleCollectionSearchModel,
            "joinedload_": None,
            "args_prefix": "col",
            "filters": [],
        },
        "tag": {
            "Model": Tag,
            "PDModel": MultipleTagListModel,
            "joinedload_": None,
            "args_prefix": "tag",
            "filters": []
        }
    }

    if tags:
        try:
            data = get_filter_collection_by_entity_tags_condition(project_id, tags, entity_name)
            meta_data['collection']['filters'].append(or_(*data))
        except NotFound:
            entities = [entity for entity in entities if entity != "collection"]
            result['collection'] = {
                "total": 0,
                "rows": []
            }

        entities_subq = get_entities_by_tags(project_id, tags, Model, ModelVersion)
        meta_data[entity_name]['filters'].append(
            Model.id.in_(entities_subq)
        )

    # pipeline hardcode
    if args_prefix == "pipeline":
        meta_data[entity_name]['filters'].append(
            Model.versions.any(ModelVersion.agent_type == args_prefix)
        )
        entities.add('application')

    if args_prefix == "application":
        meta_data[entity_name]['filters'].append(
            not_(Model.versions.any(ModelVersion.agent_type == 'pipeline'))
        )

    if author_id:
        meta_data['collection']['filters'].append(
            Collection.author_id == author_id
        )

        meta_data[entity_name]['filters'].append(
            Model.versions.any(ModelVersion.author_id == author_id)
        )

    if statuses:
        meta_data[entity_name]['filters'].append(
            (Model.versions.any(ModelVersion.status.in_(statuses)))
        )
        meta_data['collection']['filters'].append(
            Collection.status.in_(statuses)
        )

    meta_data['tag']['filters'].append(
        get_tag_filter(
            project_id=project_id,
            entity_name=entity_name,
            Model=Model,
            ModelVersion=ModelVersion,
            ModelVersionTagAssociation=ModelVersionTagAssociation,
            author_id=author_id,
            statuses=statuses,
            tags=tags,

        )
    )

    for section, data in meta_data.items():
        if section in entities:
            result[section] = get_search_options(project_id, **data)

    result[args_prefix] = result.pop(entity_name)

    return result


def get_tag_filter(
        project_id,
        entity_name,
        Model,
        ModelVersion,
        ModelVersionTagAssociation,
        author_id: int = None,
        statuses: List[str] = None,
        tags: List[int] = None,
        session=None
):
    if session is None:
        session = db.get_project_schema_session(project_id)

    entity_query = (
        session.query(Model)
        .options(joinedload(Model.versions))
    )

    filters = []
    if author_id:
        filters.append(Model.versions.any(ModelVersion.author_id == author_id))

    if statuses:
        filters.append(Model.versions.any(ModelVersion.status.in_(statuses)))

    if tags:
        entities_subq = get_entities_by_tags(project_id, tags, Model, ModelVersion, session)
        filters.append(
            Model.id.in_(entities_subq)
        )

    entity_query = entity_query.filter(*filters)
    entity_query = entity_query.with_entities(Model.id)
    entity_subquery = entity_query.subquery()

    query = (
        session.query(Tag.id)
        .filter(getattr(ModelVersion, f'{entity_name}_id').in_(entity_subquery))
        .join(ModelVersionTagAssociation, ModelVersionTagAssociation.c.tag_id == Tag.id)
        .join(ModelVersion, ModelVersion.id == ModelVersionTagAssociation.c.version_id)
        .group_by(Tag.id)
    ).subquery()
    return Tag.id.in_(query)


def get_args(prefix):
    args = request.args
    limit = args.get('limit', 0, type=int)
    offset = args.get('offset', 0, type=int)
    sort = args.get('sort')
    order = args.get('order', 'desc')

    result_args = dict(args)
    result_args['limit'] = result_args.get(f'{prefix}_limit', limit)
    result_args['offset'] = result_args.get(f'{prefix}_offset', offset)
    result_args['sort'] = result_args.get(f'{prefix}_sort', sort)
    result_args['order'] = result_args.get(f'{prefix}_order', order)
    return result_args