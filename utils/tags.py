from abc import ABCMeta, abstractmethod
from json import loads
from datetime import datetime
from sqlalchemy import func, cast, String, or_, not_
from sqlalchemy.orm import joinedload

from tools import db
from pylon.core.tools import log

from .like_utils import add_likes, add_trending_likes, add_my_liked, get_like_model
from ..models.all import Collection
from ..models.all import Tag
from ..models.all import Application, ApplicationVersion, ApplicationVersionTagAssociation


class TagListABC(ABCMeta):
    meta_registry = {}

    def __new__(mcs, name, bases, attrs):
        resulting_class = super().__new__(mcs, name, bases, attrs)
        if bases:  # exlcuding parent class
            name = name.split('TagList')[0].lower()
            mcs.meta_registry[name] = resulting_class
        return resulting_class


class TagList(metaclass=TagListABC):
    def __init__(self, project_id, args):
        self.project_id = project_id
        self.args = args
        self._set_general_query_info()
        # trending period
        self._set_trending_info()
        self.session = db.get_project_schema_session(project_id)
        self._is_collection = False

    def _set_general_query_info(self):
        self.limit = self.args.get("limit", 0)
        self.offset = self.args.get("offset", 0)
        self.my_liked = self.args.get("my_liked", False)
        self.entity_coverage = self.args.get("entity_coverage", "all")

    def _set_trending_info(self):
        trend_start_period = self.args.get('trend_start_period')
        trend_end_period = self.args.get('trend_end_period')
        self.trend_period = None
        if trend_start_period:
            trend_end_period = datetime.utcnow() if not trend_end_period else datetime.strptime(trend_end_period,
                                                                                                "%Y-%m-%dT%H:%M:%S")
            trend_start_period = datetime.strptime(trend_start_period, "%Y-%m-%dT%H:%M:%S")
            self.trend_period = (trend_start_period, trend_end_period)

    @abstractmethod
    def set_related_entity_info(self):
        pass

    def get_related_entity_filters(self):
        filters = []
        if author_id := self.args.get('author_id'):
            filters.append(self.Entity.versions.any(self.Version.author_id == author_id))
        if statuses := self.args.get('statuses'):
            statuses = statuses.split(',')
            filters.append(self.Entity.versions.any(self.Version.status.in_(statuses)))
        if query := self.args.get('query'):
            filters.append(
                or_(
                    self.Entity.name.ilike(f"%{query}%"),
                    self.Entity.description.ilike(f"%{query}%")
                )
            )
        return filters

    def add_related_entity_extra_filters(self, filters):
        return filters

    def get_related_entity_query(self, filters):
        entity_query = (
            self.session.query(self.Entity)
            .options(joinedload(self.Entity.versions))
        )
        extra_columns = []

        # OPTIMIZATION: Pre-fetch Like model once to avoid repeated RPC calls
        like_model = get_like_model()

        entity_query, new_columns = add_likes(
            original_query=entity_query,
            project_id=self.project_id,
            entity=self.Entity,
            like_model=like_model
        )
        extra_columns.extend(new_columns)
        if self.trend_period:
            entity_query, new_columns = add_trending_likes(
                original_query=entity_query,
                project_id=self.project_id,
                trend_period=self.trend_period,
                filter_results=True,
                entity=self.Entity,
                like_model=like_model
            )
            extra_columns.extend(new_columns)
        if self.my_liked:
            entity_query, new_columns = add_my_liked(
                original_query=entity_query,
                project_id=self.project_id,
                filter_results=True,
                entity=self.Entity,
                like_model=like_model
            )
            extra_columns.extend(new_columns)
        if filters:
            entity_query = entity_query.filter(*filters)

        entity_query = entity_query.with_entities(self.Entity.id)
        return entity_query.subquery()

    def get_main_query_filters(self, entity_subquery):
        tag_filters = [getattr(self.Version, self.foriegn_key).in_(entity_subquery)]
        if search := self.args.get("search"):
            tag_filters.append(Tag.name.ilike(f"%{search}%"))
        if self._is_collection:
            all_prompt_ids = [
                prompt['id'] for collection in self.session.query(Collection.prompts).all()
                for prompt in next(iter(collection))
            ]
            tag_filters.append(getattr(self.Version, self.foriegn_key).in_(all_prompt_ids))
        return tag_filters

    def execute_main_query(self, tag_filters):
        select_list = [Tag.id, Tag.name, cast(Tag.data, String)]
        select_list.append(func.count(func.distinct(getattr(self.Version, self.foriegn_key))))

        query = (
            self.session.query(*select_list)
            .filter(*tag_filters)
        )
        query = query.join(self.VersionTagAssociation, self.VersionTagAssociation.c.tag_id == Tag.id) \
            .join(self.Version, self.Version.id == self.VersionTagAssociation.c.version_id)

        query = query.group_by(Tag.id, Tag.name, cast(Tag.data, String))
        
        order_by = Tag.id.desc()
        order_by = func.count(func.distinct(getattr(self.Version, self.foriegn_key))).desc()

        query = query.order_by(order_by)
        total = query.count()

        if self.limit:
            query = query.limit(self.limit)
        if self.offset:
            query = query.offset(self.offset)

        return total, query.all()

    def _as_dict(self, x):
        result = {'id': x[0], 'name': x[1], 'data': loads(x[2])}
        result[self.count_name] = x[3]
        return result

    # template method
    def get_tags(self):
        # set entity info
        tag_filters = []
        self.set_related_entity_info()

        # getting related entity info
        filters = self.get_related_entity_filters()

        # adding extra filters into filters
        filters = self.add_related_entity_extra_filters(filters)

        # get related entity query
        subquery = self.get_related_entity_query(filters)

        # get main query filters
        tag_filters = self.get_main_query_filters(subquery)

        # execute main query
        total, tags = self.execute_main_query(tag_filters)

        result = {
            "total": total,
            "rows": [self._as_dict(tag) for tag in tags]
        }
        self.session.close()
        return result


class ApplicationTagList(TagList):
    def __init__(self, project_id, args):
        super().__init__(project_id, args)

    def set_related_entity_info(self):
        self.Entity = Application
        self.Version = ApplicationVersion
        self.VersionTagAssociation = ApplicationVersionTagAssociation
        self.foriegn_key = 'application_id'
        self.count_name = "application_count"

    def get_related_entity_filters(self):
        filters = super().get_related_entity_filters()
        filters.append(
            not_(self.Entity.versions.any(self.Version.agent_type == 'pipeline'))
        )
        return filters


class PipelineTagList(TagList):
    def __init__(self, project_id, args):
        super().__init__(project_id, args)

    def set_related_entity_info(self):
        self.Entity = Application
        self.Version = ApplicationVersion
        self.VersionTagAssociation = ApplicationVersionTagAssociation
        self.foriegn_key = 'application_id'
        self.count_name = "application_count"

    def get_related_entity_filters(self):
        filters = super().get_related_entity_filters()
        filters.append(
            self.Entity.versions.any(self.Version.agent_type == "pipeline")
        )
        return filters


class AllTagList(TagList):
    def set_related_entity_info(self):
        pass


def list_tags(project_id, args):
    entity_coverage = args.get("entity_coverage", "all")
    tags = {
        'total': 0,
        'rows': list()
    }
    if entity_coverage == 'all':
        tag_ids = set()
        entities = dict(**TagListABC.meta_registry)
        entities.pop(entity_coverage)
        args = dict(args)
        for entity in entities:
            TagClass: TagList = TagListABC.meta_registry.get(entity)
            args['entity_coverage'] = entity
            half_result = TagClass(project_id, args).get_tags()
            for row in half_result['rows']:
                if row['id'] not in tag_ids:
                    tags['rows'].append(row)
                    tag_ids.add(row['id'])
        tags['total'] = len(tags['rows'])
    else:
        TagClass: TagList = TagListABC.meta_registry.get(entity_coverage)
        if not TagClass:
            return tags
        else:
            tags = TagClass(project_id, args).get_tags()
    return tags
