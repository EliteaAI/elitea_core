from copy import deepcopy

from ..models.all import Collection
from ..models.enums.all import CollectionPatchOperations
from ..models.pd.collections import (
    CollectionPatchModel
)
from ..utils.collection_registry import (
    ENTITY_REG,
    get_entity_info_by_name
)
from ..utils.collections import (
    CollectionPublishing,
    group_by_project_id,
    patch_collection_with_entities,
)
from ..utils.utils import get_public_project_id
from ..models.enums.all import PublishStatus

from tools import db
from pylon.core.tools import log, web


class Event:
    @web.event("public_collection_status_change")
    def handle_collection_status_change(self, context, event, payload: dict):
        log.info(f'Event {payload}')
        private_project_id = payload['private_project_id']
        private_collection_id = payload['private_collection_id']
        status = payload['status']

        CollectionPublishing.set_status(
            project_id=private_project_id,
            collection_id=private_collection_id,
            status=status
        )

    @web.event("promptlib_shared_collection_updated")
    def handle_collection_updated(self, context, event, payload: dict):
        added_entities = payload['added_entities']
        removed_entities = payload['removed_entities']
        collection_data = payload['collection_data']
        public_id = get_public_project_id()

        entity_info = get_entity_info_by_name(payload['entity_name'])
        Entity = entity_info.get_entity_type()

        # add collection to entities
        grouped_added_entities: dict = group_by_project_id(added_entities, data_type="tuple")
        for owner_id, ids in grouped_added_entities.items():
            add_collection_to_entities(Entity, owner_id, ids, collection_data, context)

        # remove collection from entities
        grouped_removed_entities: dict = group_by_project_id(removed_entities, data_type="tuple")
        for owner_id, ids in grouped_removed_entities.items():
            delete_collection_from_entities(Entity, owner_id, ids, collection_data, context)

    @web.event("promptlib_shared_collection_deleted")
    def handle_collection_deleted(self, context, event, payload: dict):
        collection_data = payload

        for ent in ENTITY_REG:
            entities = group_by_project_id(collection_data[ent.entities_name])
            for owner_id, ids in entities.items():
                delete_collection_from_entities(
                    ent.get_entity_type(),
                    owner_id,
                    ids,
                    collection_data,
                    context
                )

    @web.event("promptlib_shared_collection_added")
    def handle_collection_added(self, context, event, payload: dict):
        collection_data = payload

        for ent in ENTITY_REG:
            entities = group_by_project_id(collection_data[ent.entities_name])
            for owner_id, ids in entities.items():
                add_collection_to_entities(ent.get_entity_type(), owner_id, ids, collection_data, context)

    # TODO find usage / remove
    @web.event('promptlib_shared_entity_published')
    def handle_entity_publishing(self, context, event, payload: dict) -> None:
        entity_data = payload['entity_data']
        collections = payload.get('collections', [])
        entity_name = payload['entity_name']

        for collection in collections:
            data = {
                "project_id": collection["owner_id"],
                "collection_id": collection["id"],
                "operation": CollectionPatchOperations.add.name,
                entity_name: {
                    "owner_id": entity_data['owner_id'],
                    "id": entity_data['id']
                }
            }
            collection_data = CollectionPatchModel.validate(data)
            patch_collection_with_entities(collection_data, passthrough_mode=True)

    @web.event('promptlib_shared_collection_unpublished')
    def handle_collection_unpublished(self, context, event, payload) -> None:
        private_id = payload.get('private_id')
        private_owner_id = payload.get('private_owner_id')
        with db.with_project_schema_session(private_owner_id) as session:
            collection = session.query(Collection).get(private_id)
            if not collection:
                return
            collection.status = PublishStatus.draft
            session.commit()


def delete_collection_from_entities(entity_type, owner_id: int, ids: list, collection_data: dict, context):
    with db.get_session(owner_id) as session:
        for entity in session.query(entity_type).filter(entity_type.id.in_(ids)).with_for_update().all():
            new_data = [
                deepcopy(collection) for collection in entity.collections
                if (collection['owner_id'] != collection_data['owner_id'] or
                    collection['id'] != collection_data['id'])
            ]
            entity.collections = new_data

        session.commit()


def add_collection_to_entities(entity_type, owner_id: int, ids: list, collection_data: dict, context):
    with db.get_session(owner_id) as session:
        for entity in session.query(entity_type).filter(entity_type.id.in_(ids)).with_for_update().all():
            new_data: list = deepcopy(entity.collections)
            new_data.append({
                "owner_id": collection_data['owner_id'],
                "id": collection_data['id']
            })
            entity.collections = new_data

        session.commit()
