from pylon.core.tools import log, web
from sqlalchemy import and_

from tools import db

from ..models.elitea_tools import EliteATool
from ..models.enums.events import ApplicationEvents

from ..utils.collections import group_by_project_id, delete_entity_from_collections


class Event:
    @web.event(ApplicationEvents.application_deleted)
    def application_deleted_handler(self, context, event, application_data: dict):
        """Clean up collection memberships and EliteATool references after an application is deleted.

        Published/embedded versions are blocked from deletion at the RPC
        layer, so no cascade or source-sync logic is needed here.
        """
        collections = group_by_project_id(application_data['collections'])
        for owner_id, collection_ids in collections.items():
            with db.get_session(owner_id) as session:
                delete_entity_from_collections(
                    entity_name="application",
                    collection_ids=collection_ids,
                    entity_data=application_data,
                    session=session
                )
                session.commit()

        with db.get_session(application_data['project_id']) as session:
            session.query(EliteATool).where(
                and_(
                    EliteATool.type == 'application',
                    EliteATool.settings.op("->>")("application_id") == str(application_data['id'])
                )
            ).delete()
            session.commit()

    @web.event(ApplicationEvents.application_version_deleted)
    def application_version_deleted_handler(self, context, event, version_data: dict):
        """Handle version deletion cleanup.

        Published/embedded versions are blocked from deletion at the RPC
        layer, so no cascade or source-sync logic is needed here.
        Only draft/unpublished versions reach this handler.
        """
        pass

