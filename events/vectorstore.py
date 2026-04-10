from pylon.core.tools import log, web

from tools import db
from ..models.enums.events import ApplicationEvents
from ..models.enums.all import PublishStatus


class Event:
    @web.event('project_created')
    def create_vectorstore_for_new_project(self, context, event, project_json: dict):
        self.create_pgvector_credentials(project_ids=[project_json['id']], save_connstr_to_secrets=True)