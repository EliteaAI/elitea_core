"""Event handlers: keep chat_templates participants in sync with entity changes."""

from pylon.core.tools import log, web

from ..models.enums.events import ApplicationEvents
from ..utils.chat_template_utils import (
    delete_entity_from_templates,
    update_entity_name_in_templates,
)
from ..utils.utils import get_public_project_id

# entity_name values stored in chat_template.participants for each entity type
_APP_ENTITY_NAMES = ['application', 'pipeline']
_TOOLKIT_ENTITY_NAMES = ['toolkit', 'mcp']


def _affected_projects(context, owner_id: int) -> list:
    """Return the list of project IDs whose templates must be updated.

    For public entities the participant can appear in any project's templates,
    so we return all projects. For regular entities only the owning project.
    """
    try:
        public_id = get_public_project_id()
    except Exception:  # pylint: disable=broad-except
        public_id = None

    if public_id is not None and owner_id == public_id:
        try:
            projects = context.rpc_manager.call.project_list(filter_={'create_success': True}) or []
            return [p['id'] for p in projects]
        except Exception:  # pylint: disable=broad-except
            log.exception("chat_template events: failed to list projects for public entity cleanup")
            return [owner_id]

    return [owner_id]


class Event:
    @web.event(ApplicationEvents.application_deleted)
    def on_application_deleted(self, context, event, application_data: dict):
        owner_id = application_data['owner_id']
        entity_id = application_data['id']
        for project_id in _affected_projects(context, owner_id):
            delete_entity_from_templates(project_id, _APP_ENTITY_NAMES, entity_id, owner_id)

    @web.event(ApplicationEvents.toolkit_deleted)
    def on_toolkit_deleted(self, context, event, toolkit_data: dict):
        owner_id = toolkit_data['owner_id']
        entity_id = toolkit_data['id']
        for project_id in _affected_projects(context, owner_id):
            delete_entity_from_templates(project_id, _TOOLKIT_ENTITY_NAMES, entity_id, owner_id)

    @web.event(ApplicationEvents.application_updated)
    def on_application_updated(self, context, event, application_data: dict):
        owner_id = application_data['owner_id']
        entity_id = application_data['id']
        new_name = (application_data.get('data') or {}).get('name')
        if not new_name:
            return
        for project_id in _affected_projects(context, owner_id):
            update_entity_name_in_templates(
                project_id, _APP_ENTITY_NAMES, entity_id, owner_id, new_name
            )

    @web.event(ApplicationEvents.toolkit_updated)
    def on_toolkit_updated(self, context, event, toolkit_data: dict):
        owner_id = toolkit_data['owner_id']
        entity_id = toolkit_data['id']
        new_name = (toolkit_data.get('data') or {}).get('name')
        if not new_name:
            return
        for project_id in _affected_projects(context, owner_id):
            update_entity_name_in_templates(
                project_id, _TOOLKIT_ENTITY_NAMES, entity_id, owner_id, new_name
            )
