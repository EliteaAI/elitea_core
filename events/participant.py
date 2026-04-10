from pylon.core.tools import web

from ..models.enums.all import ParticipantTypes
from ..utils.participant_utils import update_participant_meta
from ..utils.utils import get_public_project_id


def _delete_entity_participant_from_chats(module, context, entity_name, entity_data):
    owner_id = entity_data['owner_id']
    entity_meta = {
        'project_id': owner_id,
        'id': entity_data['id']
    }
    # when we deleting public entity, it might be shared in any chat of any project
    # so process and clean them all
    try:
        public_id = get_public_project_id()
    except Exception:
        public_id = None
    if owner_id == public_id:
        projects = context.rpc_manager.call.project_list(filter_={'create_success': True})
        project_ids = [p['id'] for p in projects]
    else:
        project_ids = [owner_id,]
    for project_id in project_ids:
        module.delete_entity_in_all_conversations(
            project_id,
            entity_name,
            entity_meta
        )


class Event:
    @web.event('datasource_deleted')
    def delete_datasource_participant_handler(self, context, event, datasource_data: dict):
        _delete_entity_participant_from_chats(
            self, context, ParticipantTypes.datasource.name, datasource_data
        )

    @web.event('datasource_updated')
    def update_datasource_participant_handler(self, context, event, datasource_data: dict):
        project_id = datasource_data['owner_id']
        update_participant_meta(
            project_id,
            ParticipantTypes.datasource,
            entity_meta={
                'project_id': project_id,
                'id': datasource_data['id']
            },
            meta=datasource_data['data']
        )

    @web.event('prompt_deleted')
    def delete_prompt_participant_handler(self, context, event, data: dict):
        _delete_entity_participant_from_chats(
            self, context, ParticipantTypes.prompt.name, data['prompt_data']
        )

    @web.event('application_deleted')
    def delete_application_participant_handler(self, context, event, application_data: dict):
        _delete_entity_participant_from_chats(
            self, context, ParticipantTypes.application.name, application_data
        )

    @web.event('toolkit_deleted')
    def delete_toolkit_participant_handler(self, context, event, toolkit_data: dict):
        _delete_entity_participant_from_chats(
            self, context, ParticipantTypes.toolkit.name, toolkit_data
        )

    @web.event('application_updated')
    def update_application_participant_handler(self, context, event, application_data: dict):
        project_id = application_data['owner_id']
        update_participant_meta(
            project_id,
            ParticipantTypes.application,
            entity_meta={
                'project_id': project_id,
                'id': application_data['id']
            },
            meta=application_data['data']
        )

    @web.event('toolkit_updated')
    def update_toolkit_participant_handler(self, context, event, toolkit_data: dict):
        project_id = toolkit_data['owner_id']
        update_participant_meta(
            project_id,
            ParticipantTypes.toolkit,
            entity_meta={
                'project_id': project_id,
                'id': toolkit_data['id']
            },
            meta=toolkit_data['data']
        )

    @web.event('integration_settings_changed')
    def integration_model_changed(self, context, event, settings_data: dict):
        project_ids = settings_data['project_ids']
        new_settings = settings_data['new_settings']
        old_settings = settings_data['old_settings']
        integration_uid = settings_data['integration_uid']

        new_model_names = [
            m['name'] for m in new_settings.get('models',[]) if m['capabilities']['chat_completion']
        ]
        deleted_model_names = [
            m['name'] for m in old_settings.get('models',[])
            if m['name'] not in new_model_names and m['capabilities']['chat_completion']
        ]
        for model_name in deleted_model_names:
            for project_id in project_ids:
                self.delete_entity_in_all_conversations(
                    project_id,
                    'llm',
                    {
                        'model_name': model_name,
                        'integration_uid': integration_uid
                    }
                )
