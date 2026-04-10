from typing import Optional, Tuple

from pylon.core.tools import log
from sqlalchemy import Integer, String, update, case
from sqlalchemy.exc import IntegrityError, PendingRollbackError
from tools import db, rpc_tools, context, auth, serialize, VaultClient, this

from .sio_utils import get_chat_room
from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes, ChatHistoryTemplates
from ..models.message_group import ConversationMessageGroup
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.participant import ParticipantBase, ParticipantCreate, EntityMetaType, \
    ParticipantEntityDatasource, ParticipantEntityDummy, ParticipantEntityApplication, \
    entity_meta_mapping, ParticipantEntityUser, ParticipantEntityToolkit
from ..models.pd.participant_settings import EntitySettingsApplication, \
    EntitySettingsLlm, EntitySettingsUser
from ..models.enums.all import NotificationEventTypes
from ..utils.authors import get_authors_data
from ..utils.sio_utils import SioEvents


class UnknownEntityError(Exception):
    pass


class ParticipantAlreadyAddedException(ValueError):
    """Raised when trying to add a participant that's already in the conversation"""
    def __init__(self, message: str, participant_id: int, entity_settings: Optional[dict] = None):
        super().__init__(message)
        self.participant_id = participant_id
        self.entity_settings = entity_settings or {}


def make_query_filter_for_entity(entity_name: ParticipantTypes, entity_meta: EntityMetaType) -> list:
    match entity_name:
        case ParticipantTypes.llm:
            return [
                Participant.entity_meta['model_name'].astext.cast(String) == str(entity_meta.model_name),
            ]
        case ParticipantTypes.user:
            return [Participant.entity_meta['id'].astext.cast(Integer) == int(entity_meta.id)]
        case ParticipantTypes.dummy:
            return []
        case ParticipantTypes.toolkit:
            entity_id = entity_meta.id if hasattr(entity_meta, 'id') else entity_meta.get('id')
            entity_project_id = entity_meta.project_id if hasattr(entity_meta, 'project_id') else entity_meta.get('project_id')
            return [
                Participant.entity_meta['id'].astext.cast(Integer) == int(entity_id),
                Participant.entity_meta['project_id'].astext.cast(Integer) == int(entity_project_id),
            ]
        case _:
            try:
                return [
                    Participant.entity_meta['id'].astext.cast(Integer) == int(entity_meta.id),
                    Participant.entity_meta['project_id'].astext.cast(Integer) == int(entity_meta.project_id),
                ]
            except TypeError:
                return [
                    Participant.entity_meta['id'].astext.cast(Integer) == int(entity_meta.id),
                    Participant.entity_meta['project_id'].is_(None)
                ]


def get_or_create_one(
        session,
        entity_name: ParticipantTypes,
        entity_meta: EntityMetaType,
        **kwargs
) -> Tuple[Participant, Optional[dict]]:
    flt = make_query_filter_for_entity(entity_name, entity_meta)
    p: Participant = session.query(Participant).where(
        Participant.entity_name == entity_name,
        *flt
    ).first()
    entity_details = None
    if not p:
        entity_details = get_entity_details(entity_name, entity_meta)
        if not isinstance(entity_meta, dict):
            entity_meta = entity_meta.dict()
        match entity_name:
            case ParticipantTypes.prompt:
                meta = {'name': entity_details.get('name')}
            case ParticipantTypes.datasource:
                meta = {'name': entity_details.get('name')}
            case ParticipantTypes.application:
                meta = {'name': entity_details.get('name')}
            case ParticipantTypes.llm:
                meta = {'name': entity_meta['model_name']}
            case ParticipantTypes.dummy:
                meta = {'name': "EliteA"}
            case ParticipantTypes.user:
                meta = {
                    'user_name': entity_details.get('user_name'),
                    'user_avatar': entity_details.get('user_avatar'),
                }
            case ParticipantTypes.toolkit:
                meta = {
                    'name': entity_details.get('toolkit_name'),
                }
                mcp_flag = entity_details.get('meta', {}).get('mcp', False)
                if mcp_flag:
                    meta['mcp'] = mcp_flag
            case _:
                meta = {}
        p = Participant(
            entity_name=entity_name.value,
            entity_meta=entity_meta,
            meta=serialize(meta)
        )
        session.add(p)
        session.commit()

    # FIXME remove; this should fix old participants without user_name and user_avatar
    if entity_name == ParticipantTypes.user and not p.meta:
        entity_details = get_entity_details(entity_name, entity_meta)
        p.meta = {
            'user_name': entity_details.get('user_name'),
            'user_avatar': entity_details.get('user_avatar'),
        }
        session.commit()

    return p, entity_details


def add_participant_to_conversation(
        participant: ParticipantCreate,
        conversation: Conversation,
        session,
        project_id: int,
        initiator_id: Optional[int] = None
) -> ParticipantBase:
    participant_orm, entity_details = get_or_create_one(
        session,
        entity_name=participant.entity_name,
        entity_meta=participant.entity_meta,
    )
    entity_settings = get_participant_settings(
        entity_name=participant_orm.entity_name,
        entity_meta=participant_orm.entity_meta,
        entity_details=entity_details,
    )
    entity_settings.update(participant.entity_settings)

    try:
        conversation.participants.append(participant_orm)
        session.query(ParticipantMapping).filter(
            ParticipantMapping.conversation_id == conversation.id,
            ParticipantMapping.participant_id == participant_orm.id
        ).update({'entity_settings': entity_settings})
        session.commit()
    except (IntegrityError, PendingRollbackError):
        session.rollback()
        raise ParticipantAlreadyAddedException(
            f'Participant with id {participant_orm.id} is already added',
            participant_id=participant_orm.id,
            entity_settings=entity_settings
        )

    participant_model = ParticipantBase.from_orm(participant_orm)
    participant_model.entity_settings = entity_settings

    if participant_orm.entity_name == ParticipantTypes.user:
        notify_user_added_to_conversation(
            project_id=project_id,
            participant_model=participant_model,
            conversation=conversation,
            initiator_id=initiator_id
        )

    # Fire adoption counter event when a public agent is added to a conversation
    if participant_orm.entity_name == ParticipantTypes.application:
        _fire_adoption_event(participant_orm.entity_meta, project_id)

    return participant_model


def _fire_adoption_event(entity_meta, consumer_project_id: int):
    """Fire an adoption counter increment event for a public agent added to a conversation."""
    try:
        from .utils import get_public_project_id
        meta = entity_meta if isinstance(entity_meta, dict) else entity_meta.dict() if hasattr(entity_meta, 'dict') else {}
        agent_project_id = meta.get('project_id')
        public_project_id = get_public_project_id()
        if agent_project_id != public_project_id:
            return  # Not a public agent — skip
        agent_id = meta.get('id')
        if not agent_id:
            return
        event_manager = rpc_tools.EventManagerMixin().event_manager
        event_manager.fire_event(
            'adoption_counter_increment',
            {
                'public_project_id': public_project_id,
                'agent_id': agent_id,
                'consumer_project_id': consumer_project_id,
            },
        )
    except Exception as e:
        log.warning("[ADOPTION] Failed to fire adoption event: %s", e)


def notify_user_added_to_conversation(
        project_id: int,
        participant_model: ParticipantBase,
        conversation: Conversation,
        initiator_id: Optional[int] = None
):
    event_manager = rpc_tools.EventManagerMixin().event_manager
    user_id = participant_model.entity_meta['id']
    if user_id == initiator_id:
        return

    initiator_name = None
    try:
        initiator_name = auth.get_user(user_id=initiator_id)['name']
    except Exception as ex:
        log.warning(ex)

    event_manager.fire_event(
        'notifications_stream', {
            'project_id': project_id,
            'user_id': user_id,
            'meta': {
                "conversation_id": conversation.id,
                "conversation_name": conversation.name,
                "initiator_name": initiator_name
            },
            'event_type': NotificationEventTypes.chat_user_added
        }
    )


def get_entity_details(
        entity_name: ParticipantTypes,
        entity_meta: EntityMetaType
) -> dict | None:
    match entity_name:
        # todo: handle some timeouts and empty responses
        case ParticipantTypes.datasource:
            meta: ParticipantEntityDatasource = ParticipantEntityDatasource.parse_obj(entity_meta)
            return rpc_tools.RpcMixin().rpc.timeout(
                5
            ).datasources_get_datasource_by_id(
                project_id=meta.project_id,
                datasource_id=meta.id,
                first_existing_version=True
            )

        case ParticipantTypes.application:
            meta: ParticipantEntityApplication = ParticipantEntityApplication.parse_obj(entity_meta)
            return this.module.get_application_by_id(
                project_id=meta.project_id,
                application_id=meta.id,
                first_existing_version=True
            )
        case ParticipantTypes.llm:
            # meta: ParticipantEntityLlm = ParticipantEntityLlm.parse_obj(entity_meta)
            # llm_configurations = rpc_tools.RpcMixin().rpc.timeout(
            #     5
            # ).configurations_get_filtered_project(
            #     project_id=meta.project_id,
            #     include_shared=True,
            #     filter_fields={'section': 'llm'}
            # )
            # # just return details of first found configuration
            # # we do not need details, just the fact that any model_name is available
            # for llm_config in llm_configurations:
            #     if llm_config.get('data', {}).get('model_name') == meta.model_name:
            #         return llm_config
            # else:
            #     raise RuntimeError(
            #         f"LLM with model_name '{meta.model_name}' not found in project {meta.project_id}"
            #     )
            return None
        case ParticipantTypes.user:
            meta: ParticipantEntityUser = ParticipantEntityUser.parse_obj(entity_meta)
            authors_data = get_authors_data([meta.id])
            user_meta = {}
            if authors_data:
                user_meta['user_name'] = authors_data[0].get('name')
                user_meta['user_avatar'] = authors_data[0].get('avatar')
            return user_meta
        case ParticipantTypes.toolkit:
            meta: ParticipantEntityToolkit = ParticipantEntityToolkit.parse_obj(entity_meta)
            return this.module.get_toolkit_by_id(
                project_id=meta.project_id,
                toolkit_id=meta.id
            )
    return None


def get_participant_settings(
        entity_name: ParticipantTypes,
        entity_meta: EntityMetaType,
        entity_details: Optional[dict] = None,
) -> dict:
    chat_history_template = ChatHistoryTemplates.all.value
    match entity_name:
        case ParticipantTypes.application:
            meta: ParticipantEntityApplication = entity_meta
            if entity_details is None:
                entity_details = get_entity_details(entity_name, meta)

            # Get version details
            version_details = entity_details['version_details'].copy()
            version_details['chat_history_template'] = chat_history_template

            # Exclude llm_settings - they will be fetched from version_details at prediction time
            return EntitySettingsApplication.parse_obj(version_details).dict(exclude={'llm_settings'})

        case ParticipantTypes.llm:
            # meta: ParticipantEntityLlm = entity_meta
            # if entity_details is None:
            #   entity_details = get_entity_details(entity_name, meta)
            # return EntitySettingsLlm.parse_obj(entity_details['settings']).dict()
            if entity_details is None:
                entity_details = {}
            entity_details['chat_history_template'] = chat_history_template
            return EntitySettingsLlm.parse_obj(entity_details).dict()
        case ParticipantTypes.user:
            meta: ParticipantEntityUser = entity_meta
            if entity_details is None:
                entity_details = get_entity_details(entity_name, meta)
            entity_details['chat_history_template'] = chat_history_template
            return EntitySettingsUser.parse_obj(entity_details).dict()
    return {'chat_history_template': chat_history_template}


def replace_participant_by_dummy(project_id, participant_id, session):
    log.debug(f"Substituting {project_id=} {participant_id=} by Dummy participant")
    dummy_id_subquery = (
        session.query(Participant.id)
        .filter(Participant.entity_name == ParticipantTypes.dummy)
        .limit(1)
        .scalar_subquery()
    )

    stmt = (
        update(ConversationMessageGroup)
        .values({
            ConversationMessageGroup.author_participant_id: case(
                (ConversationMessageGroup.author_participant_id == participant_id, dummy_id_subquery),
                else_=ConversationMessageGroup.author_participant_id
            ),
            ConversationMessageGroup.sent_to_id: case(
                (ConversationMessageGroup.sent_to_id == participant_id, dummy_id_subquery),
                else_=ConversationMessageGroup.sent_to_id
            )
        })
        .where(
            (ConversationMessageGroup.author_participant_id == participant_id) |
            (ConversationMessageGroup.sent_to_id == participant_id)
        )
    )
    try:
        session.execute(stmt)
        session.commit()
    except Exception as ex:
        log.error(f"Failed dummy substitution. Perhaps, dummy participant has not been created in project {project_id}")


def update_participant_meta(project_id, entity_name: ParticipantTypes, entity_meta: dict, meta: dict):
    entity_meta = entity_meta_mapping[entity_name](**entity_meta)

    with db.get_session(project_id) as session:
        participant = session.query(Participant).filter(
            Participant.entity_name == entity_name,
            Participant.entity_meta.contains(entity_meta.dict())
        ).first()

        if participant:
            # only do substitutions, not deletions
            updated_by = {}
            for field, value in participant.meta.items():
                if field in meta:
                    updated_by[field] = meta[field]
                else:
                    updated_by[field] = value
            if updated_by == participant.meta:
                log.debug(f"Participant meta has not been really changed for {entity_name}:{entity_meta}")
                return
            participant.meta = updated_by
            session.commit()


def delete_participant_from_conversation(project_id, conversation_id, participant_id, session=None):
    session_is_created = False
    if session is None:
        session = db.get_project_schema_session(project_id)
        session_is_created = True

    try:
        participant_mapping = session.query(ParticipantMapping).filter(
            ParticipantMapping.conversation_id == conversation_id,
            ParticipantMapping.participant_id == participant_id,
        ).first()
        if participant_mapping is None:
            return {"error": "Participant was not found"}, 404

        conversation = session.query(
            Conversation
        ).filter(Conversation.id == conversation_id).first()

        participant = next((p for p in conversation.participants if p.id == participant_id), None)
        if participant.entity_name == ParticipantTypes.user:
            user_id = participant.entity_meta.get('id')
            if user_id == conversation.author_id:
                return {"error": "Cannot delete author of the conversation"}, 400

        session.delete(participant_mapping)

        # Check if it was attachment participant and clear ref if yes
        if conversation.attachment_participant_id == participant_id:
            conversation.attachment_participant_id = None

        if session_is_created:
            session.commit()
        else:
            session.flush()

        room = get_chat_room(conversation.uuid)
        context.sio.emit(
            event=SioEvents.chat_participant_delete,
            data={
                'conversation_id': conversation_id,
                'participant_id': participant_id,
            },
            room=room,
        )

        return {}, 204
    finally:
        if session_is_created:
            session.close()


def delete_entity_from_all_conversations(project_id, entity_name: ParticipantTypes, entity_meta: EntityMetaType):
    with db.get_session(project_id) as session:
        participant = session.query(
            Participant
        ).filter(Participant.entity_meta.contains(entity_meta.dict())).first()
        if participant:
            conversation_ids = []
            participant_maps = session.query(ParticipantMapping).filter(
                ParticipantMapping.participant_id == participant.id,
            ).all()
            for participant_map in participant_maps:
                session.delete(participant_map)
                session.commit()
                conversation_ids.append(participant_map.conversation_id)

            conversations = session.query(
                Conversation
            ).filter(Conversation.id.in_(conversation_ids)).all()

            dummy_participant_data = ParticipantCreate(
                entity_name=ParticipantTypes.dummy,
                entity_meta=ParticipantEntityDummy()
            )
            for conversation in conversations:
                room = get_chat_room(conversation.uuid)
                context.sio.emit(
                    event=SioEvents.chat_participant_delete,
                    data={
                        'conversation_id': conversation.id,
                        'participant_id': participant.id,
                    },
                    room=room,
                )
                try:
                    add_participant_to_conversation(
                        session=session,
                        participant=dummy_participant_data,
                        conversation=conversation,
                        project_id=project_id,
                        initiator_id=None
                    )
                except ParticipantAlreadyAddedException:
                    # Dummy already in conversation
                    pass

            replace_participant_by_dummy(project_id, participant.id, session)
            session.delete(participant)
            session.commit()
