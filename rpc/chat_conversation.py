from pylon.core.tools import web, log
from tools import db, config as c, auth, serialize, rpc_tools, MinioClient

from sqlalchemy import desc, asc, Integer, or_
from sqlalchemy.orm import joinedload

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.conversation import ConversationListExtended, ConversationDetails
from ..models.pd.participant import ParticipantCreate, ParticipantEntityUser
from ..models.pd.message import MessageGroupDetail
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..utils.conversation_utils import get_conversation_details, calculate_conversation_duration
from ..utils.participant_utils import add_participant_to_conversation
from ..utils.chat_feature_flags import get_context_manager_feature_flag
from ..utils.context_analytics import set_context_strategy


class RPC:
    @web.rpc("chat_get_conversation_details", "get_conversation_details_rpc")
    def get_conversation_details_rpc(
        self,
        project_id: int,
        conversation_id: int,
        user_id: int = None,
        include_participants: bool = True,
        include_message_groups: bool = True,
    ) -> dict | None:
        """
        Get full conversation details with participants and message groups.

        This is the rich version that includes:
        - Conversation metadata
        - All participants with entity_settings
        - User names/avatars
        - Recent message groups (last 100)
        - Access control checks

        Args:
            project_id: The project ID
            conversation_id: The ID of the conversation
            user_id: Optional user ID for access control (if None, no user-specific filtering)
            include_participants: Whether to include participant details (default True)
            include_message_groups: Whether to include message groups (default True)

        Returns:
            Dict containing full conversation details or None if not found/unauthorized
        """
        with db.get_session(project_id) as session:
            try:
                result: ConversationDetails = get_conversation_details(
                    session=session,
                    conversation_id=conversation_id,
                    project_id=project_id,
                    user_id=user_id
                )

                if not result:
                    return None

                serialized = serialize(result)

                # Allow callers to skip heavy fields if not needed
                if not include_participants:
                    serialized.pop('participants', None)
                if not include_message_groups:
                    serialized.pop('message_groups', None)

                return serialized

            except Exception as e:
                log.error(f"Error getting conversation details: {str(e)}")
                return None

    @web.rpc("chat_update_conversation_meta", "update_conversation_meta")
    def update_conversation_meta(self, project_id: int, conversation_id: int, meta_updates: dict) -> dict:
        """
        Update conversation metadata

        Args:
            project_id: The project ID
            conversation_id: The ID of the conversation to update
            meta_updates: Dictionary of metadata updates to apply

        Returns:
            Dict containing success status and updated meta
        """
        with db.get_session(project_id) as session:
            try:
                conversation = session.query(Conversation).filter(
                    Conversation.id == conversation_id
                ).first()

                if not conversation:
                    raise Exception(f"Conversation with ID {conversation_id} not found")

                current_meta = conversation.meta or {}
                updated_meta = {**current_meta, **meta_updates}

                session.query(Conversation).filter(
                    Conversation.id == conversation_id
                ).update(
                    {Conversation.meta: updated_meta},
                    synchronize_session=False
                )

                session.commit()

                return {
                    "success": True,
                    "conversation_id": conversation_id,
                    "updated_meta": updated_meta
                }

            except Exception as e:
                session.rollback()
                log.error(f"Error updating conversation meta: {str(e)}")
                raise Exception(f"Failed to update conversation meta")

    @web.rpc("chat_create_conversation_rpc", "create_conversation_rpc")
    def create_conversation_rpc(
        self,
        project_id: int,
        user_id: int,
        name: str = None,
        source: str = 'elitea',
        is_private: bool = True,
        meta: dict = None,
        instructions: str = None,
        add_dummy_participant: bool = True,
        apply_user_personalization: bool = True,
        apply_context_strategy: bool = True,
    ) -> dict:
        """
        Create conversation with participants and optional context strategy.

        Shared RPC for conversation creation used by:
        - elitea_core conversations API
        - support_assistant plugin
        - Future embedded assistants
        """
        user_personalization = None
        user_context_defaults = None
        user_summarization_defaults = None

        if apply_user_personalization:
            try:
                social_user = rpc_tools.RpcMixin().rpc.timeout(2).social_get_user(user_id)
                if social_user:
                    user_personalization = social_user.get('personalization')
                    user_context_defaults = social_user.get('default_context_management')
                    user_summarization_defaults = social_user.get('default_summarization')
            except Exception:
                pass

        conversation_meta = meta or {}
        effective_instructions = instructions

        if user_personalization:
            if user_personalization.get('persona'):
                conversation_meta['persona'] = user_personalization['persona']
            if user_personalization.get('default_instructions'):
                conversation_meta['default_instructions'] = user_personalization['default_instructions']
                if not effective_instructions:
                    effective_instructions = user_personalization['default_instructions']

        with db.get_session(project_id) as session:
            conversation = Conversation(
                name=name or 'New conversation',
                source=source,
                is_private=is_private,
                meta=conversation_meta,
                instructions=effective_instructions,
                author_id=user_id,
            )
            session.add(conversation)
            session.flush()

            user_participant_data = ParticipantCreate(
                entity_name=ParticipantTypes.user,
                entity_meta=ParticipantEntityUser(id=user_id)
            )
            add_participant_to_conversation(
                project_id=project_id,
                session=session,
                participant=user_participant_data,
                conversation=conversation,
                initiator_id=user_id
            )
            session.flush()

            if add_dummy_participant:
                dummy_participant_data = ParticipantCreate(
                    entity_name=ParticipantTypes.dummy,
                    entity_meta={}
                )
                add_participant_to_conversation(
                    project_id=project_id,
                    session=session,
                    participant=dummy_participant_data,
                    conversation=conversation,
                    initiator_id=user_id
                )
                session.flush()

            context_strategy = None
            if apply_context_strategy and get_context_manager_feature_flag(project_id):
                context_strategy = set_context_strategy(
                    project_id=project_id,
                    conversation_id=conversation.id,
                    user_context_defaults=user_context_defaults,
                    user_summarization_defaults=user_summarization_defaults,
                )

            session.commit()
            session.expire_all()

            result: ConversationDetails = get_conversation_details(
                session, conversation.id, project_id, user_id
            )
            serialized = serialize(result)

            if context_strategy and 'meta' in serialized:
                serialized['meta']['context_strategy'] = context_strategy

            return serialized

    @web.rpc("chat_list_conversations_rpc", "list_conversations_rpc")
    def list_conversations_rpc(
        self,
        project_id: int,
        user_id: int,
        source: str = None,
        query: str = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: str = 'created_at',
        sort_order: str = 'desc',
        include_hidden: bool = False,
        is_admin: bool = False,
    ) -> dict:
        """
        List conversations with filtering, sorting, and pagination.

        Shared RPC for conversation listing used by:
        - elitea_core conversations API
        - support_assistant plugin (with source='support', include_hidden=True)
        """
        with db.get_session(project_id) as session:
            sorting_by = getattr(Conversation, sort_by, Conversation.created_at)
            sorting = desc if sort_order == 'desc' else asc

            participant_subquery_filters = [Participant.entity_name == ParticipantTypes.user.value]
            if not is_admin:
                participant_subquery_filters.append(
                    Participant.entity_meta['id'].astext.cast(Integer) == user_id,
                )

            participant_subquery = session.query(Participant.id).filter(
                *participant_subquery_filters
            ).subquery()

            distinct_conversation_subquery = session.query(Conversation.id).distinct().join(
                ParticipantMapping,
                Conversation.id == ParticipantMapping.conversation_id
            ).join(
                Participant,
                Participant.id == ParticipantMapping.participant_id
            ).filter(
                or_(
                    Conversation.is_private == False,
                    Participant.id.in_(participant_subquery)
                )
            ).subquery()

            base_query = session.query(Conversation).where(
                Conversation.id.in_(distinct_conversation_subquery)
            )

            if query:
                base_query = base_query.where(Conversation.name.ilike(f'%{query}%'))

            if source:
                sources = list(set(i.strip().lower() for i in source.split(',')))
                base_query = base_query.where(Conversation.source.in_(sources))

            if not include_hidden:
                base_query = base_query.filter(
                    or_(
                        Conversation.meta['is_hidden'].astext == 'false',
                        Conversation.meta['is_hidden'].astext.is_(None)
                    )
                )

            base_query = base_query.order_by(sorting(sorting_by))

            total = base_query.count()
            conversations = base_query.limit(limit).offset(offset).all()

            rows = []
            for conv in conversations:
                duration = -1 if source and 'elitea' in source else calculate_conversation_duration(conv, session)
                conv_dict = {
                    **serialize(conv),
                    "duration": duration,
                    "participants_count": len(conv.participants),
                    "message_groups_count": conv.message_groups.count(),
                    "users_count": sum(1 for p in conv.participants if p.entity_name == ParticipantTypes.user.value),
                }
                rows.append(serialize(ConversationListExtended.model_validate(conv_dict).model_dump()))

            return {'total': total, 'rows': rows}

    @web.rpc("chat_get_conversation_by_uuid_rpc", "get_conversation_by_uuid_rpc")
    def get_conversation_by_uuid_rpc(
        self,
        project_id: int,
        conversation_uuid: str,
        user_id: int = None,
        check_ownership: bool = True,
    ) -> dict | None:
        """
        Get conversation by UUID with optional ownership verification.

        Used by support_assistant to verify conversation access before operations.
        """
        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.uuid == conversation_uuid,
            ).first()

            if not conversation:
                return None

            if check_ownership and user_id and conversation.is_private:
                user_participant = session.query(Participant).filter(
                    Participant.entity_name == ParticipantTypes.user.value,
                    Participant.entity_meta['id'].astext.cast(Integer) == user_id,
                ).first()

                if not user_participant:
                    return None

                is_participant = session.query(ParticipantMapping).filter(
                    ParticipantMapping.conversation_id == conversation.id,
                    ParticipantMapping.participant_id == user_participant.id,
                ).first()

                if not is_participant:
                    return None

            return {
                'id': conversation.id,
                'uuid': str(conversation.uuid),
                'name': conversation.name,
                'source': conversation.source,
                'is_private': conversation.is_private,
                'meta': conversation.meta,
                'created_at': conversation.created_at.isoformat(),
                'created_at_ts': conversation.created_at.timestamp(),
            }

    @web.rpc("chat_delete_conversation_rpc", "delete_conversation_rpc")
    def delete_conversation_rpc(
        self,
        project_id: int,
        conversation_id: int = None,
        conversation_uuid: str = None,
        user_id: int = None,
        check_ownership: bool = True,
    ) -> dict:
        """
        Delete conversation by ID or UUID.

        Shared RPC for conversation deletion used by:
        - elitea_core conversations API (by ID)
        - support_assistant plugin (by UUID with ownership check)
        """
        with db.get_session(project_id) as session:
            if conversation_uuid:
                conversation = session.query(Conversation).filter(
                    Conversation.uuid == conversation_uuid,
                ).first()
            else:
                conversation = session.query(Conversation).filter(
                    Conversation.id == conversation_id,
                ).first()

            if not conversation:
                return {'success': False, 'error': 'Conversation not found'}

            if check_ownership and user_id and conversation.is_private:
                verified = self.get_conversation_by_uuid_rpc(
                    project_id=project_id,
                    conversation_uuid=str(conversation.uuid),
                    user_id=user_id,
                    check_ownership=True,
                )
                if not verified:
                    return {'success': False, 'error': 'Unauthorized'}

            try:
                mc = MinioClient.from_project_id(project_id)
                bucket_name = f'conversation_{conversation.uuid}'
                mc.remove_bucket(bucket_name)
            except Exception:
                pass

            session.delete(conversation)
            session.commit()

            return {'success': True}

    @web.rpc("chat_list_messages_rpc", "list_messages_rpc")
    def list_messages_rpc(
        self,
        project_id: int,
        conversation_id: int,
        query: str = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: str = 'created_at',
        sort_order: str = 'desc',
    ) -> dict:
        """
        List messages from a conversation with filtering and pagination.

        Shared RPC for message listing used by:
        - elitea_core messages API
        - support_assistant plugin
        """
        with db.get_session(project_id) as session:
            sorting_by = getattr(ConversationMessageGroup, sort_by, ConversationMessageGroup.created_at)
            sorting = desc if sort_order == 'desc' else asc

            base_query = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.conversation_id == conversation_id
            )

            if query:
                base_query = base_query.join(
                    TextMessageItem,
                    ConversationMessageGroup.message_items
                ).filter(TextMessageItem.content.ilike(f'%{query}%'))

            total = base_query.count()
            messages = base_query.order_by(sorting(sorting_by)).limit(limit).offset(offset).all()

            rows = [serialize(MessageGroupDetail.from_orm(m)) for m in messages]

            return {'total': total, 'rows': rows}

    @web.rpc("chat_add_application_participant_rpc", "add_application_participant_rpc")
    def add_application_participant_rpc(
        self,
        project_id: int,
        conversation_id: int,
        application_id: int,
        application_project_id: int = None,
    ) -> dict | None:
        """
        Add an application (agent) participant to a conversation.
        Returns the participant dict or None if failed.

        Used by support_assistant to add the support agent to conversations.
        """
        from ..models.pd.participant import ParticipantEntityApplication

        effective_project_id = application_project_id or project_id

        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()

            if not conversation:
                return None

            existing = session.query(Participant).join(
                ParticipantMapping,
                Participant.id == ParticipantMapping.participant_id
            ).filter(
                ParticipantMapping.conversation_id == conversation_id,
                Participant.entity_name == ParticipantTypes.application.value,
                Participant.entity_meta['id'].astext == str(application_id),
            ).first()

            if existing:
                return {
                    'id': existing.id,
                    'entity_name': existing.entity_name,
                    'entity_meta': existing.entity_meta,
                }

            participant_data = ParticipantCreate(
                entity_name=ParticipantTypes.application.value,
                entity_meta=ParticipantEntityApplication(
                    id=application_id,
                    project_id=effective_project_id,
                ),
            )

            try:
                participant = add_participant_to_conversation(
                    session=session,
                    participant=participant_data,
                    conversation=conversation,
                    project_id=project_id,
                )
                return {
                    'id': participant.id,
                    'entity_name': participant.entity_name,
                    'entity_meta': participant.entity_meta,
                }
            except Exception as e:
                log.error(f"Failed to add application participant: {e}")
                return None
