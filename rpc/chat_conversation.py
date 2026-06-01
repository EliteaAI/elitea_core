from pylon.core.tools import web, log
from tools import db, config as c, auth, serialize, rpc_tools, MinioClient

from sqlalchemy import desc, asc, Integer, or_, func

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.conversation import ConversationListExtended, ConversationDetails
from ..models.pd.participant import ParticipantCreate, ParticipantEntityUser
from ..models.pd.message import MessageGroupDetail
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..utils.conversation_utils import (
    get_conversation_details,
    calculate_conversation_durations_batch,
)
from ..utils.participant_utils import add_participant_to_conversation
from ..utils.chat_feature_flags import get_context_manager_feature_flag
from ..utils.context_analytics import set_context_strategy

# Hard cap on page size: defence-in-depth against unbounded IN(...) lists in
# the per-page aggregation queries below. UI default is 10; support_assistant
# and CLI tools may pass larger values, but anything above this is rejected
# silently rather than fanning into a heavy GROUP BY.
LIST_CONVERSATIONS_MAX_LIMIT = 100


class RPC:
    @web.rpc("chat_get_conversation_details", "get_conversation_details_rpc")
    def get_conversation_details_rpc(
        self,
        project_id: int,
        conversation_id: int,
        user_id: int = None,
        include_participants: bool = True,
        include_message_groups: bool = True,
        check_ownership: bool = True,
        messages_limit: int = 100,
        messages_offset: int = 0,
        sort_order: str = 'acs',
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
            messages_limit: Maximum number of message groups to return (default 15)
            messages_offset: Number of message groups to skip (default 0)
            sort_order: Sort order (default 'acs')

        Returns:
            Dict containing full conversation details or None if not found/unauthorized
        """
        with db.get_session(project_id) as session:
            try:
                result: ConversationDetails = get_conversation_details(
                    session=session,
                    conversation_id=conversation_id,
                    project_id=project_id,
                    user_id=user_id,
                    check_ownership=check_ownership,
                    messages_limit=messages_limit,
                    messages_offset=messages_offset,
                    sort_order=sort_order,
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

    @web.rpc("chat_update_conversation_rpc", "update_conversation_rpc")
    def update_conversation_rpc(
        self,
        project_id: int,
        conversation_id: int,
        name: str = None,
        instructions: str = None,
        is_private: bool = None,
        is_hidden: bool = None,
        meta: dict = None,
        attachment_participant_id: int = None,
        folder_id: int = None,
        update_folder: bool = False,
    ) -> dict:
        """
        Update conversation fields.

        Returns dict with success status and updated conversation.
        """
        from sqlalchemy.orm.attributes import flag_modified
        from ..models.enums.all import ParticipantTypes
        from ..models.participants import Participant, ParticipantMapping
        from tools import this

        with db.get_session(project_id) as session:
            try:
                conversation = session.query(Conversation).filter(
                    Conversation.id == conversation_id
                ).first()

                if not conversation:
                    return {'success': False, 'error': 'Conversation not found'}

                if is_private is not None:
                    if is_private and not conversation.is_private:
                        return {'success': False, 'error': 'Public conversation cannot be changed to private'}

                    from ..utils.utils import get_public_project_id
                    public_project_id = get_public_project_id()
                    if not is_private and conversation.is_private and public_project_id == project_id:
                        return {'success': False, 'error': 'Public conversation cannot exist in public project'}
                    conversation.is_private = is_private

                if attachment_participant_id is not None:
                    participant_mapping = session.query(ParticipantMapping).filter(
                        ParticipantMapping.conversation_id == conversation_id,
                        ParticipantMapping.participant_id == attachment_participant_id
                    ).first()
                    if not participant_mapping:
                        return {'success': False, 'error': f'Attachment participant {attachment_participant_id} is not in conversation'}

                    participant = session.query(Participant).filter(
                        Participant.id == attachment_participant_id,
                    ).first()
                    if not participant or participant.entity_name != ParticipantTypes.toolkit.value:
                        return {'success': False, 'error': f'Participant {attachment_participant_id} is not a toolkit participant'}

                    toolkit_details = this.module.get_toolkit_by_id(
                        project_id=participant.entity_meta['project_id'],
                        toolkit_id=participant.entity_meta['id']
                    )
                    if toolkit_details.get('type') != 'artifact':
                        return {'success': False, 'error': f'Participant {attachment_participant_id} is not an artifact participant'}

                    conversation.attachment_participant_id = attachment_participant_id

                if name is not None:
                    conversation.name = name

                if instructions is not None:
                    conversation.instructions = instructions

                if meta is not None:
                    conversation.meta = conversation.meta or {}
                    conversation.meta.update(meta)
                    flag_modified(conversation, 'meta')

                if is_hidden is not None:
                    conversation.meta = conversation.meta or {}
                    conversation.meta['is_hidden'] = is_hidden
                    flag_modified(conversation, 'meta')

                if folder_id is not None or update_folder:
                    conversation.folder_id = folder_id

                session.commit()
                session.refresh(conversation)

                return {
                    'success': True,
                    'conversation': serialize(conversation),
                }

            except Exception as e:
                session.rollback()
                log.error(f"Error updating conversation: {str(e)}")
                return {'success': False, 'error': 'Failed to update conversation'}

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
        participant_id: int = None,
        entity_name: str = None,
    ) -> dict:
        """
        List conversations with filtering, sorting, and pagination.

        Shared RPC for conversation listing used by:
        - elitea_core conversations API
        - support_assistant plugin (with source='support', include_hidden=True)

        Args:
            participant_id: Optional participant ID to filter by single_participant in conversation meta
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

            if participant_id is not None:
                filters = [
                    Conversation.meta.has_key('single_participant'),
                    Conversation.meta['single_participant']['entity_meta']['id'].astext.cast(Integer) == participant_id,
                ]
                if entity_name:
                    filters.append(
                        Conversation.meta['single_participant']['entity_name'].astext == entity_name,
                    )
                base_query = base_query.filter(*filters)

            base_query = base_query.order_by(sorting(sorting_by))

            # Cap page size: keeps the IN(...) lists in the aggregation queries
            # below tightly bounded regardless of caller (audit issue #1).
            limit = min(max(int(limit or 0), 1), LIST_CONVERSATIONS_MAX_LIMIT)
            offset = max(int(offset or 0), 0)

            total = base_query.count()
            conversations = base_query.limit(limit).offset(offset).all()

            if not conversations:
                return {'total': total, 'rows': []}

            conv_ids = [c.id for c in conversations]
            skip_duration = bool(source and 'elitea' in source)

            # Pre-aggregate per-conversation counts in two scalar GROUP BY queries
            # instead of N per-row .count()/list accesses (audit issue #1).
            # Both queries are bounded by len(conv_ids) <= LIST_CONVERSATIONS_MAX_LIMIT
            # and project to scalar columns only — no ORM hydration.
            mg_counts: dict[int, int] = dict(
                session.query(
                    ConversationMessageGroup.conversation_id,
                    func.count(ConversationMessageGroup.id),
                )
                .filter(ConversationMessageGroup.conversation_id.in_(conv_ids))
                .group_by(ConversationMessageGroup.conversation_id)
                .all()
            )

            participants_count: dict[int, int] = {}
            users_count: dict[int, int] = {}
            for cid, ename, n in (
                session.query(
                    ParticipantMapping.conversation_id,
                    Participant.entity_name,
                    func.count(Participant.id),
                )
                .join(Participant, Participant.id == ParticipantMapping.participant_id)
                .filter(ParticipantMapping.conversation_id.in_(conv_ids))
                .group_by(ParticipantMapping.conversation_id, Participant.entity_name)
                .all()
            ):
                participants_count[cid] = participants_count.get(cid, 0) + n
                if ename == ParticipantTypes.user.value:
                    users_count[cid] = n

            durations = (
                {} if skip_duration
                else calculate_conversation_durations_batch(conv_ids, session)
            )

            rows = []
            for conv in conversations:
                conv_dict = {
                    **serialize(conv),
                    "duration": -1 if skip_duration else durations.get(conv.id, 0.0),
                    "participants_count": participants_count.get(conv.id, 0),
                    "message_groups_count": mg_counts.get(conv.id, 0),
                    "users_count": users_count.get(conv.id, 0),
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

    @web.rpc("chat_delete_all_messages_rpc", "delete_all_messages_rpc")
    def delete_all_messages_rpc(
        self,
        project_id: int,
        conversation_id: int,
        user_id: int,
    ) -> dict:
        """
        Delete all messages from a conversation.
        Only the conversation author can delete all messages.
        """
        from ..models.message_group import ConversationMessageGroup
        from ..models.message_items.base import MessageItem
        from ..utils.sio_utils import get_chat_room, SioEvents
        from ..utils.context_analytics import update_conversation_meta

        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()

            if not conversation:
                return {'success': False, 'error': f'No such conversation with id {conversation_id}'}

            if conversation.author_id != user_id:
                return {'success': False, 'error': 'Only conversation author can delete all messages'}

            thread_ids = set()
            agent_messages = session.query(ConversationMessageGroup.meta).filter(
                ConversationMessageGroup.conversation_id == conversation_id,
                ConversationMessageGroup.meta.isnot(None)
            ).all()
            for (meta,) in agent_messages:
                if isinstance(meta, dict) and meta.get('thread_id'):
                    thread_ids.add(meta['thread_id'])
            thread_ids.add(str(conversation.uuid))

            session.query(MessageItem).filter(
                MessageItem.message_group_id.in_(
                    session.query(ConversationMessageGroup.id).filter(
                        ConversationMessageGroup.conversation_id == conversation_id
                    )
                )
            ).delete()

            session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.conversation_id == conversation_id
            ).delete()

            session.commit()

            try:
                update_conversation_meta(project_id, conversation_id, {'context_analytics': None})
            except Exception as e:
                log.error(f"Failed to reset context analytics for conversation {conversation_id}: {e}")

            if thread_ids:
                try:
                    from ..utils.vectorstore import get_pgvector_connection_string
                    from tools import this
                    pgvector_connstr = get_pgvector_connection_string(project_id)
                    this.module.event_node.emit('indexer_delete_checkpoint', {
                        'thread_ids': list(thread_ids),
                        'pgvector_connstr': pgvector_connstr,
                    })
                except Exception as e:
                    log.error(f"Failed to delete checkpoints for conversation {conversation_id}: {str(e)}")

            room = get_chat_room(conversation.uuid)
            from tools import this
            this.module.context.sio.emit(
                event=SioEvents.chat_message_delete_all,
                data={'conversation_id': conversation_id},
                room=room,
            )

            return {'success': True}

    @web.rpc("chat_send_message_rpc", "send_message_rpc")
    def send_message_rpc(
        self,
        project_id: int,
        conversation_uuid: str,
        user_input: str = None,
        participant_id: int = None,
        llm_settings: dict = None,
        attachments_info: list = None,
        user_ids: list = None,
        await_task_timeout: int = 0,
        return_task_id: bool = False,
        return_message_ids: bool = True,
    ) -> dict:
        """
        Send a message to a conversation and optionally wait for AI response.
        """
        import time
        from pydantic import ValidationError
        from ..models.pd.message import MessagePostPayload, MessageGroupDetail
        from ..utils.sio_utils import SioValidationError
        from tools import this, rpc_tools

        raw = {
            'conversation_uuid': conversation_uuid,
            'user_input': user_input,
            'participant_id': participant_id,
            'llm_settings': llm_settings,
            'attachments_info': attachments_info or [],
            'user_ids': user_ids,
            'await_task_timeout': await_task_timeout,
            'return_task_id': return_task_id,
        }

        if llm_settings is None and participant_id:
            with db.get_session(project_id) as session:
                mapping = session.query(ParticipantMapping).join(
                    Participant, Participant.id == ParticipantMapping.participant_id
                ).join(
                    Conversation, Conversation.id == ParticipantMapping.conversation_id
                ).filter(
                    ParticipantMapping.participant_id == participant_id,
                    Participant.entity_name == ParticipantTypes.application,
                    Conversation.uuid == conversation_uuid,
                ).first()
                if not mapping:
                    models_data = rpc_tools.RpcMixin().rpc.timeout(2).configurations_get_default_model(
                        project_id=project_id, section="llm", include_shared=True
                    )
                    raw['llm_settings'] = models_data

        if llm_settings is None and not participant_id:
            models_data = rpc_tools.RpcMixin().rpc.timeout(2).configurations_get_default_model(
                project_id=project_id, section="llm", include_shared=True
            )
            raw['llm_settings'] = models_data

        try:
            request_data = MessagePostPayload.model_validate(raw)
        except ValidationError as e:
            return {'success': False, 'error': f'Validation failed: {e.errors()}'}

        message_payload = {
            "project_id": project_id,
            **serialize(request_data.model_dump(exclude={"await_task_timeout"})),
        }

        if request_data.await_task_timeout > 0 and request_data.return_task_id:
            return {'success': False, 'error': 'Cannot return task id and wait for task completion simultaneously'}

        try:
            result = this.module.chat_predict_sio(
                sid=None,
                data=message_payload,
                await_task_timeout=request_data.await_task_timeout,
                return_message_ids=return_message_ids
            )
        except SioValidationError as e:
            return {'success': False, 'error': f'Wrong input data: {e.error}'}
        except Exception as ex:
            log.error(f"Error in chat_predict_sio: {ex}")
            return {'success': False, 'error': 'Cannot create message'}

        if not isinstance(result, dict):
            return {'success': False, 'error': f'Unexpected result type: {str(result)}'}

        if "error" in result:
            error_value = result["error"]
            if not isinstance(error_value, str):
                error_value = str(error_value)
            return {'success': False, 'error': error_value}

        if request_data.await_task_timeout <= 0 and request_data.return_task_id:
            sanitized_result = {}
            for k, v in result.items():
                sanitized_result[k] = str(v) if isinstance(v, Exception) else v
            return {'success': True, 'data': sanitized_result, 'status_code': 200}

        status_code = 201
        with db.get_session(project_id) as session:
            message_groups = session.query(ConversationMessageGroup).filter(
                ConversationMessageGroup.id.in_(result.values())
            ).order_by(
                ConversationMessageGroup.created_at.asc()
            ).all()
            if len(message_groups) != 2:
                return {'success': False, 'error': 'Invalid number of message groups: expected to be 2'}
            reply_message = message_groups[-1]
            if not reply_message.message_items:
                for poll_timeout in range(1, 4):
                    session.refresh(reply_message)
                    if not reply_message.is_streaming:
                        break
                    time.sleep(poll_timeout)
                else:
                    status_code = 202
            rows = [serialize(MessageGroupDetail.from_orm(i)) for i in message_groups]

        return {'success': True, 'data': {'message_groups': rows}, 'status_code': status_code}
