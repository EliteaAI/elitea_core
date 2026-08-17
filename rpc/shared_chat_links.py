from datetime import datetime

from pylon.core.tools import log, web
from werkzeug.security import check_password_hash, generate_password_hash

from tools import db

from ..models.all import (
    ConversationShareToken,
    ConversationShareTokenIndex,
)
from ..models.participants import Participant, ParticipantMapping
from ..models.conversation import Conversation
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.base import MessageItem
from ..models.message_items.text import TextMessageItem
from ..models.message_items.attachment import AttachmentMessageItem
from ..models.message_items.canvas import CanvasMessageItem
from ..models.pd.shared_chat_link import (
    SharedLinkResponse,
    SharedConversationView,
    SharedMessageGroup,
    SharedMessageItem,
    SharedAttachmentItem,
    ShareLinkExpiry,
    ShareScope,
    compute_expiry,
    generate_token,
)


def _build_share_link_response(share: ConversationShareToken, conversation_name: str) -> dict:
    return SharedLinkResponse(
        id=share.id,
        token=share.token,
        conversation_id=share.conversation_id,
        conversation_name=conversation_name,
        created_at=share.created_at,
        expires_at=share.expires_at,
        has_password=share.password_hash is not None,
        is_revoked=share.is_revoked,
        access_count=share.access_count,
        scope=share.scope,
    ).model_dump(mode='json')


def _item_passes_scope(item: MessageItem, scope: ShareScope) -> bool:
    """Return True if this item should be included given the requested scope.
    For scope='partial', group-level filtering is applied in _serialize_conversation;
    all items within a selected group pass through here."""
    if scope in (ShareScope.all, ShareScope.partial):
        return True
    is_attachment = isinstance(item, AttachmentMessageItem)
    if scope == ShareScope.attachments_only:
        return is_attachment
    if scope == ShareScope.messages_only:
        return not is_attachment
    return True


def _serialize_item(item: MessageItem) -> SharedMessageItem:
    if isinstance(item, TextMessageItem):
        return SharedMessageItem(type='text_message', content=item.content or '')
    if isinstance(item, CanvasMessageItem):
        latest = item.versions[-1] if item.versions else None
        content = latest.canvas_content if latest else ''
        return SharedMessageItem(type='canvas_message', content=content or '')
    if isinstance(item, AttachmentMessageItem):
        # Strip the conversation-uuid prefix from the filename for display
        raw_name = item.name or ''
        display_name = raw_name.split('/', 1)[-1] if '/' in raw_name else raw_name
        attachment = SharedAttachmentItem(
            name=display_name,
            attachment_type=item.attachment_type or 'document',
        )
        return SharedMessageItem(type='attachment_message', attachment=attachment)
    # Fallback for unknown/context items
    return SharedMessageItem(type=item.item_type, content='')


def _resolve_participant_name(participant: Participant) -> str:
    """Resolve a human-readable display name from a Participant row."""
    entity_name = str(participant.entity_name)
    entity_meta = participant.entity_meta or {}
    meta = participant.meta or {}
    if entity_name == 'user':
        return meta.get('user_name') or meta.get('name') or ''
    if entity_name == 'llm':
        return entity_meta.get('model_name') or entity_meta.get('name') or ''
    # application, toolkit, dummy, skill, pipeline
    name = entity_meta.get('name') or meta.get('name') or ''
    # Normalise legacy capitalisation stored in DB
    if name.lower() == 'elitea':
        return 'Elitea'
    return name


def _resolve_participant_icon_and_agent_type(
    session,
    conversation_id: int,
    participant: 'Participant',
) -> tuple:
    """Return (icon_meta, agent_type) for the participant.

    icon_meta is the dict from entity_settings, or None.
    agent_type is the string from entity_settings (e.g. 'pipeline'), or None.
    """
    mapping = (
        session.query(ParticipantMapping)
        .filter_by(conversation_id=conversation_id, participant_id=participant.id)
        .first()
    )
    if mapping is None:
        return None, None
    settings = mapping.entity_settings or {}
    icon = settings.get('icon_meta')
    icon = icon if isinstance(icon, dict) else None
    agent_type = settings.get('agent_type') or None
    return icon, agent_type


def _serialize_conversation(
    conversation: Conversation,
    scope: ShareScope,
    session,
    message_group_ids: list | None = None,
) -> list:
    query = (
        conversation.message_groups
        .filter(ConversationMessageGroup.is_streaming == False)  # noqa: E712
        .order_by(ConversationMessageGroup.created_at.asc())
    )
    if scope == ShareScope.partial and message_group_ids:
        query = query.filter(ConversationMessageGroup.id.in_(message_group_ids))
    groups = query.all()

    messages = []
    for group in groups:
        items = [
            _serialize_item(item)
            for item in group.message_items
            if _item_passes_scope(item, scope)
        ]
        if not items:
            continue

        participant = group.author_participant
        if participant:
            entity_name = str(participant.entity_name)
            author_type = 'user' if entity_name == 'user' else 'assistant'
            author_name = _resolve_participant_name(participant)
            participant_icon, participant_agent_type = _resolve_participant_icon_and_agent_type(
                session, conversation.id, participant
            )
        else:
            entity_name = None
            author_type = 'assistant'
            author_name = None
            participant_icon = None
            participant_agent_type = None

        group_meta = group.meta or {}
        is_error = bool(group_meta.get('is_error', False))
        error_text = group_meta.get('error') or None
        if is_error and not error_text:
            # fall back to first text item content, like the frontend does
            first_text = next(
                (i for i in items if i.type == 'text_message' and i.content),
                None,
            )
            error_text = first_text.content if first_text else None

        messages.append(SharedMessageGroup(
            id=group.id,
            author_type=author_type,
            author_name=author_name or None,
            participant_type=entity_name,
            participant_agent_type=participant_agent_type,
            participant_icon=participant_icon,
            created_at=group.created_at,
            items=items,
            is_error=is_error,
            error=error_text,
        ))

    return [g.model_dump(mode='json') for g in messages]


class RPC:
    @web.rpc("chat_create_share_token", "create_share_token")
    def create_share_token(
        self,
        project_id: int,
        conversation_id: int,
        expiry: str,
        created_by: int,
        password: str = None,
        scope: str = 'all',
        message_group_ids: list = None,
    ) -> dict | None:
        try:
            expiry_enum = ShareLinkExpiry(expiry)
        except ValueError:
            expiry_enum = ShareLinkExpiry.seven_days

        try:
            scope_enum = ShareScope(scope)
        except ValueError:
            scope_enum = ShareScope.all

        expires_at = compute_expiry(expiry_enum)
        token = generate_token()
        password_hash = generate_password_hash(password) if password else None

        with db.get_session(project_id) as session:
            conv = session.get(Conversation, conversation_id)
            if conv is None:
                return None
            conversation_name = conv.name

            # Only persist message_group_ids for partial scope
            stored_group_ids = message_group_ids if scope_enum == ShareScope.partial else None
            share = ConversationShareToken(
                token=token,
                conversation_id=conversation_id,
                created_by=created_by,
                expires_at=expires_at,
                password_hash=password_hash,
                scope=scope_enum.value,
                message_group_ids=stored_group_ids,
            )
            session.add(share)
            session.flush()
            result = _build_share_link_response(share, conversation_name)
            session.commit()

        # Register in global index (public schema) for cross-schema token lookup
        try:
            with db.get_session(None) as pub_session:
                index_entry = ConversationShareTokenIndex(
                    token=token,
                    project_id=project_id,
                    conversation_id=conversation_id,
                )
                pub_session.merge(index_entry)
                pub_session.commit()
        except Exception:
            log.exception("Failed to write share token index for token %s...", token[:8])

        self.context.event_manager.fire_event('conversation_shared', {
            'project_id': project_id,
            'conversation_id': conversation_id,
            'conversation_name': conversation_name,
            'token': token,
            'created_by': created_by,
            'scope': scope_enum.value,
            'message_group_ids': stored_group_ids,
        })

        return result

    @web.rpc("chat_list_share_tokens", "list_share_tokens")
    def list_share_tokens(self, project_id: int, conversation_id: int) -> list:
        with db.get_session(project_id) as session:
            conv = session.get(Conversation, conversation_id)
            if conv is None:
                return []
            tokens = (
                session.query(ConversationShareToken)
                .filter_by(conversation_id=conversation_id, is_revoked=False)
                .order_by(ConversationShareToken.created_at.desc())
                .all()
            )
            return [_build_share_link_response(t, conv.name) for t in tokens]

    @web.rpc("chat_revoke_share_token", "revoke_share_token")
    def revoke_share_token(self, project_id: int, token: str, user_id: int) -> bool:
        with db.get_session(project_id) as session:
            share = (
                session.query(ConversationShareToken)
                .filter_by(token=token, is_revoked=False)
                .first()
            )
            if share is None:
                return False
            share.is_revoked = True
            session.commit()

        self.context.event_manager.fire_event('conversation_share_revoked', {
            'project_id': project_id,
            'token': token,
            'revoked_by': user_id,
        })
        return True

    @web.rpc("chat_get_shared_conversation", "get_shared_conversation")
    def get_shared_conversation(self, token: str, unlocked: bool = False) -> dict | None:
        # Look up project_id via global index (avoids scanning every per-project schema)
        try:
            with db.get_session(None) as pub_session:
                index = pub_session.get(ConversationShareTokenIndex, token)
        except Exception:
            log.exception("Failed to query share token index for token %s...", token[:8])
            return None

        if index is None:
            return None

        project_id = index.project_id

        with db.get_session(project_id) as session:
            share = (
                session.query(ConversationShareToken)
                .filter_by(token=token)
                .first()
            )
            if share is None:
                return None
            if share.is_revoked:
                return {'status': 'revoked'}
            if share.expires_at and share.expires_at < datetime.utcnow():
                return {'status': 'expired'}
            if share.password_hash and not unlocked:
                return {'status': 'password_required'}

            share.access_count += 1

            try:
                scope_enum = ShareScope(share.scope)
            except ValueError:
                scope_enum = ShareScope.all

            messages = _serialize_conversation(
                share.conversation,
                scope_enum,
                session,
                message_group_ids=share.message_group_ids,
            )
            data = SharedConversationView(
                conversation_id=share.conversation.id,
                conversation_name=share.conversation.name,
                created_at=share.conversation.created_at,
                expires_at=share.expires_at,
                scope=share.scope,
                messages=messages,
            ).model_dump(mode='json')
            # messages already serialized as list of dicts above
            data['messages'] = messages

            session.commit()

        return {'status': 'ok', 'data': data}

    @web.rpc("chat_verify_share_token_password", "verify_share_token_password")
    def verify_share_token_password(self, token: str, password: str) -> bool | None:
        try:
            with db.get_session(None) as pub_session:
                index = pub_session.get(ConversationShareTokenIndex, token)
        except Exception:
            return None

        if index is None:
            return None

        with db.get_session(index.project_id) as session:
            share = (
                session.query(ConversationShareToken)
                .filter_by(token=token)
                .first()
            )
            if share is None or share.is_revoked:
                return None
            if share.password_hash is None:
                return True
            return check_password_hash(share.password_hash, password)
