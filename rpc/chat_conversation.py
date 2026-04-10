from pylon.core.tools import web, log
from tools import db, config as c, auth, serialize

from sqlalchemy.orm import joinedload

from ..models.conversation import Conversation


class RPC:
    @web.rpc("chat_get_conversation_details", "get_conversation_details")
    def get_conversation_details(self, project_id: int, conversation_id: int) -> dict:
        """
        Get conversation metadata

        Args:
            project_id: The project ID
            conversation_id: The ID of the conversation

        Returns:
            Dict containing conversation metadata
        """
        with db.get_session(project_id) as session:
            try:
                conversation = session.query(Conversation).filter(
                    Conversation.id == conversation_id
                ).first()

                if not conversation:
                    raise Exception(f"Conversation with ID {conversation_id} not found")

                return serialize(conversation) or {}

            except Exception as e:
                log.error(f"Error getting conversation: {str(e)}")
                raise Exception(f"Failed to get conversation")

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
