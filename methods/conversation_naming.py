#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from pylon.core.tools import log, web
from tools import db

from ..models.conversation import Conversation


class Method:
    @web.method()
    def check_and_generate_conversation_name(
        self, project_id, user_input: str, room: str, conversation: Conversation
    ):
        """
        Check if this is the first conversation exchange and generate a name if needed.
        Only triggers for conversations that still have default names and only have 2 messages.
        """
        log.debug(f"Checking auto-naming for conversation {conversation.id} with name: '{conversation.name}'")
        
        # If this is exactly the second message (user input + AI response), trigger auto-naming
        # Only do this if conversation has a default name pattern
        if conversation.name.lower().startswith('new conversation'):
            log.debug(f"Triggering auto-naming for conversation {conversation.id}")
            
            try:
                # Generate conversation name
                self.generate_conversation_name_async(
                    conversation.id,
                    project_id,
                    user_input,
                    str(conversation.uuid),
                    room
                )
            except Exception as e:
                log.error(f"Error in conversation auto-naming: {e}")
        else:
            log.debug(f"Skipping auto-naming for conversation {conversation.id}, name: '{conversation.name}'")

    @web.method()
    def generate_conversation_name_async(
        self, conversation_id: int, project_id: int,
        user_input: str, conversation_uuid: str, room: str
    ):
        """
        Generate a conversation name using a simple approach based on user input.
        
        For enhanced LLM-based naming, you can replace this with an LLM call:
        
        # Enhanced LLM-based naming approach:
        # naming_payload = {
        #     'project_id': project_id,
        #     'user_input': f"Generate a concise conversation title (max 40 characters) for this exchange:\n\nUser: {user_input}\n\nAssistant: {ai_response[:300]}",
        #     'llm_settings': {
        #         'model_settings': {
        #             'model': 'gpt-3.5-turbo',
        #             'max_tokens': 15,
        #             'temperature': 0.1,
        #             'system_prompt': 'You are a helpful assistant that creates short, descriptive titles for conversations. Return only the title, no quotes or extra text.'
        #         }
        #     },
        #     'stream_id': f'naming_{conversation_uuid}',
        #     'message_id': f'naming_{conversation_id}',
        # }
        # 
        # result = self.context.rpc_manager.call.applications_predict_sio_llm(
        #     sid=None,
        #     data=naming_payload,
        #     sio_event='conversation_naming',
        #     await_task_timeout=15
        # )
        # 
        # if result and 'result' in result:
        #     generated_name = result['result'].strip().strip('"\'')
        """
        try:
            log.debug(f"Auto-naming conversation {conversation_id} with user input: {user_input[:100]}...")
            
            # Simple approach: use first meaningful words from user input
            # Clean and truncate user input to create a conversation name
            cleaned_input = user_input.strip()
            
            # Remove common question words and phrases but keep important ones
            words_to_remove = ['how', 'what', 'where', 'when', 'why', 'can', 'could', 'would', 'should', 
                             'please', 'help', 'me', 'with', 'do', 'does', 'is', 'are', 'the', 'a', 'an']
            words = cleaned_input.split()
            
            # Filter out common words and keep meaningful ones (keep words > 2 chars)
            meaningful_words = []
            for word in words:
                # Keep words that are longer than 2 characters and not in the removal list
                if len(word) > 2 and word.lower() not in words_to_remove:
                    meaningful_words.append(word)
                # Also keep important short words that might be relevant
                elif word.lower() in ['ai', 'ml', 'ui', 'api', 'sql', 'css', 'js', 'py']:
                    meaningful_words.append(word)
            
            # Take first 5 meaningful words or fallback to first 6 words if no meaningful ones
            if meaningful_words:
                name_words = meaningful_words[:5]
            else:
                # Fallback: take first few words, excluding very common ones
                name_words = [word for word in words[:8] if word.lower() not in ['the', 'a', 'an', 'i']][:5]
            
            generated_name = ' '.join(name_words).strip()
            
            # Ensure minimum length of 3 characters and capitalize first letter
            if generated_name and len(generated_name) >= 3:
                generated_name = generated_name[0].upper() + generated_name[1:]
                if len(generated_name) > 50:
                    generated_name = generated_name[:47] + "..."
            elif generated_name and len(generated_name) == 2:
                # For 2-character names, add "Chat" to make it meaningful
                generated_name = generated_name.upper() + " Chat"
            elif generated_name and len(generated_name) == 1:
                # For single characters, create a more descriptive name
                generated_name = generated_name.upper() + " Discussion"
            else:
                # Ultimate fallback - ensure it's always at least 3 characters
                generated_name = "New Chat"
            
            log.debug(f"Generated conversation name: '{generated_name}'")

            # Update conversation name in database
            with db.get_session(project_id) as session:
                conversation = session.query(Conversation).filter(
                    Conversation.id == conversation_id
                ).first()
                if conversation:
                    conversation.name = generated_name
                    session.commit()

                    # Emit conversation name update to the room
                    self.context.sio.emit(
                        event='chat_conversation_name_updated',
                        data={
                            'conversation_id': conversation_id,
                            'conversation_uuid': conversation_uuid,
                            'name': generated_name,
                            'type': 'conversation_name_update'
                        },
                        room=room,
                    )

                    log.debug(f"Successfully updated conversation {conversation_id} name to: '{generated_name}'")
                        
        except Exception as e:
            log.error(f"Failed to generate conversation name: {e}")
