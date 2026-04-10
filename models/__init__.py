from tools import db_tools, db, config

db_tools = db_tools
db = db
config = config

# Merged from chat.models.__init__
CONVERSATION_TABLE_NAME = 'chat_conversations'
PARTICIPANT_TABLE_NAME = 'chat_participants'
CONVERSATION_MESSAGE_GROUP_TABLE_NAME = 'chat_message_group'
CONVERSATION_MESSAGE_ITEM_TABLE_NAME = 'chat_message_item'
MESSAGE_ITEMS_TABLE_NAME = 'chat_message_items'
