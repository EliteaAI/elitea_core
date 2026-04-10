try:
    from enum import StrEnum
except ImportError:
    from enum import Enum


    class StrEnum(str, Enum):
        pass


# Merged from promptlib_shared.models.enums.applications
class AgentTypes(StrEnum):
    react = 'react'
    elitea = 'elitea'
    dial = 'dial'
    openai = 'openai'
    codemie = 'codemie'
    raw = 'raw'
    autogen = 'autogen'
    llama = 'llama'
    pipeline = 'pipeline'
    xml = 'xml'


# Merged from promptlib_shared.models.enums.all
class PublishStatus(StrEnum):
    draft = 'draft'
    on_moderation = 'on_moderation'
    published = 'published'
    rejected = 'rejected'
    user_approval = 'user_approval'
    unpublished = 'unpublished'
    embedded = 'embedded'


class ChatHistoryRole(StrEnum):
    user = 'user'
    assistant = 'assistant'


class NotificationEventTypes(StrEnum):
    prompt_moderation_approve = 'prompt_moderation_approve'
    prompt_moderation_reject = 'prompt_moderation_reject'
    chat_user_added = 'chat_user_added'
    private_project_created = 'private_project_created'
    index_data_changed = 'index_data_changed'
    agent_unpublished = 'agent_unpublished'
    bucket_expiration_warning = 'bucket_expiration_warning'


class IndexDataStatus(StrEnum):
    """Enum representing index data operation statuses"""
    in_progress = 'in_progress'
    completed = 'completed'
    failed = 'failed'
    cancelled = 'cancelled'
    created = 'created'


class ToolEntityTypes(StrEnum):
    agent = 'agent'
    datasource = 'datasource'


class InitiatorType(StrEnum):
    """Enum representing the initiator of an operation"""
    user = 'user'      # User-initiated (UI, API calls)
    llm = 'llm'        # LLM-initiated (agent operations)
    schedule = 'schedule'  # Schedule-initiated (cron jobs)


class CollectionPatchOperations(StrEnum):
    add = 'add'
    remove = 'remove'


class ToolTypes(StrEnum):
    prompt = 'prompt'
    datasource = 'datasource'
    openapi = 'openapi'
    # custom = 'custom'


class EntityTypes(StrEnum):
    prompt = 'prompt'
    datasource = 'datasource'
    agent = 'agent'


# Merged from chat.models.enums.all
class ParticipantTypes(StrEnum):
    user = 'user'
    prompt = 'prompt'
    datasource = 'datasource'
    application = 'application'
    llm = 'llm'
    dummy = 'dummy'
    toolkit = 'toolkit'
    # pipeline = 'pipeline'


class CanvasTypes(StrEnum):
    CODE = "code"
    TEXT = "text"
    DIAGRAM = "diagram"
    TABLE = "table"
    OTHER = "other"


class MessageGroupItemTypes(StrEnum):
    message = "message"
    canvas = "canvas"
    attachment = "attachment"


class ChatHistoryTemplates(StrEnum):
    all = "all"
    interaction = "interaction"
