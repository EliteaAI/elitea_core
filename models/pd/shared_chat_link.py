import secrets
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class ShareLinkExpiry(str, Enum):
    one_hour = '1h'
    one_day = '24h'
    seven_days = '7d'
    thirty_days = '30d'
    never = 'never'


class ShareScope(str, Enum):
    """What content to include in the shared view."""
    all = 'all'                  # entire conversation: messages + attachments
    messages_only = 'messages'  # text/canvas messages only, no attachments
    attachments_only = 'attachments'  # attachment items only
    partial = 'partial'          # only the selected message groups (by id)


EXPIRY_DELTA = {
    ShareLinkExpiry.one_hour: timedelta(hours=1),
    ShareLinkExpiry.one_day: timedelta(hours=24),
    ShareLinkExpiry.seven_days: timedelta(days=7),
    ShareLinkExpiry.thirty_days: timedelta(days=30),
    ShareLinkExpiry.never: None,
}


def compute_expiry(expiry: ShareLinkExpiry) -> Optional[datetime]:
    delta = EXPIRY_DELTA[expiry]
    return datetime.utcnow() + delta if delta else None


def generate_token() -> str:
    return secrets.token_urlsafe(32)


class SharedLinkCreate(BaseModel):
    expiry: ShareLinkExpiry = ShareLinkExpiry.seven_days
    password: Optional[str] = Field(None, min_length=4, max_length=64)
    scope: ShareScope = ShareScope.all
    message_group_ids: Optional[list[int]] = None  # required when scope='partial'


class SharedLinkResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    token: str
    conversation_id: int
    conversation_name: str
    created_at: datetime
    expires_at: Optional[datetime]
    has_password: bool
    is_revoked: bool
    access_count: int
    scope: str


# ──────────────────────────────────────────────
# Shared view models (returned to the public page)
# ──────────────────────────────────────────────

class SharedAttachmentItem(BaseModel):
    """An attachment entry in the shared view."""
    name: str          # original filename (without conversation uuid prefix)
    attachment_type: str   # "image" | "text" | "document"
    # filepath is intentionally omitted — not safe to expose internal bucket paths


class SharedMessageItem(BaseModel):
    type: str          # "text_message" | "attachment_message" | "canvas_message"
    content: Optional[str] = None
    attachment: Optional[SharedAttachmentItem] = None


class SharedMessageGroup(BaseModel):
    id: int                              # ConversationMessageGroup.id — used for partial selection
    author_type: str                     # "user" | "assistant"
    author_name: Optional[str] = None   # resolved display name from participant
    participant_type: Optional[str] = None  # entity_name: 'user'|'application'|'llm'|'dummy'|'toolkit'
    participant_agent_type: Optional[str] = None  # agent_type from entity_settings, e.g. 'pipeline'
    participant_icon: Optional[dict] = None  # icon_meta dict from entity_settings, or None
    created_at: datetime
    items: list[SharedMessageItem]
    is_error: bool = False
    error: Optional[str] = None


class SharedConversationView(BaseModel):
    conversation_id: int
    conversation_name: str
    created_at: datetime
    expires_at: Optional[datetime]
    scope: str
    messages: list[SharedMessageGroup]
