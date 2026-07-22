"""
Pydantic models for Pipeline Trigger configuration.

Pipeline triggers determine how a pipeline is initiated:
- chat_message: Traditional chat-based invocation (default)
- schedule: Cron-based scheduled execution

The trigger configuration is stored in ApplicationVersion.pipeline_settings['trigger'].
"""
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, validator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .index import _validate_cron_expression


class TriggerType(str, Enum):
    """Pipeline trigger type enumeration."""
    chat_message = "chat_message"
    schedule = "schedule"
    webhook = "webhook"


class WebhookType(str, Enum):
    """Webhook authentication type enumeration."""
    github = "github"
    gitlab = "gitlab"
    custom = "custom"


class PipelineTriggerSchedule(BaseModel):
    """
    Schedule configuration for a pipeline trigger.

    Stored in ApplicationVersion.pipeline_settings['trigger'] when type is 'schedule'.
    """
    type: TriggerType = Field(default=TriggerType.schedule)
    cron: str = Field(..., description="Cron expression (5-part)")
    timezone: str = Field(..., description="IANA timezone name, e.g., 'America/New_York'")
    last_run: Optional[str] = Field(None, description="ISO 8601 timestamp of last execution (UTC)")
    created_by: int = Field(..., gt=0, description="User ID who created the schedule")

    @validator('timezone')
    def validate_timezone(cls, v):
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError('timezone must be a valid IANA timezone name, e.g., "Etc/GMT-3", "Asia/Tokyo"')
        return v

    @validator('cron')
    def validate_cron(cls, v: str) -> str:
        return _validate_cron_expression(v)

    @validator('last_run', pre=True)
    def normalize_last_run(cls, v):
        """Accept datetime or string, ensure tz is present, normalize to UTC, and store as ISO string."""
        if v is None:
            return None

        # Convert input to datetime first
        if isinstance(v, datetime):
            dt = v
        else:
            try:
                dt = datetime.fromisoformat(v)
            except Exception:
                raise ValueError('last_run must be a valid ISO 8601 datetime string')

        # If no timezone or naive, assume UTC
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Normalize to UTC if not already
            dt = dt.astimezone(timezone.utc)

        return dt.isoformat()


class PipelineTriggerChatMessage(BaseModel):
    """
    Chat message trigger configuration (default).

    This is a minimal model as chat_message triggers don't require additional configuration.
    """
    type: TriggerType = Field(default=TriggerType.chat_message)


class PipelineTriggerWebhook(BaseModel):
    """
    Webhook trigger configuration for pipelines.

    Allows external systems (GitHub, GitLab, custom) to trigger pipeline execution
    via HTTP POST requests with signature-based authentication.

    The webhook secret is stored in Application.webhook_secret (not in this model).
    """
    type: TriggerType = Field(default=TriggerType.webhook)
    webhook_type: WebhookType = Field(..., description="Type of webhook authentication")
    created_by: int = Field(..., gt=0, description="User ID who created the webhook trigger")

    @validator('webhook_type', pre=True)
    def normalize_webhook_type(cls, v):
        """Allow string input and normalize to WebhookType enum."""
        if isinstance(v, str):
            return WebhookType(v)
        return v

    class Config:
        use_enum_values = True


class UpdatePipelineTrigger(BaseModel):
    """
    Input model for updating pipeline trigger via API.

    Used by PUT /api/v2/applications/{project_id}/pipeline/{version_id}/trigger
    """
    type: TriggerType = Field(..., description="Trigger type: 'chat_message', 'schedule', or 'webhook'")

    # Schedule-specific fields (required when type='schedule')
    cron: Optional[str] = Field(None, description="Cron expression (required for schedule type)")
    timezone: Optional[str] = Field(None, description="IANA timezone (required for schedule type)")

    # Webhook-specific fields (required when type='webhook')
    webhook_type: Optional[str] = Field(None, description="Webhook type: 'github', 'gitlab', or 'custom'")

    @validator('timezone')
    def validate_timezone(cls, v):
        if v is None:
            return v
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError('timezone must be a valid IANA timezone name, e.g., "Etc/GMT-3", "Asia/Tokyo"')
        return v

    @validator('cron')
    def validate_cron(cls, v: str) -> str:
        if v is None:
            return v
        return _validate_cron_expression(v)

    @validator('type', pre=True)
    def normalize_type(cls, v):
        """Allow string input and normalize to TriggerType enum."""
        if isinstance(v, str):
            return TriggerType(v)
        return v

    class Config:
        use_enum_values = True


class PipelineTriggerResponse(BaseModel):
    """
    Response model for GET pipeline trigger API.

    Returns the current trigger configuration with all fields.
    """
    type: TriggerType
    cron: Optional[str] = None
    timezone: Optional[str] = None
    last_run: Optional[str] = None
    created_by: Optional[int] = None
    webhook_type: Optional[str] = None
    webhook_url: Optional[str] = None
    # Webhook secret info (only present when type=webhook)
    secret_configured: Optional[bool] = None
    secret_header: Optional[str] = None
    secret_value: Optional[str] = None
    secret_instructions: Optional[str] = None

    class Config:
        use_enum_values = True


# Re-export utility functions from utils.pipeline_trigger for backward compatibility
# These are the canonical implementations; imports that reference pd.pipeline_trigger still work.
from ...utils.pipeline_trigger import (
    get_trigger_from_pipeline_settings,
    build_trigger_for_storage,
)
