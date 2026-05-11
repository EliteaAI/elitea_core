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

    def validate_schedule_fields(self):
        """Validate that schedule-specific fields are present when type is schedule."""
        if self.type == TriggerType.schedule:
            if not self.cron:
                raise ValueError("cron is required when trigger type is 'schedule'")
            if not self.timezone:
                raise ValueError("timezone is required when trigger type is 'schedule'")

    def validate_webhook_fields(self):
        """Validate that webhook-specific fields are present when type is webhook."""
        if self.type == TriggerType.webhook:
            if not self.webhook_type:
                raise ValueError("webhook_type is required when trigger type is 'webhook'")
            if self.webhook_type not in [wt.value for wt in WebhookType]:
                raise ValueError(f"webhook_type must be one of: {[wt.value for wt in WebhookType]}")

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


def get_trigger_from_pipeline_settings(pipeline_settings: dict) -> dict:
    """
    Extract trigger configuration from pipeline_settings with fallback.

    If no trigger is configured (legacy pipelines), defaults to chat_message.

    Args:
        pipeline_settings: The pipeline_settings dict from ApplicationVersion

    Returns:
        Trigger configuration dict
    """
    if not pipeline_settings:
        return {"type": TriggerType.chat_message.value}

    trigger = pipeline_settings.get("trigger")
    if not trigger:
        return {"type": TriggerType.chat_message.value}

    return trigger


def build_trigger_for_storage(update_data: UpdatePipelineTrigger, user_id: int) -> dict:
    """
    Build trigger configuration dict for storage in pipeline_settings.

    Args:
        update_data: Validated update request
        user_id: ID of the user making the update

    Returns:
        Trigger configuration dict ready for storage
    """
    # Note: update_data.type is a string due to use_enum_values=True in Config
    trigger_type = update_data.type

    if trigger_type == TriggerType.chat_message.value:
        return {"type": TriggerType.chat_message.value}

    elif trigger_type == TriggerType.schedule.value:
        # Set last_run to current time so scheduler waits for next cron match
        # (same pattern as index scheduling)
        return {
            "type": TriggerType.schedule.value,
            "cron": update_data.cron,
            "timezone": update_data.timezone,
            "last_run": datetime.now(timezone.utc).isoformat(),
            "created_by": user_id,
        }

    elif trigger_type == TriggerType.webhook.value:
        return {
            "type": TriggerType.webhook.value,
            "webhook_type": update_data.webhook_type,
            "created_by": user_id,
        }

    return {"type": trigger_type}
