"""The body of the chat's credential form (#7228, C-1009): strict, bounded, its values ``SecretStr`` from the moment
they are parsed (a repr or a validation error never shows them)."""
from typing import Any, Dict, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StrictStr, field_validator

MAX_FIELDS = 32
MAX_FIELD_NAME_CHARS = 64
MAX_VALUE_CHARS = 8192
FIELD_NAME = r"^[A-Za-z0-9_]{1,64}$"


class CredentialFormBody(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    interrupt_id: StrictStr = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_:.-]+$")
    message_id: UUID
    credential_type: StrictStr = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_]+$")
    title: StrictStr = Field(min_length=1, max_length=64)
    fields: Dict[StrictStr, SecretStr] = Field(default_factory=dict)
    ownership: Literal["personal", "project_shared"] = "personal"
    auth_mode: Optional[Literal["entra_delegated"]] = None
    delegation: Optional[Dict[StrictStr, Any]] = None

    @field_validator("fields", mode="before")
    @classmethod
    def _bounded(cls, value):
        if not isinstance(value, dict) or len(value) > MAX_FIELDS:
            raise ValueError("at most 32 fields")
        for name, text in value.items():
            if not isinstance(name, str) or not 0 < len(name) <= MAX_FIELD_NAME_CHARS \
                    or not all(ch.isalnum() or ch == "_" for ch in name):
                raise ValueError("bad field name")
            if not isinstance(text, str) or len(text) > MAX_VALUE_CHARS:
                raise ValueError("bad value")   # the message never holds the value
        return value

    @field_validator("title")
    @classmethod
    def _printable(cls, value):
        if not value.strip() or any(ord(ch) < 32 for ch in value):
            raise ValueError("bad title")
        return value.strip()
