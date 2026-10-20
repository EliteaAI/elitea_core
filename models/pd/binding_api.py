"""Request bodies of the agent identity binding REST API (#7105, C-1009). Unknown fields are refused."""
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator

from ..agent_identity import BINDING_STATES, IDENTITY_MODES

MAX_REFERENCE_LENGTH = 128   # the width of the approval_ref and policy_revision columns
IdentityMode = Literal[IDENTITY_MODES]
BindingState = Literal[BINDING_STATES]
BODY_ACTIONS = ("submit", "activate", "suspend", "resume", "retire", "rebind")


class CreateBindingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    application_id: StrictInt = Field(gt=0)
    version_id: StrictInt = Field(gt=0)
    identity_mode: IdentityMode


class BindingActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal[BODY_ACTIONS]
    expected_state: BindingState
    approval_ref: Optional[StrictStr] = Field(default=None, max_length=MAX_REFERENCE_LENGTH)
    policy_revision: Optional[StrictStr] = Field(default=None, max_length=MAX_REFERENCE_LENGTH)

    @field_validator("approval_ref", "policy_revision")
    @classmethod
    def _stripped_and_not_blank(cls, value):
        if value is None:
            return None
        value = value.strip()
        if not value or any(not char.isprintable() for char in value):
            raise ValueError("must be printable text")
        return value
