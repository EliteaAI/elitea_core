"""Request bodies of the autonomous mandate REST API (#7131, C-1009). Unknown fields are refused."""
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from ..agent_identity import REFERENCE_PATTERN

MANDATE_ACTIONS = ("submit", "reject", "approve", "suspend", "resume", "revoke", "consent", "consent_preview",
                   "withdraw_consent")


class CreateMandateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version_id: StrictInt = Field(gt=0)
    owner_user_id: StrictInt = Field(gt=0)
    operations: list[Literal["run:schedule", "run:webhook"]] = Field(min_length=1, max_length=2)
    max_runtime_s: StrictInt = Field(ge=60, le=86400)
    valid_until: datetime
    identity: Optional[Literal["owner_delegated", "application"]] = None


class MandateActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal[MANDATE_ACTIONS]
    approval_ref: Optional[StrictStr] = Field(default=None, pattern=REFERENCE_PATTERN)
    policy_revision: Optional[StrictStr] = Field(default=None, pattern=REFERENCE_PATTERN)
