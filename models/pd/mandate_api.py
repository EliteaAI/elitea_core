"""Request bodies of the autonomous mandate REST API (#7131, C-1009). Unknown fields are refused."""
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, model_validator

from ..agent_identity import REFERENCE_PATTERN

MANDATE_ACTIONS = ("submit", "reject", "approve", "suspend", "resume", "revoke", "consent", "consent_preview",
                   "withdraw_consent")


class CreateMandateRequest(BaseModel):
    """An agent-version mandate (``version_id``; operations ``run:schedule`` / ``run:webhook``) or a toolkit mandate
    (``toolkit_id``, #7305; operation ``run:index_schedule``): exactly one of the two."""
    model_config = ConfigDict(extra="forbid")

    version_id: Optional[StrictInt] = Field(default=None, gt=0)
    toolkit_id: Optional[StrictInt] = Field(default=None, gt=0)
    owner_user_id: StrictInt = Field(gt=0)
    operations: list[Literal["run:schedule", "run:webhook", "run:index_schedule"]] = Field(min_length=1, max_length=2)
    max_runtime_s: StrictInt = Field(ge=60, le=86400)
    valid_until: datetime
    identity: Optional[Literal["owner_delegated", "application"]] = None

    @model_validator(mode="after")
    def _one_kind(self):
        if (self.version_id is None) == (self.toolkit_id is None):
            raise ValueError("exactly one of version_id and toolkit_id")
        index_only = set(self.operations) == {"run:index_schedule"}
        agent_only = "run:index_schedule" not in self.operations
        if not (index_only if self.toolkit_id is not None else agent_only):
            raise ValueError("operations do not fit the mandate kind")
        return self


class MandateActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal[MANDATE_ACTIONS]
    approval_ref: Optional[StrictStr] = Field(default=None, pattern=REFERENCE_PATTERN)
    policy_revision: Optional[StrictStr] = Field(default=None, pattern=REFERENCE_PATTERN)
