"""Request bodies of the agent identity binding REST API (#7105, C-1009). Unknown fields are refused."""
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from ..agent_identity import BINDING_STATES, IDENTITY_MODES, REFERENCE_PATTERN

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
    # Only an activation carries them (the handler refuses them for any other action); the pattern keeps them to
    # printable reference text, as long as the column
    approval_ref: Optional[StrictStr] = Field(default=None, pattern=REFERENCE_PATTERN)
    policy_revision: Optional[StrictStr] = Field(default=None, pattern=REFERENCE_PATTERN)
