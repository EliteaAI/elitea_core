"""Request model for schedule run-now and test-run API (EL-7011 US-15)."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ScheduleRunRequest(BaseModel):
    """Request body for POST .../{schedule_id}/run endpoint.

    Covers R2.3, R2.4: rejects any field other than 'trigger' and validates
    that 'trigger' is 'manual' or 'test'.
    """

    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"trigger": "manual"}]})

    trigger: Literal["manual", "test"] = Field(description="`manual` runs now; `test` is a test run",
                                               examples=["manual"])


class ScheduleRunDecision(BaseModel):
    """Request body for POST .../{schedule_id}/runs/{run_id}/decision: the owner's answer on a waiting run."""

    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"action": "approve"}]})

    action: Literal["approve", "reject"] = Field(description="`approve` starts the run again; `reject` ends it as failed",
                                                 examples=["approve"])
