"""Request model for schedule run-now and test-run API (EL-7011 US-15)."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_PAYLOAD_BYTES = 512 * 1024  # the webhook door's body cap (#7453)


class ScheduleRunRequest(BaseModel):
    """Request body for POST .../{schedule_id}/run endpoint.

    Covers R2.3, R2.4: rejects any field other than 'trigger' and 'payload' and validates
    that 'trigger' is 'manual' or 'test'. A webhook row is tried with `test` and a sample `payload` (#7455).
    """

    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"trigger": "manual"}]})

    trigger: Literal["manual", "test"] = Field(description="`manual` runs now; `test` is a test run",
                                               examples=["manual"])
    payload: str | None = Field(None, description="Webhook rows only: the sample body of a `test` run, at most "
                                                  "512 KB; it is not stored", examples=['{"action": "opened"}'])

    @field_validator("payload")
    @classmethod
    def _payload_size(cls, value: str | None) -> str | None:
        if value is not None and len(value.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise ValueError("payload must be at most 512 KB")
        return value


class ScheduleRunDecision(BaseModel):
    """Request body for POST .../{schedule_id}/runs/{run_id}/decision: the owner's answer on a waiting run."""

    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"action": "approve"}]})

    action: Literal["approve", "reject"] = Field(description="`approve` starts the run again; `reject` ends it as failed",
                                                 examples=["approve"])
