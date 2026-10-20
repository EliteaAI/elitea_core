"""Request model for schedule run-now and test-run API (EL-7011 US-15)."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ScheduleRunRequest(BaseModel):
    """Request body for POST .../{schedule_id}/run endpoint.

    Covers R2.3, R2.4: rejects any field other than 'trigger' and validates
    that 'trigger' is 'manual' or 'test'.
    """

    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"trigger": "manual"}]})

    trigger: Literal["manual", "test"] = Field(description="`manual` runs now; `test` is a test run")
