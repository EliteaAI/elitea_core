"""Request model for schedule run-now and test-run API (EL-7011 US-15)."""
from typing import Literal

from pydantic import BaseModel, ConfigDict


class ScheduleRunRequest(BaseModel):
    """Request body for POST .../{schedule_id}/run endpoint.

    Covers R2.3, R2.4: rejects any field other than 'trigger' and validates
    that 'trigger' is 'manual' or 'test'.
    """

    model_config = ConfigDict(extra="forbid")

    trigger: Literal["manual", "test"]
