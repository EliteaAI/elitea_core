"""Request bodies of tool_spend_check / tool_spend_record (#6980): typed, finite, size-bounded."""
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictFloat

Name = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
# Storage bounds of the target columns, not tuning knobs
INT32_MAX = 2_147_483_647


class ToolSpendMeta(BaseModel):
    """Only these scalars reach the ledger; anything else is dropped."""
    model_config = ConfigDict(extra="ignore")

    credits_source: Optional[Literal["reported", "estimated"]] = None
    depth: Optional[Literal["basic", "advanced"]] = None
    results_count: Optional[int] = Field(None, ge=0, le=INT32_MAX)
    urls_count: Optional[int] = Field(None, ge=0, le=INT32_MAX)
    failed_urls: Optional[int] = Field(None, ge=0, le=INT32_MAX)


class ToolSpendCheckBody(BaseModel):
    model_config = ConfigDict(extra="ignore")

    feature: str = Name
    backend: str = Name
    op: str = Name
    configuration_uuid: str = Field(min_length=1, max_length=64)
    configuration_project_id: int = Field(ge=1, le=INT32_MAX)


class ToolSpendRecordBody(BaseModel):
    model_config = ConfigDict(extra="ignore")

    # Optional here so a missing permit is the permit check's 403, not a 400
    permit: Optional[str] = Field(None, max_length=4096)
    # HMAC-SHA256 hex from the indexer; optional here so a missing one is the signature check's 403
    signature: Optional[str] = Field(None, max_length=128)
    feature: str = Name
    backend: str = Name
    op: str = Name
    configuration_uuid: str = Field(min_length=1, max_length=64)
    # Finite and non-negative: a record can never lower spend (NaN/inf/negatives rejected)
    units: StrictFloat = Field(ge=0, allow_inf_nan=False)
    ok: StrictBool = True
    duration_ms: Optional[int] = Field(None, ge=0, le=INT32_MAX)
    tool_name: Optional[str] = Field(None, max_length=256)
    meta: ToolSpendMeta = Field(default_factory=ToolSpendMeta)
