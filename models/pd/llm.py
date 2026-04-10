from typing import Optional, Annotated

from pydantic import BaseModel, Field


class LLMSettingsModel(BaseModel):
    temperature: Optional[Annotated[float, Field(gt=0, le=1)]] = None
    reasoning_effort: Optional[str] = None
    max_tokens: Optional[int] = None
    model_name: Optional[str] = None
    model_project_id: Optional[int] = None
