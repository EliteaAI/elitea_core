from typing import Optional

from pydantic import BaseModel, confloat


class LLMSettingsModel(BaseModel):
    temperature: Optional[confloat(gt=0, le=1)] = None
    reasoning_effort: Optional[str] = None
    max_tokens: Optional[int]
    model_name: Optional[str]
    model_project_id: Optional[int]
