from typing import List, Dict

from pydantic import BaseModel


class ForkApplicationInput(BaseModel):
    applications: List[Dict]


class ForkToolInput(BaseModel):
    toolkits: List[Dict]
