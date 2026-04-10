from typing import Optional

from pydantic import BaseModel


class UpdateIcon(BaseModel):
    name: str
    url: str
    resulting_file_size: Optional[str] = str()
    initial_file_size: Optional[str] = str()
    size: Optional[str] = str()
