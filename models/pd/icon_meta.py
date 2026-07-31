from typing import Optional

from pydantic import BaseModel


class UpdateIcon(BaseModel):
    name: str
    url: str
    resulting_file_size: Optional[str] = str()
    initial_file_size: Optional[str] = str()
    size: Optional[str] = str()

    def as_entity_meta(self) -> dict:
        """Reset-to-default arrives as blank name/url; store the same empty
        marker the icon-delete path writes so 'has a custom icon' checks agree."""
        if not self.url.strip():
            return {}
        return self.dict()
