import json
from typing import Optional

from tools import db

from ..models.skill import SkillVersion


def fetch_skill_for_edit(project_id: int, skill_id: int, version_id: int) -> Optional[dict]:
    """Return ``{name, description, instructions}`` for the version, or ``None`` if not found."""
    with db.with_project_schema_session(project_id) as session:
        version = session.query(SkillVersion).filter(
            SkillVersion.id == version_id,
            SkillVersion.skill_id == skill_id,
        ).first()

        if not version:
            return None

        skill = version.skill
        return {
            "name": skill.name,
            "description": skill.description or "",
            "instructions": version.instructions or "",
        }


def build_edit_skill_system_prompt(template: str, current_config: dict) -> str:
    """Render the edit-skill prompt; the template's only placeholder is ``{current_config}``."""
    return template.format(current_config=json.dumps(current_config, indent=2))
