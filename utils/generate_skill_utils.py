import json
from typing import Optional

from tools import db

from ..models.pd.generate_skill_draft import (
    DESCRIPTION_MAX_LENGTH,
    INSTRUCTIONS_MAX_LENGTH,
    NAME_MAX_LENGTH,
)
from ..models.skill import SkillVersion

SKILL_OUTPUT_CONTRACT = f"""Required output contract (this overrides any earlier output-shape wording):
Return only a valid JSON object with exactly these keys:
{{
  "name": "Slugified skill name: lowercase letters, digits and hyphens only, no leading or trailing hyphen, never containing 'claude' or 'anthropic', max {NAME_MAX_LENGTH} characters",
  "description": "A concise description of what the skill helps with, max {DESCRIPTION_MAX_LENGTH} characters",
  "instructions": "The complete skill instructions in Markdown, max {INSTRUCTIONS_MAX_LENGTH} characters"
}}

Do not include explanations, markdown fences, or extra keys."""


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


def append_skill_output_contract(prompt: str) -> str:
    return f"{prompt.rstrip()}\n\n{SKILL_OUTPUT_CONTRACT}"


def build_edit_skill_system_prompt(template: str, current_config: dict) -> str:
    """Render the edit-skill prompt; the template's only placeholder is ``{current_config}``."""
    rendered = template.format(current_config=json.dumps(current_config, indent=2))
    return append_skill_output_contract(rendered)
