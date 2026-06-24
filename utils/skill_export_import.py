from typing import Any, Dict, List, Optional

import yaml

from .export_import_utils import slugify
from .skill_utils import (
    _skill_session,
    get_skill_details,
    import_skill,
)


DEFAULT_VERSION_NAME = 'base'
ALLOWED_SKILL_EXTENSIONS = {'.md'}

REQUIRED_FRONTMATTER_FIELDS = ('name', 'description')


def _select_version(skill_data: dict, version_name: Optional[str]) -> dict:
    versions = skill_data.get('versions') or []
    if not versions:
        raise ValueError(f"Skill '{skill_data.get('name')}' has no versions")

    if version_name:
        match = next((v for v in versions if v.get('name') == version_name), None)
        if match is None:
            raise ValueError(
                f"Version '{version_name}' not found for skill "
                f"'{skill_data.get('name')}'"
            )
        return match

    return next(
        (v for v in versions if v.get('name') == DEFAULT_VERSION_NAME),
        versions[0],
    )


def _normalize_tags(version: dict) -> List[str]:
    tags = version.get('tags') or []
    names: List[str] = []
    for tag in tags:
        if isinstance(tag, dict):
            name = tag.get('name')
            if name:
                names.append(name)
        elif isinstance(tag, str):
            names.append(tag)
    return names


def skill_to_md(skill_data: dict, version_name: Optional[str] = None) -> str:
    """Convert a skill detail dict to Markdown (YAML frontmatter + body)."""
    version = _select_version(skill_data, version_name)

    details = skill_data.get('version_details') or {}
    if not version.get('instructions') and details.get('name') == version.get('name'):
        instructions = details.get('instructions', '')
        tags_source = details if details.get('tags') else version
    else:
        instructions = version.get('instructions', '')
        tags_source = version

    frontmatter: Dict[str, Any] = {
        'name': skill_data.get('name', ''),
        'description': skill_data.get('description', ''),
    }

    resolved_version_name = version.get('name', DEFAULT_VERSION_NAME)
    if resolved_version_name and resolved_version_name != DEFAULT_VERSION_NAME:
        frontmatter['elitea_version'] = resolved_version_name

    tags = _normalize_tags(tags_source)
    if tags:
        frontmatter['tags'] = tags

    yaml_str = yaml.dump(
        frontmatter,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120,
    )

    return f"---\n{yaml_str}---\n\n{instructions}"


def export_skill_md(
    project_id: int,
    skill_id: int,
    version_name: Optional[str] = None,
    version_id: Optional[int] = None,
    session=None,
) -> dict:
    """Export a skill as Markdown."""
    result = get_skill_details(
        project_id=project_id,
        skill_id=skill_id,
        version_name=version_name,
        version_id=version_id,
        session=session,
    )
    skill_data = result.get('data')
    if not skill_data:
        return {'ok': False, 'msg': f'Skill with id {skill_id} not found'}

    if version_id is not None:
        version = next(
            (v for v in (skill_data.get('versions') or []) if v.get('id') == version_id),
            None,
        )
        if not version:
            return {
                'ok': False,
                'msg': f"Version '{version_id}' not found for skill {skill_id}",
            }
        # Render/name the export by the resolved version name.
        version_name = version.get('name')

    if version_name:
        names = {v.get('name') for v in (skill_data.get('versions') or [])}
        if version_name not in names:
            return {
                'ok': False,
                'msg': f"Version '{version_name}' not found for skill {skill_id}",
            }

    try:
        content = skill_to_md(skill_data, version_name=version_name)
    except ValueError as exc:
        return {'ok': False, 'msg': str(exc)}

    name_slug = slugify(skill_data.get('name', '')) or 'skill'
    if version_name and version_name != DEFAULT_VERSION_NAME:
        filename = f"{name_slug}.{slugify(version_name) or 'skill'}.md"
    else:
        filename = f"{name_slug}.md"

    return {'ok': True, 'filename': filename, 'content': content}


def parse_skill_md(content: str) -> dict:
    """Parse skill Markdown into frontmatter and body."""
    if not content or not content.strip():
        raise ValueError('Empty file: skill Markdown content is required')

    # Normalize line endings for robust delimiter handling.
    normalized = content.replace('\r\n', '\n').replace('\r', '\n').strip()

    if not normalized.startswith('---'):
        raise ValueError('Missing YAML frontmatter. File must start with "---"')

    # Split into ['', frontmatter, body] on the leading and closing delimiters.
    parts = normalized.split('---', 2)
    if len(parts) < 3:
        raise ValueError('Invalid frontmatter format. Missing closing "---"')

    frontmatter_str = parts[1].strip()
    body = parts[2].strip()

    try:
        frontmatter = yaml.safe_load(frontmatter_str)
    except yaml.YAMLError as exc:
        raise ValueError(f'Invalid YAML in frontmatter: {exc}')

    if not isinstance(frontmatter, dict):
        raise ValueError('Frontmatter must be a YAML mapping (key: value pairs)')

    if not body:
        raise ValueError('Skill instructions (Markdown body) are required and cannot be empty')

    return {'frontmatter': frontmatter, 'body': body}


def validate_skill_frontmatter(meta: dict) -> None:
    """Validate skill frontmatter.

    - ``name`` and ``description`` are required and non-empty.
    - ``elitea_version``, when present, must be a non-empty string.
    - ``tags``, when present, must be a list of strings.

    """
    if not isinstance(meta, dict):
        raise ValueError('Frontmatter must be a YAML mapping')

    for field in REQUIRED_FRONTMATTER_FIELDS:
        value = meta.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValueError(f'Required field "{field}" is missing or empty')

    if 'elitea_version' in meta:
        version = meta['elitea_version']
        if not isinstance(version, str) or not version.strip():
            raise ValueError('Version name must be a non-empty string')

    if 'tags' in meta and meta['tags'] is not None:
        tags = meta['tags']
        if not isinstance(tags, list):
            raise ValueError('Tags must be a list')
        if any(not isinstance(t, str) for t in tags):
            raise ValueError('All tags must be strings')


def validate_skill_import_filename(filename: Optional[str]) -> None:
    """Validate the import file extension: ``.md`` only."""
    if not filename:
        return
    lowered = filename.lower()
    if not any(lowered.endswith(ext) for ext in ALLOWED_SKILL_EXTENSIONS):
        raise ValueError(
            'Invalid file extension. Skill import only accepts .md files.'
        )


def import_skill_md(
    project_id: int,
    content: str,
    author_id: int,
    session=None,
) -> dict:
    """Import a skill from Markdown content (parse -> validate -> upsert).

    The imported skill is always created with version 'base', regardless of
    what version name was in the exported file's frontmatter. This ensures
    imports create fresh, independent skills rather than inheriting version
    names from the source.
    """

    parsed = parse_skill_md(content)
    frontmatter = parsed['frontmatter']
    body = parsed['body']
    validate_skill_frontmatter(frontmatter)

    # Always use 'base' as the version name for imports. The elitea_version
    # field in the frontmatter is only informational (indicates which version
    # was exported) and should not affect the imported skill's version name.
    tags = frontmatter.get('tags')
    tag_payload = [{'name': tag} for tag in tags] if tags else None

    version = {'name': DEFAULT_VERSION_NAME, 'instructions': body, 'author_id': author_id}
    if tag_payload:
        version['tags'] = tag_payload

    with _skill_session(session, project_id) as s:
        imported = import_skill(
            project_id=project_id,
            name=frontmatter['name'],
            description=frontmatter['description'],
            versions=[version],
            author_id=author_id,
            session=s,
        )
        result = get_skill_details(project_id=project_id, skill_id=imported.id, session=s)
        return result.get('data')
