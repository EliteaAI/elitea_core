import json
import uuid
import zipfile
import zlib
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional

import yaml
from sqlalchemy.orm import selectinload

from ..models.skill import Skill, SkillVersion
from ..models.pd.skill import SkillExportModel, validate_skill_name
from .export_import_utils import slugify
from .skill_files import (
    add_version_files,
    build_file_entry,
    describe_size,
    list_version_manifest,
    read_version_contents,
    stage_version_files,
    validate_package,
)
from .skill_frontmatter import (
    FRONTMATTER_META_KEY,
    LEGACY_RUN_SETTINGS_KEY,
    compose_export_frontmatter,
    extract_non_elitea_frontmatter,
    list_unused_frontmatter_keys,
)
from .skill_utils import (
    SkillFileError,
    _skill_session,
    get_max_file_bytes,
    get_skill_details,
    import_skill,
    validate_instructions_length,
)
from .zip_safety import is_encrypted_zip_member, is_unsafe_zip_member


DEFAULT_VERSION_NAME = 'base'
ALLOWED_SKILL_EXTENSIONS = {'.md', '.zip'}
SKILL_MD_NAME = 'SKILL.md'
MD_MIMETYPE = 'text/markdown; charset=utf-8'
ZIP_MIMETYPE = 'application/zip'
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
ZIP_LITTER_PREFIXES = ('__MACOSX/',)
ZIP_LITTER_NAMES = ('.DS_Store',)
ZIP_READ_ERRORS = (zipfile.BadZipFile, zlib.error, OSError, RuntimeError, ValueError, EOFError)

REQUIRED_FRONTMATTER_FIELDS = ('name', 'description')

RUN_SETTINGS_FRONTMATTER_KEY = LEGACY_RUN_SETTINGS_KEY
FRONTMATTER_LLM_FIELDS = ('model_name', 'temperature', 'max_tokens', 'reasoning_effort')
FRONTMATTER_RUN_SETTINGS_FIELDS = (*FRONTMATTER_LLM_FIELDS, 'selection', 'ignore_project_context')

TAGS_METADATA_KEY = 'elitea-tags'
VERSION_METADATA_KEY = 'elitea-version'
RUN_SETTINGS_METADATA_KEYS = {
    'model_name': 'elitea-model',
    'temperature': 'elitea-temperature',
    'max_tokens': 'elitea-max-tokens',
    'reasoning_effort': 'elitea-reasoning-effort',
    'ignore_project_context': 'elitea-ignore-project-context',
    'selection': 'elitea-model-selection',
}
JSON_ENCODED_METADATA_KEYS = (TAGS_METADATA_KEY, RUN_SETTINGS_METADATA_KEYS['selection'])
AUTO_MAX_TOKENS = -1
FLAG_STRINGS = {'true': True, 'false': False}


def _parse_flag(value: str) -> bool:
    """Read a boolean written as text, rejecting anything other than true or false."""
    try:
        return FLAG_STRINGS[value.strip().lower()]
    except KeyError:
        raise ValueError(f'not a boolean: {value!r}')


STRING_RUN_SETTINGS_PARSERS = {'temperature': float, 'max_tokens': int, 'ignore_project_context': _parse_flag}


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


def ensure_base_version(raw_versions):
    """Guarantee a 'base' version exists and comes first (issue #5469).

    Unlike import_skill_md (#5414) which COLLAPSES an imported skill to a
    single 'base' version and drops the source version name, the agent
    import path is ADDITIVE: it PREPENDS a 'base' clone of the first
    version while PRESERVING all named versions (e.g. 'yoda'), so agent
    versions that reference a named version still resolve. A skill_ref
    with version_name=None then attaches to this base, matching
    Skill.get_default_version()'s base fallback.

    Position matters as much as presence: import_skill creates the skill from
    the first entry and appends the rest, and Skill.versions carries no
    order_by, so an export can hand back ['yoda', 'base'].
    """
    versions = list(raw_versions or [])
    if not versions:
        return versions
    base_index = next(
        (i for i, v in enumerate(versions) if (v or {}).get('name') == DEFAULT_VERSION_NAME),
        None,
    )
    if base_index is not None:
        return [versions[base_index], *versions[:base_index], *versions[base_index + 1:]]
    # Spread is safe: import_skill whitelists payload keys, so no stray
    # id/created_at/skill_id leaks and absent author_id -> importing user.
    base = {**(versions[0] or {}), 'name': DEFAULT_VERSION_NAME}
    return [base, *versions]


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
    version_source = (
        details
        if not version.get('instructions') and details.get('name') == version.get('name')
        else version
    )
    tags_source = details if version_source is details and details.get('tags') else version

    resolved_version_name = version.get('name', DEFAULT_VERSION_NAME)
    elitea_metadata = encode_elitea_metadata(
        version_name=resolved_version_name,
        tags=_normalize_tags(tags_source),
        run_settings=version_source.get('run_settings'),
    )
    passthrough = (version_source.get('meta') or {}).get(FRONTMATTER_META_KEY) or {}
    frontmatter = compose_export_frontmatter(
        identity={'name': skill_data.get('name', ''), 'description': skill_data.get('description', '')},
        passthrough=passthrough,
        elitea_metadata=elitea_metadata,
    )

    yaml_str = yaml.dump(
        frontmatter,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120,
    )

    return f"---\n{yaml_str}---\n\n{version_source.get('instructions', '')}"


def encode_elitea_metadata(version_name: Optional[str], tags: List[str], run_settings: Optional[dict]) -> Dict[str, str]:
    """Write ELITEA's own fields as metadata.elitea-* strings, the only place the spec allows them."""
    metadata: Dict[str, str] = {}
    if tags:
        metadata[TAGS_METADATA_KEY] = json.dumps(tags, ensure_ascii=False)
    if version_name and version_name != DEFAULT_VERSION_NAME:
        metadata[VERSION_METADATA_KEY] = version_name
    for field, value in run_settings_to_frontmatter(run_settings).items():
        if field == 'max_tokens' and value == AUTO_MAX_TOKENS:
            continue
        key = RUN_SETTINGS_METADATA_KEYS[field]
        if key in JSON_ENCODED_METADATA_KEYS:
            metadata[key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, bool):
            metadata[key] = str(value).lower()
        else:
            metadata[key] = str(value)
    return metadata


def decode_elitea_metadata(metadata: dict) -> Dict[str, Any]:
    """Read metadata.elitea-* back into the legacy frontmatter shape the import validators already know."""
    decoded: Dict[str, Any] = {}
    if TAGS_METADATA_KEY in metadata:
        decoded['tags'] = _decode_json_metadata(metadata, TAGS_METADATA_KEY)

    run_settings = {
        field: _decode_run_setting(field, metadata, key)
        for field, key in RUN_SETTINGS_METADATA_KEYS.items()
        if key in metadata
    }
    if run_settings:
        run_settings.setdefault('max_tokens', AUTO_MAX_TOKENS)
        decoded[RUN_SETTINGS_FRONTMATTER_KEY] = run_settings
    return decoded


def _decode_json_metadata(metadata: dict, key: str) -> Any:
    """Parse an elitea-* value stored as a JSON string, accepting a value already written as YAML."""
    value = metadata[key]
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f'"metadata.{key}" must be valid JSON: {exc}')


def _decode_run_setting(field: str, metadata: dict, key: str) -> Any:
    """Turn one elitea-* run setting back into its typed value, leaving bad values for later validation to drop."""
    if key in JSON_ENCODED_METADATA_KEYS:
        return _decode_json_metadata(metadata, key)
    value = metadata[key]
    parse = STRING_RUN_SETTINGS_PARSERS.get(field)
    if parse is None or not isinstance(value, str):
        return value
    try:
        return parse(value)
    except ValueError:
        return value


def run_settings_to_frontmatter(run_settings: Optional[dict]) -> Dict[str, Any]:
    if not run_settings:
        return {}
    llm_settings = run_settings.get('llm_settings') or {}
    block = {k: llm_settings[k] for k in FRONTMATTER_LLM_FIELDS if llm_settings.get(k) is not None}
    selection = llm_settings.get('selection') or {}
    if selection.get('mode') == 'auto':
        block['selection'] = selection
    if run_settings.get('ignore_project_context'):
        block['ignore_project_context'] = True
    return block


def run_settings_from_frontmatter(meta: dict) -> Optional[dict]:
    block = meta.get(RUN_SETTINGS_FRONTMATTER_KEY)
    if not block:
        return None
    llm_settings = {k: block[k] for k in (*FRONTMATTER_LLM_FIELDS, 'selection') if block.get(k) is not None}
    run_settings = {'ignore_project_context': block.get('ignore_project_context', False)}
    if llm_settings:
        run_settings['llm_settings'] = llm_settings
    return run_settings


def export_skill_md(
    project_id: int,
    skill_id: int,
    version_name: Optional[str] = None,
    version_id: Optional[int] = None,
    session=None,
) -> dict:
    """Export a skill version as Markdown, or as a ZIP of ``<name>/SKILL.md`` and its files when it has files."""
    with _skill_session(session, project_id) as s:
        result = get_skill_details(
            project_id=project_id,
            skill_id=skill_id,
            version_name=version_name,
            version_id=version_id,
            session=s,
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
            # Render/name the export by the resolved version name (already validated).
            version_name = version.get('name')
        elif version_name:
            names = {v.get('name') for v in (skill_data.get('versions') or [])}
            if version_name not in names:
                return {
                    'ok': False,
                    'msg': f"Version '{version_name}' not found for skill {skill_id}",
                }

        try:
            content = skill_to_md(skill_data, version_name=version_name)
            exported_version_id = _select_version(skill_data, version_name).get('id')
        except ValueError as exc:
            return {'ok': False, 'msg': str(exc)}
        files = read_version_contents(project_id, exported_version_id, session=s) if exported_version_id else []

    name_slug = slugify(skill_data.get('name', '')) or 'skill'
    if version_name and version_name != DEFAULT_VERSION_NAME:
        stem = f"{name_slug}.{slugify(version_name) or 'skill'}"
    else:
        stem = name_slug

    if not files:
        return {'ok': True, 'filename': f"{stem}.md", 'content': content, 'mimetype': MD_MIMETYPE}
    return {
        'ok': True,
        'filename': f"{stem}.zip",
        'content': build_skill_zip(_skill_folder(skill_data.get('name', ''), name_slug), content, files),
        'mimetype': ZIP_MIMETYPE,
    }


def _skill_folder(name: str, slug: str) -> str:
    """The archive folder Claude Code expects: the skill name when it is a valid Agent Skills name, else its slug."""
    try:
        return validate_skill_name(name)
    except ValueError:
        return slug


def build_skill_zip(folder: str, skill_md: str, files: list) -> bytes:
    """A Claude-layout skill archive with fixed timestamps, so the same version always exports the same bytes."""
    members = [(SKILL_MD_NAME, skill_md.encode('utf-8'))] + [(entry['path'], data) for entry, data in files]
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for path, data in members:
            info = zipfile.ZipInfo(f'{folder}/{path}', date_time=ZIP_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, data)
    return buffer.getvalue()


def build_skill_fork_payload(
    project_id: int,
    skill_id: int,
    version_id: Optional[int] = None,
    session=None,
) -> Optional[dict]:
    """Build the fork payload for a skill, or None if the skill/version is missing."""
    with _skill_session(session, project_id) as s:
        skill = (
            s.query(Skill)
            .options(selectinload(Skill.versions).selectinload(SkillVersion.tags))
            .filter(Skill.id == skill_id)
            .first()
        )
        if skill is None:
            return None

        if version_id is not None:
            source_version = next((v for v in skill.versions if v.id == version_id), None)
        else:
            source_version = skill.get_default_version() or (skill.versions[0] if skill.versions else None)
        if source_version is None:
            return None

        skill_dict = skill.to_json()
        skill_dict['project_id'] = project_id
        skill_dict['user_id'] = skill.author_id
        version_dict = source_version.to_json()
        version_dict['tags'] = [t.to_json() for t in source_version.tags]
        version_dict['name'] = DEFAULT_VERSION_NAME
        skill_dict['versions'] = [version_dict]

        source_version_id = source_version.id
        source_author_id = source_version.author_id
        files = list_version_manifest(s, source_version_id)

        payload = SkillExportModel.model_validate(skill_dict).model_dump(mode='json')

    # SkillExportModel excludes owner_id and has no entity field; the version
    # export model omits per-version id/author_id. fork.py stamps parent lineage
    # from these, so re-add them explicitly.
    payload['owner_id'] = project_id
    payload['entity'] = 'skills'
    payload['import_uuid'] = str(uuid.uuid5(
        uuid.NAMESPACE_OID,
        f"skill_fork:{project_id}:{skill_id}:{payload['name']}",
    ))
    payload['versions'][0]['id'] = source_version_id
    payload['versions'][0]['author_id'] = source_author_id
    if files:
        payload['versions'][0]['files'] = files

    return payload


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
    - ``elitea_run_settings``, when present, must be a mapping of known fields; its
      values are validated on import, where an invalid model setting is dropped.

    """
    if not isinstance(meta, dict):
        raise ValueError('Frontmatter must be a YAML mapping')

    for field in REQUIRED_FRONTMATTER_FIELDS:
        value = meta.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValueError(f'Required field "{field}" is missing or empty')

    validate_elitea_frontmatter_fields(meta)


def validate_elitea_frontmatter_fields(meta: dict) -> None:
    """Check the shape of ELITEA's own fields, wherever in the frontmatter they came from."""
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

    run_settings = meta.get(RUN_SETTINGS_FRONTMATTER_KEY)
    if run_settings is not None:
        if not isinstance(run_settings, dict):
            raise ValueError(f'"{RUN_SETTINGS_FRONTMATTER_KEY}" must be a YAML mapping')
        unknown = sorted(set(run_settings) - set(FRONTMATTER_RUN_SETTINGS_FIELDS))
        if unknown:
            raise ValueError(
                f'Unknown "{RUN_SETTINGS_FRONTMATTER_KEY}" field(s): {", ".join(unknown)}'
            )


def build_embedded_skill_frontmatter(version: dict) -> Dict[str, Any]:
    """Carry a skill's frontmatter, tags and run settings into an agent Markdown export."""
    return compose_export_frontmatter(
        identity={},
        passthrough=(version.get('meta') or {}).get(FRONTMATTER_META_KEY) or {},
        elitea_metadata=encode_elitea_metadata(
            version_name=None,
            tags=_normalize_tags(version),
            run_settings=version.get('run_settings'),
        ),
    )


def unpack_embedded_skill_frontmatter(version: dict) -> dict:
    """Restore tags, run settings and kept keys from a skill embedded in an agent Markdown import."""
    frontmatter = (version.get('meta') or {}).get(FRONTMATTER_META_KEY)
    if not frontmatter:
        return version
    if not isinstance(frontmatter, dict):
        raise ValueError(f'"meta.{FRONTMATTER_META_KEY}" must be a mapping')

    passthrough = extract_non_elitea_frontmatter(frontmatter)
    elitea_fields = decode_elitea_metadata(frontmatter.get('metadata') or {})
    validate_elitea_frontmatter_fields(elitea_fields)

    unpacked = {**version, 'meta': {**version['meta'], FRONTMATTER_META_KEY: passthrough}}
    if elitea_fields.get('tags') and not version.get('tags'):
        unpacked['tags'] = [{'name': tag} for tag in elitea_fields['tags']]
    run_settings = run_settings_from_frontmatter(elitea_fields)
    if run_settings and not version.get('run_settings'):
        unpacked['run_settings'] = run_settings
    return unpacked


def validate_skill_import_filename(filename: Optional[str]) -> None:
    """Validate the import file extension: ``.md``, or ``.zip`` for a skill with files."""
    if not filename:
        return
    lowered = filename.lower()
    if not any(lowered.endswith(ext) for ext in ALLOWED_SKILL_EXTENSIONS):
        raise ValueError(
            'Invalid file extension. Skill import only accepts .md or .zip files.'
        )


def _read_zip_member(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes:
    """The member's bytes, inflated only up to the size it declares, which the caps have already checked."""
    try:
        with archive.open(info) as member:
            return member.read(info.file_size + 1)
    except ZIP_READ_ERRORS as exc:
        raise SkillFileError(f"'{info.filename}' could not be read from the ZIP") from exc


def _find_skill_md(members: list) -> zipfile.ZipInfo:
    """The SKILL.md of either Claude layout: at the root, or alone in one top-level folder."""
    candidates = [info for info in members if PurePosixPath(info.filename).name.lower() == SKILL_MD_NAME.lower()]
    at_root = [info for info in candidates if '/' not in info.filename]
    in_folder = [info for info in candidates if info.filename.count('/') == 1]
    found = at_root or in_folder
    if len(found) != 1:
        raise SkillFileError(
            'A skill ZIP must hold exactly one SKILL.md, at the root or inside one top-level folder; '
            f'found {len(found)}'
        )
    return found[0]


def read_skill_zip(data: bytes) -> tuple:
    """The SKILL.md text and the file entries of a skill ZIP, refusing anything that could escape or overflow."""
    try:
        archive = zipfile.ZipFile(BytesIO(data))
    except ZIP_READ_ERRORS as exc:
        raise SkillFileError('The file is not a valid ZIP archive') from exc
    with archive:
        members = [
            info for info in archive.infolist()
            if not info.is_dir()
            and not info.filename.startswith(ZIP_LITTER_PREFIXES)
            and PurePosixPath(info.filename).name not in ZIP_LITTER_NAMES
        ]
        for info in members:
            if is_unsafe_zip_member(info):
                raise SkillFileError(f"'{info.filename}' cannot be unpacked safely")
            if is_encrypted_zip_member(info):
                raise SkillFileError(f"'{info.filename}' is encrypted; export the skill without a password")
        skill_md = _find_skill_md(members)
        root = skill_md.filename[:-len(SKILL_MD_NAME)]
        files = [info for info in members if info is not skill_md]
        outside = next((info.filename for info in files if not info.filename.startswith(root)), None)
        if outside:
            raise SkillFileError(f"'{outside}' is outside the skill folder '{root.rstrip('/')}'")
        if skill_md.file_size > get_max_file_bytes():
            raise SkillFileError(
                f'SKILL.md is {describe_size(skill_md.file_size)}; '
                f'a skill file can be at most {describe_size(get_max_file_bytes())}'
            )
        validate_package([{'path': info.filename[len(root):], 'size': info.file_size} for info in files])
        entries = [build_file_entry(info.filename[len(root):], _read_zip_member(archive, info)) for info in files]
        validate_package(entries)
        try:
            content = _read_zip_member(archive, skill_md).decode('utf-8-sig')
        except UnicodeDecodeError as exc:
            raise SkillFileError('SKILL.md is not UTF-8 text') from exc
    if not content.strip():
        raise SkillFileError('SKILL.md is empty')
    return content, entries


def import_skill_md(
    project_id: int,
    content: str,
    author_id: int,
    session=None,
    files: tuple = (),
) -> dict:
    """Import a skill from Markdown content (parse -> validate -> upsert), with the file entries of its ZIP.

    The imported skill is always created with version 'base', regardless of
    what version name was in the exported file's frontmatter. This ensures
    imports create fresh, independent skills rather than inheriting version
    names from the source.
    """

    parsed = parse_skill_md(content)
    raw_frontmatter = parsed['frontmatter']
    body = parsed['body']
    passthrough = extract_non_elitea_frontmatter(raw_frontmatter)
    frontmatter = {**raw_frontmatter, **decode_elitea_metadata(raw_frontmatter.get('metadata') or {})}
    validate_skill_frontmatter(frontmatter)
    validate_instructions_length(body)

    # Always use 'base' as the version name for imports. The elitea_version
    # field in the frontmatter is only informational (indicates which version
    # was exported) and should not affect the imported skill's version name.
    tags = frontmatter.get('tags')
    tag_payload = [{'name': tag} for tag in tags] if tags else None

    version = {
        'name': DEFAULT_VERSION_NAME,
        'instructions': body,
        'author_id': author_id,
        'run_settings': run_settings_from_frontmatter(frontmatter),
    }
    if tag_payload:
        version['tags'] = tag_payload
    if passthrough:
        version['meta'] = {FRONTMATTER_META_KEY: passthrough}

    entries = list(files)
    staged = stage_version_files(project_id, entries)
    with _skill_session(session, project_id) as s:
        imported = import_skill(
            project_id=project_id,
            name=frontmatter['name'],
            description=frontmatter['description'],
            versions=[version],
            author_id=author_id,
            session=s,
        )
        if entries:
            add_version_files(s, project_id, imported.versions[DEFAULT_VERSION_NAME], entries, staged)
        result = get_skill_details(project_id=project_id, skill_id=imported.id, session=s)
        return {**result.get('data'), 'unused_frontmatter_keys': list_unused_frontmatter_keys(passthrough)}
