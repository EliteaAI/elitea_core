import json
from typing import Any, Dict, List

SPEC_MAX_DESCRIPTION_LENGTH = 1024
SPEC_MAX_COMPATIBILITY_LENGTH = 500
SPEC_OPTIONAL_KEYS = ('license', 'compatibility', 'allowed-tools')

FRONTMATTER_META_KEY = 'frontmatter'
ELITEA_METADATA_PREFIX = 'elitea-'
LEGACY_RUN_SETTINGS_KEY = 'elitea_run_settings'
LEGACY_VERSION_KEY = 'elitea_version'
ELITEA_OWNED_KEYS = frozenset({'name', 'description', 'tags', LEGACY_VERSION_KEY, LEGACY_RUN_SETTINGS_KEY})
KEYS_SHOWN_BY_ELITEA = frozenset({'license'})


def check_spec_name(name: str) -> None:
    if '--' in str(name):
        raise ValueError('Skill name cannot contain consecutive hyphens (--)')


def check_spec_description(description: str) -> None:
    if any(bracket in str(description) for bracket in '<>'):
        raise ValueError('Skill description cannot contain angle brackets (< or >)')


def _drop_elitea_metadata(metadata: dict) -> dict:
    return {key: value for key, value in metadata.items() if not str(key).startswith(ELITEA_METADATA_PREFIX)}


def _validate_spec_fields(frontmatter: dict) -> None:
    license_value = frontmatter.get('license')
    if license_value is not None and not isinstance(license_value, str):
        raise ValueError('Field "license" must be a string')

    compatibility = frontmatter.get('compatibility')
    if compatibility is not None:
        if not isinstance(compatibility, str):
            raise ValueError('Field "compatibility" must be a string')
        if len(compatibility) > SPEC_MAX_COMPATIBILITY_LENGTH:
            raise ValueError(
                f'Field "compatibility" is {len(compatibility)} characters long. '
                f'Maximum allowed is {SPEC_MAX_COMPATIBILITY_LENGTH} characters.'
            )

    metadata = frontmatter.get('metadata')
    if metadata is not None and not isinstance(metadata, dict):
        raise ValueError('Field "metadata" must be a mapping')


def _to_jsonb_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def extract_passthrough_frontmatter(frontmatter: dict) -> Dict[str, Any]:
    _validate_spec_fields(frontmatter)
    passthrough = {key: value for key, value in frontmatter.items() if key not in ELITEA_OWNED_KEYS}
    foreign_metadata = _drop_elitea_metadata(passthrough.pop('metadata', None) or {})
    if foreign_metadata:
        passthrough['metadata'] = foreign_metadata
    return _to_jsonb_safe(passthrough)


def list_unused_frontmatter_keys(passthrough: dict) -> List[str]:
    return sorted(key for key in passthrough if key not in KEYS_SHOWN_BY_ELITEA)


def compose_export_frontmatter(identity: dict, passthrough: dict, elitea_metadata: dict) -> Dict[str, Any]:
    remaining = {key: value for key, value in passthrough.items() if key not in ELITEA_OWNED_KEYS}
    frontmatter = dict(identity)
    for key in SPEC_OPTIONAL_KEYS:
        if key in remaining:
            frontmatter[key] = remaining.pop(key)

    stored_metadata = remaining.pop('metadata', None)
    metadata = {
        **_drop_elitea_metadata(stored_metadata if isinstance(stored_metadata, dict) else {}),
        **elitea_metadata,
    }
    if metadata:
        frontmatter['metadata'] = metadata

    frontmatter.update(remaining)
    return frontmatter
