import json
from typing import Any, Dict, List

SPEC_MAX_COMPATIBILITY_LENGTH = 500
SPEC_OPTIONAL_KEYS = ('license', 'compatibility', 'allowed-tools')

FRONTMATTER_META_KEY = 'frontmatter'
ELITEA_METADATA_PREFIX = 'elitea-'
LEGACY_RUN_SETTINGS_KEY = 'elitea_run_settings'
LEGACY_VERSION_KEY = 'elitea_version'
ELITEA_OWNED_KEYS = frozenset({'name', 'description', 'tags', LEGACY_VERSION_KEY, LEGACY_RUN_SETTINGS_KEY})
KEYS_SHOWN_BY_ELITEA = frozenset({'license'})


def _drop_elitea_metadata(metadata: dict) -> dict:
    """Remove ELITEA's own elitea-* keys so they are never stored twice or exported stale."""
    return {key: value for key, value in metadata.items() if not str(key).startswith(ELITEA_METADATA_PREFIX)}


def _validate_spec_fields(frontmatter: dict) -> None:
    """Reject spec fields whose type or length would make the exported skill invalid."""
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
    """Turn YAML values such as dates into JSON values the JSONB column can store."""
    return json.loads(json.dumps(value, default=str))


def extract_non_elitea_frontmatter(frontmatter: dict) -> Dict[str, Any]:
    """Keep the frontmatter keys ELITEA does not own, so they are stored and exported unchanged."""
    _validate_spec_fields(frontmatter)
    passthrough = {key: value for key, value in frontmatter.items() if key not in ELITEA_OWNED_KEYS}
    foreign_metadata = _drop_elitea_metadata(passthrough.pop('metadata', None) or {})
    if foreign_metadata:
        passthrough['metadata'] = foreign_metadata
    return _to_jsonb_safe(passthrough)


def list_unused_frontmatter_keys(passthrough: dict) -> List[str]:
    """Name the kept keys ELITEA does not act on, so the import message can list them."""
    return sorted(key for key in passthrough if key not in KEYS_SHOWN_BY_ELITEA)


def compose_export_frontmatter(identity: dict, passthrough: dict, elitea_metadata: dict) -> Dict[str, Any]:
    """Order the exported frontmatter like the spec and put ELITEA's fields under metadata."""
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
