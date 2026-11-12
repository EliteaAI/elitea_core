import json
from typing import Any, Dict, List, Optional

SPEC_MAX_COMPATIBILITY_LENGTH = 500
SPEC_OPTIONAL_KEYS = ('license', 'compatibility', 'allowed-tools')

FRONTMATTER_META_KEY = 'frontmatter'
ELITEA_METADATA_PREFIX = 'elitea-'
LEGACY_RUN_SETTINGS_KEY = 'elitea_run_settings'
LEGACY_VERSION_KEY = 'elitea_version'
ELITEA_OWNED_KEYS = frozenset({'name', 'description', 'tags', LEGACY_VERSION_KEY, LEGACY_RUN_SETTINGS_KEY})
MODEL_INVOCATION_KEY = 'disable-model-invocation'
USER_INVOCATION_KEY = 'user-invocable'
INVOCATION_KEYS = (MODEL_INVOCATION_KEY, USER_INVOCATION_KEY)
ARGUMENT_HINT_KEY = 'argument-hint'
KEYS_SHOWN_BY_ELITEA = frozenset({'license', ARGUMENT_HINT_KEY, *INVOCATION_KEYS})
INVOCATION_FLAG_KEYS = ('disable_model_invocation', 'user_invocable')


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

    for key in INVOCATION_KEYS:
        value = frontmatter.get(key)
        if value is not None and not isinstance(value, bool):
            raise ValueError(f'Field "{key}" must be true or false')


def invocation_flags(version_meta: Optional[dict]) -> Dict[str, bool]:
    """The two Claude invocation keys of a version, as payload keys, only when set."""
    frontmatter = (version_meta or {}).get(FRONTMATTER_META_KEY) or {}
    flags = {}
    if frontmatter.get(MODEL_INVOCATION_KEY) is True:
        flags['disable_model_invocation'] = True
    if frontmatter.get(USER_INVOCATION_KEY) is False:
        flags['user_invocable'] = False
    return flags


def _hint_as_text(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, list):
        items = [item for item in (_hint_as_text(item) for item in value) if item]
        return '[' + ', '.join(items) + ']' if items else ''
    if isinstance(value, dict):
        pairs = ((key, _hint_as_text(item)) for key, item in value.items())
        return ', '.join(f'{key}: {item}' for key, item in pairs if item)
    return str(value).strip()


def argument_hint_text(version_meta: Optional[dict]) -> Optional[str]:
    """The version's argument-hint as one line, with the brackets the author typed restored around a YAML list."""
    return _hint_as_text(((version_meta or {}).get(FRONTMATTER_META_KEY) or {}).get(ARGUMENT_HINT_KEY)) or None


def argument_hint_payload(version_meta: Optional[dict]) -> Dict[str, str]:
    """The hint as a payload key for the ~ menu, only when the version has one."""
    text = argument_hint_text(version_meta)
    return {'argument_hint': text} if text else {}


def carried_invocation_flags(entry: dict) -> Dict[str, bool]:
    """The invocation payload keys already present on a skill entry, so a rebuilt entry keeps them."""
    return {key: entry[key] for key in INVOCATION_FLAG_KEYS if key in entry}


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
