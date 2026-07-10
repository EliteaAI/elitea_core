"""Pure helpers for participant entity_settings normalization (no pylon deps)."""


def coerce_version_id(data: dict) -> None:
    """Coerce ``data['version_id']`` to int in place.

    entity_settings is stored verbatim as JSONB; a string "10" from an MCP/JSON
    caller won't match the integer ApplicationVersion.id, so version resolution
    fails. Raises ValueError/TypeError on a non-integer value; leaves the key
    untouched when absent/None.
    """
    if data.get('version_id') is not None:
        data['version_id'] = int(data['version_id'])
