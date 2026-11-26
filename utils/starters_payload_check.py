from .conversation_starters_utils import import_starters_errors
from .export_import_utils import ENTITY_IMPORT_MAPPER, _wrap_import_error


def reject_payload_with_bad_starters(entities: list, skipped_message: str):
    """Reject a whole fork or import, since import_wizard saves each item alone and would leave a partial copy."""
    messages = [
        import_starters_errors(item.get('name', ''), item.get('versions')) if isinstance(item, dict) else []
        for item in entities
    ]
    if not any(messages):
        return None
    rejected = {key: [] for key in ENTITY_IMPORT_MAPPER}
    for index, (item, own_messages) in enumerate(zip(entities, messages)):
        if isinstance(item, dict):
            rejected.setdefault(item.get('entity') or 'agents', []).extend(
                _wrap_import_error(index, message, import_uuid=item.get('import_uuid'))
                for message in own_messages or [skipped_message]
            )
    return {'result': {key: [] for key in ENTITY_IMPORT_MAPPER}, 'errors': rejected}, 400
