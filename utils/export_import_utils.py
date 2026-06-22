import re


ENTITY_IMPORT_MAPPER = {
    'datasources': 'datasources_import_datasource',
    'agents': 'applications_import_application',
    'toolkits': 'applications_import_toolkit',
    'skills': 'applications_import_skill',
}


def slugify(text: str) -> str:
    """Convert a string to a filename-safe slug."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text[:50]


def _wrap_import_error(ind, msg):
    return {
        "index": ind,
        "msg": msg
    }


def _wrap_import_result(ind, result):
    result['index'] = ind
    return result