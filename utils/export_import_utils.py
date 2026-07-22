import re
from urllib.parse import quote


def content_disposition_attachment(filename: str) -> str:
    """Build an RFC 5987-compliant Content-Disposition header value.

    HTTP headers must be ASCII-only, so a non-Latin ``filename`` (Cyrillic,
    Arabic, CJK, ...) inserted verbatim makes Werkzeug raise a
    ``UnicodeEncodeError`` and return a 500. We emit an ASCII fallback plus a
    ``filename*`` parameter with the UTF-8 name, matching what
    ``flask.send_file`` does for its ``download_name``.
    """
    ascii_fallback = filename.encode('ascii', 'replace').decode('ascii')
    return (
        f'attachment; filename="{ascii_fallback}"; '
        f"filename*=UTF-8''{quote(filename)}"
    )


ENTITY_IMPORT_MAPPER = {
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