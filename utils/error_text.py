"""Short, safe forms of error texts for people (EL-7011 BUG-37; drafts since #6344)."""
import re

ERROR_LINE_MAX_LENGTH = 300
# A query string of an http(s) URL: it may hold a token, so only its place is kept
_URL_QUERY = re.compile(r'(https?://[^\s?#]+)\?[^\s#]*')
# A Windows path (C:\work\x.py) or a POSIX absolute path (/data/x.py, /x.py), up to the first space. The
# POSIX form starts only after the text start, a space, a quote, a bracket or "=", so URLs (https://host/a)
# and dates (12/05/2026) stay as they are
_WINDOWS_PATH = re.compile(r'(?<![\w])[A-Za-z]:\\[\w.\-\\~]*')
_POSIX_PATH = re.compile(r'(?:(?<=^)|(?<=[\s\'"(\[{=]))/[\w.\-/~]+')


def last_exception_line(error_text) -> str:
    """The last non-empty line of an error text, cut at ERROR_LINE_MAX_LENGTH."""
    lines = [line.strip() for line in str(error_text).splitlines() if line.strip()]
    line = lines[-1] if lines else str(error_text).strip()
    if len(line) > ERROR_LINE_MAX_LENGTH:
        return line[:ERROR_LINE_MAX_LENGTH] + "..."
    return line


def hide_locations(text: str) -> str:
    """Absolute file paths become <path> and URL query strings become ?<query>; URL hosts and dates stay."""
    text = _URL_QUERY.sub(r'\1?<query>', text)
    text = _WINDOWS_PATH.sub('<path>', text)
    return _POSIX_PATH.sub('<path>', text)
