"""Short, safe forms of error texts for people (EL-7011 BUG-37; drafts since #6344)."""
import re

ERROR_LINE_MAX_LENGTH = 300
FAILED_REASON = 'The run failed'
# Shown when the error text is written for engineers; the text itself goes to the server log only
GENERIC_FAILED_REASON = f'{FAILED_REASON}. Open the run log or ask an administrator for details.'
# A query string of an http(s) URL: it may hold a token, so only its place is kept
_URL_QUERY = re.compile(r'(https?://[^\s?#]+)\?[^\s#]*')
# A Windows path (C:\work\x.py) or a POSIX absolute path (/data/x.py, /x.py), up to the first space. The
# POSIX form starts only after the text start, a space, a quote, a bracket or "=", so URLs (https://host/a)
# and dates (12/05/2026) stay as they are
_WINDOWS_PATH = re.compile(r'(?<![\w])[A-Za-z]:\\[\w.\-\\~]*')
_POSIX_PATH = re.compile(r'(?:(?<=^)|(?<=[\s\'"(\[{=]))/[\w.\-/~]+')
# The dotted module of an exception class ("langchain_core.tools.base.ToolException: ..."): people read the class
# name and the text, not the Python package
_MODULE_PREFIX = re.compile(r'^(?:[A-Za-z_]\w*\.)+(?=[A-Z]\w*(?:Error|Exception)\b)')
# Control characters and the invisible or direction-changing characters that could reorder or hide text in a notice
_INVISIBLE = re.compile(r'[\x00-\x1f\x7f-\x9f\u200b-\u200f\u2028-\u202e\u2060-\u2069\ufeff]+')

# Signs of an engineer's text: an exception class, a traceback, a source file or a file path
_TECHNICAL = re.compile(r'\b[A-Z]\w*(?:Error|Exception)\b|Traceback|\.py\b|[A-Za-z]:\\|(?:^|[\s\'"(=])/[\w.\-]+/')
# Characters that a notice or a log could show as a link, a tag or code
_MARKUP = re.compile(r'[\[\]<>`]+')


def is_technical(text) -> bool:
    return bool(_TECHNICAL.search(str(text)))


def without_markup(text: str) -> str:
    return ' '.join(_MARKUP.sub(' ', text).split())


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


def readable_reason(text: str) -> str:
    """One plain line for people: no control or direction characters and no Python module before the class name."""
    return _MODULE_PREFIX.sub('', ' '.join(_INVISIBLE.sub(' ', text).split()))


def public_reason(reason):
    """A stored reason as people may see it: a run saved before the generic text existed may hold an engineer's text."""
    if reason and is_technical(reason):
        return GENERIC_FAILED_REASON
    return reason


def public_event(event: dict) -> dict:
    """The same rule for the text of a stored `finished` event ("Run finished: failed (<reason>)")."""
    text = event.get('text')
    if event.get('kind') == 'finished' and text and is_technical(text):
        return {**event, 'text': f'Run finished: failed ({GENERIC_FAILED_REASON})'}
    return event
