"""The one host rule for the Microsoft (Entra) login address.

Stdlib only, so the on-behalf-of exchange and the MCP token proxy share it without loading each other.
"""
from urllib.parse import urlsplit

AUTHORITY_HOST = "login.microsoftonline.com"


def is_entra_authority_url(url) -> bool:
    """Whether ``url`` is an https address on the Entra authority host, with no user info and port 443 at most.

    Only the host is pinned: the tenant and the path may be of any shape. A backslash, a space or a control
    character is refused because parsers differ on where the host ends. The one host rule of every caller.
    """
    if not isinstance(url, str) or any(char.isspace() or char == "\\" or ord(char) < 32 for char in url):
        return False
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return False
    return (parts.scheme == "https" and parts.hostname == AUTHORITY_HOST
            and "@" not in parts.netloc and port in (None, 443))
