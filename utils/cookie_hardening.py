"""
Cookie hardening middleware for Flask applications.

Ensures all Set-Cookie headers emitted by any response have security flags
(Secure, HttpOnly, SameSite) applied. This acts as a safety net: the auth
session cookie is already configured via Flask's SESSION_COOKIE_* settings,
but this middleware catches any future cookies set by plugins or libraries.
"""

import re

from pylon.core.tools import log  # pylint: disable=E0401


_COOKIE_FLAG_PATTERN = re.compile(
    r'^([^=]+=[^;]*)(;.*)?$', re.IGNORECASE
)


def harden_set_cookie_header(header_value, secure=True, samesite='Lax'):
    """
    Patch a single Set-Cookie header value to include security flags
    if they are missing.

    Args:
        header_value: Raw Set-Cookie header string
        secure: Whether to add Secure flag
        samesite: SameSite value (Lax, Strict, None)

    Returns:
        Modified header value with security flags enforced
    """
    lower = header_value.lower()

    if '; httponly' not in lower and ';httponly' not in lower:
        header_value += '; HttpOnly'

    if secure and '; secure' not in lower and ';secure' not in lower:
        header_value += '; Secure'

    if '; samesite' not in lower and ';samesite' not in lower:
        header_value += f'; SameSite={samesite}'
        if samesite == 'None' and secure:
            if '; secure' not in header_value.lower() and ';secure' not in header_value.lower():
                header_value += '; Secure'

    return header_value


def register_cookie_hardening(app, secure=True, samesite='Lax', excluded_names=None):
    """
    Register an after_request hook that hardens all Set-Cookie headers.

    Args:
        app: Flask application instance
        secure: Whether to enforce Secure flag (disable for local dev over HTTP)
        samesite: Default SameSite policy
        excluded_names: Cookie names to skip (e.g., third-party cookies we can't control)
    """
    excluded = set(excluded_names or [])

    @app.after_request
    def _harden_cookies(response):
        if 'Set-Cookie' not in response.headers:
            return response

        hardened = []
        for header in response.headers.getlist('Set-Cookie'):
            cookie_name = header.split('=', 1)[0].strip() if '=' in header else ''
            if cookie_name in excluded:
                hardened.append(header)
            else:
                hardened.append(
                    harden_set_cookie_header(header, secure=secure, samesite=samesite)
                )

        response.headers.remove('Set-Cookie')
        for h in hardened:
            response.headers.add('Set-Cookie', h)

        return response

    log.info(
        "Cookie hardening registered: secure=%s, samesite=%s, excluded=%s",
        secure, samesite, list(excluded)
    )
