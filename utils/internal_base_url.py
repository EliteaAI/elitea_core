"""The base URL the internal Elitea MCP toolkits are addressed at (#7278).

Chosen once per process: elitea_core ``base_url`` when set; else the in-cluster ``http://pylon_main:8080``
when the host ``pylon_main`` resolves; else the shared ``app_host`` (the behaviour before #7278). The
lookup runs at module init, never on a request path.
"""

import socket
import threading

from pylon.core.tools import log
from tools import config as c, this

INTERNAL_HOST = "pylon_main"
INTERNAL_DEFAULT_URL = f"http://{INTERNAL_HOST}:8080"
LOOKUP_TIMEOUT_S = 2.0

_lock = threading.Lock()
_chosen = None


def host_resolves(host: str, timeout: float = LOOKUP_TIMEOUT_S) -> bool:
    """One bounded DNS lookup; a lookup still running after ``timeout`` counts as not resolving."""
    found = []

    def lookup():
        try:
            found.append(bool(socket.getaddrinfo(host, None)))
        except OSError:
            pass

    worker = threading.Thread(target=lookup, name="internal-base-url-lookup", daemon=True)
    worker.start()
    worker.join(timeout)
    return found == [True]


def _choose(config) -> tuple:
    if config is None:
        try:
            config = this.descriptor.config
        except AttributeError:  # the descriptor is not populated yet
            config = None
    configured = (config or {}).get("base_url")
    if configured:
        return configured.rstrip("/"), "configured base_url"
    if host_resolves(INTERNAL_HOST):
        return INTERNAL_DEFAULT_URL, f"default, {INTERNAL_HOST} resolves"
    app_host = getattr(c, "APP_HOST", None)
    if app_host:
        return app_host.rstrip("/"), f"app_host fallback, {INTERNAL_HOST} does not resolve"
    return INTERNAL_DEFAULT_URL, f"default, {INTERNAL_HOST} does not resolve and no app_host"


def internal_base_url(config=None) -> str:
    """``config``: the elitea_core config at module init; later calls return the URL chosen then."""
    global _chosen  # pylint: disable=W0603
    if _chosen is None:
        with _lock:
            if _chosen is None:
                url, reason = _choose(config)
                log.info(f"Internal MCP base URL: {url} ({reason})")
                _chosen = url
    return _chosen
