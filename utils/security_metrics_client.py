"""Security counters of this plugin (#6754, C-1009).

Counts through the tracing plugin's ``tools.security_metrics`` when it is loaded: an OpenTelemetry
counter, an in-process total and the structured log line. The tool is resolved at call time because
plugin init order is not guaranteed. Without it the counter keeps a local in-process total and writes
the same line::

    security_metric name=<name> value=<n> <label>=<value> ...
"""

import re
import threading

_UNSAFE = re.compile(r"[^A-Za-z0-9_.:/@+-]")


def _token(value):
    return _UNSAFE.sub("_", str(value))[:64] or "-"


def _shared():
    try:
        from tools import security_metrics  # pylint: disable=C0415,E0401
    except ImportError:
        return None
    return security_metrics


class SecurityCounter:
    """``inc(value=1, **labels)`` / ``total(**labels)``, same interface as the tracing helper."""

    def __init__(self, name, description, labels=()):
        self.name, self.description, self.labels = name, description, tuple(labels)
        self._lock = threading.Lock()
        self._local = {}

    def inc(self, value=1, **labels):
        shared = _shared()
        if shared is not None:
            shared.counter(self.name, self.description, self.labels).inc(value, **labels)
            return
        if set(labels) != set(self.labels):
            raise ValueError(f"{self.name} takes labels {list(self.labels)}, got {sorted(labels)}")
        clean = {key: _token(labels[key]) for key in self.labels}
        with self._lock:
            key = tuple(clean.values())
            self._local[key] = self._local.get(key, 0) + value
        try:
            from pylon.core.tools import log  # pylint: disable=C0415,E0401
            log.info(" ".join(["security_metric", f"name={self.name}", f"value={value}",
                               *(f"{k}={v}" for k, v in clean.items())]))
        except Exception:  # pylint: disable=W0718
            pass

    def total(self, **labels):
        wanted = {self.labels.index(k): _token(v) for k, v in labels.items()}
        with self._lock:
            local = sum(n for key, n in self._local.items() if all(key[i] == v for i, v in wanted.items()))
        shared = _shared()
        if shared is None:
            return local
        return local + shared.counter(self.name, self.description, self.labels).total(**labels)
