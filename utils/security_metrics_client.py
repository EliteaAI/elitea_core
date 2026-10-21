"""Security counters of this plugin (#6754, C-1009).

Counts through the tracing plugin's ``tools.security_metrics`` when it is loaded: an OpenTelemetry
counter, an in-process total and the structured log line. The tool is resolved at call time because
plugin init order is not guaranteed. Without it the counter keeps a local in-process total and writes
the same line::

    security_metric name=<name> value=<n> <label>=<value> ...

This module is a copy of the accessor in the tracing plugin (``utils/security_metrics.py``): the line
format, token rules, label allow-list and rate cap are pinned by ``tests/unit/test_6754_security_metrics_contract.py``
here and in each consumer, so the copies cannot drift apart silently.
"""

import re
import threading
import time

MAX_LABEL_VALUE_LEN = 64
# The only label keys a security counter may declare; values are enum-like codes, never identifiers.
ALLOWED_LABEL_KEYS = frozenset({"capability"})
# At most one INFO line per counter and label set in this window; the rest is folded into ``value``.
LOG_INTERVAL_S = 10.0
_UNSAFE = re.compile(r"[^A-Za-z0-9_.:/@+-]")
_clock = time.monotonic


def label_value(value):
    """One log/metric token: unsafe characters become ``_`` and the length is bounded."""
    return _UNSAFE.sub("_", str(value))[:MAX_LABEL_VALUE_LEN] or "-"


def format_line(name, value, labels):
    """The structured log line. ``labels`` is an ordered mapping of already-clean values."""
    parts = ["security_metric", f"name={name}", f"value={value}"]
    parts.extend(f"{key}={val}" for key, val in labels.items())
    return " ".join(parts)


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
        if not set(self.labels) <= ALLOWED_LABEL_KEYS:
            raise ValueError(f"{name}: labels {sorted(set(self.labels) - ALLOWED_LABEL_KEYS)} are not allowed")
        self._lock = threading.Lock()
        self._local = {}
        self._log_state = {}  # label values -> (time of the last line, increments not logged yet)

    def inc(self, value=1, **labels):
        shared = _shared()
        if shared is not None:
            shared.counter(self.name, self.description, self.labels).inc(value, **labels)
            return
        if set(labels) != set(self.labels):
            raise ValueError(f"{self.name} takes labels {list(self.labels)}, got {sorted(labels)}")
        clean = {key: label_value(labels[key]) for key in self.labels}
        key = tuple(clean.values())
        with self._lock:
            self._local[key] = self._local.get(key, 0) + value
            to_log = self._take_log_value(key, value)
        if to_log:
            try:
                from pylon.core.tools import log  # pylint: disable=C0415,E0401
                log.info(format_line(self.name, to_log, clean))
            except Exception:  # pylint: disable=W0718
                pass

    def _take_log_value(self, key, value):
        """The ``value`` of the line to write now, or 0 inside the rate-cap window (caller holds the lock)."""
        now = _clock()
        last, pending = self._log_state.get(key, (None, 0))
        pending += value
        if last is not None and now - last < LOG_INTERVAL_S:
            self._log_state[key] = (last, pending)
            return 0
        self._log_state[key] = (now, 0)
        return pending

    def total(self, **labels):
        wanted = {self.labels.index(k): label_value(v) for k, v in labels.items()}
        with self._lock:
            local = sum(n for key, n in self._local.items() if all(key[i] == v for i, v in wanted.items()))
        shared = _shared()
        if shared is None:
            return local
        return local + shared.counter(self.name, self.description, self.labels).total(**labels)
