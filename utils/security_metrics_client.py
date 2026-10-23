"""Security counters of this plugin (#6754, C-1009).

Counts through the tracing plugin's ``tools.security_metrics`` when it is loaded: an OpenTelemetry
counter, an in-process total and the structured log line. The tool is resolved at call time because
plugin init order is not guaranteed. Without it (or when it raises) the counter keeps a local
in-process total and writes the same line::

    security_metric name=<name> value=<n> <label>=<value> ...

This file is a copy of the accessor in the tracing plugin (``utils/security_metrics.py``) and is identical
in auth, indexer_worker and elitea_core. The line format, token rules, label allow-list and rate cap are
pinned by the contract test of each repo, and the source of the copies by a hash test next to it, so they
cannot drift apart. Change them together or not at all.
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


def is_count(value):
    """An increment is a positive int (bool excluded), so a folded ``value`` is never <= 0."""
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def schedule_flush(delay, fn):
    """Run ``fn`` once after ``delay`` seconds: a greenlet when gevent is patched in, else a daemon timer."""
    try:
        from gevent import monkey, spawn_later  # pylint: disable=C0415,E0401
        if monkey.is_module_patched("threading"):
            spawn_later(delay, fn)
            return
    except ImportError:
        pass
    timer = threading.Timer(delay, fn)
    timer.daemon = True
    timer.start()


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
        # label values -> [time of the last line, increments not logged yet, flush scheduled]
        self._log_state = {}

    def inc(self, value=1, **labels):
        if not is_count(value):
            return
        shared = _shared()
        if shared is not None:
            try:
                shared.counter(self.name, self.description, self.labels).inc(value, **labels)
                return
            except Exception:  # pylint: disable=W0718
                pass  # recording must never break the security path: count locally instead
        if set(labels) != set(self.labels):
            raise ValueError(f"{self.name} takes labels {list(self.labels)}, got {sorted(labels)}")
        clean = {key: label_value(labels[key]) for key in self.labels}
        key = tuple(clean.values())
        with self._lock:
            self._local[key] = self._local.get(key, 0) + value
            to_log, flush_in = self._admit(key, value)
        if to_log:
            self._write(to_log, clean)
        if flush_in is not None:
            self._arm(flush_in, key)

    def _admit(self, key, value):
        """``(value of the line to write now or 0, delay of the flush to schedule or None)``; caller holds the lock."""
        now = _clock()
        state = self._log_state.setdefault(key, [None, 0, False])
        state[1] += value
        if state[0] is not None and now - state[0] < LOG_INTERVAL_S:
            if state[2]:
                return 0, None
            state[2] = True
            return 0, state[0] + LOG_INTERVAL_S - now
        state[0], to_log, state[1] = now, state[1], 0
        return to_log, None

    def _write(self, value, clean):
        try:
            from pylon.core.tools import log  # pylint: disable=C0415,E0401
            log.info(format_line(self.name, value, clean))
        except Exception:  # pylint: disable=W0718
            pass

    def _arm(self, delay, key):
        """Schedule the end-of-window flush of one label set (already marked as scheduled)."""
        try:
            schedule_flush(delay, lambda: self._flush(key))
        except Exception:  # pylint: disable=W0718
            with self._lock:
                self._log_state[key][2] = False

    def _flush(self, key):
        """Log what the window held back; wait out the rest of the window if a line was written meanwhile."""
        with self._lock:
            state = self._log_state[key]
            now = _clock()
            if state[1] == 0:
                state[2] = False
                return
            if now - state[0] < LOG_INTERVAL_S:
                delay, to_log = state[0] + LOG_INTERVAL_S - now, 0
            else:
                delay, to_log = None, state[1]
                state[0], state[1], state[2] = now, 0, False
        if to_log:
            self._write(to_log, dict(zip(self.labels, key)))
        else:
            self._arm(delay, key)

    def total(self, **labels):
        wanted = {self.labels.index(k): label_value(v) for k, v in labels.items()}
        with self._lock:
            local = sum(n for key, n in self._local.items() if all(key[i] == v for i, v in wanted.items()))
        shared = _shared()
        if shared is None:
            return local
        try:
            return local + shared.counter(self.name, self.description, self.labels).total(**labels)
        except Exception:  # pylint: disable=W0718
            return local
