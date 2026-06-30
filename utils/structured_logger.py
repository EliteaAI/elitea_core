"""Structured JSON logging for horizontal scaling.

Provides a JSON log formatter and request context utilities
for consistent, machine-parseable log output across all pylon services.

Fields emitted per log line:
  timestamp, level, service, request_id, logger, message, extra

Integrates with the existing tracing plugin's trace context
(X-Trace-ID header → flask.g.trace_id → ContextVar).
"""

import json
import logging
import os
import time
import traceback
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pylon.core.tools import log  # noqa: F401

# ---------------------------------------------------------------------------
# Request context management
# ---------------------------------------------------------------------------

_request_id_var: ContextVar[Optional[str]] = ContextVar(
    "structured_logger_request_id", default=None
)


def get_request_id() -> Optional[str]:
    """Get current request ID from context var, Flask g, or tracing plugin."""
    rid = _request_id_var.get()
    if rid:
        return rid

    try:
        from flask import g, has_request_context
        if has_request_context():
            rid = getattr(g, "trace_id", None) or getattr(g, "request_id", None)
            if rid:
                return rid
    except (ImportError, RuntimeError):
        pass

    return None


def set_request_id(request_id: str) -> None:
    """Set request ID in context var for the current execution context."""
    _request_id_var.set(request_id)


def generate_request_id() -> str:
    """Generate a new unique request ID."""
    return f"req-{uuid.uuid4().hex[:16]}"


def ensure_request_id() -> str:
    """Get existing or generate a new request ID."""
    rid = get_request_id()
    if not rid:
        rid = generate_request_id()
        set_request_id(rid)
    return rid


# ---------------------------------------------------------------------------
# JSON Formatter
# ---------------------------------------------------------------------------

_SERVICE_NAME = os.environ.get("NAME", os.environ.get("SERVICE_NAME", "unknown"))


class StructuredJSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects.

    Output schema:
    {
        "timestamp": "2026-06-30T12:00:00.000Z",
        "level": "INFO",
        "service": "pylon-main",
        "request_id": "req-abc123...",
        "logger": "elitea_core.utils.foo",
        "message": "Something happened",
        "extra": {...}  // optional additional fields
    }
    """

    def __init__(
        self,
        service_name: Optional[str] = None,
        include_extra: bool = True,
        include_exception: bool = True,
        timestamp_format: str = "iso",
    ):
        super().__init__()
        self.service_name = service_name or _SERVICE_NAME
        self.include_extra = include_extra
        self.include_exception = include_exception
        self.timestamp_format = timestamp_format
        self._skip_keys = {
            "name", "msg", "args", "created", "relativeCreated",
            "thread", "threadName", "msecs", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "levelname", "levelno",
            "processName", "process", "message", "taskName",
        }

    def _format_timestamp(self, record: logging.LogRecord) -> str:
        if self.timestamp_format == "epoch":
            return str(record.created)
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(record.msecs):03d}Z"

    def _get_extra_fields(self, record: logging.LogRecord) -> Dict[str, Any]:
        extra = {}
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in self._skip_keys:
                continue
            try:
                json.dumps(value)
                extra[key] = value
            except (TypeError, ValueError, OverflowError):
                extra[key] = str(value)
        return extra

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()

        log_entry: Dict[str, Any] = {
            "timestamp": self._format_timestamp(record),
            "level": record.levelname,
            "service": self.service_name,
            "request_id": get_request_id(),
            "logger": record.name,
            "message": record.message,
        }

        if self.include_extra:
            extra = self._get_extra_fields(record)
            if extra:
                log_entry["extra"] = extra

        if self.include_exception and record.exc_info and record.exc_info[0]:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info),
            }

        return json.dumps(log_entry, default=str, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Request context middleware / hook
# ---------------------------------------------------------------------------

def attach_request_id_before_request():
    """Flask before_request hook to attach request_id from headers or generate one.

    Register via: app.before_request(attach_request_id_before_request)
    Or via pylon's request hook system.
    """
    try:
        from flask import request, g
    except ImportError:
        return

    rid = (
        request.headers.get("X-Request-ID")
        or request.headers.get("X-Trace-ID")
        or generate_request_id()
    )
    g.request_id = rid
    set_request_id(rid)


def attach_request_id_after_request(response):
    """Flask after_request hook to echo request_id in response headers."""
    rid = get_request_id()
    if rid and response:
        response.headers.setdefault("X-Request-ID", rid)
    return response


# ---------------------------------------------------------------------------
# Logger configuration helpers
# ---------------------------------------------------------------------------

def configure_structured_logging(
    service_name: Optional[str] = None,
    level: int = logging.INFO,
    include_extra: bool = True,
) -> logging.Handler:
    """Configure root logger with structured JSON output.

    Returns the handler for testing/verification.
    This is designed to work alongside centry_logging — call it after
    centry_logging.init() to replace/add a JSON handler.
    """
    handler = logging.StreamHandler()
    formatter = StructuredJSONFormatter(
        service_name=service_name,
        include_extra=include_extra,
    )
    handler.setFormatter(formatter)
    handler.setLevel(level)
    return handler


def get_structured_logger(
    name: str,
    service_name: Optional[str] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Get a logger pre-configured with structured JSON output.

    Use when you need a standalone structured logger without
    modifying the root logger.
    """
    logger = logging.getLogger(name)
    if not any(
        isinstance(h.formatter, StructuredJSONFormatter)
        for h in logger.handlers
    ):
        handler = configure_structured_logging(
            service_name=service_name,
            level=level,
        )
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger


# ---------------------------------------------------------------------------
# Contextual log adapter
# ---------------------------------------------------------------------------

class StructuredLogAdapter(logging.LoggerAdapter):
    """Logger adapter that automatically injects request_id and service into logs.

    Usage:
        logger = StructuredLogAdapter(logging.getLogger(__name__))
        logger.info("Processing request", extra={"user_id": 123})
    """

    def __init__(self, logger: logging.Logger, extra: Optional[Dict] = None):
        super().__init__(logger, extra or {})

    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {})
        extra.setdefault("request_id", get_request_id())
        extra.setdefault("service", _SERVICE_NAME)
        kwargs["extra"] = extra
        return msg, kwargs
