"""
Distributed tracing for elitea_core.

Provides OpenTelemetry integration: TracerProvider initialization, @traced decorator,
and instrumentation for Redis, HTTP, and database calls.

Works standalone or delegates to the tracing plugin when available.
"""

import os
import functools
import time
from typing import Optional, Dict, Any, Callable

from pylon.core.tools import log


_tracer = None
_tracer_provider = None
_initialized = False


def init_tracing(
    service_name: str = "elitea-core",
    otlp_endpoint: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> bool:
    """
    Initialize OpenTelemetry TracerProvider with OTLP exporter.

    Checks if the tracing plugin is already active and defers to it.
    Otherwise sets up a standalone TracerProvider.

    Args:
        service_name: Name for the service resource attribute.
        otlp_endpoint: OTLP gRPC endpoint. Defaults to env OTEL_EXPORTER_OTLP_ENDPOINT
                       or 'http://jaeger:4317'.
        enabled: Force enable/disable. Defaults to env TRACING_ENABLED.

    Returns:
        True if tracing was initialized successfully.
    """
    global _tracer, _tracer_provider, _initialized

    if _initialized:
        return _tracer is not None

    _initialized = True

    if enabled is None:
        env_val = os.environ.get("TRACING_ENABLED", "").lower()
        enabled = env_val == "true"

    if not enabled:
        log.info("Distributed tracing disabled (TRACING_ENABLED != true)")
        return False

    if _try_plugin_tracer():
        return True

    if otlp_endpoint is None:
        otlp_endpoint = os.environ.get(
            "OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317"
        )

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME, DEPLOYMENT_ENVIRONMENT

        environment = os.environ.get("DEPLOYMENT_ENVIRONMENT", "development")

        resource = Resource.create({
            SERVICE_NAME: service_name,
            DEPLOYMENT_ENVIRONMENT: environment,
        })

        _tracer_provider = TracerProvider(resource=resource)

        exporter = OTLPSpanExporter(
            endpoint=otlp_endpoint,
            insecure=otlp_endpoint.startswith("http://"),
        )
        _tracer_provider.add_span_processor(BatchSpanProcessor(exporter))

        trace.set_tracer_provider(_tracer_provider)
        _tracer = trace.get_tracer(service_name, "1.0.0")

        log.info(
            f"Distributed tracing initialized: service={service_name}, "
            f"endpoint={otlp_endpoint}"
        )
        return True

    except Exception as e:
        log.warning(f"Failed to initialize distributed tracing: {e}")
        _tracer = None
        _tracer_provider = None
        return False


def _try_plugin_tracer() -> bool:
    """Try to get tracer from the tracing plugin if loaded."""
    global _tracer
    try:
        from tools import this
        tracing_module = this.for_module("tracing").module
        if tracing_module.enabled:
            _tracer = tracing_module.get_tracer()
            if _tracer is not None:
                log.info("Distributed tracing: using existing tracing plugin")
                return True
    except Exception:
        pass
    return False


def get_tracer():
    """
    Get the active OpenTelemetry tracer.

    Returns:
        Tracer instance or None if tracing is not initialized.
    """
    global _tracer, _initialized
    if not _initialized:
        init_tracing()
    return _tracer


def shutdown():
    """Flush and shutdown the TracerProvider."""
    global _tracer_provider
    if _tracer_provider is not None:
        try:
            _tracer_provider.force_flush()
            _tracer_provider.shutdown()
        except Exception as e:
            log.warning(f"Error shutting down tracing: {e}")
        _tracer_provider = None


def traced(
    name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    record_exception: bool = True,
    kind: Optional[str] = None,
):
    """
    Decorator to add a tracing span around a function.

    Args:
        name: Span name. Defaults to the function's qualified name.
        attributes: Static span attributes to set.
        record_exception: Whether to record exceptions on the span.
        kind: Span kind - 'client', 'server', 'producer', 'consumer', or 'internal'.

    Usage:
        @traced("process_task")
        def process_task(task_id):
            ...

        @traced(attributes={"component": "indexer"})
        def run_indexer():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            if tracer is None:
                return func(*args, **kwargs)

            from opentelemetry.trace import SpanKind, Status, StatusCode

            span_name = name or f"{func.__module__}.{func.__qualname__}"
            span_attrs = dict(attributes) if attributes else {}

            kind_map = {
                "client": SpanKind.CLIENT,
                "server": SpanKind.SERVER,
                "producer": SpanKind.PRODUCER,
                "consumer": SpanKind.CONSUMER,
                "internal": SpanKind.INTERNAL,
            }
            span_kind = kind_map.get(kind, SpanKind.INTERNAL) if kind else SpanKind.INTERNAL

            with tracer.start_as_current_span(
                span_name, attributes=span_attrs, kind=span_kind
            ) as span:
                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    if record_exception:
                        span.record_exception(e)
                    raise

        return wrapper
    return decorator


def traced_async(
    name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    record_exception: bool = True,
    kind: Optional[str] = None,
):
    """
    Decorator to add a tracing span around an async function.

    Same parameters as @traced.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            if tracer is None:
                return await func(*args, **kwargs)

            from opentelemetry.trace import SpanKind, Status, StatusCode

            span_name = name or f"{func.__module__}.{func.__qualname__}"
            span_attrs = dict(attributes) if attributes else {}

            kind_map = {
                "client": SpanKind.CLIENT,
                "server": SpanKind.SERVER,
                "producer": SpanKind.PRODUCER,
                "consumer": SpanKind.CONSUMER,
                "internal": SpanKind.INTERNAL,
            }
            span_kind = kind_map.get(kind, SpanKind.INTERNAL) if kind else SpanKind.INTERNAL

            with tracer.start_as_current_span(
                span_name, attributes=span_attrs, kind=span_kind
            ) as span:
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    if record_exception:
                        span.record_exception(e)
                    raise

        return wrapper
    return decorator


def instrument_redis(redis_client=None):
    """
    Instrument Redis calls with OpenTelemetry spans.

    If redis_client is provided, instruments that specific instance.
    Otherwise instruments all redis connections globally.

    Args:
        redis_client: Optional redis.Redis or redis.StrictRedis instance.
    """
    if get_tracer() is None:
        return

    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentor

        if redis_client is not None:
            RedisInstrumentor().instrument(client=redis_client)
        else:
            RedisInstrumentor().instrument()
        log.info("Redis instrumentation enabled for distributed tracing")
    except ImportError:
        log.debug("opentelemetry-instrumentation-redis not available, skipping")
    except Exception as e:
        log.warning(f"Failed to instrument Redis: {e}")


def instrument_http_client():
    """Instrument outgoing HTTP requests (requests library) with tracing."""
    if get_tracer() is None:
        return

    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument()
        log.info("HTTP client instrumentation enabled for distributed tracing")
    except ImportError:
        log.debug("opentelemetry-instrumentation-requests not available, skipping")
    except Exception as e:
        log.warning(f"Failed to instrument HTTP client: {e}")


def instrument_sqlalchemy(engine=None):
    """
    Instrument SQLAlchemy database queries with tracing.

    Args:
        engine: SQLAlchemy engine instance. If None, instruments globally.
    """
    if get_tracer() is None:
        return

    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        kwargs = {}
        if engine is not None:
            kwargs["engine"] = engine
        SQLAlchemyInstrumentor().instrument(**kwargs)
        log.info("SQLAlchemy instrumentation enabled for distributed tracing")
    except ImportError:
        log.debug("opentelemetry-instrumentation-sqlalchemy not available, skipping")
    except Exception as e:
        log.warning(f"Failed to instrument SQLAlchemy: {e}")


def inject_trace_context(headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Inject current trace context into headers for propagation.

    Used to propagate trace context across Socket.IO events or inter-service calls.

    Args:
        headers: Existing headers dict to inject into. Creates new dict if None.

    Returns:
        Headers dict with W3C traceparent/tracestate injected.
    """
    if headers is None:
        headers = {}

    try:
        from opentelemetry import context as otel_context
        from opentelemetry.propagate import inject

        inject(headers)
    except ImportError:
        pass
    except Exception as e:
        log.debug(f"Failed to inject trace context: {e}")

    return headers


def extract_trace_context(headers: Dict[str, str]):
    """
    Extract trace context from incoming headers.

    Used to restore parent span context from Socket.IO events or inter-service calls.

    Args:
        headers: Headers dict containing traceparent/tracestate.

    Returns:
        OpenTelemetry context object, or None if extraction fails.
    """
    try:
        from opentelemetry.propagate import extract
        return extract(headers)
    except ImportError:
        return None
    except Exception as e:
        log.debug(f"Failed to extract trace context: {e}")
        return None


def propagate_via_socketio(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add trace context to Socket.IO event data for cross-pod propagation.

    Injects W3C traceparent into the event's _trace_context field.

    Args:
        data: Socket.IO event payload dict.

    Returns:
        Modified data dict with _trace_context added.
    """
    if get_tracer() is None:
        return data

    trace_headers = inject_trace_context()
    if trace_headers:
        data["_trace_context"] = trace_headers

    return data


def restore_from_socketio(data: Dict[str, Any]):
    """
    Restore trace context from Socket.IO event data.

    Extracts W3C traceparent from the event's _trace_context field
    and sets it as the current context.

    Args:
        data: Socket.IO event payload dict.

    Returns:
        OpenTelemetry context or None.
    """
    trace_headers = data.get("_trace_context")
    if not trace_headers or not isinstance(trace_headers, dict):
        return None

    return extract_trace_context(trace_headers)
