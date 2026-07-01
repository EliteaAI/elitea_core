"""Global API rate limiting middleware using Redis sliding window counters.

Implements per-IP and per-authenticated-user rate limits with Redis INCR
and key expiry. Provides bypass for internal service-to-service calls via
a shared token, and a status endpoint for current usage inspection.

Redis key layout:
  rate_limit:ip:{ip}:{minute_bucket}     — integer counter per IP per minute
  rate_limit:user:{user_id}:{minute_bucket} — integer counter per user per minute

Default limits:
  - Unauthenticated (by IP): 100 requests/minute
  - Authenticated (by user): 1000 requests/minute
"""

import hmac
import os
import time

import flask

from pylon.core.tools import log


DEFAULT_IP_LIMIT = 100
DEFAULT_USER_LIMIT = 1000
WINDOW_SECONDS = 60
INTERNAL_TOKEN_HEADER = "X-Internal-Token"
TRUSTED_PROXY_CIDRS_ENV = "RATE_LIMIT_TRUSTED_PROXIES"
RATE_LIMIT_HEADER_LIMIT = "X-RateLimit-Limit"
RATE_LIMIT_HEADER_REMAINING = "X-RateLimit-Remaining"
RATE_LIMIT_HEADER_RESET = "X-RateLimit-Reset"

EXEMPT_PATHS = frozenset((
    "/health/live",
    "/health/ready",
    "/health/events",
    "/health/streams",
    "/metrics",
))


def _get_minute_bucket():
    """Return the current minute bucket as an integer (seconds since epoch // 60)."""
    return int(time.time()) // WINDOW_SECONDS


def _parse_trusted_proxies():
    """Parse RATE_LIMIT_TRUSTED_PROXIES env var into a set of CIDR networks."""
    import ipaddress
    raw = os.environ.get(TRUSTED_PROXY_CIDRS_ENV, "")
    if not raw:
        return None
    networks = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            networks.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            pass
    return networks or None


_TRUSTED_PROXIES = None


def _get_trusted_proxies():
    global _TRUSTED_PROXIES
    if _TRUSTED_PROXIES is None:
        _TRUSTED_PROXIES = _parse_trusted_proxies() or []
    return _TRUSTED_PROXIES


def _is_trusted_proxy(remote_addr):
    """Check if the direct connection is from a trusted proxy."""
    import ipaddress
    proxies = _get_trusted_proxies()
    if not proxies:
        return False
    try:
        addr = ipaddress.ip_address(remote_addr)
    except (ValueError, TypeError):
        return False
    return any(addr in network for network in proxies)


def _get_client_ip(request):
    """Extract client IP, only trusting X-Forwarded-For from known proxies."""
    remote = request.remote_addr or "unknown"
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded and _is_trusted_proxy(remote):
        return forwarded.split(",")[0].strip()
    return remote


def _get_reset_timestamp(bucket):
    """Return the Unix timestamp when the current window resets."""
    return (bucket + 1) * WINDOW_SECONDS


class RateLimiter:
    """Redis-backed sliding window rate limiter for Flask applications.

    Uses INCR + EXPIRE per minute-bucket key for O(1) per-request cost.
    Fails open: if Redis is unavailable, requests are allowed through.
    """

    def __init__(self, redis_client, ip_limit=None, user_limit=None, internal_token=None):
        """Initialize the rate limiter.

        Args:
            redis_client: Redis client instance (must support INCR, EXPIRE, GET, pipeline)
            ip_limit: Max requests per minute per IP (default: 100)
            user_limit: Max requests per minute per authenticated user (default: 1000)
            internal_token: Shared token for service-to-service bypass (default: from env)
        """
        self._redis = redis_client
        self.ip_limit = ip_limit or int(os.environ.get("RATE_LIMIT_IP", DEFAULT_IP_LIMIT))
        self.user_limit = user_limit or int(os.environ.get("RATE_LIMIT_USER", DEFAULT_USER_LIMIT))
        self._internal_token = internal_token or os.environ.get("INTERNAL_SERVICE_TOKEN", "")

    def check_rate_limit(self, identifier, limit):
        """Check and increment the counter for an identifier.

        Args:
            identifier: The rate limit key suffix (ip:<ip> or user:<user_id>)
            limit: Maximum allowed requests in the window

        Returns:
            Tuple of (allowed: bool, current_count: int, limit: int, reset_at: int)
        """
        bucket = _get_minute_bucket()
        key = f"rate_limit:{identifier}:{bucket}"
        reset_at = _get_reset_timestamp(bucket)

        try:
            pipe = self._redis.pipeline()
            pipe.incr(key)
            pipe.expire(key, WINDOW_SECONDS + 5)
            results = pipe.execute()
            current = results[0]
        except Exception as e:
            log.warning("Rate limit check failed (allowing request): %s", e)
            return True, 0, limit, reset_at

        allowed = current <= limit
        return allowed, current, limit, reset_at

    def get_current_usage(self, identifier, limit):
        """Get current usage without incrementing.

        Args:
            identifier: The rate limit key suffix
            limit: The configured limit for this identifier

        Returns:
            Dict with current usage information
        """
        bucket = _get_minute_bucket()
        key = f"rate_limit:{identifier}:{bucket}"
        reset_at = _get_reset_timestamp(bucket)

        try:
            current = self._redis.get(key)
            current = int(current) if current else 0
        except Exception as e:
            log.warning("Rate limit usage check failed: %s", e)
            current = 0

        return {
            "identifier": identifier,
            "current": current,
            "limit": limit,
            "remaining": max(0, limit - current),
            "reset_at": reset_at,
            "window_seconds": WINDOW_SECONDS,
        }

    def is_internal_request(self, request):
        """Check if the request is an internal service-to-service call.

        Args:
            request: Flask request object

        Returns:
            True if the request carries a valid internal service token
        """
        if not self._internal_token:
            return False
        token = request.headers.get(INTERNAL_TOKEN_HEADER, "")
        if not token:
            return False
        return hmac.compare_digest(token, self._internal_token)

    def is_exempt_path(self, path):
        """Check if the request path is exempt from rate limiting.

        Args:
            path: The request path

        Returns:
            True if the path is exempt
        """
        normalized = path.rstrip("/")
        return normalized in EXEMPT_PATHS


def register_rate_limiter(app, redis_client, ip_limit=None, user_limit=None, internal_token=None):
    """Register rate limiting as a Flask before_request hook.

    Args:
        app: Flask application instance
        redis_client: Redis client for counter storage
        ip_limit: Max requests/min per IP (default: 100)
        user_limit: Max requests/min per authenticated user (default: 1000)
        internal_token: Token for service-to-service bypass
    """
    limiter = RateLimiter(
        redis_client=redis_client,
        ip_limit=ip_limit,
        user_limit=user_limit,
        internal_token=internal_token,
    )

    @app.before_request
    def _check_rate_limit():
        request = flask.request
        path = request.path

        if limiter.is_exempt_path(path):
            return None

        if limiter.is_internal_request(request):
            return None

        user_id = _get_authenticated_user_id(request)

        if user_id:
            identifier = f"user:{user_id}"
            limit = limiter.user_limit
        else:
            ip = _get_client_ip(request)
            identifier = f"ip:{ip}"
            limit = limiter.ip_limit

        allowed, current, limit_val, reset_at = limiter.check_rate_limit(identifier, limit)

        flask.g.rate_limit_info = {
            "identifier": identifier,
            "current": current,
            "limit": limit_val,
            "reset_at": reset_at,
        }

        if not allowed:
            retry_after = max(1, reset_at - int(time.time()))
            response = flask.jsonify({
                "error": {
                    "message": f"Rate limit exceeded: {current}/{limit_val} requests per minute",
                    "type": "rate_limit_error",
                    "code": "rate_limit_exceeded",
                }
            })
            response.status_code = 429
            response.headers[RATE_LIMIT_HEADER_LIMIT] = str(limit_val)
            response.headers[RATE_LIMIT_HEADER_REMAINING] = "0"
            response.headers[RATE_LIMIT_HEADER_RESET] = str(reset_at)
            response.headers["Retry-After"] = str(retry_after)
            return response

        return None

    @app.after_request
    def _add_rate_limit_headers(response):
        info = getattr(flask.g, "rate_limit_info", None)
        if info is None:
            return response

        response.headers[RATE_LIMIT_HEADER_LIMIT] = str(info["limit"])
        response.headers[RATE_LIMIT_HEADER_REMAINING] = str(
            max(0, info["limit"] - info["current"])
        )
        response.headers[RATE_LIMIT_HEADER_RESET] = str(info["reset_at"])
        return response

    log.info(
        "Rate limiter registered: ip_limit=%d/min, user_limit=%d/min, bypass=%s",
        limiter.ip_limit,
        limiter.user_limit,
        "enabled" if limiter._internal_token else "disabled",
    )

    return limiter


def _get_authenticated_user_id(request):
    """Extract authenticated user ID from the request context.

    Checks flask.g.user (set by auth middleware) and falls back to
    the Authorization header presence as a signal.

    Returns:
        User ID string or None if unauthenticated.
    """
    user = getattr(flask.g, "user", None)
    if user:
        uid = getattr(user, "id", None) or getattr(user, "user_id", None)
        if uid:
            return str(uid)

    auth_info = getattr(flask.g, "auth_info", None)
    if auth_info:
        uid = auth_info.get("user_id") or auth_info.get("sub")
        if uid:
            return str(uid)

    return None


def get_rate_limit_status(limiter, request):
    """Get current rate limit status for the request caller.

    Args:
        limiter: RateLimiter instance
        request: Flask request object

    Returns:
        Dict with rate limit status for both IP and user (if authenticated)
    """
    ip = _get_client_ip(request)
    ip_usage = limiter.get_current_usage(f"ip:{ip}", limiter.ip_limit)

    result = {
        "ip": ip_usage,
        "authenticated": False,
    }

    user_id = _get_authenticated_user_id(request)
    if user_id:
        user_usage = limiter.get_current_usage(f"user:{user_id}", limiter.user_limit)
        result["user"] = user_usage
        result["authenticated"] = True

    return result
