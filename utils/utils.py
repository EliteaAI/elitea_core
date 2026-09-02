import hashlib
import hmac
import json
import re
from functools import wraps
from typing import Callable, List, Set, Generator, Optional

from sqlalchemy import distinct, func

from pylon.core.tools import log
from tools import db, context, SecretString, config as c

from .exceptions import VerifySignatureError

from ..models.all import Tag
from ..models.enums.all import PublishStatus

try:
    import gevent  # pylint: disable=C0413
except ImportError:  # pragma: no cover - gevent absent in non-gevent deploys
    gevent = None


def make_yield_to_hub(web_runtime: str) -> Callable[[], None]:
    """Cooperative yield only when gevent is the actual web runtime; no-op under flask/waitress/hypercorn."""
    return (lambda: gevent.sleep(0)) if (gevent is not None and web_runtime == "gevent") else (lambda: None)


def end_ambient_transaction() -> None:
    """Commit the thread-local session so a long loop does not hold one transaction open for its whole duration."""
    from pylon.core.tools import db_support  # pylint: disable=C0415
    #
    db_support.check_local_entities()
    local_session = context.local.db_session
    # Absent or not yet materialised means nothing is open, and going through the
    # proxy here would create a session this caller has no hook to close.
    if local_session is None or isinstance(local_session, db_support.LazyLocalSession):
        return
    #
    try:
        local_session.commit()
    except Exception:  # pylint: disable=W0703
        try:
            local_session.rollback()
        except Exception:  # pylint: disable=W0703
            pass


# Redis cache for public project ID (ai_project_id)
_PUBLIC_PROJECT_ID_CACHE_KEY = "elitea:config:ai_project_id"
_PUBLIC_PROJECT_ID_TTL = 86400  # 24 hours in seconds


def get_public_project_id() -> int:
    """
    Get the public project ID (ai_project_id) with Redis caching.

    Reads from elitea_core plugin config (descriptor.config).
    Cached in Redis for 24 hours since it rarely changes.
    """
    # Try to get from Redis cache first
    redis_client = None
    try:
        from tools import auth
        redis_client = auth.get_cache_redis_client()
        #
        if redis_client:
            cached_value = redis_client.get(_PUBLIC_PROJECT_ID_CACHE_KEY)
            #
            if cached_value is not None:
                return int(cached_value)
    except Exception as e:
        log.debug(f"[AI_PROJECT_CACHE] Redis get failed: {e}")

    # Read from plugin config
    from tools import this  # pylint: disable=C0415,E0401
    project_id = this.descriptor.config.get("ai_project_id", 1)
    project_id_int = int(project_id)

    # Cache in Redis for future requests
    if redis_client:
        try:
            redis_client.setex(_PUBLIC_PROJECT_ID_CACHE_KEY, _PUBLIC_PROJECT_ID_TTL, project_id_int)
        except Exception as e:
            log.debug(f"[AI_PROJECT_CACHE] Redis set failed: {e}")

    return project_id_int


# def get_ai_integration_settings(uid: str, unsecret: bool = True) -> dict:
#     integration = rpc_tools.RpcMixin().rpc.call.integrations_get_by_uid(
#         uid,
#         project_id=_guess_project_id(),
#     )
#     if integration is None:
#         raise ValueError(f'Integration with {uid=} not found')
#     #
#     integration = integration.to_json()
#     project_id = integration.get('project_id')
#     #
#     if unsecret and 'api_token' in integration['settings']:
#         token_value = integration['settings']['api_token']
#         token_field = SecretString(token_value)
#         #
#         try:
#             token = token_field.unsecret(project_id)
#         except AttributeError:
#             token = token_field.unsecret(None)
#         #
#         integration['settings']['api_token'] = token
#     #
#     return integration


def _guess_project_id():
    import inspect  # pylint: disable=C0415
    #
    project_id = None
    frame = None
    #
    try:
        frame = inspect.currentframe()
        #
        while frame:
            local_project_id = inspect.getargvalues(frame).locals.get("project_id", None)
            #
            if local_project_id:
                project_id = local_project_id
                break
            #
            frame = frame.f_back
    finally:
        if frame is not None:
            del frame
    #
    return project_id


def add_public_project_id(f: Callable) -> Callable:
    """Decorator to add public project_id to kwargs using cached lookup."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            public_project_id = get_public_project_id()
        except Exception as e:
            return {'error': f"'ai_project_id' not set: {e}"}, 400

        kwargs.update({'project_id': public_project_id})
        return f(*args, **kwargs)

    return wrapper


def get_entities_by_tags(
    project_id,
    tags: List[int],
    entity_type,
    entity_version_type,
    session=None, subquery=True
):

    Entity = entity_type
    EntityVersion = entity_version_type
    session_created = False
    result = None
    if session is None:
        session = db.get_project_schema_session(project_id)
        session_created = True

    try:
        query = (
            session.query(Entity.id)
            .join(Entity.versions)
            .join(EntityVersion.tags)
            .filter(Tag.id.in_(tags))
            .group_by(Entity.id)
            .having(
                func.count(distinct(Tag.id)) == len(tags)
            )
        )
        if not subquery:
            entities = query.all()
            result = [entity.id for entity in entities]
        else:
            result = query.subquery()

    finally:
        if session_created:
            session.close()

    return result


def determine_entity_status(version_statuses: Set[PublishStatus]) -> PublishStatus:
    """Determine the overall entity status from its version statuses by priority."""
    status_priority = (
        PublishStatus.rejected,
        PublishStatus.on_moderation,
        PublishStatus.published,
        PublishStatus.unpublished,
        PublishStatus.draft,
        PublishStatus.embedded,
        # PublishStatus.user_approval,
    )

    for status in status_priority:
        if status in version_statuses:
            return status


def verify_signature(payload_body, secret_token, signature_header):
    """Verify that the payload was sent from GitHub by validating SHA256.

    Raise VerifySignatureError if not authorized.

    Args:
        payload_body: original request body to verify (request.body())
        secret_token: GitHub app webhook token (WEBHOOK_SECRET)
        signature_header: header received from GitHub (x-hub-signature-256)
    """
    if not signature_header:
        raise VerifySignatureError({'error': f"x-hub-signature-256 header is missing!"})
    # empty str secret_token is allowed
    if secret_token is None:
        raise VerifySignatureError({'error': f"secret token is missing!"})

    hash_object = hmac.new(secret_token.encode('utf-8'), msg=payload_body, digestmod=hashlib.sha256)
    expected_signature = "sha256=" + hash_object.hexdigest()
    if not hmac.compare_digest(expected_signature, signature_header):
        raise VerifySignatureError({'error': f"x-hub-signature-256 signature mismatch!"})


# get_public_project_id() is now defined at the top of this file with Redis caching


def set_columns_as_attrs(q_result, extra_columns: list) -> Generator:
    for i in q_result:
        try:
            entity, *extra_data = i
            for k, v in zip(extra_columns, extra_data):
                setattr(entity, k, v)
        except TypeError:
            entity = i
        yield entity

def mask_secret(secret: str, visible_chars: int = 4) -> str:
    """
    Mask a secret string, showing only the last N characters.

    Args:
        secret: The secret string to mask
        visible_chars: Number of characters to show at the end (default: 4)

    Returns:
        Masked string like '****abcd' or fully masked if shorter than visible_chars
    """
    if not secret:
        return ""
    if len(secret) >= visible_chars:
        return '*' * (len(secret) - visible_chars) + secret[-visible_chars:]
    return '*' * len(secret)

def parse_ids_filter(ids: str | list | None, max_ids: int = 100) -> list[int]:
    """
    Parse and validate an IDs filter parameter.

    Args:
        ids: Comma-separated string or list of IDs
        max_ids: Maximum number of IDs allowed (default 100)

    Returns:
        List of integer IDs, capped at max_ids
    """
    if not ids:
        return []
    if isinstance(ids, str):
        ids = [int(id_str.strip()) for id_str in ids.split(',') if id_str.strip().isdigit()]
    return ids[:max_ids]


_JSON_STRING_ESCAPES = {"\n": "\\n", "\r": "\\r", "\t": "\\t", "\b": "\\b", "\f": "\\f"}
# a model may introduce its answer with prose that itself contains a brace; the object is found by
# trying each in turn, bounded because a Markdown draft can contain a great many
_MAX_OBJECT_STARTS = 5


def _escape_control_characters_in_strings(text: str) -> str:
    """Escape the raw control characters a model leaves inside a JSON string value.

    Asking for a Markdown document inside a JSON string asks the model to escape every newline it
    writes, and the longer the document the likelier it forgets. A character below ``0x20`` is
    illegal there by spec, so escaping one can never change the meaning of a valid document — but
    the quote tracking this relies on can be thrown off by an unescaped quote elsewhere, which is
    why the caller keeps the result only if it parses.
    """
    out = []
    in_string = False
    escaped = False
    for char in text:
        if escaped:
            escaped = False
        elif char == "\\" and in_string:
            escaped = True
        elif char == '"':
            in_string = not in_string
        elif in_string and ord(char) < 0x20:
            out.append(_JSON_STRING_ESCAPES.get(char, f"\\u{ord(char):04x}"))
            continue
        out.append(char)
    return "".join(out)


def _decode_object(body: str) -> tuple[Optional[str], int]:
    """The JSON object at the head of ``body``, with how far the decoder got.

    Returns ``(span, offset it ended at)`` on success and ``(None, furthest offset reached)`` on
    failure. The repair is attempted only after a plain decode fails, and is adopted only when it
    yields something the decoder accepts.

    The reported reach is the furthest of the two attempts. A body that needs the repair *and* was
    truncated breaks early on the plain decode — at its first raw newline — and only the repaired
    attempt reaches the truncation; reporting the plain offset would understate how much of the
    body is real. Over-stating is the safe direction: it only makes the caller's guard stricter.
    """
    try:
        _, end = json.JSONDecoder().raw_decode(body)
        return body[:end], end
    except ValueError as exc:
        broke_at = getattr(exc, "pos", 0)

    repaired = _escape_control_characters_in_strings(body)
    try:
        _, end = json.JSONDecoder().raw_decode(repaired)
        return repaired[:end], end
    except ValueError as exc:
        return None, max(broke_at, getattr(exc, "pos", 0))


def extract_json_from_text(text: str) -> str:
    """Extract a JSON object from text, stripping markdown fences if present.

    The object ends where the decoder says it ends, not at the last brace: a model signing off with
    "use {placeholders} as needed" would otherwise drag that sentence in and fail as ``Extra data``.

    A later brace is tried only when it explains *more* of the text than the first one managed.
    Without that test a truncated draft quietly yields one of its own nested objects — the first
    dimension out of a cut-off list, say — which parses, so the caller never learns the answer was
    truncated and the parse diagnostic never fires.

    Failing everything, the widest span is returned unrepaired so the caller's own parse error still
    describes what the model sent.
    """
    match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if match:
        span, _ = _decode_object(match.group(1))
        return span if span is not None else match.group(1)

    starts = []
    index = text.find("{")
    while index != -1 and len(starts) < _MAX_OBJECT_STARTS:
        starts.append(index)
        index = text.find("{", index + 1)
    if not starts:
        return text

    span, reach = _decode_object(text[starts[0]:])
    if span is not None:
        return span

    unexplained_from = starts[0] + reach
    for start in starts[1:]:
        span, end = _decode_object(text[start:])
        if span is not None and start + end >= unexplained_from:
            return span

    body = text[starts[0]:]
    end = body.rfind("}") + 1
    return body[:end] if end > 0 else body
