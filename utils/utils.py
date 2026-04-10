import hashlib
import hmac
from functools import wraps
from typing import Callable, List, Set, Generator, Optional

from sqlalchemy import distinct, func

from pylon.core.tools import log
from tools import db, SecretString, config as c

from .exceptions import VerifySignatureError

from ..models.all import Tag
from ..models.enums.all import PublishStatus


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