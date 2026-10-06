""" Owner binding for toolkit-test stream rooms (stream_id -> user_id) """
from uuid import UUID

from pylon.core.tools import log

from .sio_utils import SioValidationError

# Events whose rooms are bound to the user who first claims the stream_id
OWNED_EVENTS = frozenset({'test_toolkit_tool', 'test_mcp_connection'})
# Long enough to outlive any test run; the owner's own claims refresh it
STREAM_OWNER_TTL = 86400

# One round-trip: first claimer wins, the owner re-claiming refreshes the TTL
_CLAIM_SCRIPT = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then return 1 end
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    return 1
end
return 0
"""


def _key(stream_id: UUID) -> str:
    return f'elitea:toolkit_test_stream_owner:{stream_id}'


def claim_stream(client, stream_id, user_id) -> bool:
    """ First claimer owns the stream; True only for that user. Non-UUID ids are never claimed """
    if not user_id:
        return False
    try:
        stream_uuid = UUID(str(stream_id))
    except ValueError:
        return False
    return bool(client.eval(_CLAIM_SCRIPT, 1, _key(stream_uuid), str(user_id), STREAM_OWNER_TTL))


def guard_test_stream(module, sid, sio_event, data: dict) -> None:
    """ Refuse a toolkit-test start on a stream_id another user owns; fails closed if Redis is down """
    if sio_event not in OWNED_EVENTS:
        return  # chat streams are keyed on the shared conversation uuid
    try:
        owned = claim_stream(module.get_redis_client(), data['stream_id'], data.get('user_id'))
        error = None if owned else 'stream_id is not available'
    except Exception as e:  # pylint: disable=W0703
        log.warning("%s: stream owner check failed: %s", sio_event, e)
        error = 'Could not verify the stream, please try again'
    if error:
        raise SioValidationError(
            sio=module.context.sio,
            sid=sid,
            event=sio_event,
            error=error,
            stream_id=data.get('stream_id'),
            message_id=data.get('message_id'),
        )
