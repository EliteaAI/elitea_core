""" Owner binding for toolkit-test stream rooms (stream_id -> user_id) """

# Events whose rooms are bound to the user who first claims the stream_id
OWNED_EVENTS = frozenset({'test_toolkit_tool', 'test_mcp_connection'})
# Long enough to outlive any test run; the owner's own claims refresh it
STREAM_OWNER_TTL = 86400


def _key(stream_id) -> str:
    return f'elitea:toolkit_test_stream_owner:{stream_id}'


def claim_stream(client, stream_id, user_id) -> bool:
    """ First claimer owns the stream; True only for that user """
    if not user_id:
        return False
    key = _key(stream_id)
    if client.set(key, str(user_id), nx=True, ex=STREAM_OWNER_TTL):
        return True
    owner = client.get(key)
    if isinstance(owner, bytes):
        owner = owner.decode()
    if owner != str(user_id):
        return False
    client.expire(key, STREAM_OWNER_TTL)
    return True
