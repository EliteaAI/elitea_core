from .parallel_hitl import pending_interrupts
from .toolkit_authorization import pending_authorization_requests


def is_live_run(is_streaming: bool, meta) -> bool:
    return bool(is_streaming or pending_interrupts(meta) or pending_authorization_requests(meta))
