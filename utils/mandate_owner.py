"""A mandate owner who is no longer active takes their mandates with them (#6947, C-1009).

The guarantee is lazy and lives in admission (every firing and every worker start reads the owner's ``active``
verdict). This is the accelerator: the ``auth_invalidate`` event fired after every revocation bump of a user. Role
changes bump the epoch too and leave the user active, so only an explicit ``active is False`` goes on to suspend
anything. A lost, late or dropped event costs nothing but delay.

The event can arrive before this process's own cached epoch read is evicted, so the verdict is read again only after
the auth cache ttl has passed. Events do not start work of their own: they queue the user once, a single worker
thread reads the verdicts in order, and further events for a user already queued are ignored until the worker takes
that user up.
"""
import queue
import threading
import time

from pylon.core.tools import log
from tools import auth, rpc_tools

from .admission import suspend_owner_in_project
from .governance import is_governed

FALLBACK_EPOCH_TTL_S = 5.0   # the epoch_ttl default of the auth plugin
MAX_PENDING_USERS = 1024


def owner_read_delay() -> float:
    """How long to wait before trusting a verdict: the auth plugin's epoch ttl (when it exposes one) plus a second."""
    ttl = getattr(auth, "epoch_ttl", None)
    if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or ttl < 0:
        ttl = FALLBACK_EPOCH_TTL_S
    return float(ttl) + 1.0


def invalidated_user_id(payload):
    """The user id an ``auth_invalidate`` payload announces, or ``None`` (not a user, or not well formed)."""
    if not isinstance(payload, dict) or payload.get("kind") != "user":
        return None
    user_id = payload.get("id")
    return user_id if isinstance(user_id, int) and not isinstance(user_id, bool) else None


def suspend_inactive_owner(user_id, project_ids=None) -> dict:
    """If ``user_id`` is inactive, suspend their active mandates in every project (one transaction per project).
    ``{project_id: [mandate ids]}`` for what was suspended. An unreadable or unclear verdict changes nothing."""
    epochs = auth.read_identity_epochs("user", user_id)
    if not isinstance(epochs, dict) or epochs.get("active") is not False:
        return {}
    if project_ids is None:
        project_ids = [project["id"] for project in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
            filter_={"create_success": True})]
    suspended = {}
    for project_id in project_ids:
        try:
            ids = suspend_owner_in_project(project_id, user_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("mandate owner: project %s could not be scanned (%s)", project_id, type(exc).__name__)
            continue
        if ids:
            suspended[project_id] = ids
    return suspended


class OwnerScanner:
    """Queue of users whose verdict is to be read, served by one worker thread (started on first use)."""

    def __init__(self, scan=suspend_inactive_owner, delay=owner_read_delay, clock=time.monotonic, sleep=time.sleep,
                 max_pending=MAX_PENDING_USERS, start_worker=True):
        self._scan, self._delay, self._clock, self._sleep = scan, delay, clock, sleep
        self._max_pending, self._start_worker = max_pending, start_worker
        self._lock = threading.Lock()
        self._pending = set()
        self._queue = queue.Queue()
        self._worker = None

    def submit(self, user_id) -> bool:
        """Queue a user. False when they are already queued, or when too many users are (the lazy path covers them)."""
        with self._lock:
            if user_id in self._pending or len(self._pending) >= self._max_pending:
                return False
            self._pending.add(user_id)
            self._queue.put((self._clock() + self._delay(), user_id))
            if self._start_worker and self._worker is None:
                self._worker = threading.Thread(target=self._run, name="mandate-owner-scan", daemon=True)
                self._worker.start()
        return True

    def _run(self):
        while True:
            self.run_one()

    def run_one(self, wait=True) -> bool:
        """Serve the oldest queued user: wait until their time, read the verdict, suspend. False when none is queued
        (``wait=False``: never blocks). Never raises."""
        try:
            due, user_id = self._queue.get() if wait else self._queue.get_nowait()
        except queue.Empty:
            return False
        if wait:
            self._sleep(max(0.0, due - self._clock()))
        with self._lock:
            self._pending.discard(user_id)   # an event from here on is about a newer state: it queues a new read
        try:
            self._scan(user_id)
        except Exception as exc:  # pylint: disable=W0703
            log.error("mandate owner: the suspension scan failed (%s)", type(exc).__name__)
        return True

    def __len__(self):
        return len(self._pending)


_default = None
_default_lock = threading.Lock()


def default_scanner() -> OwnerScanner:
    global _default  # pylint: disable=W0603
    with _default_lock:
        if _default is None:
            _default = OwnerScanner()
        return _default


def on_auth_invalidate(module, payload, scanner=None) -> bool:
    """Handle one ``auth_invalidate`` event: return at once unless governed and about a user; otherwise queue the
    user for a delayed read of their verdict. True when the user was queued (not already waiting)."""
    if not is_governed(module):
        return False
    user_id = invalidated_user_id(payload)
    if user_id is None:
        return False
    return (default_scanner() if scanner is None else scanner).submit(user_id)
