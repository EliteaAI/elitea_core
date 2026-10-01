import hashlib
import json
import threading
import time
from collections import OrderedDict
from copy import deepcopy
from typing import Optional


MAX_CACHEABLE_BYTES = 256 * 1024


def make_validator_cache_key(type_: str, settings: dict, schema: dict) -> Optional[str]:
    # Validator is a pure schema parse, so its output is fully determined by these three inputs
    try:
        payload = json.dumps([type_, settings, schema], sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return None
    if len(payload) > MAX_CACHEABLE_BYTES:
        return None
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ValidatorResultCache:
    # In-process only: cached results hold unsecreted settings, so never move this to Redis/DB
    def __init__(self, max_entries: int = 2000, ttl_seconds: float = 600, clock=time.monotonic):
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self._clock = clock
        self._entries: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: Optional[str]) -> Optional[dict]:
        if key is None:
            return None
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            expires_at, result = entry
            if expires_at <= self._clock():
                del self._entries[key]
                return None
            self._entries.move_to_end(key)
        return deepcopy(result)

    def put(self, key: Optional[str], result: dict) -> None:
        if key is None:
            return
        with self._lock:
            self._entries[key] = (self._clock() + self.ttl_seconds, deepcopy(result))
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)


toolkit_validator_cache = ValidatorResultCache()
