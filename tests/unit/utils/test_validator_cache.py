"""Unit tests for the hash-keyed toolkit validator result cache.

The indexer validator is a pure pydantic parse of (toolkit type, expanded settings, SDK schema),
so the cache is content-addressed: any change to any input must produce a different key, and
invalidation needs no write-path hooks. These tests pin that contract plus LRU/TTL behavior.

Run standalone: python3 tests/unit/utils/test_validator_cache.py
"""

import importlib.util
import os
import unittest


def _load_module():
    plugin_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    spec = importlib.util.spec_from_file_location(
        "validator_cache", os.path.join(plugin_root, "utils", "validator_cache.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


vc = _load_module()

SCHEMA = {"title": "artifact", "properties": {"bucket": {"type": "string"}}}
SETTINGS = {"bucket": "reminder-queue", "selected_tools": ["list_files", "read_file"]}


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class TestCacheKey(unittest.TestCase):
    def test_same_inputs_same_key(self):
        a = vc.make_validator_cache_key("artifact", SETTINGS, SCHEMA)
        b = vc.make_validator_cache_key("artifact", dict(SETTINGS), dict(SCHEMA))
        self.assertEqual(a, b)

    def test_dict_key_order_does_not_matter(self):
        reordered = {"selected_tools": ["list_files", "read_file"], "bucket": "reminder-queue"}
        self.assertEqual(
            vc.make_validator_cache_key("artifact", SETTINGS, SCHEMA),
            vc.make_validator_cache_key("artifact", reordered, SCHEMA),
        )

    def test_each_input_changes_key(self):
        base = vc.make_validator_cache_key("artifact", SETTINGS, SCHEMA)
        # A settings edit, a rotated secret value after expansion, and an SDK schema upgrade
        # must each miss — that is what makes invalidation automatic.
        self.assertNotEqual(base, vc.make_validator_cache_key("github", SETTINGS, SCHEMA))
        self.assertNotEqual(base, vc.make_validator_cache_key("artifact", {**SETTINGS, "bucket": "other"}, SCHEMA))
        self.assertNotEqual(base, vc.make_validator_cache_key("artifact", {**SETTINGS, "token": "rotated"}, SCHEMA))
        new_schema = {**SCHEMA, "properties": {**SCHEMA["properties"], "extra": {"type": "integer"}}}
        self.assertNotEqual(base, vc.make_validator_cache_key("artifact", SETTINGS, new_schema))

    def test_list_order_matters(self):
        swapped = {**SETTINGS, "selected_tools": ["read_file", "list_files"]}
        self.assertNotEqual(
            vc.make_validator_cache_key("artifact", SETTINGS, SCHEMA),
            vc.make_validator_cache_key("artifact", swapped, SCHEMA),
        )

    def test_unserializable_settings_are_not_cached(self):
        self.assertIsNone(vc.make_validator_cache_key("artifact", {"x": object()}, SCHEMA))

    def test_oversized_payload_is_not_cached(self):
        # Entry count alone is a weak memory bound: one toolkit with a huge inline spec would
        # dominate the cache, so anything over the cap bypasses it entirely.
        big = {"spec": "x" * vc.MAX_CACHEABLE_BYTES}
        self.assertIsNone(vc.make_validator_cache_key("openapi", big, SCHEMA))

    def test_payload_at_the_cap_is_still_cached(self):
        overhead = len(vc.json.dumps(["openapi", {"spec": ""}, SCHEMA], sort_keys=True, separators=(",", ":")))
        fits = {"spec": "x" * (vc.MAX_CACHEABLE_BYTES - overhead)}
        self.assertIsNotNone(vc.make_validator_cache_key("openapi", fits, SCHEMA))
        over = {"spec": "x" * (vc.MAX_CACHEABLE_BYTES - overhead + 1)}
        self.assertIsNone(vc.make_validator_cache_key("openapi", over, SCHEMA))


class TestValidatorResultCache(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.cache = vc.ValidatorResultCache(max_entries=2, ttl_seconds=60, clock=self.clock)

    def test_miss_then_hit(self):
        self.assertIsNone(self.cache.get("k"))
        self.cache.put("k", {"bucket": "a"})
        self.assertEqual(self.cache.get("k"), {"bucket": "a"})

    def test_none_key_is_never_stored(self):
        self.cache.put(None, {"bucket": "a"})
        self.assertEqual(len(self.cache), 0)
        self.assertIsNone(self.cache.get(None))

    def test_ttl_expiry(self):
        self.cache.put("k", {"bucket": "a"})
        self.clock.now += 59
        self.assertIsNotNone(self.cache.get("k"))
        self.clock.now += 2
        self.assertIsNone(self.cache.get("k"))
        self.assertEqual(len(self.cache), 0)

    def test_lru_eviction_keeps_recently_used(self):
        self.cache.put("a", {"v": 1})
        self.cache.put("b", {"v": 2})
        self.cache.get("a")  # 'b' becomes least recently used
        self.cache.put("c", {"v": 3})
        self.assertIsNotNone(self.cache.get("a"))
        self.assertIsNone(self.cache.get("b"))
        self.assertIsNotNone(self.cache.get("c"))

    def test_clear_drops_all_entries(self):
        self.cache.put("a", {"v": 1})
        self.cache.put("b", {"v": 2})
        self.cache.clear()
        self.assertEqual(len(self.cache), 0)
        self.assertIsNone(self.cache.get("a"))
        self.cache.put("c", {"v": 3})  # still usable after a clear
        self.assertEqual(self.cache.get("c"), {"v": 3})

    def test_returned_results_are_isolated_copies(self):
        # Callers assign the result into pydantic values and may mutate it; that must not
        # leak into what the next predict receives.
        original = {"selected_tools": ["a"]}
        self.cache.put("k", original)
        original["selected_tools"].append("mutated-before")
        got = self.cache.get("k")
        got["selected_tools"].append("mutated-after")
        self.assertEqual(self.cache.get("k"), {"selected_tools": ["a"]})


if __name__ == "__main__":
    unittest.main()
