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

    def test_oversized_settings_are_not_cached(self):
        # The stored result mirrors the settings, so the per-entry memory bound is a cap on
        # settings size: one toolkit with a huge inline spec must not crowd out the rest.
        big = {"spec": "x" * vc.MAX_CACHEABLE_SETTINGS_BYTES}
        self.assertIsNone(vc.make_validator_cache_key("openapi", big, SCHEMA))

    def test_settings_at_the_cap_are_still_cached(self):
        overhead = len(vc._dumps({"spec": ""}))
        fits = {"spec": "x" * (vc.MAX_CACHEABLE_SETTINGS_BYTES - overhead)}
        self.assertIsNotNone(vc.make_validator_cache_key("openapi", fits, SCHEMA))
        over = {"spec": "x" * (vc.MAX_CACHEABLE_SETTINGS_BYTES - overhead + 1)}
        self.assertIsNone(vc.make_validator_cache_key("openapi", over, SCHEMA))

    def test_a_large_schema_does_not_count_against_the_cap(self):
        # Real SDK schemas reach ~42KB (github, jira, confluence). They are only hashed, never
        # stored, so they must not stop the most common toolkits from being cached.
        big_schema = {**SCHEMA, "description": "x" * (2 * vc.MAX_CACHEABLE_SETTINGS_BYTES)}
        self.assertIsNotNone(vc.make_validator_cache_key("github", SETTINGS, big_schema))

    def test_fields_cannot_bleed_into_each_other(self):
        # The pieces are joined into one hash input; shifting text between them must not collide.
        self.assertNotEqual(
            vc.make_validator_cache_key("a", {"k": "b"}, {}),
            vc.make_validator_cache_key("ab", {"k": ""}, {}),
        )


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

    def test_total_byte_budget_evicts_least_recently_used(self):
        one_kb = {"blob": "x" * 1000}
        size = vc._result_size(one_kb)
        cache = vc.ValidatorResultCache(max_entries=100, ttl_seconds=60,
                                        max_total_bytes=size * 2, clock=self.clock)
        cache.put("a", one_kb)
        cache.put("b", one_kb)
        cache.get("a")  # 'b' becomes least recently used
        cache.put("c", one_kb)

        self.assertIsNotNone(cache.get("a"))
        self.assertIsNone(cache.get("b"))
        self.assertIsNotNone(cache.get("c"))
        self.assertEqual(cache.total_bytes, size * 2)

    def test_result_larger_than_the_whole_budget_is_not_stored(self):
        cache = vc.ValidatorResultCache(max_total_bytes=100, clock=self.clock)
        cache.put("small", {"v": 1})
        cache.put("huge", {"blob": "x" * 500})

        self.assertIsNone(cache.get("huge"))
        self.assertIsNotNone(cache.get("small"))  # an unstorable entry must not evict others

    def test_byte_accounting_stays_exact_across_replace_expire_and_clear(self):
        # A drifting counter would either evict everything forever or stop bounding memory.
        cache = vc.ValidatorResultCache(ttl_seconds=60, clock=self.clock)
        cache.put("k", {"v": "short"})
        cache.put("k", {"v": "a much longer replacement value"})
        self.assertEqual(cache.total_bytes, vc._result_size({"v": "a much longer replacement value"}))

        self.clock.now += 61
        self.assertIsNone(cache.get("k"))
        self.assertEqual(cache.total_bytes, 0)

        cache.put("a", {"v": 1})
        cache.put("b", {"v": 2})
        cache.clear()
        self.assertEqual(cache.total_bytes, 0)

    def test_count_eviction_also_releases_bytes(self):
        self.cache.put("a", {"v": 1})
        self.cache.put("b", {"v": 2})
        self.cache.put("c", {"v": 3})  # max_entries=2 evicts 'a'
        self.assertEqual(self.cache.total_bytes, vc._result_size({"v": 2}) + vc._result_size({"v": 3}))

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
