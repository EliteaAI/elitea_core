"""Unit tests for the member-facing usage endpoint's pure helpers.

Covers period maths and the non-admin redaction path, which cannot be reached with an
admin token in manual testing.

Run standalone: python3 tests/unit/test_usage_helpers.py
"""

import os
import sys
import types
import unittest


def _load_usage_module():
    """Load api/v2/usage.py with the pylon/tools imports stubbed out."""
    plugin_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    #
    tools_stub = types.ModuleType("tools")
    tools_stub.api_tools = types.SimpleNamespace(
        APIModeHandler=object,
        APIBase=object,
        with_modes=lambda params: params,
        endpoint_metrics=lambda func: func,
    )
    tools_stub.auth = types.SimpleNamespace(
        current_user=lambda: {"id": 1},
        decorators=types.SimpleNamespace(check_api=lambda *a, **kw: (lambda func: func)),
    )
    tools_stub.config = types.SimpleNamespace(DEFAULT_MODE="default", ADMINISTRATION_MODE="administration")
    tools_stub.rpc_tools = types.SimpleNamespace(RpcMixin=object)
    #
    flask_stub = types.ModuleType("flask")
    flask_stub.request = types.SimpleNamespace(args={})
    #
    constants_stub = types.ModuleType("constants")
    constants_stub.PROMPT_LIB_MODE = "prompt_lib"
    #
    # Overwrite rather than setdefault: the shared test harness installs its own "tools"
    # stub without api_tools, and it would otherwise win here
    saved = {name: sys.modules.get(name) for name in ("tools", "flask")}
    sys.modules["tools"] = tools_stub
    sys.modules["flask"] = flask_stub
    #
    sys.path.insert(0, os.path.join(plugin_root, "api", "v2"))
    try:
        import importlib.util  # pylint: disable=C0415
        #
        spec = importlib.util.spec_from_file_location(
            "usage_under_test", os.path.join(plugin_root, "api", "v2", "usage.py"),
        )
        module = importlib.util.module_from_spec(spec)
        #
        # usage.py uses a package-relative import for constants; satisfy it
        pkg = types.ModuleType("_uroot")
        pkg.__path__ = []
        sys.modules.setdefault("_uroot", pkg)
        sys.modules.setdefault("_uroot.utils", types.ModuleType("_uroot.utils"))
        sys.modules.setdefault("_uroot.utils.constants", constants_stub)
        module.__package__ = "_uroot.api.v2"
        sys.modules.setdefault("_uroot.api", types.ModuleType("_uroot.api"))
        sys.modules.setdefault("_uroot.api.v2", types.ModuleType("_uroot.api.v2"))
        #
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


usage = _load_usage_module()


class TestPeriodReset(unittest.TestCase):
    """The tag rolls over monthly, so the reset instant is the 1st of next month UTC."""

    def test_mid_year_period_rolls_to_next_month(self):
        self.assertEqual(usage._period_reset("202607"), "2026-08-01T00:00:00+00:00")

    def test_december_rolls_into_next_year(self):
        self.assertEqual(usage._period_reset("202612"), "2027-01-01T00:00:00+00:00")

    def test_january_stays_in_same_year(self):
        self.assertEqual(usage._period_reset("202701"), "2027-02-01T00:00:00+00:00")

    def test_malformed_period_does_not_raise(self):
        for bad in (None, "", "abc", "2026", "20261"):
            self.assertTrue(usage._period_reset(bad).endswith("+00:00"))


class TestPeriodBounds(unittest.TestCase):
    """Bounds let the chart plot a full month, including the correct last day."""

    def test_31_day_month(self):
        self.assertEqual(usage._period_bounds("202607"), ("2026-07-01", "2026-07-31"))

    def test_30_day_month(self):
        self.assertEqual(usage._period_bounds("202606"), ("2026-06-01", "2026-06-30"))

    def test_february_non_leap(self):
        self.assertEqual(usage._period_bounds("202602"), ("2026-02-01", "2026-02-28"))

    def test_february_leap_year(self):
        self.assertEqual(usage._period_bounds("202802"), ("2028-02-01", "2028-02-29"))

    def test_malformed_period_falls_back_to_current_month(self):
        start, end = usage._period_bounds("nonsense")
        self.assertRegex(start, r"^\d{4}-\d{2}-01$")
        self.assertRegex(end, r"^\d{4}-\d{2}-\d{2}$")


class TestRedaction(unittest.TestCase):
    """Non-admins in team projects must not receive cost figures at all.

    Fields are removed rather than zeroed, so nothing sensitive reaches the browser.
    """

    def _payload(self):
        return {
            "project_id": 25,
            "spend": 123.45,
            "monthly_limit": 100.0,
            "effective_limit": 100.0,
            "remaining": 0.0,
            "currency": "USD",
            "percent_used": 123.45,
            "total_tokens": 5000,
            "api_requests": 12,
            "models": [{"model": "gpt-5", "spend": 1.5, "total_tokens": 10, "api_requests": 2}],
            "daily": [{"date": "2026-07-27", "spend": 1.5, "total_tokens": 10, "api_requests": 2}],
        }

    def test_all_amount_fields_removed(self):
        out = usage._redact(self._payload())
        for field in ("spend", "monthly_limit", "effective_limit", "remaining", "currency"):
            self.assertNotIn(field, out)

    def test_percent_and_tokens_survive(self):
        out = usage._redact(self._payload())
        self.assertEqual(out["percent_used"], 123.45)
        self.assertEqual(out["total_tokens"], 5000)
        self.assertEqual(out["api_requests"], 12)

    def test_nested_model_spend_removed_but_usage_kept(self):
        out = usage._redact(self._payload())
        self.assertNotIn("spend", out["models"][0])
        self.assertEqual(out["models"][0]["total_tokens"], 10)
        self.assertEqual(out["models"][0]["model"], "gpt-5")

    def test_nested_daily_spend_removed_but_date_kept(self):
        out = usage._redact(self._payload())
        self.assertNotIn("spend", out["daily"][0])
        self.assertEqual(out["daily"][0]["date"], "2026-07-27")

    def test_no_dollar_value_survives_anywhere(self):
        # Guards against a future field carrying cost through untouched
        out = usage._redact(self._payload())
        for row in out["models"] + out["daily"]:
            self.assertNotIn("spend", row)

    def test_tolerates_missing_collections(self):
        out = usage._redact({"spend": 1.0})
        self.assertNotIn("spend", out)

    def test_redaction_is_idempotent(self):
        out = usage._redact(usage._redact(self._payload()))
        self.assertNotIn("spend", out)

    def test_display_name_survives_redaction(self):
        # A model's name is not a cost figure; hiding it would leave the table unreadable
        payload = self._payload()
        payload["models"][0]["display_name"] = "GPT-5"
        #
        out = usage._redact(payload)
        self.assertEqual(out["models"][0]["display_name"], "GPT-5")


class TestStripModelPrefixes(unittest.TestCase):
    """LiteLLM reports a resolved name; the registry stores it without prefixes."""

    def test_provider_prefix_removed(self):
        self.assertEqual(
            usage._strip_model_prefixes("bedrock/eu.anthropic.claude-sonnet-4-6"),
            "eu.anthropic.claude-sonnet-4-6",
        )

    def test_multi_segment_provider_path_keeps_only_the_model(self):
        self.assertEqual(
            usage._strip_model_prefixes("bedrock/converse/eu.anthropic.claude-sonnet-4-5"),
            "eu.anthropic.claude-sonnet-4-5",
        )

    def test_project_id_prefix_removed(self):
        self.assertEqual(usage._strip_model_prefixes("1_gpt-5"), "gpt-5")

    def test_both_prefixes_removed(self):
        self.assertEqual(usage._strip_model_prefixes("azure/1_gpt-5"), "gpt-5")

    def test_bare_name_unchanged(self):
        self.assertEqual(usage._strip_model_prefixes("gpt-5.4-mini"), "gpt-5.4-mini")

    def test_version_suffix_is_not_mistaken_for_a_prefix(self):
        # The trailing ":0" and digits inside the name must survive
        self.assertEqual(
            usage._strip_model_prefixes("bedrock/eu.anthropic.claude-haiku-4-5-20251001-v1:0"),
            "eu.anthropic.claude-haiku-4-5-20251001-v1:0",
        )


class TestAttachDisplayNames(unittest.TestCase):
    """Rows resolve to configured names, matching what the analytics pages show."""

    def _rows(self):
        return [
            {"model": "gpt-5", "spend": 1.0},
            {"model": "bedrock/eu.anthropic.claude-sonnet-4-6", "spend": 2.0},
            {"model": "never-registered", "spend": 3.0},
        ]

    def _names(self):
        return {
            "gpt-5": "GPT-5",
            "eu.anthropic.claude-sonnet-4-6": "Anthropic Claude 4.6 Sonnet",
        }

    def test_exact_match_resolves(self):
        out = usage._attach_display_names(self._rows(), self._names())
        self.assertEqual(out[0]["display_name"], "GPT-5")

    def test_prefixed_row_resolves_after_normalisation(self):
        out = usage._attach_display_names(self._rows(), self._names())
        self.assertEqual(out[1]["display_name"], "Anthropic Claude 4.6 Sonnet")

    def test_unresolved_row_gets_no_display_name(self):
        # The client formats the raw name itself, so a wrong guess here is worse than none
        out = usage._attach_display_names(self._rows(), self._names())
        self.assertNotIn("display_name", out[2])

    def test_exact_match_wins_over_normalised(self):
        # A model registered under its full prefixed name must not be resolved to another
        rows = [{"model": "bedrock/gpt-5"}]
        names = {"bedrock/gpt-5": "Prefixed registration", "gpt-5": "Bare registration"}
        #
        out = usage._attach_display_names(rows, names)
        self.assertEqual(out[0]["display_name"], "Prefixed registration")

    def test_empty_map_leaves_rows_untouched(self):
        out = usage._attach_display_names(self._rows(), {})
        for row in out:
            self.assertNotIn("display_name", row)

    def test_spend_and_counts_are_not_altered(self):
        out = usage._attach_display_names(self._rows(), self._names())
        self.assertEqual([row["spend"] for row in out], [1.0, 2.0, 3.0])

    def test_missing_model_key_does_not_raise(self):
        out = usage._attach_display_names([{"spend": 1.0}], self._names())
        self.assertNotIn("display_name", out[0])

    def test_empty_rows(self):
        self.assertEqual(usage._attach_display_names([], self._names()), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
