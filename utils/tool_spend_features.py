""" Tool Spend feature list, derived from toolkit-schema metadata `metadata.tool_spend` (#6980) """

import threading

from pylon.core.tools import log  # pylint: disable=E0611,E0401

# 'llm' is reserved so #6006 can store LLM day/week limits in the feature tables
RESERVED_FEATURES = frozenset({"llm"})
PERIOD_KINDS = ("month",)
DEFAULT_UNIT = "credit"
REQUIRED_KEYS = ("feature", "backend", "configuration_type", "max_units_per_call")

_lock = threading.Lock()
_cache = {"source": None, "map": {}}


def _valid_entry(title, spend):
    if not isinstance(spend, dict) or any(not spend.get(key) for key in REQUIRED_KEYS):
        log.error("tool_spend metadata on toolkit %s is malformed, ignored", title)
        return False
    if spend["feature"] in RESERVED_FEATURES:
        log.error("toolkit %s declares reserved tool_spend feature %s, ignored", title, spend["feature"])
        return False
    caps = spend["max_units_per_call"]
    if not isinstance(caps, dict) or not all(
            isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0
            for value in caps.values()):
        log.error("tool_spend max_units_per_call on toolkit %s is malformed, ignored", title)
        return False
    return True


def build_feature_map(schemas):
    """{feature: {label, backends: {backend: {toolkit_type, configuration_type, unit, max_units_per_call, chat_tools}}}}"""
    features = {}
    for title, schema in (schemas or {}).items():
        spend = ((schema or {}).get("metadata") or {}).get("tool_spend")
        if spend is None or not _valid_entry(title, spend):
            continue
        feature = features.setdefault(spend["feature"], {
            "feature": spend["feature"], "label": spend.get("label") or spend["feature"], "backends": {},
        })
        feature["backends"][spend["backend"]] = {
            "toolkit_type": title,
            "configuration_type": spend["configuration_type"],
            "unit": spend.get("unit") or DEFAULT_UNIT,
            "max_units_per_call": dict(spend["max_units_per_call"]),
            "chat_tools": list(spend.get("chat_tools") or []),
        }
    return features


def feature_map(schemas):
    """Cached map, rebuilt whenever the indexer swaps in a new schema snapshot (identity check)."""
    with _lock:
        if _cache["source"] is not schemas:
            _cache["map"] = build_feature_map(schemas)
            _cache["source"] = schemas
        return _cache["map"]


def backend_spec(features, feature, backend):
    """The backend entry, or None when the feature/backend pair is not declared."""
    return ((features.get(feature) or {}).get("backends") or {}).get(backend)


def feature_defaults(config, feature):
    """`cost_budgets.features.<feature>` block from the plugin config."""
    return (((config or {}).get("cost_budgets") or {}).get("features") or {}).get(feature) or {}


def validate_feature_period(feature, period_kind):
    if not feature or feature in RESERVED_FEATURES:
        raise ValueError(f"Invalid tool spend feature: {feature!r}")
    if period_kind not in PERIOD_KINDS:
        raise ValueError(f"Unsupported period kind: {period_kind!r}")
