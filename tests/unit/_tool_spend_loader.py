"""Loads the #6980 Tool Spend modules under their real package names, pylon stubbed by run_tests."""
import importlib.util
import pathlib
import sys
import types

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _package(name):
    if name not in sys.modules:
        pkg = types.ModuleType(name)
        pkg.__path__ = []
        sys.modules[name] = pkg


# Leaf modules first: relative imports resolve through sys.modules, not the filesystem
DEPENDENCIES = ("utils/tool_spend_features.py", "utils/tool_spend_permit.py")


def _ensure_pylon():
    # Some suites swap pylon.core.tools for a narrower stub and leave it in place
    tools = sys.modules.get("pylon.core.tools")
    if tools is None or not hasattr(tools, "web") or not hasattr(tools, "log"):
        for name in ("pylon", "pylon.core"):
            sys.modules.setdefault(name, types.ModuleType(name))
        tools = types.ModuleType("pylon.core.tools")
        tools.web = types.SimpleNamespace(rpc=lambda *a, **k: (lambda f: f), method=lambda *a, **k: (lambda f: f))
        tools.log = types.SimpleNamespace(**{n: (lambda *a, **k: None) for n in
                                             ("info", "warning", "error", "exception", "debug")})
        sys.modules["pylon.core.tools"] = tools


def load(relative):
    """relative like 'utils/tool_spend.py' → plugins.elitea_core.utils.tool_spend"""
    _ensure_pylon()
    if relative not in DEPENDENCIES:
        for dependency in DEPENDENCIES:
            load(dependency)
    for name in ("plugins", "plugins.elitea_core", "plugins.elitea_core.utils", "plugins.elitea_core.rpc"):
        _package(name)
    dotted = "plugins.elitea_core." + relative[:-3].replace("/", ".")
    if dotted in sys.modules:
        return sys.modules[dotted]
    spec = importlib.util.spec_from_file_location(dotted, PLUGIN_ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[dotted] = module
    spec.loader.exec_module(module)
    return module


def load_legacy_ladder():
    """rpc/budget_limits.py through the existing ladder test's loader."""
    spec = importlib.util.spec_from_file_location(
        "ladder_test_6573", PLUGIN_ROOT / "tests" / "unit" / "test_6573_budget_limits_ladder.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
