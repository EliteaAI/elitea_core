"""Test helper functions.

Usage:
    from fixtures.helpers import load_module_with_stubs, load_utils_module
"""
import importlib.util
import pathlib
import sys
import types
from typing import Any, Dict, Optional


def register_index_pd_module(plugin_root: pathlib.Path):
    """Make ``models/pd/index.py`` importable as ``..models.pd.index``, for real.

    ``utils/index_scheduling.py`` imports the schedule-expiration calculator from it, and a
    stub would let the tick's expiry behaviour be tested against a fake window that the
    scheduler and the API never agree on. The file itself only needs pydantic and croniter,
    so loading the genuine module is cheap. Idempotent: several suites load index_scheduling
    in the same interpreter.
    """
    name = 'plugins.elitea_core.models.pd.index'
    if name in sys.modules:
        return sys.modules[name]
    for pkg_name in ('plugins', 'plugins.elitea_core',
                     'plugins.elitea_core.models', 'plugins.elitea_core.models.pd'):
        if pkg_name not in sys.modules:
            pkg = types.ModuleType(pkg_name)
            pkg.__path__ = []
            sys.modules[pkg_name] = pkg
    return load_module_with_stubs(plugin_root / 'models' / 'pd' / 'index.py', name)


def load_module_with_stubs(
    module_path: pathlib.Path,
    module_name: str,
    stubs: Optional[Dict[str, Any]] = None
):
    """Load a module from path with optional stub modules injected.

    Args:
        module_path: Absolute path to the .py file
        module_name: Fully qualified module name
        stubs: Dict mapping module names to stub objects

    Returns:
        The loaded module
    """
    if stubs:
        for name, obj in stubs.items():
            sys.modules[name] = obj

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


def load_utils_module(
    utils_path: pathlib.Path,
    module_name: str,
    stubs: Optional[Dict[str, Any]] = None,
    extra_stubs: Optional[Dict[str, Any]] = None
):
    """Convenience wrapper for loading utils modules.

    Args:
        utils_path: Path to the utils/ directory
        module_name: Module name without path (e.g., 'tool_call_dedup')
        stubs: Optional dict of stub modules (replaces defaults)
        extra_stubs: Additional stubs to merge with defaults

    Returns:
        The loaded module
    """
    final_stubs = stubs or {}
    if extra_stubs:
        final_stubs = {**final_stubs, **extra_stubs}

    return load_module_with_stubs(
        utils_path / f'{module_name}.py',
        f'plugins.elitea_core.utils.{module_name}',
        final_stubs if final_stubs else None
    )
