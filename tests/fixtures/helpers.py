"""Test helper functions.

Usage:
    from fixtures.helpers import load_module_with_stubs, load_utils_module
"""
import importlib.util
import pathlib
import sys
from typing import Any, Dict, Optional


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
