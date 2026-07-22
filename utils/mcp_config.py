"""
MCP Configuration Utilities

Provides functions to check MCP exposure settings.
Settings are read from the current module instance at call time.
"""

from tools import this


def _current_module():
    # this.module resolves via a cached ModuleThis whose .descriptor is set at construction time.
    # After a hot-reload, that cached descriptor still points to the old module instance.
    # Bypass the cache by looking up the current descriptor directly from the module manager.
    try:
        descriptor = this.context.module_manager.descriptors[this.module_name]
        return descriptor.module
    except Exception:  # pylint: disable=W0703
        return None


def is_mcp_exposure_enabled() -> bool:
    """
    Check if MCP exposure is enabled at deployment level.

    Returns:
        True if MCP exposure is enabled, False otherwise
    """
    return getattr(_current_module(), 'mcp_exposure_enabled', True)


def is_mcp_in_menu_enabled() -> bool:
    """
    Check if MCPs menu item should be shown in sidebar.

    Returns:
        True if MCPs menu should be visible, False otherwise
    """
    return getattr(_current_module(), 'mcp_in_menu_enabled', True)
