"""
MCP Configuration Utilities

Provides functions to check MCP exposure settings.
Settings are cached at plugin startup for performance.
"""

from tools import this


def is_mcp_exposure_enabled() -> bool:
    """
    Check if MCP exposure is enabled at deployment level.

    Returns cached value from module initialization.
    Default is True for backwards compatibility.

    Returns:
        True if MCP exposure is enabled, False otherwise
    """
    return getattr(this.module, 'mcp_exposure_enabled', True)


def is_mcp_in_menu_enabled() -> bool:
    """
    Check if MCPs menu item should be shown in sidebar.

    Returns cached value from module initialization.
    Default is True for backwards compatibility.

    Returns:
        True if MCPs menu should be visible, False otherwise
    """
    return getattr(this.module, 'mcp_in_menu_enabled', True)
