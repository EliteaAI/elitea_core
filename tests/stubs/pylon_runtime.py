"""Reusable Pylon runtime stubs."""
import sys
import types


class _Log:
    """Stub for pylon.core.tools.log."""
    @staticmethod
    def info(*args, **kwargs): pass
    @staticmethod
    def warning(*args, **kwargs): pass
    @staticmethod
    def error(*args, **kwargs): pass
    @staticmethod
    def exception(*args, **kwargs): pass
    @staticmethod
    def debug(*args, **kwargs): pass
    @staticmethod
    def critical(*args, **kwargs): pass


class _Web:
    """Stub for pylon.core.tools.web decorators."""
    def __getattr__(self, name):
        def decorator_factory(*args, **kwargs):
            def decorator(func):
                return func
            return decorator
        return decorator_factory


class _Module:
    """Stub for pylon.core.tools.module."""
    @staticmethod
    def require(*args, **kwargs):
        def decorator(func):
            return func
        return decorator


def install_pylon_stubs():
    """Install minimal Pylon runtime stubs into sys.modules.

    Returns:
        The tools module with log, web, module attributes
    """
    pylon = types.ModuleType("pylon")
    core = types.ModuleType("pylon.core")
    tools = types.ModuleType("pylon.core.tools")

    tools.log = _Log()
    tools.web = _Web()
    tools.module = _Module()

    sys.modules.setdefault("pylon", pylon)
    sys.modules.setdefault("pylon.core", core)
    sys.modules["pylon.core.tools"] = tools

    return tools
