"""EL-7325 test loader: import ``utils.node_draft`` and ``models.pd.node_draft`` as the plugin package.

``utils/node_draft`` imports its models with ``from ...models.pd.node_draft``, which only resolves under
``plugins.elitea_core``. The plugin is mounted there with empty parent packages (no ``__init__`` runs,
so ``module.py`` and the DB layer are never imported). ``jinja2`` is imported for real: another suite
replaces it in ``sys.modules`` at import time (test_chat_mcp_auth_resume).
"""
import importlib
import pathlib
import sys
import types

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
ROOT = 'plugins.elitea_core'


def _package(name: str, path: pathlib.Path = None) -> None:
    if name in sys.modules and getattr(sys.modules[name], '__path__', None):
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)] if path else []
    sys.modules[name] = module


def load(module: str):
    """``load('utils.node_draft.validate')`` → the module, imported as ``plugins.elitea_core.<module>``."""
    _package('plugins')
    _package(ROOT, PLUGIN_ROOT)
    for sub in ('models', 'models/pd', 'utils'):
        _package(f"{ROOT}.{sub.replace('/', '.')}", PLUGIN_ROOT / sub)
    saved = {n: sys.modules.pop(n) for n in list(sys.modules) if n == 'jinja2' or n.startswith('jinja2.')}
    try:
        importlib.import_module('jinja2')
        return importlib.import_module(f'{ROOT}.{module}')
    finally:
        real = {n: sys.modules.pop(n) for n in list(sys.modules) if n == 'jinja2' or n.startswith('jinja2.')}
        sys.modules.update(saved or real)
