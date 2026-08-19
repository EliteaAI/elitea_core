"""Editor notes reach the exported Markdown.

``_application_to_md`` builds its frontmatter from an explicit key allow-list,
and a field missing from that list is dropped silently — which is how editor
notes came to be absent from every exported agent.
"""

import importlib.util
import pathlib
import sys
import types

import pytest
import yaml

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

NOTES_TEXT = 'Reviewed by QA.\nSecond line.'


def _module(name, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


def _package(name):
    mod = types.ModuleType(name)
    mod.__path__ = []
    return mod


def _load(dotted_name, relative_path):
    spec = importlib.util.spec_from_file_location(
        dotted_name, PLUGIN_ROOT / relative_path,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[dotted_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def export_import():
    saved = {}

    def install(name, module):
        if name not in saved:
            saved[name] = sys.modules.get(name)
        sys.modules[name] = module

    noop = lambda *a, **k: None  # noqa: E731
    log = types.SimpleNamespace(
        info=noop, error=noop, warning=noop, debug=noop, exception=noop,
    )
    placeholder = type('Placeholder', (), {})

    for pkg in (
        'plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
        'plugins.elitea_core.models.pd', 'plugins.elitea_core.utils',
    ):
        install(pkg, _package(pkg))

    install('pylon', _package('pylon'))
    install('pylon.core', _package('pylon.core'))
    install('pylon.core.tools', _module('pylon.core.tools', log=log))
    install('tools', _module(
        'tools',
        db=types.SimpleNamespace(get_session=noop),
        rpc_tools=types.SimpleNamespace(RpcMixin=placeholder),
        serialize=lambda value: value,
    ))

    for name, attrs in {
        'plugins.elitea_core.models.all': {
            'Application': placeholder, 'ApplicationVersion': placeholder,
        },
        'plugins.elitea_core.models.elitea_tools': {'EliteATool': placeholder},
        'plugins.elitea_core.models.skill': {
            'Skill': placeholder, 'SkillVersion': placeholder,
        },
        'plugins.elitea_core.models.pd.application': {
            'ApplicationExportModel': placeholder,
        },
        'plugins.elitea_core.models.pd.export_import': {
            'ApplicationForkModel': placeholder,
        },
        'plugins.elitea_core.models.pd.tool': {
            'ToolExportDetails': placeholder, 'ToolForkDetails': placeholder,
        },
        'plugins.elitea_core.models.pd.skill': {'SkillExportModel': placeholder},
    }.items():
        install(name, _module(name, **attrs))

    _load(
        'plugins.elitea_core.utils.export_import_utils',
        'utils/export_import_utils.py',
    )
    module = _load(
        'plugins.elitea_core.utils.export_import', 'utils/export_import.py',
    )

    yield module

    for name, original in saved.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original


def _frontmatter(export_import, **version_overrides):
    version = {
        'name': 'base',
        'agent_type': 'openai',
        'instructions': 'You are a fixture agent.',
        'llm_settings': {'model_name': 'gpt-4o'},
        'meta': {'step_limit': 25},
        **version_overrides,
    }
    app = {'name': 'fixture-agent', 'description': 'fixture', 'versions': [version]}

    markdown = export_import._application_to_md(app, toolkits=[], version=version)

    return yaml.safe_load(markdown.split('---')[1])


def test_notes_exported_when_set(export_import):
    frontmatter = _frontmatter(export_import, notes=NOTES_TEXT)

    assert frontmatter['notes'] == NOTES_TEXT
