import importlib.util
import pathlib
import sys
import types


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _module(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module


def test_backup_clone_preserves_version_scoped_payload_and_skill_source(monkeypatch):
    for package in (
        'plugins',
        'plugins.elitea_core',
        'plugins.elitea_core.models',
        'plugins.elitea_core.models.pd',
        'plugins.elitea_core.utils',
    ):
        monkeypatch.setitem(sys.modules, package, _package(package))

    placeholder = type('Placeholder', (), {})
    monkeypatch.setitem(sys.modules, 'tools', _module(
        'tools', serialize=lambda value: value, store_secrets=lambda *args, **kwargs: None,
    ))
    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.models.all', _module(
        'plugins.elitea_core.models.all',
        ApplicationVersion=placeholder,
        Application=placeholder,
        ApplicationVariable=placeholder,
        EliteATool=placeholder,
        EntityToolMapping=placeholder,
        Tag=placeholder,
    ))
    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.models.pd.application', _module(
        'plugins.elitea_core.models.pd.application',
        ApplicationCreateModel=placeholder,
        ApplicationImportModel=placeholder,
    ))

    validated_payload = {}

    class _CloneModel:
        @classmethod
        def model_validate(cls, payload):
            validated_payload.update(payload)
            return types.SimpleNamespace(validated=True)

    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.models.pd.version', _module(
        'plugins.elitea_core.models.pd.version',
        ApplicationVersionBaseCreateModel=placeholder,
        ApplicationVersionCloneModel=_CloneModel,
        ApplicationVersionCreateModel=placeholder,
        TagBaseModel=placeholder,
    ))
    monkeypatch.setitem(sys.modules, 'plugins.elitea_core.utils.skill_utils', _module(
        'plugins.elitea_core.utils.skill_utils', copy_skill_mappings=lambda **kwargs: None,
    ))

    spec = importlib.util.spec_from_file_location(
        'plugins.elitea_core.utils.create_utils',
        PLUGIN_ROOT / 'utils' / 'create_utils.py',
    )
    create_utils = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, create_utils)
    spec.loader.exec_module(create_utils)

    create_call = {}

    def _create_version(version_data, application, session, *, copy_skills_from_version_id):
        create_call.update({
            'version_data': version_data,
            'application': application,
            'session': session,
            'copy_skills_from_version_id': copy_skills_from_version_id,
        })
        return 'backup-version'

    monkeypatch.setattr(create_utils, 'create_version', _create_version)
    source_payload = {
        'name': 'base',
        'shared_id': 9,
        'shared_owner_id': 8,
        'tools': [{'id': 41, 'settings': {'selected_tools': ['run']}}],
        'variables': [{'name': 'region', 'value': 'eu'}],
        'tags': [{'name': 'builder'}],
    }
    source = types.SimpleNamespace(id=101, to_dict=lambda: dict(source_payload))
    application = types.SimpleNamespace(id=7, owner_id=42)
    session = object()

    result = create_utils.clone_persisted_application_version(
        source_version=source,
        application=application,
        new_version_name='mcp-backup-101-timestamp',
        author_id=3,
        session=session,
    )

    assert result == 'backup-version'
    assert validated_payload['tools'] == source_payload['tools']
    assert validated_payload['variables'] == source_payload['variables']
    assert validated_payload['tags'] == source_payload['tags']
    assert validated_payload['shared_id'] is None
    assert validated_payload['shared_owner_id'] is None
    assert create_call['version_data'].validated is True
    assert create_call['application'] is application
    assert create_call['session'] is session
    assert create_call['copy_skills_from_version_id'] == 101
