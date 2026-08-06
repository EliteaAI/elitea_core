"""Issue #6125 — publishing must not re-validate the toolkits it only re-links.

``clone_version`` snapshots an already-persisted version. It used to feed that
snapshot through ``ApplicationVersionCreateModel``, whose ``ToolCreateModel``
re-runs the creation-time toolkit validators, so an agent whose toolkit had
drifted out of spec (schema change, removed credential, renamed tool) failed
publish with an opaque "Can't clone version". Nothing was protected: the clone
only re-links existing toolkit rows, and the published copy has its toolkits
stripped.

This suite pins the asymmetry — the clone model accepts a tool payload the
create model rejects — and the boundary, that the create/save path still
rejects it. Merging ``ApplicationVersionCloneModel`` back into its parent, or
pointing its ``tools`` field at ``ToolCreateModel``, breaks these.

``test_clone_model_accepts_a_well_formed_version`` is the one case that cannot
fail on settings content, since the clone never runs the validator. It covers
the rest of the model rejecting a well-formed version.

Run via:
    python tests/run_tests.py integration/test_6125_clone_skips_toolkit_validation.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest
from pydantic import ValidationError

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

STALE_SETTINGS = {
    'active_branch': 'main',
    'github_configuration': {'private': True, 'elitea_title': 'gh-creds'},
}

VALID_SETTINGS = {**STALE_SETTINGS, 'repository': 'EliteaAI/elitea_core'}


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


class _ValidatorSpy:
    """Stands in for ``this.module.toolkit_settings_validator``.

    Rejects settings missing ``repository`` the way the real validator rejects a
    required field, and records every call so a test can assert the clone path
    never reaches it.
    """

    def __init__(self):
        self.calls = []

    def __call__(self, settings, type_, project_id, user_id):
        self.calls.append((type_, dict(settings)))
        if 'repository' in settings:
            return {'ok': True}
        return {'ok': False, 'error': [{
            'type': 'value_error',
            'loc': ('repository',),
            'input': None,
            'ctx': {'error': ValueError('Field required')},
        }]}


@pytest.fixture(scope='module')
def pd_models():
    """Load models.pd.tool + models.pd.version against stubbed runtime deps.

    Everything below the import line is stubbed; the toolkit validator entry
    point is the only behaviour under test.
    """
    saved = {}

    def install(name, module):
        if name not in saved:
            saved[name] = sys.modules.get(name)
        sys.modules[name] = module

    noop = lambda *a, **k: None  # noqa: E731
    log = types.SimpleNamespace(
        info=noop, error=noop, warning=noop, debug=noop, exception=noop,
    )
    spy = _ValidatorSpy()

    for pkg in (
        'plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
        'plugins.elitea_core.models.pd', 'plugins.elitea_core.models.enums',
        'plugins.elitea_core.utils',
    ):
        install(pkg, _package(pkg))

    install('pylon', _package('pylon'))
    install('pylon.core', _package('pylon.core'))
    install('pylon.core.tools', _module('pylon.core.tools', log=log))
    install('tools', _module(
        'tools',
        auth=types.SimpleNamespace(current_user=lambda: {'id': 1}),
        this=types.SimpleNamespace(
            module=types.SimpleNamespace(toolkit_settings_validator=spy),
        ),
        serialize=lambda value: value,
        db=types.SimpleNamespace(get_session=noop),
    ))

    class _ExpandError(Exception):
        def __init__(self, errors):
            super().__init__(errors)
            self.errors = errors

    def raise_validation_error_if_any(errors, model):
        raise ValidationError.from_exception_data(model.__name__, errors)

    stubs = {
        'plugins.elitea_core.models.all': {
            'Application': type('Application', (), {}),
            'ApplicationVersion': type('ApplicationVersion', (), {}),
        },
        'plugins.elitea_core.utils.authors': {'get_authors_data': lambda **k: []},
        'plugins.elitea_core.utils.toolkits_utils': {'get_mcp_schemas': lambda *a, **k: {}},
        'plugins.elitea_core.utils.pipeline_utils': {'validate_yaml_from_str': noop},
        'plugins.elitea_core.utils.application_tools': {
            'expand_toolkit_settings': lambda type_, settings, project_id, user_id: settings,
            'ValidatorNotSupportedError': type('ValidatorNotSupportedError', (Exception,), {}),
            'ConfigurationExpandError': _ExpandError,
            'raise_validation_error_if_any': raise_validation_error_if_any,
            'find_suggested_toolkit_name_field': lambda **k: None,
            'find_suggested_toolkit_max_length': lambda **k: None,
        },
    }
    for name, attrs in stubs.items():
        install(name, _module(name, **attrs))

    from pydantic import BaseModel, ConfigDict

    class _Permissive(BaseModel):
        model_config = ConfigDict(extra='allow', from_attributes=True)

    class _Named(_Permissive):
        name: str

    install('plugins.elitea_core.models.pd.collection_base', _module(
        'plugins.elitea_core.models.pd.collection_base',
        TagBaseModel=_Named, AuthorBaseModel=_Permissive,
        PromptTagUpdateModel=_Permissive,
    ))
    install('plugins.elitea_core.models.pd.llm', _module(
        'plugins.elitea_core.models.pd.llm',
        LLMSettingsModel=_Permissive, LLMSettingsWriteModel=_Permissive,
    ))
    install('plugins.elitea_core.models.pd.tag', _module(
        'plugins.elitea_core.models.pd.tag', TagDetailModel=_Permissive,
    ))

    _load('plugins.elitea_core.models.enums.all', 'models/enums/all.py')
    _load('plugins.elitea_core.models.pd.tool', 'models/pd/tool.py')
    version = _load('plugins.elitea_core.models.pd.version', 'models/pd/version.py')
    version.validator_spy = spy

    yield version

    for name, previous in saved.items():
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


def _payload(settings):
    return {
        'name': '1.0.0',
        'author_id': 3,
        'project_id': 2,
        'user_id': 3,
        'tools': [{
            'id': 46,
            'type': 'github',
            'name': 'Github',
            'settings': dict(settings),
            'meta': {},
        }],
    }


def test_create_model_rejects_stale_toolkit(pd_models):
    with pytest.raises(ValidationError):
        pd_models.ApplicationVersionCreateModel.model_validate(_payload(STALE_SETTINGS))


def test_clone_model_accepts_stale_toolkit(pd_models):
    version = pd_models.ApplicationVersionCloneModel.model_validate(
        _payload(STALE_SETTINGS),
    )

    assert version.tools[0].settings == STALE_SETTINGS


def test_clone_model_never_calls_the_toolkit_validator(pd_models):
    pd_models.validator_spy.calls.clear()

    pd_models.ApplicationVersionCloneModel.model_validate(_payload(STALE_SETTINGS))

    assert pd_models.validator_spy.calls == []


def test_create_model_accepts_valid_toolkit(pd_models):
    version = pd_models.ApplicationVersionCreateModel.model_validate(
        _payload(VALID_SETTINGS),
    )

    assert version.tools[0].settings == VALID_SETTINGS


def test_clone_model_accepts_a_well_formed_version(pd_models):
    version = pd_models.ApplicationVersionCloneModel.model_validate(
        _payload(VALID_SETTINGS),
    )

    assert version.tools[0].settings == VALID_SETTINGS


def test_clone_preserves_the_persisted_toolkit_row_id(pd_models):
    version = pd_models.ApplicationVersionCloneModel.model_validate(
        _payload(STALE_SETTINGS),
    )

    assert version.tools[0].id == 46


def test_clone_tools_field_is_not_the_validating_model(pd_models):
    from plugins.elitea_core.models.pd.tool import ToolCopyModel, ToolCreateModel

    assert pd_models.ApplicationVersionCloneModel.model_fields['tools'].annotation \
        is not pd_models.ApplicationVersionCreateModel.model_fields['tools'].annotation
    assert not issubclass(ToolCopyModel, ToolCreateModel)
