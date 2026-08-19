"""Issue #6071 — the publish icon gate must fire on every "no icon" state.

Resetting an entity to the default icon is sent as blank name/url, which used
to be stored verbatim as ``{'name': '', 'url': '', ...}``. Both icon checkers
tested truthiness of the dict alone, so that blank-but-present dict read as "has
an icon" and the gate went silent after an add-then-remove. This suite pins the
three equivalent empty shapes (absent key, ``{}``, blank fields) against the one
shape that counts, plus the write-side normalization that keeps new rows in the
``{}`` form.

Run via:
    python tests/run_tests.py integration/test_6071_publish_icon_check.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

NO_ICON_SHAPES = [
    pytest.param(None, id='key-absent'),
    pytest.param({}, id='empty-dict'),
    pytest.param(
        {'url': '', 'name': '', 'size': '',
         'initial_file_size': '', 'resulting_file_size': ''},
        id='reset-to-default',
    ),
    pytest.param({'url': '   ', 'name': 'x.png'}, id='whitespace-url'),
    pytest.param({'name': 'x.png'}, id='name-without-url'),
    pytest.param({'url': None, 'name': None}, id='null-fields'),
    pytest.param('not-a-dict', id='non-dict'),
]

CUSTOM_ICON = {
    'url': 'http://localhost/app/default_entity_icons/image_12.png',
    'name': 'image_12.png',
    'size': '', 'initial_file_size': '', 'resulting_file_size': '',
}


def _module(name, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


def _load(module_name, filename):
    spec = importlib.util.spec_from_file_location(
        f'plugins.elitea_core.{module_name}', PLUGIN_ROOT / filename,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def pu():
    """publish_utils with its runtime/model deps stubbed — the checkers and
    has_custom_icon are pure, so nothing below the import line is exercised."""
    noop = lambda *a, **k: None  # noqa: E731
    log = types.SimpleNamespace(info=noop, error=noop, warning=noop, debug=noop)

    sys.modules.setdefault('pylon', _module('pylon'))
    sys.modules.setdefault('pylon.core', _module('pylon.core'))
    sys.modules.setdefault('pylon.core.tools', _module('pylon.core.tools', log=log))
    if not hasattr(sys.modules['pylon.core.tools'], 'log'):
        sys.modules['pylon.core.tools'].log = log
    sys.modules.setdefault('tools', _module(
        'tools',
        db=types.SimpleNamespace(get_session=None),
        this=types.SimpleNamespace(module=None, descriptor=None),
        rpc_tools=types.SimpleNamespace(RpcMixin=object),
    ))

    col_model = lambda cls_name: type(cls_name, (), {})  # noqa: E731
    stubs = {
        'plugins.elitea_core.models.all': {
            'Application': col_model('Application'),
            'ApplicationVersion': col_model('ApplicationVersion'),
        },
        'plugins.elitea_core.models.elitea_tools': {
            'EliteATool': col_model('EliteATool'),
            'EntityToolMapping': col_model('EntityToolMapping'),
        },
        'plugins.elitea_core.models.enums.all': {
            'AgentTypes': types.SimpleNamespace(pipeline=types.SimpleNamespace(value='pipeline')),
            'NotificationEventTypes': object,
            'PublishStatus': types.SimpleNamespace(
                draft=types.SimpleNamespace(value='draft'),
                published=types.SimpleNamespace(value='published'),
                embedded=types.SimpleNamespace(value='embedded'),
            ),
            'SkillEntityTypes': types.SimpleNamespace(agent='agent'),
            'ToolEntityTypes': types.SimpleNamespace(agent='agent'),
        },
        'plugins.elitea_core.models.pd.application': {'ApplicationImportModel': object},
        'plugins.elitea_core.models.pd.version': {'ApplicationVersionForkCreateModel': object},
        'plugins.elitea_core.models.pd.publish': {'PublishAIResult': object},
        'plugins.elitea_core.models.skill': {
            'EntitySkillMapping': col_model('EntitySkillMapping'),
            'Skill': col_model('Skill'),
            'SkillVersion': col_model('SkillVersion'),
        },
        'plugins.elitea_core.utils.create_utils': {'create_application': noop, 'create_version': noop},
        'plugins.elitea_core.utils.llm_judge': {'run_llm_judge': noop},
        'plugins.elitea_core.utils.utils': {'get_public_project_id': lambda: 1},
        'plugins.elitea_core.utils.category_utils': {
            'apply_category_to_tag_dicts': lambda tags, cat: tags,
            'is_valid_category': lambda name: True,
        },
        'plugins.elitea_core.utils.application_utils': {'build_skill_mappings_list': lambda ms: list(ms)},
        'plugins.elitea_core.utils.skill_export_import': {'build_skill_fork_payload': noop},
        'plugins.elitea_core.utils.skill_utils': {'attach_skill_to_public_copy': noop},
    }
    for modname, attrs in stubs.items():
        sys.modules[modname] = _module(modname, **attrs)

    # Earlier tests in the session may have replaced sqlalchemy.orm / tools with
    # stubs missing names publish_utils imports at load time.
    sqla_orm = sys.modules.get('sqlalchemy.orm')
    if sqla_orm is not None and not hasattr(sqla_orm, 'selectinload'):
        sqla_orm.selectinload = lambda *a, **k: None
    tools_mod = sys.modules.get('tools')
    if tools_mod is not None:
        for attr, default in (
            ('db', types.SimpleNamespace(get_session=None)),
            ('this', types.SimpleNamespace(module=None, descriptor=None)),
            ('rpc_tools', types.SimpleNamespace(RpcMixin=object)),
        ):
            if not hasattr(tools_mod, attr):
                setattr(tools_mod, attr, default)

    return _load('utils.publish_utils', 'utils/publish_utils.py')


@pytest.fixture(scope='module')
def spu(pu):
    """skill_publish_utils — depends on the stubs pu installed."""
    version_pattern = r'^[a-zA-Z0-9._-]+$'
    stubs = {
        'plugins.elitea_core.models.pd.collection_base': {'TagBaseModel': object},
        'plugins.elitea_core.models.pd.publish': {
            'PublishAIResult': object, 'VERSION_NAME_PATTERN': version_pattern,
        },
        'plugins.elitea_core.models.pd.skill_publish': {'SkillPublishAIResult': object},
        'plugins.elitea_core.models.pd.skill_version': {'SkillVersionCreateModel': object},
        'plugins.elitea_core.utils.constants': {'DEFAULT_FALLBACK_CATEGORY': 'Other'},
        'plugins.elitea_core.utils.skill_category_utils': {
            'apply_skill_category_to_tag_dicts': lambda tags, cat: tags,
            'get_active_skill_categories': lambda: ['Other'],
            'validate_skill_category': lambda name: True,
        },
    }
    for modname, attrs in stubs.items():
        sys.modules[modname] = _module(modname, **attrs)
    sys.modules['plugins.elitea_core.models.all'].Tag = type('Tag', (), {})
    return _load('utils.skill_publish_utils', 'utils/skill_publish_utils.py')


@pytest.fixture(scope='module')
def update_icon():
    """models/pd/icon_meta.py imports only pydantic — load it as-is."""
    return _load('models.pd.icon_meta', 'models/pd/icon_meta.py').UpdateIcon


class TestHasCustomIcon:
    @pytest.mark.parametrize('icon_meta', NO_ICON_SHAPES)
    def test_empty_shapes_are_not_a_custom_icon(self, pu, icon_meta):
        assert pu.has_custom_icon(icon_meta) is False

    def test_bound_icon_is_a_custom_icon(self, pu):
        assert pu.has_custom_icon(CUSTOM_ICON) is True


class TestSkillIconChecker:
    """Skills gate publishing on the icon — a miss here ships an icon-less
    skill to the catalog."""

    @pytest.mark.parametrize('icon_meta', NO_ICON_SHAPES)
    def test_missing_icon_is_critical(self, pu, spu, icon_meta):
        result = pu.ValidationResult()
        spu.SkillIconChecker().check({'icon_meta': icon_meta}, result)
        assert [i['field'] for i in result.critical] == ['icon']
        assert result.critical[0]['issue'] == 'No custom icon set'
        assert result.warnings == []

    def test_bound_icon_raises_nothing(self, pu, spu):
        result = pu.ValidationResult()
        spu.SkillIconChecker().check({'icon_meta': CUSTOM_ICON}, result)
        assert result.critical == []
        assert result.warnings == []


class TestAgentIconChecker:
    @pytest.mark.parametrize('icon_meta', NO_ICON_SHAPES)
    def test_missing_icon_is_a_warning(self, pu, icon_meta):
        result = pu.ValidationResult()
        pu.IconChecker().check({'icon_meta': icon_meta}, result)
        assert [i['field'] for i in result.warnings] == ['icon']
        assert result.critical == []

    def test_bound_icon_raises_nothing(self, pu):
        result = pu.ValidationResult()
        pu.IconChecker().check({'icon_meta': CUSTOM_ICON}, result)
        assert result.critical == []
        assert result.warnings == []


class TestUpdateIconNormalization:
    """The write side keeps new rows in the {} form the icon-delete path uses,
    so 'has a custom icon' never depends on which path cleared the icon."""

    def test_reset_to_default_stores_empty_marker(self, update_icon):
        assert update_icon(name='', url='').as_entity_meta() == {}

    def test_whitespace_url_stores_empty_marker(self, update_icon):
        assert update_icon(name='x.png', url='   ').as_entity_meta() == {}

    def test_bound_icon_stores_full_meta(self, update_icon):
        meta = update_icon(name='image_12.png', url=CUSTOM_ICON['url']).as_entity_meta()
        assert meta['url'] == CUSTOM_ICON['url']
        assert meta['name'] == 'image_12.png'

    def test_stored_shapes_agree_with_the_checker(self, pu, update_icon):
        assert pu.has_custom_icon(update_icon(name='', url='').as_entity_meta()) is False
        assert pu.has_custom_icon(
            update_icon(name='image_12.png', url=CUSTOM_ICON['url']).as_entity_meta()
        ) is True
