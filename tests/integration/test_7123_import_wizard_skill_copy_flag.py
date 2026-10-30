import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'wizardpkg_7123'


class _SkillImportItem:
    import_uuid = 'skill-uuid'

    def __init__(self, item):
        self._item = item

    @classmethod
    def parse_obj(cls, item):
        return cls(item)

    def dict(self):
        return dict(self._item)

    def map_postponed_ids(self, imported_entity):
        return {}


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


@pytest.fixture
def wizard():
    saved = {name: sys.modules.get(name) for name in ('tools', 'pylon', 'pylon.core', 'pylon.core.tools')}
    calls = []

    class _RpcCall:
        def applications_import_skill(self, model_data, project_id, author_id, **kwargs):
            calls.append(kwargs)
            return {'id': 7, 'name': model_data['name'], 'versions': {'base': 70}}, []

    class _RpcMixin:
        rpc = types.SimpleNamespace(call=_RpcCall())

    for name in (PKG, f'{PKG}.rpc', f'{PKG}.models', f'{PKG}.models.pd', f'{PKG}.utils'):
        _module(name)
    _module(f'{PKG}.models.pd.import_wizard', IMPORT_MODEL_ENTITY_MAPPER={'skills': _SkillImportItem}, DEPRECATED_ENTITIES=())
    _module(f'{PKG}.models.pd.skill', SkillImportResultModel=lambda **kwargs: types.SimpleNamespace(**kwargs))
    _module(f'{PKG}.utils.skill_utils', attach_skill_to_agent=None, SkillError=Exception)
    _module(
        f'{PKG}.utils.export_import_utils',
        ENTITY_IMPORT_MAPPER={'skills': 'applications_import_skill'},
        _wrap_import_error=lambda index, msg: {'index': index, 'msg': msg},
        _wrap_import_result=lambda index, result: {'index': index, **result},
    )
    _module('tools', rpc_tools=types.SimpleNamespace(RpcMixin=_RpcMixin), db=types.SimpleNamespace())
    noop = lambda *args, **kwargs: None  # noqa: E731
    _module('pylon')
    _module('pylon.core')
    _module(
        'pylon.core.tools',
        web=types.SimpleNamespace(rpc=lambda *args, **kwargs: (lambda func: func)),
        log=types.SimpleNamespace(debug=noop, info=noop, warning=noop, error=noop),
    )

    spec = importlib.util.spec_from_file_location(f'{PKG}.rpc.import_wizzard', PLUGIN_ROOT / 'rpc' / 'import_wizzard.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    yield module.RPC(), calls

    for name in [name for name in sys.modules if name.startswith(PKG)]:
        del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


SKILL_ITEM = {'entity': 'skills', 'name': 'sk', 'versions': [{'name': 'base', 'instructions': 'i' * 5000}]}


def test_a_server_copy_reaches_the_skill_import_as_already_saved(wizard):
    rpc, calls = wizard

    rpc.import_wizard([SKILL_ITEM], 2, 7, skill_instructions_already_saved=True)

    assert calls == [{'force_new': False, 'instructions_already_saved': True}]


def test_a_user_import_or_fork_is_validated(wizard):
    rpc, calls = wizard

    rpc.import_wizard([SKILL_ITEM], 2, 7, force_new_skills=True)

    assert calls == [{'force_new': True, 'instructions_already_saved': False}]


@pytest.fixture
def skill_rpc():
    saved = {name: sys.modules.get(name) for name in ('tools', 'pylon', 'pylon.core', 'pylon.core.tools', 'sqlalchemy')}
    calls = []
    pkg = f'{PKG}_skill'
    for name in (pkg, f'{pkg}.rpc', f'{pkg}.models', f'{pkg}.models.enums', f'{pkg}.models.pd', f'{pkg}.utils'):
        _module(name)
    _module(f'{pkg}.models.skill', Skill=None, SkillVersion=None, EntitySkillMapping=None)
    _module(f'{pkg}.models.enums.all', PublishStatus=None, SkillEntityTypes=types.SimpleNamespace(agent='agent'))
    _module(f'{pkg}.models.pd.skill', SkillCreateModel=None)
    _module(f'{pkg}.models.pd.search', MultipleApplicationSearchModel=None)
    _module(f'{pkg}.utils.searches', get_search_options=None)
    _module(f'{pkg}.utils.folder_access', folder_exclusion_clause=None)
    _module(
        f'{pkg}.utils.skill_utils',
        get_skill_details=None, create_skill=None, build_skill_detail=None, attach_skill_to_agent=None,
        detach_skill_from_agent=None, get_available_skills_for_agent=None,
        import_skill=lambda **kwargs: calls.append(kwargs) or types.SimpleNamespace(id=7, reused=False, versions={}),
    )
    _module(
        f'{pkg}.utils.skill_export_import',
        ensure_base_version=lambda versions: versions,
        unpack_embedded_skill_frontmatter=lambda version: version,
    )
    _module(f'{pkg}.utils.skill_publish_utils', get_default_skill_validation_rules=None)
    _module('sqlalchemy', and_=None)
    _module('tools', db=types.SimpleNamespace(), serialize=None)
    noop = lambda *args, **kwargs: None  # noqa: E731
    _module('pylon')
    _module('pylon.core')
    _module(
        'pylon.core.tools',
        web=types.SimpleNamespace(rpc=lambda *args, **kwargs: (lambda func: func)),
        log=types.SimpleNamespace(debug=noop, info=noop, warning=noop, error=noop),
    )

    spec = importlib.util.spec_from_file_location(f'{pkg}.rpc.skill', PLUGIN_ROOT / 'rpc' / 'skill.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    yield module.RPC(), calls

    for name in [name for name in sys.modules if name.startswith(pkg)]:
        del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


@pytest.mark.parametrize('already_saved', [True, False])
def test_the_skill_import_rpc_forwards_the_copy_flag(skill_rpc, already_saved):
    rpc, calls = skill_rpc

    rpc.applications_import_skill(SKILL_ITEM, 2, 7, instructions_already_saved=already_saved)

    assert [call['instructions_already_saved'] for call in calls] == [already_saved]
