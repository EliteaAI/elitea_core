"""#7233 - an agent whose skills exceed the target project's limit gets one clear import error."""
import contextlib
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'wizardpkg_7233'
MAX_ALLOWED = 10


class SkillError(Exception):
    pass


class SkillLimitExceededError(SkillError):
    def __init__(self, max_allowed):
        super().__init__(f'Maximum allowed is {max_allowed}.')
        self.max_allowed = max_allowed


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


@pytest.fixture
def wizard():
    saved = {name: sys.modules.get(name) for name in ('tools', 'pylon', 'pylon.core', 'pylon.core.tools')}
    attached = []

    def attach_skill_to_agent(project_id, entity_version_id, skill_id, skill_version_id, session):
        if len(attached) >= MAX_ALLOWED:
            raise SkillLimitExceededError(MAX_ALLOWED)
        attached.append(skill_id)

    for name in (PKG, f'{PKG}.rpc', f'{PKG}.models', f'{PKG}.models.pd', f'{PKG}.utils'):
        _module(name)
    _module(f'{PKG}.models.pd.import_wizard', IMPORT_MODEL_ENTITY_MAPPER={}, DEPRECATED_ENTITIES=())
    _module(f'{PKG}.models.pd.skill', SkillImportResultModel=None)
    _module(
        f'{PKG}.utils.skill_utils', attach_skill_to_agent=attach_skill_to_agent,
        SkillError=SkillError, SkillLimitExceededError=SkillLimitExceededError,
    )
    _module(
        f'{PKG}.utils.export_import_utils',
        ENTITY_IMPORT_MAPPER={},
        _wrap_import_error=lambda index, msg: {'index': index, 'msg': msg},
        _wrap_import_result=lambda index, result: {'index': index, **result},
    )
    session = types.SimpleNamespace(commit=lambda: None)
    _module('tools', rpc_tools=types.SimpleNamespace(RpcMixin=object),
            db=types.SimpleNamespace(get_session=lambda project_id: contextlib.nullcontext(session)))
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

    yield module, attached

    for name in [name for name in sys.modules if name.startswith(PKG)]:
        del sys.modules[name]
    for name, mod in saved.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


def _import(module, skill_count):
    refs = [{'import_uuid': f's{i}'} for i in range(skill_count)]
    mapper = {ref['import_uuid']: types.SimpleNamespace(id=i + 1, versions={'base': i + 1}) for i, ref in enumerate(refs)}
    errors = {'skills': []}
    module._attach_imported_skills(
        project_id=2,
        agents_with_skills=[(0, 'my-agent', {'versions': [{'name': 'base', 'skills': refs}]})],
        skill_id_mapper=mapper,
        application_version_mapper={('my-agent', 'base'): (1, 11)},
        errors=errors,
    )
    return errors['skills']


def test_exceeding_the_limit_reports_one_actionable_error_not_one_per_skill(wizard):
    module, attached = wizard

    errors = _import(module, 50)

    assert len(attached) == MAX_ALLOWED
    assert len(errors) == 1
    assert errors[0]['index'] == 0
    assert errors[0]['msg'] == (
        "Cannot attach all 50 skills to agent 'my-agent' (version 'base'): the project allows at most 10 "
        "skills per agent. 10 attached, 40 not attached. Remove at least 40 skills from the agent and try again."
    )


def test_skills_within_the_limit_report_no_error(wizard):
    module, attached = wizard

    assert _import(module, MAX_ALLOWED) == []
    assert len(attached) == MAX_ALLOWED
