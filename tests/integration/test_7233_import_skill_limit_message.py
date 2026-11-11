"""#7233 - an agent whose skills exceed the target project's limit gets one clear import error."""
import ast
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
    """Same signature and attributes as utils.skill_utils.SkillLimitExceededError."""

    def __init__(self, entity_version_id, current_count, max_allowed):
        super().__init__(
            f'Agent version {entity_version_id} already has {current_count} skills attached. '
            f'Maximum allowed is {max_allowed}.'
        )
        self.entity_version_id = entity_version_id
        self.current_count = current_count
        self.max_allowed = max_allowed


def _real_wrap_import_error():
    """The real error wrapper: its module needs the platform, so only this function is compiled."""
    path = PLUGIN_ROOT / 'utils' / 'export_import_utils.py'
    tree = ast.parse(path.read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_wrap_import_error')
    namespace = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), namespace)  # noqa: S102
    return namespace['_wrap_import_error']


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
            raise SkillLimitExceededError(entity_version_id, len(attached), MAX_ALLOWED)
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
        _wrap_import_error=_real_wrap_import_error(),
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
        agents_with_skills=[(0, 'my-agent', {'import_uuid': 'agent-uuid', 'versions': [{'name': 'base', 'skills': refs}]})],
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
    assert errors[0]['msg'] == (
        "Agent 'my-agent' (version 'base') was imported with 10 of 50 skills: the project allows at most 10 "
        "skills per agent, so 40 skills were not attached. Open the imported agent to remove skills or add "
        "the missing ones after raising the limit."
    )


def test_the_error_names_its_target_in_fields_so_clients_do_not_parse_the_message(wizard):
    module, _ = wizard

    [error] = _import(module, 50)

    assert (error['index'], error['import_uuid'], error['version_name']) == (0, 'agent-uuid', 'base')


def test_the_message_does_not_advise_a_retry_that_would_duplicate_the_agent(wizard):
    module, _ = wizard

    [error] = _import(module, 50)

    assert 'try again' not in error['msg'].lower()


def test_skills_within_the_limit_report_no_error(wizard):
    module, attached = wizard

    assert _import(module, MAX_ALLOWED) == []
    assert len(attached) == MAX_ALLOWED
