"""#7184: an agent's attached-skill entries carry the invocation keys of the pinned version, only when set."""
import importlib.util
import pathlib
import types


def _load_sibling(file_name, module_name):
    spec = importlib.util.spec_from_file_location(module_name, pathlib.Path(__file__).with_name(file_name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_copies = _load_sibling('test_7185_skill_file_copies.py', 'skill_file_copies_harness_7184')
env = _copies.env
application_utils = _copies.application_utils


def _mapping(skill_id, name, meta):
    version = types.SimpleNamespace(name='base', meta=meta, instructions='Body.', files=[])
    skill = types.SimpleNamespace(name=name, description=f'{name} skill')
    return types.SimpleNamespace(skill_id=skill_id, skill_version_id=100 + skill_id, skill=skill, skill_version=version)


def test_attached_entries_carry_the_flags_of_their_pinned_version(application_utils):
    entries = application_utils.build_skill_mappings_list([
        _mapping(1, 'deploy', {'frontmatter': {'disable-model-invocation': True}}),
        _mapping(2, 'style-guide', {'frontmatter': {'user-invocable': False}}),
        _mapping(3, 'plain', {'frontmatter': {'license': 'MIT'}}),
    ])
    assert [(e['name'], e.get('disable_model_invocation'), e.get('user_invocable')) for e in entries] == [
        ('deploy', True, None), ('style-guide', None, False), ('plain', None, None),
    ]
    assert set(entries[2]) == {'skill_id', 'skill_version_id', 'name', 'description', 'version_name', 'icon_meta', 'instructions'}


def test_a_mapping_without_a_version_carries_no_flag(application_utils):
    mapping = _mapping(4, 'gone', {})
    mapping.skill_version = None
    [entry] = application_utils.build_skill_mappings_list([mapping])
    assert 'disable_model_invocation' not in entry and 'user_invocable' not in entry
