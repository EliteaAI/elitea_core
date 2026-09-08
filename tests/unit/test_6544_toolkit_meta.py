"""#6544 - a toolkit crossing a project boundary must not carry index schedules.

``indexes_meta`` holds nothing but cron schedules. Carried into another project they fire
against an index that does not exist there, notifying an author who may not even be a
member of it, with no screen anywhere that could list or delete them.
"""
import importlib.util
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def toolkit_meta():
    spec = importlib.util.spec_from_file_location(
        'toolkit_meta_6544', PLUGIN_ROOT / 'utils' / 'toolkit_meta.py'
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestDropIndexSchedules:
    def test_it_removes_indexes_meta(self, toolkit_meta):
        meta = {'indexes_meta': {'docs': {'schedules': {'7': {'cron': '* * * * *'}}}}}
        assert toolkit_meta.drop_index_schedules(meta) == {}

    def test_it_keeps_every_other_key(self, toolkit_meta):
        meta = {
            'indexes_meta': {'docs': {'schedules': {}}},
            'parent_entity_id': 3,
            'parent_project_id': 406,
            'icon_meta': {'name': 'x'},
        }
        assert toolkit_meta.drop_index_schedules(meta) == {
            'parent_entity_id': 3,
            'parent_project_id': 406,
            'icon_meta': {'name': 'x'},
        }

    def test_it_does_not_mutate_the_source(self, toolkit_meta):
        meta = {'indexes_meta': {'docs': {}}, 'parent_entity_id': 3}
        toolkit_meta.drop_index_schedules(meta)
        assert 'indexes_meta' in meta

    @pytest.mark.parametrize('empty', [None, {}])
    def test_it_returns_a_dict_for_a_falsy_meta(self, toolkit_meta, empty):
        """Never hand the falsy argument back. Every call site assigns the result straight into
        a payload dict, so returning None would write an explicit `meta: None` over a field that
        was merely absent. `ToolBase.meta` is `Optional[dict] = {}`, and pydantic applies that
        default only when the key is missing - an explicit None survives validation and then hits
        `elitea_tools.meta`, which is `nullable=False`. That broke toolkit creation for every
        caller that omits meta, the UI's artifact/bucket create among them."""
        assert toolkit_meta.drop_index_schedules(empty) == {}

    def test_stripping_the_only_key_still_yields_a_dict(self, toolkit_meta):
        assert toolkit_meta.drop_index_schedules({'indexes_meta': {'docs': {}}}) == {}

    def test_a_meta_without_schedules_is_unchanged(self, toolkit_meta):
        meta = {'parent_author_id': 12}
        assert toolkit_meta.drop_index_schedules(meta) == meta
