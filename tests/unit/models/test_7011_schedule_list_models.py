"""Unit tests for the schedule list filter and item models (EL-7011, US-05)."""
import pytest
from fixtures.helpers import load_module_with_stubs
from pydantic import ValidationError


@pytest.fixture(scope='module')
def pd_list(models_path):
    return load_module_with_stubs(
        models_path / 'pd' / 'schedule_list.py',
        'plugins.elitea_core.models.pd.schedule_list',
    )


def _item(**extra):
    base = {
        'id': 1, 'name': 'n', 'entity_type': 'agent', 'application_id': 2, 'entity_name': 'a',
        'version_id': 3, 'version_name': 'v', 'cron': '* * * * *', 'timezone': 'UTC',
        'enabled': True, 'owner': {'id': 7, 'name': None}, 'allowed_actions': ['edit'],
    }
    return {**base, **extra}


def test_r3_4_filters_default_page(pd_list):
    f = pd_list.ScheduleListFilters()
    assert (f.limit, f.offset) == (20, 0)
    assert f.enabled is None and f.owner_ids is None


def test_r3_4_filters_parse_query_strings(pd_list):
    f = pd_list.ScheduleListFilters(enabled='true', entity_id='5', owner_ids='1,2')
    assert f.enabled is True
    assert f.entity_id == 5
    assert f.owner_ids == [1, 2]


@pytest.mark.parametrize('name', ['enabled', 'entity_id', 'owner_ids', 'offset'])
def test_r3_4_wrong_type_names_parameter(pd_list, name):
    kwargs = {name: 'abc'}
    with pytest.raises(ValidationError) as exc:
        pd_list.ScheduleListFilters(**kwargs)
    assert exc.value.errors()[0]['loc'][0] == name


@pytest.mark.parametrize('limit', [0, -1, 101])
def test_r5_2_limit_out_of_range(pd_list, limit):
    with pytest.raises(ValidationError) as exc:
        pd_list.ScheduleListFilters(limit=limit)
    assert exc.value.errors()[0]['loc'][0] == 'limit'


@pytest.mark.parametrize('limit', [1, 100])
def test_r5_2_limit_bounds_accepted(pd_list, limit):
    assert pd_list.ScheduleListFilters(limit=limit).limit == limit


def test_r4_3_description_and_run_mode_null(pd_list):
    dumped = pd_list.ScheduleListItem(**_item()).model_dump()
    assert dumped['description'] is None
    assert dumped['run_mode'] is None


def test_r4_3_description_and_run_mode_kept(pd_list):
    dumped = pd_list.ScheduleListItem(**_item(description='d', run_mode='chat')).model_dump()
    assert (dumped['description'], dumped['run_mode']) == ('d', 'chat')


def test_r5_3_no_tags_field(pd_list):
    dumped = pd_list.ScheduleListItem(**_item(tags=[{'name': 'x'}])).model_dump()
    assert 'tags' not in dumped
