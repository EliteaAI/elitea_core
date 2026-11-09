"""The schedule list query with Toolkit Index rows: names, filters and search (SQLite, real query and models)."""
import pathlib
import sys

import pytest
from fixtures.helpers import load_module_with_stubs
from unit.test_toolkit_index_schedule_service import (  # noqa: F401  pylint: disable=W0611 -- the fixture and its names
    OTHER_ID, OWNER_ID, PACKAGE, PLUGIN_ROOT, env, sqlite_db,
)


@pytest.fixture
def listing(env):
    """Rows to list: one agent schedule, an index on the named toolkit 3 and an index on the unnamed toolkit 4."""
    queries = load_module_with_stubs(pathlib.Path(PLUGIN_ROOT) / 'utils' / 'schedule_queries.py',
                                     f'{PACKAGE}.utils.schedule_queries')
    filters = sys.modules[f'{PACKAGE}.models.pd.schedule_list'].ScheduleListFilters
    with env.session_factory() as session:
        session.add(sys.modules[f'{PACKAGE}.models.all'].EliteATool(id=4, name=None, type='github'))
        session.commit()
    sqlite_db.seed_schedule(env.session_factory, env.EntitySchedule, OWNER_ID, name='Agent nightly')
    env.seed_index('docs', name='Docs refresh')
    env.seed_index('wiki', name='Wiki refresh', toolkit_id=4)

    def run(**values):
        with env.session_factory() as session:
            total, rows = queries.list_schedules(session, env.scope(), filters(sort_by='name', **values), 20, 0)
        return total, {row.name: row for row in rows}
    return run


def test_agent_rows_are_still_listed_next_to_index_rows(listing):
    total, rows = listing()
    assert total == 3 and set(rows) == {'Agent nightly', 'Docs refresh', 'Wiki refresh'}
    agent = rows['Agent nightly']
    assert (agent.application_id, agent.toolkit_id, agent.index_name) == (1, None, None)
    assert agent.entity_name == 'Entity' and agent.version_name == 'base'


def test_an_index_row_shows_its_toolkit_name_and_index_name(listing):
    row = listing()[1]['Docs refresh']
    assert (row.entity_type, row.toolkit_id, row.index_name) == ('toolkit_index', 3, 'docs')
    assert (row.entity_name, row.application_id, row.version_id, row.version_name) == ('Docs toolkit', None, None, None)


def test_an_index_row_of_an_unnamed_toolkit_shows_the_toolkit_type(listing):
    assert listing()[1]['Wiki refresh'].entity_name == 'github'


@pytest.mark.parametrize('term, names', [
    ('Docs toolkit', {'Docs refresh'}),
    ('wiki', {'Wiki refresh'}),
    ('Entity', {'Agent nightly'}),
    ('refresh', {'Docs refresh', 'Wiki refresh'}),
])
def test_search_matches_schedule_toolkit_application_and_index_names(listing, term, names):
    total, rows = listing(search=term)
    assert set(rows) == names and total == len(names)


def test_index_name_filter_keeps_the_rows_of_that_index(listing):
    assert set(listing(index_name='docs')[1]) == {'Docs refresh'}


def test_entity_id_narrows_by_toolkit_for_index_rows_and_by_application_for_others(listing):
    assert set(listing(entity_type='toolkit_index', entity_id=4)[1]) == {'Wiki refresh'}
    assert set(listing(entity_type='agent', entity_id=1)[1]) == {'Agent nightly'}


def test_entity_type_filter_separates_index_rows(listing):
    assert set(listing(entity_type='toolkit_index')[1]) == {'Docs refresh', 'Wiki refresh'}


def test_another_users_index_row_is_not_listed(env, listing):
    sqlite_db.seed_schedule(
        env.session_factory, env.EntitySchedule, OTHER_ID, name='Other refresh', entity_type='toolkit_index',
        application_id=None, version_id=None, prompt=None, toolkit_id=3, index_name='other')
    assert 'Other refresh' not in listing()[1]
