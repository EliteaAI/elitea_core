"""#6809 P2 item 4: the per-case ``expected_trajectory`` reference (design §3.2).

The normalizer is the one validator behind the case API, CSV/JSON/JSONL import and the promote
pre-fill; these tests lock its shape and errors, the import paths, and the pre-fill builder.
"""

import json
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def et(utils_path):
    return load_utils_module(utils_path, 'evaluation_expected_trajectory')


@pytest.fixture(scope='module')
def imp(utils_path, et):
    return load_utils_module(utils_path, 'evaluation_dataset_import')


@pytest.fixture(scope='module')
def turns(utils_path):
    return load_utils_module(utils_path, 'evaluation_turn_extraction')


# --- normalizer ------------------------------------------------------------------------------

def test_defaults_fill_in(et):
    assert et.normalize_expected_trajectory({'tools': ['a', {'name': 'b'}]}) == {
        'match': 'superset', 'tools': [{'name': 'a'}, {'name': 'b'}],
        'forbidden': [], 'allow_repeat': []}


def test_full_shape_round_trips(et):
    value = {'match': 'in_order',
             'tools': [{'name': 'jira_search', 'args': {'project': 'EL'}, 'args_match': 'exact'}],
             'forbidden': ['github_delete_branch'], 'max_tool_calls': 6,
             'allow_repeat': ['get_job_status']}
    assert et.normalize_expected_trajectory(value) == value
    assert et.normalize_expected_trajectory(et.normalize_expected_trajectory(value)) == value


def test_args_default_to_subset_match(et):
    out = et.normalize_expected_trajectory({'tools': [{'name': 'a', 'args': {'x': 1}}]})
    assert out['tools'] == [{'name': 'a', 'args': {'x': 1}, 'args_match': 'subset'}]


@pytest.mark.parametrize('empty', [None, {}, '', '   '])
def test_empty_means_no_reference(et, empty):
    assert et.normalize_expected_trajectory(empty) is None


def test_json_string_is_parsed(et):
    assert et.normalize_expected_trajectory('{"match": "exact"}')['match'] == 'exact'


def test_names_are_stripped(et):
    out = et.normalize_expected_trajectory({'tools': [' a '], 'forbidden': [' b']})
    assert out['tools'] == [{'name': 'a'}] and out['forbidden'] == ['b']


@pytest.mark.parametrize('bad, message', [
    ('{not json', 'not valid JSON'),
    (['a'], 'must be an object'),
    ({'match': 'fuzzy'}, 'match must be one of'),
    ({'mode': 'exact'}, 'unknown keys'),
    ({'tools': 'a'}, 'tools must be a list'),
    ({'tools': [{'nme': 'a'}]}, 'unknown keys'),
    ({'tools': [{'name': ''}]}, 'non-empty string'),
    ({'tools': [3]}, 'object or a tool name'),
    ({'tools': [{'name': 'a', 'args': 'x'}]}, 'args must be an object'),
    ({'tools': [{'name': 'a', 'args_match': 'exact'}]}, 'needs args'),
    ({'tools': [{'name': 'a', 'args': {}, 'args_match': 'loose'}]}, 'args_match must be one of'),
    ({'forbidden': 'a'}, 'forbidden must be a list'),
    ({'max_tool_calls': -1}, 'non-negative integer'),
    ({'max_tool_calls': True}, 'non-negative integer'),
    ({'max_tool_calls': 2.5}, 'non-negative integer'),
    ({'tools': [{'name': 'a', 'args': {'x': object()}}]}, 'JSON-serializable'),
])
def test_bad_values_raise_field_level_errors(et, bad, message):
    with pytest.raises(ValueError, match=message):
        et.normalize_expected_trajectory(bad)


def test_caps(et):
    with pytest.raises(ValueError, match='limited to'):
        et.normalize_expected_trajectory({'tools': ['a'] * (et.MAX_TOOLS + 1)})
    with pytest.raises(ValueError, match='exceeds'):
        et.normalize_expected_trajectory({'tools': [{'name': 'a', 'args': {'x': 'y' * et.MAX_SERIALIZED_CHARS}}]})


def test_max_tool_calls_zero_is_kept(et):
    assert et.normalize_expected_trajectory({'max_tool_calls': 0})['max_tool_calls'] == 0


# --- promote pre-fill builder ----------------------------------------------------------------

def test_from_tool_calls_is_names_only_superset(et):
    assert et.from_tool_calls(['a', 'b', 'a']) == {
        'match': 'superset', 'tools': [{'name': 'a'}, {'name': 'b'}, {'name': 'a'}],
        'forbidden': [], 'allow_repeat': []}


def test_from_tool_calls_without_calls_is_none(et):
    assert et.from_tool_calls([]) is None
    assert et.from_tool_calls(['', None]) is None


def test_from_tool_calls_output_is_already_normalized(et):
    pre = et.from_tool_calls(['a'])
    assert et.normalize_expected_trajectory(pre) == pre


def test_from_tool_calls_is_capped(et):
    assert len(et.from_tool_calls(['a'] * (et.MAX_TOOLS + 5))['tools']) == et.MAX_TOOLS


# --- import ----------------------------------------------------------------------------------

def test_csv_json_cell(imp):
    content = 'input,expected_trajectory\nq,"{""tools"": [""jira_search""]}"\nq2,\n'
    rows, errors = imp.parse_csv(content)
    assert errors == []
    assert rows[0]['expected_trajectory']['tools'] == [{'name': 'jira_search'}]
    assert 'expected_trajectory' not in rows[1]
    assert 'expected_trajectory' not in rows[0]['variables']


def test_json_object(imp):
    rows, errors = imp.parse_json(json.dumps(
        [{'input': 'q', 'expected_trajectory': {'match': 'exact', 'tools': ['a']}}]))
    assert errors == []
    assert rows[0]['expected_trajectory']['match'] == 'exact'


def test_bad_trajectory_rejects_only_its_row(imp):
    rows, errors = imp.parse_json(json.dumps(
        [{'input': 'ok'}, {'input': 'bad', 'expected_trajectory': {'match': 'fuzzy'}}]))
    assert [r['input'] for r in rows] == ['ok']
    assert errors[0]['row'] == 2 and 'match must be one of' in errors[0]['error']


def test_jsonl(imp):
    content = ('{"input": "q1", "expected_output": "a1"}\n'
               '\n'
               '{"input": "q2", "expected_trajectory": {"tools": ["x"]}}\n'
               'not json\n'
               '[1]\n')
    rows, errors = imp.parse_import('jsonl', content)
    assert [r['input'] for r in rows] == ['q1', 'q2']
    assert rows[1]['expected_trajectory']['tools'] == [{'name': 'x'}]
    assert [e['row'] for e in errors] == [4, 5]


def test_jsonl_empty(imp):
    rows, errors = imp.parse_import('jsonl', '\n\n')
    assert rows == [] and 'empty JSONL' in errors[0]['error']


def test_jsonl_case_cap(imp, monkeypatch):
    monkeypatch.setattr(imp, 'MAX_CASES', 2)
    rows, errors = imp.parse_jsonl('{"input": "a"}\n{"input": "b"}\n{"input": "c"}\n')
    assert len(rows) <= 2 and errors


def test_rows_without_trajectory_keep_their_old_shape(imp):
    rows, _ = imp.parse_csv('input,expected_output\nq,a\n')
    assert rows == [{'input': 'q', 'variables': {}, 'expected_output': 'a', 'source_ref': None}]


# --- turn extraction carries agent group ids -------------------------------------------------

def test_pairs_carry_their_agent_groups(turns):
    out = turns.pair_turns_with_groups([
        ('agent', 'lead', 1),
        ('user', 'q1', 2), ('agent', 'a', 3), ('agent', '', 4),
        ('user', 'q2', 5),
        ('user', 'q3', 6), ('agent', 'c', 7),
    ])
    assert out == [('q1', 'a', [3, 4]), ('q2', None, []), ('q3', 'c', [7])]


def test_pair_turns_contract_is_unchanged(turns):
    assert turns.pair_turns([('user', 'q'), ('agent', 'a'), ('agent', 'b')]) == [('q', 'a\n\nb')]


class _Col:
    def __init__(self, name):
        self.name = name

    def in_(self, values):
        return ('in', self.name, tuple(values))

    def __eq__(self, other):
        return ('eq', self.name, other)

    __hash__ = object.__hash__


class _Query:
    def __init__(self, log, rows):
        self.log, self.rows = log, rows

    def filter(self, *c):
        self.log.append(('filter', c))
        return self

    def order_by(self, *c):
        self.log.append(('order_by', c))
        return self

    def all(self):
        self.log.append(('all',))
        return self.rows


class _Session:
    def __init__(self, rows):
        self.log, self.rows = [], rows

    def query(self, *targets):
        self.log.append(('query', tuple(t.name for t in targets)))
        return _Query(self.log, self.rows)


@pytest.fixture
def trace_model(monkeypatch, turns):
    import types
    import sqlalchemy
    step = types.SimpleNamespace(message_group_id=_Col('message_group_id'),
                                 tool_name=_Col('tool_name'), kind=_Col('kind'),
                                 started_at=_Col('started_at'), id=_Col('id'))
    mod = types.ModuleType('plugins.elitea_core.models.message_trace_step')
    mod.MessageTraceStep = step
    pkg = turns.__name__.rsplit('.', 2)[0]
    monkeypatch.setitem(sys.modules, f'{pkg}.models.message_trace_step', mod)
    if f'{pkg}.models' not in sys.modules:
        models_pkg = types.ModuleType(f'{pkg}.models')
        models_pkg.__path__ = []
        monkeypatch.setitem(sys.modules, f'{pkg}.models', models_pkg)

    class _Ordered:
        def __init__(self, col):
            self.col = col

        def nullslast(self):
            return ('asc_nullslast', self.col.name)

    monkeypatch.setattr(sqlalchemy, 'asc', lambda col: _Ordered(col) if col.name == 'started_at'
                        else ('asc', col.name))
    return step


def test_tool_calls_by_group_is_one_names_only_query(turns, trace_model):
    session = _Session([(3, 'jira_search'), (7, 'post'), (3, None), (3, 'jira_get')])
    assert turns.tool_calls_by_group(session, [3, 7]) == {3: ['jira_search', 'jira_get'], 7: ['post']}
    assert [e[0] for e in session.log].count('query') == 1
    assert session.log[0] == ('query', ('message_group_id', 'tool_name'))
    filters = session.log[1][1]
    assert ('in', 'message_group_id', (3, 7)) in filters and ('eq', 'kind', 'tool_call') in filters
    assert session.log[2][1] == (('asc_nullslast', 'started_at'), ('asc', 'id'))


def test_tool_calls_by_group_without_groups_skips_the_query(turns):
    session = _Session([])
    assert turns.tool_calls_by_group(session, []) == {}
    assert session.log == []
