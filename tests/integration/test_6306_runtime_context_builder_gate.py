"""Guards that target-application facts are read from the agent's own schema.

Re-broken by resolving pipeline status or the pinned version's internal_tools against
the conversation's session, where the same ids belong to unrelated rows.
"""
import ast
import pathlib
import types

import pytest


CHAT_ALL_PATH = pathlib.Path(__file__).resolve().parents[2] / 'rpc' / 'chat_all.py'

HELPERS = (
    'resolve_target_application_context',
    '_pinned_version_id',
    '_read_application_context',
    '_has_any_pipeline_version',
    '_version_internal_tools',
)


class FakeQuery:
    def __init__(self, result):
        self._result = result

    def filter(self, *args):
        return self

    def where(self, *args):
        return self

    def first(self):
        return self._result


class FakeSession:
    def __init__(self, *results_in_query_order):
        self._results = list(results_in_query_order)
        self.query_count = 0

    def query(self, *_columns):
        self.query_count += 1
        return FakeQuery(self._results.pop(0) if self._results else None)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class ExplodingSession(FakeSession):
    def __init__(self, *results_in_query_order, fail_after=0):
        super().__init__(*results_in_query_order)
        self._fail_after = fail_after

    def query(self, *columns):
        if self.query_count >= self._fail_after:
            self.query_count += 1
            raise RuntimeError('schema gone')
        return super().query(*columns)


class FakeDb:
    def __init__(self):
        self.owner_session = None
        self.opened_project_ids = []
        self.raise_on_open = False

    def get_session(self, project_id):
        self.opened_project_ids.append(project_id)
        if self.raise_on_open:
            raise RuntimeError('schema gone')
        return self.owner_session or FakeSession()


@pytest.fixture()
def loaded():
    tree = ast.parse(CHAT_ALL_PATH.read_text())
    funcs = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in HELPERS
    ]
    assert {f.name for f in funcs} == set(HELPERS), 'helper renamed or removed'

    class _Column:
        def __eq__(self, other):
            return True

    fake_db = FakeDb()
    namespace = {
        'ParticipantMapping': types.SimpleNamespace(
            participant_id=_Column(), conversation_id=_Column(), entity_settings=_Column(),
        ),
        'ApplicationVersion': types.SimpleNamespace(
            id=_Column(), meta=_Column(), application_id=_Column(), agent_type=_Column(),
        ),
        'AgentTypes': types.SimpleNamespace(pipeline=types.SimpleNamespace(value='pipeline')),
        'db': fake_db,
        'log': types.SimpleNamespace(debug=lambda *a, **k: None, warning=lambda *a, **k: None),
    }
    exec(compile(ast.Module(body=funcs, type_ignores=[]), '<chat_all>', 'exec'), namespace)
    return namespace['resolve_target_application_context'], fake_db


def _mapping(version_id):
    return types.SimpleNamespace(entity_settings={'version_id': version_id})


def _version_meta(internal_tools):
    return ({'internal_tools': internal_tools},)


def _participant(project_id=2, application_id=4):
    return types.SimpleNamespace(id=9, entity_meta={'id': application_id, 'project_id': project_id})


class TestSameProjectAgent:

    def test_version_meta_tools_are_surfaced(self, loaded):
        resolve, _db = loaded
        session = FakeSession(_mapping(77), None, _version_meta(['skill_builder']))

        assert resolve(session, 1, _participant(), 2) == (False, ['skill_builder'])

    def test_pipeline_detected(self, loaded):
        resolve, _db = loaded
        session = FakeSession(_mapping(77), ('pipeline-version',), _version_meta([]))

        is_pipeline, _tools = resolve(session, 1, _participant(), 2)
        assert is_pipeline is True

    def test_never_opens_a_second_session(self, loaded):
        resolve, fake_db = loaded
        session = FakeSession(_mapping(77), None, _version_meta(['skill_builder']))

        resolve(session, 1, _participant(project_id=2), 2)
        assert fake_db.opened_project_ids == []


class TestCrossProjectAgent:

    def test_reads_its_own_schema(self, loaded):
        resolve, fake_db = loaded
        fake_db.owner_session = FakeSession(None, _version_meta(['project_context_builder']))
        ambient = FakeSession(_mapping(77), ('WRONG',), _version_meta(['WRONG']))

        assert resolve(ambient, 1, _participant(project_id=7), 2) == (
            False, ['project_context_builder']
        )
        assert fake_db.opened_project_ids == [7]

    def test_opens_the_owner_schema_exactly_once(self, loaded):
        resolve, fake_db = loaded
        fake_db.owner_session = FakeSession(('pipeline-version',), _version_meta([]))

        resolve(FakeSession(_mapping(77)), 1, _participant(project_id=7), 2)
        assert fake_db.opened_project_ids == [7]

    def test_unreachable_owner_schema_degrades_instead_of_raising(self, loaded):
        resolve, fake_db = loaded
        fake_db.raise_on_open = True

        assert resolve(FakeSession(_mapping(77)), 1, _participant(project_id=7), 2) == (False, [])

    def test_owner_query_failure_degrades(self, loaded):
        resolve, fake_db = loaded
        fake_db.owner_session = ExplodingSession(fail_after=0)

        assert resolve(FakeSession(_mapping(77)), 1, _participant(project_id=7), 2) == (False, [])


class TestAmbientFailuresStillPropagate:
    def test_pipeline_probe_failure_propagates(self, loaded):
        resolve, _db = loaded
        session = ExplodingSession(_mapping(77), fail_after=1)

        with pytest.raises(RuntimeError, match='schema gone'):
            resolve(session, 1, _participant(project_id=2), 2)

    def test_mapping_lookup_failure_propagates(self, loaded):
        resolve, _db = loaded

        with pytest.raises(RuntimeError, match='schema gone'):
            resolve(ExplodingSession(fail_after=0), 1, _participant(project_id=2), 2)


class TestDegenerateInputs:

    def test_no_mapping_means_no_version_tools(self, loaded):
        resolve, _db = loaded
        assert resolve(FakeSession(None, None), 1, _participant(), 2) == (False, [])

    def test_mapping_without_version_id(self, loaded):
        resolve, _db = loaded
        session = FakeSession(types.SimpleNamespace(entity_settings={}), None)
        assert resolve(session, 1, _participant(), 2) == (False, [])

    def test_mapping_with_null_entity_settings(self, loaded):
        resolve, _db = loaded
        session = FakeSession(types.SimpleNamespace(entity_settings=None), None)
        assert resolve(session, 1, _participant(), 2) == (False, [])

    def test_participant_without_application_id(self, loaded):
        resolve, _db = loaded
        participant = types.SimpleNamespace(id=9, entity_meta={'project_id': 2})
        session = FakeSession(_mapping(77), _version_meta(['skill_builder']))

        assert resolve(session, 1, participant, 2) == (False, ['skill_builder'])

    def test_deleted_version_yields_no_tools(self, loaded):
        resolve, _db = loaded
        assert resolve(FakeSession(_mapping(77), None, None), 1, _participant(), 2) == (False, [])

    def test_version_without_internal_tools(self, loaded):
        resolve, _db = loaded
        assert resolve(FakeSession(_mapping(77), None, ({},)), 1, _participant(), 2) == (False, [])

    def test_version_with_null_meta(self, loaded):
        resolve, _db = loaded
        assert resolve(FakeSession(_mapping(77), None, (None,)), 1, _participant(), 2) == (False, [])
