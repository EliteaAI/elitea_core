import importlib.util
import pathlib
import sys
import types
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from sqlalchemy import ForeignKey, Integer, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = 'plugins_6927.elitea_core'
TENANT = 'tenant'

MODEL_FILES = (
    'models.enums.all',
    'models.folder',
    'models.participants',
    'models.conversation',
    'models.message_items.base',
    'models.message_items.text',
    'models.message_group',
    'models.pd.skill_run_history',
    'utils.parallel_hitl',
    'utils.skill_run_history',
)


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def _load(dotted):
    relative = dotted.replace('.', '/')
    path = PLUGIN_ROOT / f'{relative}.py'
    name = f'{PACKAGE}.{dotted}'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_init(dotted):
    name = f'{PACKAGE}.{dotted}'
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / dotted / '__init__.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _drop_stubbed(*prefixes):
    for name in [name for name in sys.modules if name.split('.')[0] in prefixes]:
        if getattr(sys.modules[name], '__file__', None) is None:
            sys.modules.pop(name, None)


class Harness:
    def __init__(self):
        self.users = {}
        self.usage_calls = []
        self.usage_answer = {}
        self.usage_error = None
        self.models_answer = []


@pytest.fixture
def history(isolated_sys_modules):
    _drop_stubbed('sqlalchemy', 'pydantic')
    harness = Harness()

    class Base(DeclarativeBase):
        pass

    def list_users(user_ids):
        return [harness.users[user_id] for user_id in user_ids if user_id in harness.users]

    class UsageRpc:
        def timeout(self, _seconds):
            return self

        def usage_conversation_totals(self, **kwargs):
            harness.usage_calls.append(kwargs)
            if harness.usage_error:
                raise harness.usage_error
            return harness.usage_answer

        def usage_root_entity_models(self, **kwargs):
            harness.usage_calls.append({'models': kwargs})
            return harness.models_answer

    tools = types.ModuleType('tools')
    tools.db = SimpleNamespace(Base=Base)
    tools.db_tools = SimpleNamespace()
    tools.config = SimpleNamespace(POSTGRES_TENANT_SCHEMA=TENANT)
    tools.auth = SimpleNamespace(list_users=list_users)
    tools.rpc_tools = SimpleNamespace(RpcMixin=lambda: SimpleNamespace(rpc=UsageRpc()))
    sys.modules['tools'] = tools
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = SimpleNamespace(warning=lambda *a, **k: None, info=lambda *a, **k: None)
    sys.modules['pylon.core.tools'] = pylon_tools

    _package('plugins_6927')
    _package(PACKAGE)
    for name in ('utils', 'models.enums', 'models.pd', 'models.message_items'):
        _package(f'{PACKAGE}.{name}')
    models_init = _load_init('models')
    sys.modules[f'{PACKAGE}.models'] = models_init
    models_init.__path__ = [str(PLUGIN_ROOT / 'models')]

    loaded = {}
    for dotted in MODEL_FILES:
        loaded[dotted] = _load(dotted)
        if dotted == 'models.conversation':
            class ConversationShareToken(Base):
                __tablename__ = 'chat_conversation_share_tokens'
                __table_args__ = {'schema': TENANT}
                id: Mapped[int] = mapped_column(Integer, primary_key=True)
                conversation_id: Mapped[int] = mapped_column(
                    ForeignKey(f'{TENANT}.chat_conversations.id'),
                )
                conversation = relationship('Conversation', back_populates='share_tokens')

    Base.registry.configure()
    module = loaded['utils.skill_run_history']
    return SimpleNamespace(
        module=module,
        harness=harness,
        Conversation=loaded['models.conversation'].Conversation,
        SkillRunFilters=loaded['models.pd.skill_run_history'].SkillRunFilters,
    )


def _sql(statement):
    return str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={'literal_binds': True}))


def _history(history, filters=None, skill_project_id=2, session=None):
    return history.module.SkillRunHistory(
        session, project_id=2, skill_id=174, skill_project_id=skill_project_id,
        filters=history.SkillRunFilters.model_validate(filters or {}),
    )


def _base_query(history):
    return select(history.Conversation)


class RecordingQuery:

    def __init__(self, conversation, candidates=()):
        self.conversation = conversation
        self.criteria = []
        self.candidates = list(candidates)

    def where(self, *criteria):
        self.criteria.extend(criteria)
        return self

    def with_entities(self, *_columns):
        return self

    def all(self):
        return [(candidate,) for candidate in self.candidates]

    def sql(self):
        return _sql(select(self.conversation).where(*self.criteria))


class TestRunSet:
    def test_runs_are_conversations_the_skill_replied_in(self, history):
        sql = _sql(_history(history).restrict_to_runs(_base_query(history)))

        assert 'chat_message_group.author_participant_id IN' in sql
        assert "chat_participants.entity_name = 'skill'" in sql
        assert "CAST((tenant.chat_participants.entity_meta ->> 'id') AS INTEGER) = 174" in sql

    def test_owner_project_separates_own_and_catalog_skill_with_the_same_id(self, history):
        own = _sql(_history(history, skill_project_id=2).restrict_to_runs(_base_query(history)))
        catalog = _sql(_history(history, skill_project_id=1).restrict_to_runs(_base_query(history)))

        assert "CAST((tenant.chat_participants.entity_meta ->> 'project_id') AS INTEGER) = 2" in own
        assert "CAST((tenant.chat_participants.entity_meta ->> 'project_id') AS INTEGER) = 1" in catalog

    def test_no_single_participant_meta_is_required(self, history):
        sql = _sql(_history(history).restrict_to_runs(_base_query(history)))

        assert 'single_participant' not in sql


class TestFilters:
    def test_version_reads_the_skill_participant_mapping(self, history):
        query = RecordingQuery(history.Conversation)
        sql = _history(history, {'version_id': 276}).apply_filters(query).sql()

        assert "CAST((tenant.chat_participant_mapping.entity_settings ->> 'version_id') AS INTEGER) = 276" in sql

    def test_status_is_the_skills_latest_reply(self, history):
        query = RecordingQuery(history.Conversation)
        sql = _history(history, {'status': 'stopped'}).apply_filters(query).sql()

        assert 'DISTINCT ON (tenant.chat_message_group.conversation_id)' in sql
        assert 'chat_message_group.reply_to_id IS NOT NULL' in sql
        assert "(tenant.chat_message_group.meta ->> 'stopped') = 'true'" in sql
        assert "(tenant.chat_message_group.meta ->> 'is_error') = 'true'" in sql
        assert "anon_1.status = 'stopped'" in sql

    def test_dates_and_author_bound_the_conversation(self, history):
        query = RecordingQuery(history.Conversation)
        sql = _history(history, {
            'created_from': '2026-10-01T00:00:00+02:00',
            'created_to': '2026-10-02T00:00:00Z',
            'author_id': 3,
        }).apply_filters(query).sql()

        assert "chat_conversations.created_at >= '2026-09-30 22:00:00'" in sql
        assert "chat_conversations.created_at <= '2026-10-02 00:00:00'" in sql
        assert 'chat_conversations.author_id = 3' in sql

    def test_text_search_reaches_message_text_not_only_the_shared_run_name(self, history):
        query = RecordingQuery(history.Conversation)
        sql = _history(history).apply_filters(query, text_query='invoice').sql()

        assert "chat_conversations.name ILIKE '%%invoice%%'" in sql
        assert "chat_messages_text.content ILIKE '%%invoice%%'" in sql

    def test_no_filter_adds_no_criteria(self, history):
        query = RecordingQuery(history.Conversation)
        _history(history).apply_filters(query)

        assert query.criteria == []
        assert history.harness.usage_calls == []

    def test_model_filter_narrows_candidates_through_one_usage_call(self, history):
        kept, dropped = uuid.uuid4(), uuid.uuid4()
        history.harness.usage_answer = {str(kept): {'tokens': 5, 'cost': 0.1, 'models': ['m'], 'has_error': False}}
        query = RecordingQuery(history.Conversation, candidates=[kept, dropped])

        sql = _history(history, {'model': 'm'}).apply_filters(query).sql()

        assert history.harness.usage_calls == [{
            'project_id': 2,
            'conversation_ids': [str(kept), str(dropped)],
            'root_entity_type': 'skill',
            'root_entity_id': 174,
            'root_entity_project_id': 2,
            'model_name': 'm',
        }]
        assert str(kept).replace('-', '') in sql.replace('-', '')
        assert str(dropped).replace('-', '') not in sql.replace('-', '')

    def test_unreachable_usage_matches_no_run_for_a_model_filter(self, history):
        history.harness.usage_error = RuntimeError('usage down')
        query = RecordingQuery(history.Conversation, candidates=[uuid.uuid4()])

        sql = _history(history, {'model': 'm'}).apply_filters(query).sql()

        assert 'IN (NULL) AND (1 != 1)' in sql or '1 != 1' in sql


class TestFilterValidation:
    def test_blank_query_values_mean_no_filter(self, history):
        filters = history.SkillRunFilters.model_validate({'status': '', 'model': '', 'author_id': ''})

        assert filters.status is None and filters.model is None and filters.author_id is None

    def test_unknown_status_is_rejected(self, history):
        with pytest.raises(ValidationError):
            history.SkillRunFilters.model_validate({'status': 'finished'})

    def test_aware_dates_become_naive_utc(self, history):
        filters = history.SkillRunFilters.model_validate({'created_from': '2026-10-01T03:00:00+03:00'})

        assert filters.created_from == datetime(2026, 10, 1, 0, 0)


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class SummarySession:

    def __init__(self, versions, statuses):
        self.answers = [versions, statuses]
        self.statements = []

    def execute(self, statement):
        self.statements.append(_sql(statement))
        return FakeResult(self.answers.pop(0))


def _conversation(conversation_id, author_id=3):
    return SimpleNamespace(id=conversation_id, uuid=uuid.uuid4(), author_id=author_id)


class TestPageSummary:
    def test_one_usage_call_covers_the_whole_page(self, history):
        page = [_conversation(1), _conversation(2, author_id=7)]
        history.harness.users = {3: {'id': 3, 'name': 'Admin', 'email': 'a@x'}}
        history.harness.usage_answer = {
            str(page[0].uuid): {'tokens': 40, 'cost': 0.25, 'models': ['gpt'], 'has_error': True,
                                'last_run_id': 'run-1'},
        }
        session = SummarySession(versions=[(1, 276)], statuses=[(1, 'error'), (2, 'success')])

        summaries = _history(history, session=session).summarize(page)

        assert len(history.harness.usage_calls) == 1
        assert history.harness.usage_calls[0]['conversation_ids'] == [str(c.uuid) for c in page]
        assert summaries[1] == {
            'version_id': 276, 'status': 'error',
            'author': {'id': 3, 'name': 'Admin', 'email': 'a@x'},
            'models': ['gpt'], 'tokens': 40, 'cost': 0.25, 'last_run_id': 'run-1',
            'usage_available': True,
        }
        assert summaries[2]['tokens'] == 0 and summaries[2]['models'] == []
        assert summaries[2]['author'] == {'id': 7, 'name': None, 'email': None}

    def test_a_model_filtered_page_reuses_the_filter_usage_read(self, history):
        page = [_conversation(1)]
        history.harness.usage_answer = {str(page[0].uuid): {'tokens': 9, 'cost': 0.5, 'models': ['m']}}
        run_history = _history(history, {'model': 'm'}, session=SummarySession(versions=[], statuses=[]))
        run_history.apply_filters(RecordingQuery(history.Conversation, candidates=[page[0].uuid]))

        summary = run_history.summarize(page)[1]

        assert len(history.harness.usage_calls) == 1
        assert summary['tokens'] == 9

    def test_unreachable_usage_leaves_usage_columns_empty(self, history):
        history.harness.usage_error = RuntimeError('usage down')
        session = SummarySession(versions=[], statuses=[])

        summary = _history(history, session=session).summarize([_conversation(1)])[1]

        assert summary['usage_available'] is False
        assert summary['tokens'] is None and summary['cost'] is None and summary['models'] is None

    def test_page_reads_are_scoped_to_the_skill_participant(self, history):
        session = SummarySession(versions=[], statuses=[])
        _history(history, session=session).summarize([_conversation(1)])

        version_sql, status_sql = session.statements
        assert 'chat_participant_mapping.participant_id IN' in version_sql
        assert 'chat_message_group.author_participant_id IN' in status_sql
        assert 'chat_message_group.conversation_id IN (1)' in status_sql


class TestFacets:
    def test_authors_and_models_come_from_the_visible_runs(self, history):
        history.harness.users = {3: {'id': 3, 'name': 'Zed'}, 5: {'id': 5, 'name': 'amy'}}
        history.harness.models_answer = ['gpt', 'opus']
        first, second = uuid.uuid4(), uuid.uuid4()

        class VisibleRuns:
            def with_entities(self, *_columns):
                return self

            def all(self):
                return [(first, 3), (second, 5), (uuid.uuid4(), 9), (second, 5)]

        facets = _history(history).facets(VisibleRuns())

        assert [author['id'] for author in facets['authors']] == [9, 5, 3]
        assert facets['models'] == ['gpt', 'opus']
        models_call = history.harness.usage_calls[-1]['models']
        assert str(first) in models_call['conversation_ids'] and str(second) in models_call['conversation_ids']
        assert models_call['root_entity_id'] == 174 and models_call['root_entity_project_id'] == 2

    def test_no_visible_runs_asks_usage_nothing(self, history):
        class NoRuns:
            def with_entities(self, *_columns):
                return self

            def all(self):
                return []

        facets = _history(history).facets(NoRuns())

        assert facets == {'authors': [], 'models': []}
        assert history.harness.usage_calls == []


class TestFacetsOffer:
    def test_only_the_unfiltered_first_page_carries_filter_options(self, history):
        assert _history(history).offers_facets(0, None) is True
        assert _history(history).offers_facets(20, None) is False
        assert _history(history).offers_facets(0, 'invoice') is False
        assert _history(history, {'status': 'error'}).offers_facets(0, None) is False
        assert _history(history, {'model': 'm'}).offers_facets(0, '') is False
