import importlib.util
import pathlib
import sys
import time
import types
import uuid
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace

import pytest
from pydantic import BaseModel, ValidationError
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
    'models.pd.participant',
    'utils.chat_constants',
    'models.pd.conversation',
    'utils.parallel_hitl',
    'utils.skill_run_history',
)


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def _stub(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module


def _load(dotted):
    path = PLUGIN_ROOT / f"{dotted.replace('.', '/')}.py"
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
        self.rpc_calls = []
        self.answers = {}
        self.errors = {}
        self.session_open = False
        self.calls_while_session_open = []
        self.rpc_timeouts = []


@pytest.fixture
def history(isolated_sys_modules):
    _drop_stubbed('sqlalchemy', 'pydantic')
    harness = Harness()

    class Base(DeclarativeBase):
        pass

    def answer(name, kwargs):
        if harness.session_open:
            harness.calls_while_session_open.append(name)
        harness.rpc_calls.append((name, kwargs))
        if name in harness.errors:
            raise harness.errors[name]
        return harness.answers.get(name)

    class Rpc:
        def timeout(self, seconds):
            harness.rpc_timeouts.append(seconds)
            return BoundedRpc()

    class BoundedRpc:
        def __getattr__(self, name):
            return lambda **kwargs: answer(name, kwargs)

    def list_users(user_ids):
        answer('auth_list_users', {'user_ids': user_ids})
        return [harness.users[user_id] for user_id in user_ids if user_id in harness.users]

    @contextmanager
    def get_session(_project_id):
        harness.session_open = True
        try:
            yield SimpleNamespace()
        finally:
            harness.session_open = False

    tools = types.ModuleType('tools')
    tools.db = SimpleNamespace(Base=Base, get_session=get_session)
    tools.db_tools = SimpleNamespace()
    tools.config = SimpleNamespace(POSTGRES_TENANT_SCHEMA=TENANT)
    tools.auth = SimpleNamespace(list_users=list_users)
    tools.rpc_tools = SimpleNamespace(RpcMixin=lambda: SimpleNamespace(rpc=Rpc()))
    tools.serialize = lambda value: value
    sys.modules['tools'] = tools
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = SimpleNamespace(warning=lambda *a, **k: None, info=lambda *a, **k: None)
    sys.modules['pylon.core.tools'] = pylon_tools

    _package('plugins_6927')
    _package(PACKAGE)
    for name in ('utils', 'models.enums', 'models.pd', 'models.message_items'):
        _package(f'{PACKAGE}.{name}')
    models_init = _load_init('models')
    models_init.__path__ = [str(PLUGIN_ROOT / 'models')]

    class MessageGroupDetail(BaseModel):
        pass

    _stub(f'{PACKAGE}.models.pd.message', MessageGroupDetail=MessageGroupDetail)
    _stub(f'{PACKAGE}.utils.conversation_access', visible_conversation_ids=lambda *args: select(1).subquery())
    _stub(f'{PACKAGE}.utils.conversation_utils', calculate_conversation_durations_batch=lambda *args: {})

    loaded = {}
    for dotted in MODEL_FILES:
        loaded[dotted] = _load(dotted)
        if dotted == 'models.conversation':
            class ConversationShareToken(Base):
                __tablename__ = 'chat_conversation_share_tokens'
                __table_args__ = {'schema': TENANT}
                id: Mapped[int] = mapped_column(Integer, primary_key=True)
                conversation_id: Mapped[int] = mapped_column(ForeignKey(f'{TENANT}.chat_conversations.id'))
                conversation = relationship('Conversation', back_populates='share_tokens')

    Base.registry.configure()
    return SimpleNamespace(
        module=loaded['utils.skill_run_history'],
        harness=harness,
        Conversation=loaded['models.conversation'].Conversation,
        SkillRunFilters=loaded['models.pd.skill_run_history'].SkillRunFilters,
    )


def _sql(statement):
    return str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={'literal_binds': True}))


def _conditions_sql(history, filters=None, text_query=None, model_conversation_uuids=None):
    module = history.module
    participant_ids = module.skill_participant_ids(174, 2)
    stats = module.skill_turn_stats(participant_ids)
    conditions = module.run_conditions(
        participant_ids, stats, history.SkillRunFilters.model_validate(filters or {}),
        text_query, model_conversation_uuids,
    )
    return _sql(select(history.Conversation.id).join(stats, stats.c.conversation_id == history.Conversation.id)
                .where(*conditions))


class TestRunSet:
    def test_the_skill_is_matched_by_id_and_owner_project(self, history):
        sql = _sql(history.module.skill_participant_ids(174, 1))

        assert "chat_participants.entity_name = 'skill'" in sql
        assert "CAST((tenant.chat_participants.entity_meta ->> 'id') AS INTEGER) = 174" in sql
        assert "CAST((tenant.chat_participants.entity_meta ->> 'project_id') AS INTEGER) = 1" in sql

    def test_a_run_starts_at_the_first_prompt_the_skill_answered(self, history):
        module = history.module
        sql = _sql(select(module.skill_turn_stats(module.skill_participant_ids(174, 2))))

        assert 'min(chat_message_group_1.created_at) AS started_at' in sql
        assert 'chat_message_group_1.id = tenant.chat_message_group.reply_to_id' in sql
        assert 'tenant.chat_message_group.author_participant_id IN' in sql
        assert 'GROUP BY tenant.chat_message_group.conversation_id' in sql

    def test_message_count_is_the_skill_turns_not_the_whole_chat(self, history):
        module = history.module
        sql = _sql(select(module.skill_turn_stats(module.skill_participant_ids(174, 2))))

        assert 'count(tenant.chat_message_group.id) + count(distinct(chat_message_group_1.id))' in sql

    def test_the_run_user_is_who_prompted_the_latest_skill_reply(self, history):
        module = history.module
        sql = _sql(select(module.latest_skill_turns(module.skill_participant_ids(174, 2), [1])))

        assert 'DISTINCT ON (tenant.chat_message_group.conversation_id)' in sql
        assert "chat_participants_1.entity_name = 'user'" in sql
        assert "CAST(chat_participants_1.entity_meta ->> 'id' AS INTEGER) AS user_id" in sql
        assert "(tenant.chat_message_group.meta ->> 'stopped') = 'true'" in sql
        assert 'tenant.chat_message_group.conversation_id IN (1)' in sql


class TestFilters:
    def test_dates_bound_when_the_skill_started_not_when_the_chat_did(self, history):
        sql = _conditions_sql(history, {
            'created_from': '2026-10-01T00:00:00+02:00', 'created_to': '2026-10-02T00:00:00Z',
        })

        assert "anon_1.started_at >= '2026-09-30 22:00:00'" in sql
        assert "anon_1.started_at <= '2026-10-02 00:00:00'" in sql
        assert 'chat_conversations.created_at' not in sql

    def test_user_filter_matches_who_prompted_the_skill(self, history):
        sql = _conditions_sql(history, {'author_id': 3})

        assert 'user_id = 3' in sql
        assert 'chat_conversations.author_id' not in sql

    def test_text_search_covers_the_name_and_only_the_skill_turns(self, history):
        sql = _conditions_sql(history, text_query='invoice')

        assert "chat_conversations.name ILIKE '%%invoice%%'" in sql
        assert "chat_messages_text.content ILIKE '%%invoice%%'" in sql
        assert 'UNION' in sql
        assert 'tenant.chat_message_group.reply_to_id' in sql

    def test_version_reads_the_skill_participant_mapping(self, history):
        sql = _conditions_sql(history, {'version_id': 276})

        assert "CAST((tenant.chat_participant_mapping.entity_settings ->> 'version_id') AS INTEGER) = 276" in sql

    def test_status_is_the_skills_latest_reply(self, history):
        sql = _conditions_sql(history, {'status': 'stopped'})

        assert "status = 'stopped'" in sql

    def test_model_filter_keeps_the_conversations_usage_named(self, history):
        kept = uuid.uuid4()
        sql = _conditions_sql(history, {'model': 'm'}, model_conversation_uuids=[str(kept)])

        assert kept.hex in sql.replace('-', '')

    def test_unknown_model_usage_matches_no_run(self, history):
        sql = _conditions_sql(history, {'model': 'm'}, model_conversation_uuids=None)

        assert '1 != 1' in sql

    def test_no_filter_adds_no_condition(self, history):
        module = history.module
        participant_ids = module.skill_participant_ids(174, 2)
        stats = module.skill_turn_stats(participant_ids)

        assert module.run_conditions(participant_ids, stats, history.SkillRunFilters(), None, None) == []


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

    def test_only_the_unfiltered_first_page_carries_filter_options(self, history):
        module, filters = history.module, history.SkillRunFilters

        assert module.offers_facets(filters(), None, 0) is True
        assert module.offers_facets(filters(), None, 20) is False
        assert module.offers_facets(filters(), 'invoice', 0) is False
        assert module.offers_facets(filters.model_validate({'status': 'error'}), None, 0) is False


class TestEnrichmentBudget:
    def test_a_slow_source_is_cut_at_the_deadline_and_not_asked_again(self, history):
        budget = history.module.EnrichmentBudget(0.2)
        calls = []

        def slow():
            calls.append('slow')
            time.sleep(1)
            return 'late'

        started = time.monotonic()
        assert budget.call('usage', slow) is None
        assert time.monotonic() - started < 0.6
        assert budget.call('usage', lambda: 'second') is None
        assert calls == ['slow']

    def test_one_failing_source_does_not_silence_another(self, history):
        budget = history.module.EnrichmentBudget(1)

        def broken():
            raise RuntimeError('usage down')

        assert budget.call('usage', broken) is None
        assert budget.call('auth', lambda: ['admin']) == ['admin']

    def test_nothing_is_asked_once_the_page_budget_is_spent(self, history, monkeypatch):
        warnings = []
        monkeypatch.setattr(history.module.log, 'warning', lambda *args: warnings.append(args))
        budget = history.module.EnrichmentBudget(0)
        calls = []

        assert budget.call('auth', lambda: calls.append('auth')) is None
        assert calls == []
        assert len(warnings) == 1


def _page(history, conversations, facet_runs=None):
    page = history.module.SkillRunPage(total=len(conversations), facet_runs=facet_runs)
    for conversation_id, conversation_uuid, user_id in conversations:
        page.conversations.append({
            'id': conversation_id, 'uuid': conversation_uuid, 'name': f'run {conversation_id}',
            'is_private': True, 'author_id': 1, 'created_at': datetime(2026, 10, 1), 'meta': {},
            'source': 'elitea',
        })
        page.stats[conversation_id] = {
            'started_at': datetime(2026, 10, 9, 12, 0), 'message_count': 2, 'duration': 1.5,
            'participants_count': 3, 'version_id': 276, 'status': 'success', 'user_id': user_id,
        }
    return page


def _list_runs(history, monkeypatch, page, filters=None, read_seconds=0):
    harness = history.harness
    read_while_session_open = []

    def read_page(*args):
        read_while_session_open.append(harness.session_open)
        time.sleep(read_seconds)
        return page

    monkeypatch.setattr(history.module, 'read_skill_run_page', read_page)
    result = history.module.list_skill_runs(
        project_id=2, user_id=3, is_admin=False, skill_id=174, skill_project_id=None,
        filters=history.SkillRunFilters.model_validate(filters or {}), text_query=None, source='skill,elitea',
        include_hidden=False, limit=20, offset=0, sort_order='desc',
    )
    assert read_while_session_open == [True]
    return result


class TestListSkillRuns:
    def test_usage_and_auth_are_asked_only_after_the_session_closes(self, history, monkeypatch):
        harness = history.harness
        run_uuid = str(uuid.uuid4())
        harness.users = {8: {'id': 8, 'name': 'Viewer', 'email': 'v@x'}}
        harness.answers['usage_conversation_totals'] = {run_uuid: {'tokens': 9, 'cost': 0.5, 'models': ['m']}}
        page = _page(history, [(1, run_uuid, 8)], facet_runs={'uuids': [run_uuid], 'user_ids': {8}})

        result = _list_runs(history, monkeypatch, page)

        assert harness.calls_while_session_open == []
        assert [name for name, _kwargs in harness.rpc_calls] == [
            'auth_list_users', 'usage_conversation_totals', 'usage_root_entity_models',
        ]
        assert harness.rpc_timeouts == [history.module.ENRICHMENT_DEADLINE_SECONDS] * 2
        summary = result['rows'][0]['run_summary']
        assert summary['author'] == {'id': 8, 'name': 'Viewer', 'email': 'v@x'}
        assert summary['message_count'] == 2 and summary['tokens'] == 9
        assert result['rows'][0]['message_groups_count'] == 2

    def test_the_owner_project_defaults_to_the_current_project(self, history, monkeypatch):
        _list_runs(history, monkeypatch, _page(history, [(1, str(uuid.uuid4()), 3)]))

        _name, kwargs = history.harness.rpc_calls[-1]
        assert kwargs['root_entity_project_id'] == 2

    def test_a_model_filter_asks_usage_for_its_conversations_before_reading_the_page(self, history, monkeypatch):
        harness = history.harness
        harness.answers['usage_root_entity_conversations'] = []

        result = _list_runs(history, monkeypatch, _page(history, []), filters={'model': 'm'})

        assert harness.rpc_calls[0][0] == 'usage_root_entity_conversations'
        assert harness.rpc_calls[0][1]['model_name'] == 'm'
        assert harness.calls_while_session_open == []
        assert result['model_filter_unavailable'] is False

    def test_unknown_model_usage_is_reported(self, history, monkeypatch):
        history.harness.errors['usage_root_entity_conversations'] = RuntimeError('usage down')

        result = _list_runs(history, monkeypatch, _page(history, []), filters={'model': 'm'})

        assert result['model_filter_unavailable'] is True

    def test_a_slow_page_read_does_not_spend_the_enrichment_budget(self, history, monkeypatch):
        harness = history.harness
        monkeypatch.setattr(history.module, 'ENRICHMENT_DEADLINE_SECONDS', 0.3)
        run_uuid = str(uuid.uuid4())
        harness.answers['usage_root_entity_conversations'] = [run_uuid]
        harness.answers['usage_conversation_totals'] = {run_uuid: {'tokens': 9}}
        page = _page(history, [(1, run_uuid, None)])

        result = _list_runs(history, monkeypatch, page, filters={'model': 'm'}, read_seconds=0.5)

        assert result['rows'][0]['run_summary']['tokens'] == 9

    def test_failed_usage_leaves_usage_columns_empty_and_is_asked_once(self, history, monkeypatch):
        harness = history.harness
        harness.errors['usage_conversation_totals'] = RuntimeError('usage down')
        run_uuid = str(uuid.uuid4())
        page = _page(history, [(1, run_uuid, None)], facet_runs={'uuids': [run_uuid], 'user_ids': set()})

        result = _list_runs(history, monkeypatch, page)

        summary = result['rows'][0]['run_summary']
        assert summary['usage_available'] is False and summary['tokens'] is None
        assert summary['author'] is None
        assert result['facets']['models'] is None
        assert [name for name, _kwargs in harness.rpc_calls] == ['usage_conversation_totals']
