import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Optional
from uuid import UUID

from pylon.core.tools import log
from sqlalchemy import Integer, and_, asc, case, desc, func, or_, select
from sqlalchemy.orm import aliased
from tools import auth, db, rpc_tools, serialize

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.conversation import ConversationListExtended
from ..models.pd.skill_run_history import SkillRunFilters, SkillRunStatus
from .conversation_access import visible_conversation_ids
from .conversation_utils import calculate_conversation_durations_batch
from .parallel_hitl import RUN_STOPPED_META_KEY

ENRICHMENT_DEADLINE_SECONDS = 3
SKILL_RUNS_MAX_LIMIT = 100
USAGE_SOURCE = 'usage'
AUTH_SOURCE = 'auth'

SkillReply = ConversationMessageGroup


def skill_participant_ids(skill_id: int, skill_project_id: int):
    return select(Participant.id).where(
        Participant.entity_name == ParticipantTypes.skill.value,
        Participant.entity_meta['id'].astext.cast(Integer) == skill_id,
        Participant.entity_meta['project_id'].astext.cast(Integer) == skill_project_id,
    )


def skill_reply_conditions(participant_ids) -> list:
    return [
        SkillReply.reply_to_id.isnot(None),
        SkillReply.author_participant_id.in_(participant_ids),
    ]


def run_status_expression():
    return case(
        (SkillReply.is_streaming.is_(True), SkillRunStatus.running.value),
        (SkillReply.meta[RUN_STOPPED_META_KEY].astext == 'true', SkillRunStatus.stopped.value),
        (SkillReply.meta['is_error'].astext == 'true', SkillRunStatus.error.value),
        else_=SkillRunStatus.success.value,
    )


def skill_turn_stats(participant_ids):
    prompt = aliased(ConversationMessageGroup)
    return select(
        SkillReply.conversation_id.label('conversation_id'),
        func.min(prompt.created_at).label('started_at'),
        (func.count(SkillReply.id) + func.count(func.distinct(prompt.id))).label('message_count'),
    ).join(
        prompt, prompt.id == SkillReply.reply_to_id,
    ).where(
        *skill_reply_conditions(participant_ids),
    ).group_by(SkillReply.conversation_id).subquery()


def prompting_user_id(prompt_author):
    return prompt_author.entity_meta['id'].astext.cast(Integer)


def prompts_by_users(participant_ids):
    prompt = aliased(ConversationMessageGroup)
    prompt_author = aliased(Participant)
    return select(
        SkillReply.conversation_id.label('conversation_id'),
        prompting_user_id(prompt_author).label('user_id'),
    ).join(
        prompt, prompt.id == SkillReply.reply_to_id,
    ).join(
        prompt_author, and_(
            prompt_author.id == prompt.author_participant_id,
            prompt_author.entity_name == ParticipantTypes.user.value,
        ),
    ).where(*skill_reply_conditions(participant_ids))


def latest_skill_turns(participant_ids, conversation_ids=None):
    prompt = aliased(ConversationMessageGroup)
    prompt_author = aliased(Participant)
    conditions = skill_reply_conditions(participant_ids)
    if conversation_ids is not None:
        conditions.append(SkillReply.conversation_id.in_(conversation_ids))
    return select(
        SkillReply.conversation_id.label('conversation_id'),
        run_status_expression().label('status'),
        prompting_user_id(prompt_author).label('user_id'),
    ).join(
        prompt, prompt.id == SkillReply.reply_to_id,
    ).outerjoin(
        prompt_author, and_(
            prompt_author.id == prompt.author_participant_id,
            prompt_author.entity_name == ParticipantTypes.user.value,
        ),
    ).where(*conditions).distinct(SkillReply.conversation_id).order_by(
        SkillReply.conversation_id,
        SkillReply.created_at.desc(),
        SkillReply.id.desc(),
    ).subquery()


def skill_turn_text_matches(participant_ids, pattern: str):
    turn_group_ids = select(SkillReply.id).where(*skill_reply_conditions(participant_ids)).union(
        select(SkillReply.reply_to_id).where(*skill_reply_conditions(participant_ids)),
    )
    return select(ConversationMessageGroup.conversation_id).join(
        TextMessageItem, TextMessageItem.message_group_id == ConversationMessageGroup.id,
    ).where(
        ConversationMessageGroup.id.in_(turn_group_ids),
        TextMessageItem.content.ilike(pattern),
    )


def skill_version_id_expression():
    return ParticipantMapping.entity_settings['version_id'].astext.cast(Integer)


def run_conditions(participant_ids, stats, filters: SkillRunFilters, text_query: Optional[str],
                   model_conversation_uuids: Optional[list]) -> list:
    conditions = []
    if text_query:
        pattern = f'%{text_query}%'
        conditions.append(or_(
            Conversation.name.ilike(pattern),
            Conversation.id.in_(skill_turn_text_matches(participant_ids, pattern)),
        ))
    if filters.created_from is not None:
        conditions.append(stats.c.started_at >= filters.created_from)
    if filters.created_to is not None:
        conditions.append(stats.c.started_at <= filters.created_to)
    if filters.author_id is not None:
        prompts = prompts_by_users(participant_ids).subquery()
        conditions.append(Conversation.id.in_(
            select(prompts.c.conversation_id).where(prompts.c.user_id == filters.author_id)
        ))
    if filters.version_id is not None:
        conditions.append(Conversation.id.in_(
            select(ParticipantMapping.conversation_id).where(
                ParticipantMapping.participant_id.in_(participant_ids),
                skill_version_id_expression() == filters.version_id,
            )
        ))
    if filters.status is not None:
        turns = latest_skill_turns(participant_ids)
        conditions.append(Conversation.id.in_(
            select(turns.c.conversation_id).where(turns.c.status == filters.status.value)
        ))
    if filters.model:
        matching_uuids = [UUID(conversation_uuid) for conversation_uuid in model_conversation_uuids or []]
        conditions.append(Conversation.uuid.in_(matching_uuids))
    return conditions


def offers_facets(filters: SkillRunFilters, text_query: Optional[str], offset: int) -> bool:
    return not offset and not text_query and filters == SkillRunFilters()


class EnrichmentBudget:
    def __init__(self, seconds: float):
        self.deadline = time.monotonic() + seconds
        self.failed_sources = set()

    def call(self, source: str, read: Callable):
        if source in self.failed_sources:
            return None
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            log.warning('Skill run history: %s skipped, the page budget is spent', source)
            self.failed_sources.add(source)
            return None
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            return executor.submit(read).result(timeout=remaining)
        except Exception as error:  # pylint: disable=W0703
            log.warning('Skill run history: %s did not answer within the page budget: %r', source, error)
            self.failed_sources.add(source)
            return None
        finally:
            executor.shutdown(wait=False)


class SkillUsageReader:
    def __init__(self, budget: EnrichmentBudget, project_id: int, skill_id: int, skill_project_id: int):
        self.budget = budget
        self.project_id = project_id
        self.skill_scope = {
            'root_entity_type': ParticipantTypes.skill.value,
            'root_entity_id': skill_id,
            'root_entity_project_id': skill_project_id,
        }

    def model_conversations(self, model_name: str) -> Optional[list]:
        return self._read('usage_root_entity_conversations', model_name=model_name)

    def conversation_totals(self, conversation_uuids: list) -> Optional[dict]:
        if not conversation_uuids:
            return {}
        return self._read('usage_conversation_totals', conversation_ids=conversation_uuids)

    def entity_models(self, conversation_uuids: list) -> Optional[list]:
        if not conversation_uuids:
            return []
        return self._read('usage_root_entity_models', conversation_ids=conversation_uuids)

    def _read(self, rpc_name: str, **kwargs):
        def read():
            rpc = getattr(rpc_tools.RpcMixin().rpc.timeout(ENRICHMENT_DEADLINE_SECONDS), rpc_name)
            return rpc(project_id=self.project_id, **kwargs, **self.skill_scope)
        return self.budget.call(USAGE_SOURCE, read)


def fetch_authors(budget: EnrichmentBudget, user_ids: set) -> dict:
    if not user_ids:
        return {}
    users = budget.call(AUTH_SOURCE, lambda: auth.list_users(user_ids=list(user_ids))) or []
    return {
        user['id']: {'id': user['id'], 'name': user.get('name'), 'email': user.get('email')}
        for user in users
    }


@dataclass
class SkillRunPage:
    total: int = 0
    conversations: list = field(default_factory=list)
    stats: dict = field(default_factory=dict)
    facet_runs: Optional[dict] = None


def read_skill_run_page(session, project_id: int, user_id: int, is_admin: bool, participant_ids,
                        filters: SkillRunFilters, text_query: Optional[str], source: Optional[str],
                        include_hidden: bool, limit: int, offset: int, sort_order: str,
                        model_conversation_uuids: Optional[list]) -> SkillRunPage:
    stats = skill_turn_stats(participant_ids)
    runs = session.query(Conversation, stats.c.started_at, stats.c.message_count).join(
        stats, stats.c.conversation_id == Conversation.id,
    ).where(Conversation.id.in_(visible_conversation_ids(session, user_id, is_admin)))
    if source:
        runs = runs.where(Conversation.source.in_({item.strip().lower() for item in source.split(',')}))
    if not include_hidden:
        runs = runs.where(or_(
            Conversation.meta['is_hidden'].astext == 'false',
            Conversation.meta['is_hidden'].astext.is_(None),
        ))

    page = SkillRunPage()
    if offers_facets(filters, text_query, offset):
        prompts = prompts_by_users(participant_ids).subquery()
        page.facet_runs = {
            'uuids': [str(run_uuid) for (run_uuid,) in runs.with_entities(Conversation.uuid)],
            'user_ids': {
                prompt_user_id for (prompt_user_id,) in session.execute(
                    select(prompts.c.user_id).where(
                        prompts.c.conversation_id.in_(runs.with_entities(Conversation.id).subquery()),
                    ).distinct()
                )
            },
        }

    runs = runs.where(*run_conditions(participant_ids, stats, filters, text_query, model_conversation_uuids))
    order = desc if sort_order == 'desc' else asc
    runs = runs.order_by(order(stats.c.started_at), order(Conversation.id))
    page.total = runs.count()
    rows = runs.limit(min(max(int(limit or 0), 1), SKILL_RUNS_MAX_LIMIT)).offset(max(int(offset or 0), 0)).all()
    if not rows:
        return page

    conversation_ids = [conversation.id for (conversation, _started_at, _count) in rows]
    version_ids = dict(session.execute(
        select(ParticipantMapping.conversation_id, skill_version_id_expression()).where(
            ParticipantMapping.conversation_id.in_(conversation_ids),
            ParticipantMapping.participant_id.in_(participant_ids),
        )
    ).all())
    turns = latest_skill_turns(participant_ids, conversation_ids)
    latest_turns = {
        conversation_id: (status, prompt_user_id)
        for conversation_id, status, prompt_user_id in session.execute(
            select(turns.c.conversation_id, turns.c.status, turns.c.user_id)
        )
    }
    durations = calculate_conversation_durations_batch(conversation_ids, session, participant_ids)
    participant_counts = dict(session.execute(
        select(ParticipantMapping.conversation_id, func.count(ParticipantMapping.id)).where(
            ParticipantMapping.conversation_id.in_(conversation_ids),
        ).group_by(ParticipantMapping.conversation_id)
    ).all())

    for conversation, started_at, message_count in rows:
        status, prompt_user_id = latest_turns.get(conversation.id, (None, None))
        page.conversations.append(serialize(conversation))
        page.stats[conversation.id] = {
            'started_at': started_at,
            'message_count': message_count,
            'duration': durations.get(conversation.id, 0.0),
            'participants_count': participant_counts.get(conversation.id, 0),
            'version_id': version_ids.get(conversation.id),
            'status': status,
            'user_id': prompt_user_id,
        }
    return page


def run_author(user_id: Optional[int], authors: dict) -> Optional[dict]:
    if user_id is None:
        return None
    return authors.get(user_id) or {'id': user_id, 'name': None, 'email': None}


def build_run_summary(conversation_uuid: str, stats: dict, authors: dict, usage_totals: Optional[dict]) -> dict:
    usage_known = usage_totals is not None
    totals = (usage_totals or {}).get(conversation_uuid) or {}
    return {
        'started_at': stats['started_at'],
        'message_count': stats['message_count'],
        'version_id': stats['version_id'],
        'status': stats['status'],
        'author': run_author(stats['user_id'], authors),
        'models': totals.get('models', []) if usage_known else None,
        'tokens': totals.get('tokens', 0) if usage_known else None,
        'cost': totals.get('cost', 0.0) if usage_known else None,
        'last_run_id': totals.get('last_run_id'),
        'usage_available': usage_known,
    }


def build_run_row(conversation: dict, stats: dict, authors: dict, usage_totals: Optional[dict]) -> dict:
    row = {
        **conversation,
        'duration': stats['duration'],
        'participants_count': stats['participants_count'],
        'message_groups_count': stats['message_count'],
        'users_count': 0,
        'run_summary': build_run_summary(str(conversation['uuid']), stats, authors, usage_totals),
    }
    return serialize(ConversationListExtended.model_validate(row).model_dump())


def build_facets(facet_runs: dict, authors: dict, usage: SkillUsageReader) -> dict:
    return {
        'authors': sorted(
            (run_author(user_id, authors) for user_id in facet_runs['user_ids']),
            key=lambda author: (author['name'] or '').lower(),
        ),
        'models': usage.entity_models(facet_runs['uuids']),
    }


def list_skill_runs(project_id: int, user_id: int, is_admin: bool, skill_id: int,
                    skill_project_id: Optional[int], filters: SkillRunFilters, text_query: Optional[str],
                    source: Optional[str], include_hidden: bool, limit: int, offset: int,
                    sort_order: str) -> dict:
    skill_project_id = project_id if skill_project_id is None else skill_project_id
    model_filter_usage = SkillUsageReader(
        EnrichmentBudget(ENRICHMENT_DEADLINE_SECONDS), project_id, skill_id, skill_project_id,
    )
    model_conversation_uuids = model_filter_usage.model_conversations(filters.model) if filters.model else None

    with db.get_session(project_id) as session:
        page = read_skill_run_page(
            session, project_id, user_id, is_admin, skill_participant_ids(skill_id, skill_project_id),
            filters, text_query, source, include_hidden, limit, offset, sort_order, model_conversation_uuids,
        )

    budget = EnrichmentBudget(ENRICHMENT_DEADLINE_SECONDS)
    usage = SkillUsageReader(budget, project_id, skill_id, skill_project_id)
    prompting_users = {stats['user_id'] for stats in page.stats.values() if stats['user_id'] is not None}
    facet_users = page.facet_runs['user_ids'] if page.facet_runs else set()
    authors = fetch_authors(budget, prompting_users | facet_users)
    usage_totals = usage.conversation_totals([str(conversation['uuid']) for conversation in page.conversations])
    return {
        'total': page.total,
        'rows': [
            build_run_row(conversation, page.stats[conversation['id']], authors, usage_totals)
            for conversation in page.conversations
        ],
        'facets': build_facets(page.facet_runs, authors, usage) if page.facet_runs else None,
        'model_filter_unavailable': bool(filters.model) and model_conversation_uuids is None,
    }
