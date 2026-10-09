from typing import Optional
from uuid import UUID

from pylon.core.tools import log
from sqlalchemy import Integer, case, or_, select
from tools import auth, rpc_tools

from ..models.conversation import Conversation
from ..models.enums.all import ParticipantTypes
from ..models.message_group import ConversationMessageGroup
from ..models.message_items.text import TextMessageItem
from ..models.participants import Participant, ParticipantMapping
from ..models.pd.skill_run_history import SkillRunFilters, SkillRunStatus
from .parallel_hitl import RUN_STOPPED_META_KEY

USAGE_TOTALS_TIMEOUT = 5


def skill_participant_ids(skill_id: int, skill_project_id: Optional[int]):
    conditions = [
        Participant.entity_name == ParticipantTypes.skill.value,
        Participant.entity_meta['id'].astext.cast(Integer) == skill_id,
    ]
    if skill_project_id is not None:
        conditions.append(Participant.entity_meta['project_id'].astext.cast(Integer) == skill_project_id)
    return select(Participant.id).where(*conditions)


def run_status_expression():
    return case(
        (ConversationMessageGroup.is_streaming.is_(True), SkillRunStatus.running.value),
        (
            ConversationMessageGroup.meta[RUN_STOPPED_META_KEY].astext == 'true',
            SkillRunStatus.stopped.value,
        ),
        (ConversationMessageGroup.meta['is_error'].astext == 'true', SkillRunStatus.error.value),
        else_=SkillRunStatus.success.value,
    )


def last_skill_reply_statuses(participant_ids, conversation_ids=None):
    conditions = [
        ConversationMessageGroup.reply_to_id.isnot(None),
        ConversationMessageGroup.author_participant_id.in_(participant_ids),
    ]
    if conversation_ids is not None:
        conditions.append(ConversationMessageGroup.conversation_id.in_(conversation_ids))
    return select(
        ConversationMessageGroup.conversation_id,
        run_status_expression().label('status'),
    ).where(*conditions).distinct(ConversationMessageGroup.conversation_id).order_by(
        ConversationMessageGroup.conversation_id,
        ConversationMessageGroup.created_at.desc(),
        ConversationMessageGroup.id.desc(),
    ).subquery()


def skill_version_id_expression():
    return ParticipantMapping.entity_settings['version_id'].astext.cast(Integer)


def fetch_usage_totals(
        project_id: int, conversation_uuids: list, skill_id: int, skill_project_id: Optional[int],
        model_name: Optional[str] = None,
) -> Optional[dict]:
    if not conversation_uuids:
        return {}
    try:
        return rpc_tools.RpcMixin().rpc.timeout(USAGE_TOTALS_TIMEOUT).usage_conversation_totals(
            project_id=project_id,
            conversation_ids=conversation_uuids,
            root_entity_type=ParticipantTypes.skill.value,
            root_entity_id=skill_id,
            root_entity_project_id=skill_project_id,
            model_name=model_name,
        )
    except Exception:  # pylint: disable=W0703
        log.warning('Skill run history: usage totals unavailable for project %s', project_id)
        return None


def fetch_entity_models(
        project_id: int, conversation_uuids: list, skill_id: int, skill_project_id: Optional[int],
) -> Optional[list]:
    if not conversation_uuids:
        return []
    try:
        return rpc_tools.RpcMixin().rpc.timeout(USAGE_TOTALS_TIMEOUT).usage_root_entity_models(
            project_id=project_id,
            conversation_ids=conversation_uuids,
            root_entity_type=ParticipantTypes.skill.value,
            root_entity_id=skill_id,
            root_entity_project_id=skill_project_id,
        )
    except Exception:  # pylint: disable=W0703
        log.warning('Skill run history: run models unavailable for project %s', project_id)
        return None


def fetch_authors(author_ids: set) -> dict:
    if not author_ids:
        return {}
    try:
        users = auth.list_users(user_ids=list(author_ids))
    except Exception:  # pylint: disable=W0703
        log.warning('Skill run history: run authors unavailable')
        return {}
    return {
        user['id']: {'id': user['id'], 'name': user.get('name'), 'email': user.get('email')}
        for user in users
    }


def build_run_summary(conversation, version_id, status, author, usage_totals) -> dict:
    usage_known = usage_totals is not None
    totals = (usage_totals or {}).get(str(conversation.uuid)) or {}
    return {
        'version_id': version_id,
        'status': status,
        'author': author or {'id': conversation.author_id, 'name': None, 'email': None},
        'models': totals.get('models', []) if usage_known else None,
        'tokens': totals.get('tokens', 0) if usage_known else None,
        'cost': totals.get('cost', 0.0) if usage_known else None,
        'last_run_id': totals.get('last_run_id'),
        'usage_available': usage_known,
    }


class SkillRunHistory:
    def __init__(self, session, project_id: int, skill_id: int, skill_project_id: Optional[int],
                 filters: Optional[SkillRunFilters] = None):
        self.session = session
        self.project_id = project_id
        self.skill_id = skill_id
        self.skill_project_id = skill_project_id
        self.filters = filters or SkillRunFilters()
        self.participant_ids = skill_participant_ids(skill_id, skill_project_id)
        self.run_conversation_ids = select(ConversationMessageGroup.conversation_id).where(
            ConversationMessageGroup.author_participant_id.in_(self.participant_ids),
        )
        self.usage_totals = None
        self.usage_prefetched = False

    def restrict_to_runs(self, query):
        return query.where(Conversation.id.in_(self.run_conversation_ids))

    def offers_facets(self, offset: int, text_query: Optional[str]) -> bool:
        return not offset and not text_query and self.filters == SkillRunFilters()

    def facets(self, runs_query) -> dict:
        runs = runs_query.with_entities(Conversation.uuid, Conversation.author_id).all()
        author_ids = {author_id for (_uuid, author_id) in runs}
        authors = fetch_authors(author_ids)
        return {
            'authors': sorted(
                (authors.get(author_id) or {'id': author_id, 'name': None, 'email': None}
                 for author_id in author_ids),
                key=lambda author: (author['name'] or '').lower(),
            ),
            'models': fetch_entity_models(
                self.project_id, [str(run_uuid) for (run_uuid, _author_id) in runs],
                self.skill_id, self.skill_project_id,
            ),
        }

    def apply_filters(self, query, text_query: Optional[str] = None):
        if text_query:
            query = query.where(self._matches_text(text_query, self.run_conversation_ids))
        filters = self.filters
        if filters.created_from is not None:
            query = query.where(Conversation.created_at >= filters.created_from)
        if filters.created_to is not None:
            query = query.where(Conversation.created_at <= filters.created_to)
        if filters.author_id is not None:
            query = query.where(Conversation.author_id == filters.author_id)
        if filters.version_id is not None:
            query = query.where(Conversation.id.in_(
                select(ParticipantMapping.conversation_id).where(
                    ParticipantMapping.participant_id.in_(self.participant_ids),
                    skill_version_id_expression() == filters.version_id,
                )
            ))
        if filters.status is not None:
            statuses = last_skill_reply_statuses(self.participant_ids)
            query = query.where(Conversation.id.in_(
                select(statuses.c.conversation_id).where(statuses.c.status == filters.status.value)
            ))
        if filters.model:
            query = self._apply_model_filter(query)
        return query

    @staticmethod
    def _matches_text(text_query: str, run_conversation_ids):
        pattern = f'%{text_query}%'
        return or_(
            Conversation.name.ilike(pattern),
            Conversation.id.in_(
                select(ConversationMessageGroup.conversation_id).join(
                    TextMessageItem, TextMessageItem.message_group_id == ConversationMessageGroup.id,
                ).where(
                    ConversationMessageGroup.conversation_id.in_(run_conversation_ids),
                    TextMessageItem.content.ilike(pattern),
                )
            ),
        )

    def _apply_model_filter(self, query):
        candidate_uuids = [str(uuid) for (uuid,) in query.with_entities(Conversation.uuid).all()]
        self.usage_totals = fetch_usage_totals(
            self.project_id, candidate_uuids, self.skill_id, self.skill_project_id,
            model_name=self.filters.model,
        )
        self.usage_prefetched = True
        matching_uuids = [UUID(conversation_uuid) for conversation_uuid in self.usage_totals or {}]
        return query.where(Conversation.uuid.in_(matching_uuids))

    def summarize(self, conversations: list) -> dict:
        conversation_ids = [conversation.id for conversation in conversations]
        version_ids = dict(self.session.execute(
            select(ParticipantMapping.conversation_id, skill_version_id_expression()).where(
                ParticipantMapping.conversation_id.in_(conversation_ids),
                ParticipantMapping.participant_id.in_(self.participant_ids),
            )
        ).all())
        statuses_query = last_skill_reply_statuses(self.participant_ids, conversation_ids)
        statuses = dict(self.session.execute(
            select(statuses_query.c.conversation_id, statuses_query.c.status)
        ).all())
        authors = fetch_authors({conversation.author_id for conversation in conversations})
        usage_totals = self.usage_totals if self.usage_prefetched else fetch_usage_totals(
            self.project_id,
            [str(conversation.uuid) for conversation in conversations],
            self.skill_id,
            self.skill_project_id,
        )
        return {
            conversation.id: build_run_summary(
                conversation,
                version_ids.get(conversation.id),
                statuses.get(conversation.id),
                authors.get(conversation.author_id),
                usage_totals,
            )
            for conversation in conversations
        }
