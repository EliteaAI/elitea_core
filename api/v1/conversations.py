from flask import request
from tools import api_tools, auth, db, config as c, MinioClient, rpc_tools
from tools import serialize

from pydantic import ValidationError
from sqlalchemy import desc, asc, Integer, or_

from ...models.conversation import Conversation
from ...models.enums.all import ParticipantTypes
from ...models.participants import Participant, ParticipantMapping
from ...models.pd.conversation import ConversationListExtended, ConversationCreate, ConversationDetails
from ...models.pd.participant import ParticipantCreate, ParticipantEntityUser
from ...utils.conversation_utils import get_conversation_details, calculate_conversation_duration
from ...utils.participant_utils import add_participant_to_conversation
from ...utils.chat_feature_flags import get_context_manager_feature_flag
from ...utils.context_analytics import set_context_strategy
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": [
            "models.chat.conversations.list",
            "models.chat.conversations.list_custom",
        ],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        with db.get_session(project_id) as session:
            q = request.args.get('query')
            sources = request.args.get('source', default='elitea')
            limit = request.args.get('limit', default=10, type=int)
            offset = request.args.get('offset', default=0, type=int)
            sort_by = request.args.get('sort_by', default='created_at')
            sorting_by = getattr(Conversation, sort_by)
            sort_order = request.args.get('sort_order', default='desc')
            sorting = desc if sort_order == 'desc' else asc

            entity_name = request.args.get('entity_name', type=str)
            entity_meta_id = request.args.get('entity_meta_id', type=int)
            entity_meta_project_id = request.args.get('entity_meta_project_id', type=int)

            user_id = auth.current_user().get("id")

            user_is_admin: bool = rpc_tools.RpcMixin().rpc.timeout(3).admin_check_user_is_admin(project_id, user_id)
            single_participant = (entity_meta_id is not None) and (entity_meta_project_id is not None)
            participant_subquery_filters = [Participant.entity_name == ParticipantTypes.user.value]

            if not single_participant or not user_is_admin:
                participant_subquery_filters.append(
                    Participant.entity_meta['id'].astext.cast(Integer) == user_id,
                )

            participant_subquery = session.query(Participant.id).filter(
                *participant_subquery_filters
            ).subquery()


            distinct_conversation_subquery = session.query(Conversation.id).distinct().join(
                ParticipantMapping,
                Conversation.id == ParticipantMapping.conversation_id
            ).join(
                Participant,
                Participant.id == ParticipantMapping.participant_id
            ).filter(
                or_(
                    Conversation.is_private == False,
                    Participant.id.in_(participant_subquery)
                )
            ).subquery()

            query = session.query(Conversation).where(
                Conversation.id.in_(distinct_conversation_subquery)
            )

            if q:
                query = query.where(Conversation.name.ilike(f'%{q}%'))

            if sources:
                sources = list(set(i.strip().lower() for i in sources.split(',')))
                query = query.where(Conversation.source.in_(sources))

            if entity_name:
                query = query.filter(
                    Conversation.meta['single_participant']['entity_name'].astext == entity_name
                )

            if single_participant:
                query = query.filter(
                    Conversation.meta['single_participant']['entity_meta']['id'].astext.cast(Integer) == entity_meta_id,
                    Conversation.meta['single_participant']['entity_meta']['project_id'].astext.cast(Integer) == entity_meta_project_id
                )

            query = query.filter(
                or_(
                    Conversation.meta['is_hidden'].astext == 'false',
                    Conversation.meta['is_hidden'].astext.is_(None)
                )
            )
            query = query.order_by(sorting(sorting_by))

            total = query.count()
            query = query.limit(limit).offset(offset)
            result = query.all()

            rows = []

            for conversation in result:
                if 'elitea' not in sources:
                    duration = calculate_conversation_duration(conversation, session)
                else:
                    duration = -1
                conversation_dict = {
                    **serialize(conversation),
                    "duration": duration,
                    "participants_count": len(conversation.participants),
                    "message_groups_count": conversation.message_groups.count(),
                    "users_count": sum(1 for p in conversation.participants if p.entity_name == ParticipantTypes.user.value),
                }
                conversation_data = serialize(ConversationListExtended.model_validate(conversation_dict).model_dump())
                rows.append(conversation_data)

            return {
                'total': total,
                'rows': rows
            }, 200

    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        raw = dict(request.json)
        user_id = auth.current_user().get("id")
        raw['author_id'] = user_id

        try:
            parsed = ConversationCreate.model_validate(raw)
        except ValidationError as e:
            return e.errors(), 400

        from ...utils.utils import get_public_project_id  # pylint: disable=C0415
        public_project_id = get_public_project_id()
        if not parsed.is_private and public_project_id == project_id:
            return {"error": "Public conversation can not exist in public project"}, 400

        user_participant_data = ParticipantCreate(
            entity_name=ParticipantTypes.user,
            entity_meta=ParticipantEntityUser(id=user_id)
        )
        dummy_participant_data = ParticipantCreate(
            entity_name=ParticipantTypes.dummy,
            entity_meta={}
        )
        parsed.participants.append(user_participant_data)
        parsed.participants.append(dummy_participant_data)

        with db.get_session(project_id) as session:
            conversation_dict = parsed.model_dump(exclude={'participants'})
            new_conversation = Conversation(**conversation_dict)
            session.add(new_conversation)
            session.flush()
            for p_data in parsed.participants:
                add_participant_to_conversation(
                    project_id=project_id,
                    session=session,
                    participant=p_data,
                    conversation=new_conversation,
                    initiator_id=user_id
                )
                session.flush()

            if get_context_manager_feature_flag(
                project_id,
                session=session
            ):
                rpc_tools.RpcMixin().rpc.timeout(3).context_manager_set_strategy(
                    project_id=project_id,
                    conversation_id=new_conversation.id,
                )

            session.commit()
            conversation: ConversationDetails = get_conversation_details(
                session, new_conversation.id, project_id, user_id
            )
            serialized = serialize(conversation)

            # room = get_chat_room(new_conversation.uuid)
            # self.module.context.sio.emit(
            #     event=SioEvents.chat_conversation_create,
            #     data=serialized,
            #     room=room,
            # )
            return serialized, 201

    @auth.decorators.check_api({
        "permissions": ["models.chat.conversations.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, conversation_id: int):
        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()
            if conversation is None:
                return {"error": "Conversation not found"}, 404

            try:
                mc = MinioClient.from_project_id(project_id)
                bucket_name = f'conversation_{conversation.uuid}'
                mc.remove_bucket(bucket_name)
            except Exception:
                pass

            session.delete(conversation)
            session.commit()
            # room = get_chat_room(conversation.uuid)
            # self.module.context.sio.emit(
            #     event=SioEvents.chat_conversation_create,
            #     data=serialized,
            #     room=room,
            # )
            return {}, 204


class API(api_tools.APIBase):
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
