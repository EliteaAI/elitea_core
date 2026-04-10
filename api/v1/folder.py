from flask import request
from pydantic import ValidationError
from sqlalchemy import desc, asc, or_, and_, Integer
from tools import api_tools, auth, db, config as c
from tools import serialize

from ...models.all import SelectedConversations
from ...models.conversation import Conversation
from ...models.enums.all import ParticipantTypes
from ...models.folder import ConversationFolder
from ...models.participants import Participant, ParticipantMapping
from ...models.pd.conversation import ConversationList
from ...models.pd.folder import FolderCreate, FolderUpdate, FolderDetails, FolderList
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):

    @auth.decorators.check_api({
        "permissions": ["models.chat.folders.get"],
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

            user_id = auth.current_user().get("id")

            participant_subquery = session.query(Participant.id).filter(
                Participant.entity_meta['id'].astext.cast(Integer) == user_id,
                Participant.entity_name == ParticipantTypes.user.value
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
                Conversation.folder_id.is_(None),
                Conversation.id.in_(distinct_conversation_subquery)
            )

            if q:
                query = query.where(Conversation.name.ilike(f'%{q}%'))

            if sources:
                sources = list(set(i.strip().lower() for i in sources.split(',')))
                query = query.where(Conversation.source.in_(sources))

            query = query.order_by(sorting(sorting_by))

            total = query.count()
            query = query.limit(limit).offset(offset)
            result = query.all()

            folder_query = session.query(ConversationFolder).outerjoin(Conversation).filter(
                or_(
                    ConversationFolder.owner_id == user_id,
                    Conversation.id.in_(distinct_conversation_subquery)
                )
            )

            if q:
                q = f"%{q.lower()}%"
                folder_query = folder_query.filter(
                    or_(
                        ConversationFolder.name.ilike(q),  # Filter by folder name
                        Conversation.id.in_(
                            session.query(Conversation.id).filter(
                                Conversation.name.ilike(q)  # Filter by conversation name
                            )
                        )
                    )
                )

            total_folders = folder_query.count()
            folders = folder_query.all()
            folder_data = []

            if folders:
                for folder in folders:
                    conversations_query = session.query(Conversation).filter(
                        and_(
                            Conversation.folder_id == folder.id,
                            Conversation.id.in_(distinct_conversation_subquery)
                        )
                    )
                    if q:
                        conversations_query = conversations_query.filter(
                            Conversation.name.ilike(q)
                        )
                    conversations = conversations_query.all()

                    folder_item = serialize(FolderList.model_validate(folder))
                    folder_item["conversations"] = [
                       {
                           "participants_count": len(conversation.participants),
                           # TODO rename to message_groups_count
                           "messages_count": conversation.message_groups.count(),
                           "users_count": sum(1 for p in conversation.participants if p.entity_name == ParticipantTypes.user.value),
                           **serialize(ConversationList.from_orm(conversation)),
                       } for conversation in conversations
                    ]
                    folder_data.append(folder_item)

            selected_conversation_id = None
            existing_selection = session.query(SelectedConversations).filter(
                SelectedConversations.user_id == user_id
            ).first()
            if existing_selection:
                selected_conversation_id = existing_selection.conversation_id

            return {
                "total_folders": total_folders,
                "folders": folder_data,
                "total_ungrouped": total,
                "selected_conversation_id": selected_conversation_id,
                "ungrouped_conversations": [
                    {
                        **serialize(ConversationList.from_orm(i)),
                        "participants_count": len(i.participants),
                        # TODO rename to message_groups_count
                        "messages_count": i.message_groups.count(),
                        "users_count": sum(1 for p in i.participants if p.entity_name == ParticipantTypes.user.value),
                    } for i in result
                ],
            }, 200

    # @auth.decorators.check_api({
    #     "permissions": ["models.chat.folders.list"],
    #     "recommended_roles": {
    #         c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
    #         c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
    #     },
    # })
    # @api_tools.endpoint_metrics
    # def get(self, project_id: int, folder_id: int = None, **kwargs):
    #     """
    #     Fetch a list of folders or details of a specific folder.
    #     """
    #     with db.get_session(project_id) as session:
    #         if folder_id:
    #             folder = session.query(ConversationFolder).filter(
    #                 ConversationFolder.id == folder_id
    #             ).first()
    #             if not folder:
    #                 return {"error": "Folder not found"}, 404
    #             return serialize(FolderDetails.model_validate(folder)), 200
    #
    #         # Fetch all folders
    #         limit = request.args.get('limit', default=10, type=int)
    #         offset = request.args.get('offset', default=0, type=int)
    #         sort_by = request.args.get('sort_by', default='created_at')
    #         sort_order = request.args.get('sort_order', default='desc')
    #         sorting_by = getattr(ConversationFolder, sort_by)
    #         sorting = desc if sort_order == 'desc' else asc
    #
    #         query = session.query(ConversationFolder).order_by(sorting(sorting_by))
    #         total = query.count()
    #         folders = query.limit(limit).offset(offset).all()
    #
    #         return {
    #             'total': total,
    #             'rows': [serialize(FolderList.model_validate(folder)) for folder in folders]
    #         }, 200

    @auth.decorators.check_api({
        "permissions": ["models.chat.folders.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        """
        Create a new folder.
        """
        raw = dict(request.json)
        user_id = auth.current_user().get("id")
        raw['owner_id'] = user_id

        try:
            parsed = FolderCreate.model_validate(raw)
        except ValidationError as e:
            return e.errors(), 400

        with db.get_session(project_id) as session:
            new_folder = ConversationFolder(**parsed.dict())
            session.add(new_folder)
            session.commit()
            return serialize(FolderDetails.from_orm(new_folder)), 201

    @auth.decorators.check_api({
        "permissions": ["models.chat.folders.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def put(self, project_id: int, folder_id: int, **kwargs):
        """
        Update an existing folder.
        """
        raw = dict(request.json)

        try:
            parsed = FolderUpdate.model_validate(raw)
        except ValidationError as e:
            return e.errors(), 400

        with db.get_session(project_id) as session:
            folder = session.query(ConversationFolder).filter(
                ConversationFolder.id == folder_id
            ).first()
            if not folder:
                return {"error": "Folder not found"}, 404

            for key, value in parsed.dict(exclude_unset=True).items():
                setattr(folder, key, value)

            session.commit()
            return serialize(FolderDetails.from_orm(folder)), 200

    @auth.decorators.check_api({
        "permissions": ["models.chat.folders.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, folder_id: int):
        """
        Delete a folder.
        """
        with db.get_session(project_id) as session:
            folder = session.query(ConversationFolder).filter(
                ConversationFolder.id == folder_id
            ).first()
            if not folder:
                return {"error": "Folder not found"}, 404

            # Optionally, handle conversations in the folder (e.g., move them to a default folder or delete them)
            conversations = session.query(Conversation).filter(
                Conversation.folder_id == folder_id
            ).all()
            for conversation in conversations:
                conversation.folder_id = None  # Remove folder association

            session.delete(folder)
            session.commit()
            return {}, 204


class API(api_tools.APIBase):
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:folder_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
