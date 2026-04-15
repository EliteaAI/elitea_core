import logging

from flask import request
from pydantic import ValidationError
from sqlalchemy import desc, asc, or_, and_, Integer, func
from tools import api_tools, auth, db, config as c
from tools import serialize

log = logging.getLogger(__name__)

from ...models.all import SelectedConversations
from ...models.conversation import Conversation
from ...models.enums.all import ParticipantTypes
from ...models.folder import ConversationFolder
from ...models.message_group import ConversationMessageGroup
from ...models.participants import Participant, ParticipantMapping
from ...models.pd.conversation import ConversationList
from ...models.pd.folder import FolderCreate, FolderUpdate, FolderDetails, FolderList
from ...utils.constants import PROMPT_LIB_MODE


POSITION_GAP = 1_000_000  # Large gap for ~20 halvings before collision

def recalculate_folder_positions(session, user_id: int) -> None:
    """Reset all folder positions with fresh gaps. Called only when collision imminent."""
    folders = session.query(ConversationFolder).filter(
        ConversationFolder.owner_id == user_id
    ).order_by(
        desc(ConversationFolder.position),
        ConversationFolder.created_at  # Tiebreaker preserves current order
    ).all()

    # Reassign positions with fresh gaps
    for i, folder in enumerate(folders):
        folder.position = (len(folders) - i) * POSITION_GAP

    session.flush()


def check_needs_recalculation(pos_above: int | None, pos_below: int | None) -> bool:
    """Check if inserting between these positions would cause a collision."""
    if pos_above is None or pos_below is None:
        return False  # Inserting at top or bottom - always room

    gap = pos_above - pos_below
    # If gap is 1 or less, we can't insert between them
    return gap <= 1


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
            # For ungrouped conversations sorting
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
            # Sort at database level - highest position first, created_at as tiebreaker
            folders = folder_query.order_by(
                desc(ConversationFolder.position),
                ConversationFolder.created_at
            ).all()

            folder_data = []

            if folders:
                # Fetch all conversations at once to avoid N+1 query problem
                folder_ids = [f.id for f in folders]
                all_conversations = session.query(Conversation).filter(
                    Conversation.folder_id.in_(folder_ids),
                    Conversation.id.in_(distinct_conversation_subquery)
                ).all()

                # Group conversations by folder_id
                from collections import defaultdict
                conv_by_folder = defaultdict(list)
                for conv in all_conversations:
                    conv_by_folder[conv.folder_id].append(conv)

                folder_conv_ids = [conv.id for conv in all_conversations]
                mg_counts_folder = dict(
                    session.query(
                        ConversationMessageGroup.conversation_id,
                        func.count(ConversationMessageGroup.id)
                    ).filter(
                        ConversationMessageGroup.conversation_id.in_(folder_conv_ids)
                    ).group_by(ConversationMessageGroup.conversation_id).all()
                ) if folder_conv_ids else {}

                for folder in folders:
                    conversations = conv_by_folder.get(folder.id, [])
                    if q:
                        conversations = [c for c in conversations if q.lower() in c.name.lower()]

                    folder_item = serialize(FolderList.model_validate(folder))
                    folder_item["conversations"] = [
                       {
                           "folder_id": folder.id,
                           "participants_count": len(conversation.participants),
                           "messages_count": mg_counts_folder.get(conversation.id, 0),
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

            ungrouped_ids = [conv.id for conv in result]
            mg_counts_ungrouped = dict(
                session.query(
                    ConversationMessageGroup.conversation_id,
                    func.count(ConversationMessageGroup.id)
                ).filter(
                    ConversationMessageGroup.conversation_id.in_(ungrouped_ids)
                ).group_by(ConversationMessageGroup.conversation_id).all()
            ) if ungrouped_ids else {}

            return {
                "total_folders": total_folders,
                "folders": folder_data,
                "total_ungrouped": total,
                "selected_conversation_id": selected_conversation_id,
                "ungrouped_conversations": [
                    {
                        "folder_id": None,
                        **serialize(ConversationList.from_orm(i)),
                        "participants_count": len(i.participants),
                        "messages_count": mg_counts_ungrouped.get(i.id, 0),
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
            # If position not provided, calculate the highest position + POSITION_GAP (new folder goes to top)
            folder_data = parsed.dict()
            if folder_data.get('position') is None:
                # Get max position for this user's folders
                from sqlalchemy import func as sql_func
                max_position = session.query(sql_func.max(ConversationFolder.position)).filter(
                    ConversationFolder.owner_id == user_id
                ).scalar()
                folder_data['position'] = (max_position or 0) + POSITION_GAP

            new_folder = ConversationFolder(**folder_data)
            session.add(new_folder)
            session.commit()
            log.info(f"Created folder {new_folder.id} with position {new_folder.position} for user {user_id}")
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
        """Update an existing folder."""
        raw = dict(request.json)
        user_id = auth.current_user().get("id")

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

            # Handle position update with collision detection
            if parsed.position is not None:
                old_position = folder.position
                new_position = parsed.position

                # Check if neighbor IDs provided (precise positioning context from frontend)
                has_neighbor_context = parsed.neighbor_above_id is not None or parsed.neighbor_below_id is not None

                # Determine user's intent BEFORE any rebalancing
                if has_neighbor_context:
                    # Precise intent from neighbor IDs
                    intent_top = parsed.neighbor_above_id is None and parsed.neighbor_below_id is not None
                    intent_bottom = parsed.neighbor_below_id is None and parsed.neighbor_above_id is not None
                else:
                    # Fallback: infer intent from position values
                    current_max = session.query(func.max(ConversationFolder.position)).filter(
                        ConversationFolder.owner_id == user_id,
                        ConversationFolder.id != folder_id
                    ).scalar() or 0

                    current_min = session.query(func.min(ConversationFolder.position)).filter(
                        ConversationFolder.owner_id == user_id,
                        ConversationFolder.id != folder_id
                    ).scalar() or 0

                    intent_top = new_position > current_max
                    intent_bottom = new_position <= current_min

                # Check if exact position collision exists (another folder has same position)
                exact_collision = session.query(ConversationFolder).filter(
                    ConversationFolder.owner_id == user_id,
                    ConversationFolder.position == new_position,
                    ConversationFolder.id != folder_id
                ).first()

                # Find neighbors at target position
                pos_above = session.query(func.min(ConversationFolder.position)).filter(
                    ConversationFolder.owner_id == user_id,
                    ConversationFolder.position > new_position,
                    ConversationFolder.id != folder_id
                ).scalar()

                pos_below = session.query(func.max(ConversationFolder.position)).filter(
                    ConversationFolder.owner_id == user_id,
                    ConversationFolder.position < new_position,
                    ConversationFolder.id != folder_id
                ).scalar()

                # Rebalance if: exact collision OR gap too small for halving
                needs_rebalance = exact_collision is not None or check_needs_recalculation(pos_above, pos_below)
                if needs_rebalance:
                    reason = "exact position collision" if exact_collision else "gap too small"
                    log.info(f"Rebalancing folders for user {user_id} due to {reason}")
                    # Rebalance all folders first
                    recalculate_folder_positions(session, user_id)

                    # After rebalance, get positions using neighbor IDs (most accurate)
                    if has_neighbor_context:
                        pos_above = None
                        pos_below = None

                        if parsed.neighbor_above_id:
                            neighbor_above = session.query(ConversationFolder).filter(
                                ConversationFolder.id == parsed.neighbor_above_id
                            ).first()
                            pos_above = neighbor_above.position if neighbor_above else None

                        if parsed.neighbor_below_id:
                            neighbor_below = session.query(ConversationFolder).filter(
                                ConversationFolder.id == parsed.neighbor_below_id
                            ).first()
                            pos_below = neighbor_below.position if neighbor_below else None
                    else:
                        # Fallback: use min/max for top/bottom intent
                        new_max = session.query(func.max(ConversationFolder.position)).filter(
                            ConversationFolder.owner_id == user_id,
                            ConversationFolder.id != folder_id
                        ).scalar() or 0

                        new_min = session.query(func.min(ConversationFolder.position)).filter(
                            ConversationFolder.owner_id == user_id,
                            ConversationFolder.id != folder_id
                        ).scalar() or 0

                        if intent_top:
                            pos_above = None
                            pos_below = new_max
                        elif intent_bottom:
                            pos_above = new_min
                            pos_below = None
                        else:
                            # Middle without context - use folder's rebalanced position
                            pos_above = session.query(func.min(ConversationFolder.position)).filter(
                                ConversationFolder.owner_id == user_id,
                                ConversationFolder.position > folder.position,
                                ConversationFolder.id != folder_id
                            ).scalar()

                            pos_below = session.query(func.max(ConversationFolder.position)).filter(
                                ConversationFolder.owner_id == user_id,
                                ConversationFolder.position < folder.position,
                                ConversationFolder.id != folder_id
                            ).scalar()

                    # Calculate new position based on neighbors
                    if pos_above is None and pos_below is None:
                        new_position = POSITION_GAP  # Only folder
                    elif pos_above is None:
                        new_position = pos_below + POSITION_GAP  # Top
                    elif pos_below is None:
                        new_position = pos_above // 2  # Bottom
                    else:
                        new_position = (pos_above + pos_below) // 2  # Middle

                folder.position = new_position
                log.info(f"Updated folder {folder_id} position from {old_position} to {new_position}")

            # Update other fields (exclude position and neighbor IDs which are not stored)
            for key, value in parsed.dict(exclude_unset=True, exclude={'position', 'neighbor_above_id', 'neighbor_below_id'}).items():
                setattr(folder, key, value)

            session.commit()
            return serialize(FolderDetails.from_orm(folder)), 200

    @auth.decorators.check_api({
        "permissions": ["models.chat.folders.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def patch(self, project_id: int, folder_id: int, **kwargs):
        """Update folder pin status."""
        raw = dict(request.json)
        is_pinned_raw = raw.get('is_pinned')

        if is_pinned_raw is None:
            return {"error": "is_pinned is required"}, 400

        if isinstance(is_pinned_raw, int):
            is_pinned = is_pinned_raw != 0
        elif isinstance(is_pinned_raw, str):
            is_pinned = is_pinned_raw.lower() in ('true', '1')
        else:
            return {"error": "is_pinned must be a boolean value"}, 400

        with db.get_session(project_id) as session:
            folder = session.query(ConversationFolder).filter(
                ConversationFolder.id == folder_id
            ).first()

            if not folder:
                return {"error": "Folder not found"}, 404

            meta = dict(folder.meta) if folder.meta else {}
            meta['is_pinned'] = is_pinned
            folder.meta = meta

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
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:folder_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
