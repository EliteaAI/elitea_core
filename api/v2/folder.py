import logging
from datetime import datetime, timedelta

from flask import request
from pydantic import ValidationError
from sqlalchemy import desc, asc, or_, and_, Integer, func
from tools import api_tools, auth, db, config as c, rpc_tools, register_openapi
from tools import serialize

log = logging.getLogger(__name__)


DATE_GROUP_ORDER = ['today', 'this_week', 'older']


def get_date_boundaries():
    """Calculate date boundaries for grouping conversations."""
    now = datetime.utcnow()
    today_start = datetime(now.year, now.month, now.day)
    week_ago_start = today_start - timedelta(days=7)
    return today_start, week_ago_start


def build_date_group_filter(model_field, group_name: str):
    """Build SQLAlchemy filter for a specific date group."""
    today_start, week_ago_start = get_date_boundaries()

    if group_name == 'today':
        return model_field >= today_start
    elif group_name == 'this_week':
        return and_(model_field >= week_ago_start, model_field < today_start)
    elif group_name == 'older':
        return model_field < week_ago_start
    return None

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

    @register_openapi(
        name="List Folders and Conversations",
        description="List conversation folders and their conversations with date-group filtering, folder-level pagination, and optional grouped sidebar view",
        mcp_description="""
        USE to render the conversation sidebar (grouped mode), to list conversations inside a specific folder, or
        to paginate conversations by date group.

        DO NOT USE for a flat conversation list without folder structure → use list_conversations instead.
        DO NOT USE to get folder details only without conversations — this endpoint always returns conversations too.

        Mode selection guide:
        - UI sidebar rendering: add grouped=true
        - Folder contents: add folder_id=<N>
        - Date-filtered view: add date_group=today / this_week / older
        - Simple flat list: omit all special params

        Examples:
        1. Full sidebar view: GET .../folder/prompt_lib/42?grouped=true
        2. Contents of folder 5: GET .../folder/prompt_lib/42?folder_id=5&limit=20
        3. Only today's conversations: GET .../folder/prompt_lib/42?date_group=today
        4. Search conversations in all folders: GET .../folder/prompt_lib/42?query=sprint
        """,
        tags=["elitea_core/chat"],
        mcp_tool=True,
        parameters=[
            {"name": "query", "in": "query", "required": False, "schema": {"type": "string"}, "description": "Search query."},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Pagination limit."},
            {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Pagination offset."},
            {"name": "grouped", "in": "query", "required": False, "schema": {"type": "boolean"}, "description": "Return grouped response format."},
            {"name": "folder_id", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Return conversations for a specific folder."},
            {"name": "date_group", "in": "query", "required": False, "schema": {"type": "string", "enum": ["today", "this_week", "older"]}, "description": "Return conversations for a specific date group."},
            {"name": "source", "in": "query", "required": False, "schema": {"type": "string"}, "description": "Comma-separated conversation sources."},
            {"name": "sort_by", "in": "query", "required": False, "schema": {"type": "string"}, "description": "Sort field."},
            {"name": "sort_order", "in": "query", "required": False, "schema": {"type": "string", "enum": ["asc", "desc"]}, "description": "Sort order."},
        ],
        available_to_users=True,
    )

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
            limit = request.args.get('limit', default=10, type=int)
            offset = request.args.get('offset', default=0, type=int)
            date_group = request.args.get('date_group')  # today, this_week, older
            folder_id_param = request.args.get('folder_id', type=int)  # for folder pagination
            grouped = request.args.get('grouped', default='false', type=str).lower() == 'true'
            sort_by = request.args.get('sort_by', default='created_at')
            sorting_by = getattr(Conversation, sort_by)
            sort_order = request.args.get('sort_order', default='desc')
            sorting = desc if sort_order == 'desc' else asc

            user_id = auth.current_user().get("id")
            rpc = rpc_tools.RpcMixin().rpc

            try:
                support_config = rpc.timeout(3).support_assistant_get_config()
            except Exception:
                support_config = {}
            is_support_project = support_config.get('project_id') == project_id
            user_is_admin = rpc.timeout(3).admin_check_user_is_admin(
                project_id, user_id
            ) if is_support_project else False

            if is_support_project:
                sources = ['support']
                if user_is_admin:
                    distinct_conversation_subquery = session.query(Conversation.id).distinct().filter(
                        Conversation.source == 'support'
                    ).subquery()
                else:
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
                        Conversation.source == 'support',
                        Participant.id.in_(participant_subquery)
                    ).subquery()
            else:
                sources = list(set(
                    i.strip().lower() for i in request.args.get('source', default='elitea').split(',')
                ))
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

            date_field = func.coalesce(Conversation.updated_at, Conversation.created_at)

            # Folder pagination: return conversations for a specific folder
            if folder_id_param:
                folder = session.query(ConversationFolder).filter(
                    ConversationFolder.id == folder_id_param
                ).first()
                if not folder:
                    return {"error": "Folder not found"}, 404

                folder_conv_query = session.query(Conversation).where(
                    Conversation.folder_id == folder_id_param,
                    Conversation.id.in_(distinct_conversation_subquery)
                )
                if q:
                    folder_conv_query = folder_conv_query.where(Conversation.name.ilike(f'%{q}%'))
                if sources:
                    folder_conv_query = folder_conv_query.where(Conversation.source.in_(sources))

                folder_conv_query = folder_conv_query.order_by(sorting(sorting_by), Conversation.id.desc())
                total = folder_conv_query.count()
                folder_conv_query = folder_conv_query.limit(limit).offset(offset)
                result = folder_conv_query.all()

                conv_ids = [conv.id for conv in result]
                mg_counts = dict(
                    session.query(
                        ConversationMessageGroup.conversation_id,
                        func.count(ConversationMessageGroup.id)
                    ).filter(
                        ConversationMessageGroup.conversation_id.in_(conv_ids)
                    ).group_by(ConversationMessageGroup.conversation_id).all()
                ) if conv_ids else {}

                return {
                    "folder_id": folder_id_param,
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "conversations": [
                        {
                            "folder_id": folder_id_param,
                            **serialize(ConversationList.from_orm(i)),
                            "participants_count": len(i.participants),
                            "messages_count": mg_counts.get(i.id, 0),
                            "users_count": sum(1 for p in i.participants if p.entity_name == ParticipantTypes.user.value),
                        } for i in result
                    ],
                }, 200

            base_query = session.query(Conversation).where(
                Conversation.folder_id.is_(None),
                Conversation.id.in_(distinct_conversation_subquery)
            )

            if q:
                base_query = base_query.where(Conversation.name.ilike(f'%{q}%'))

            if sources:
                base_query = base_query.where(Conversation.source.in_(sources))

            # Date group pagination: return conversations for a specific date group
            if date_group:
                date_filter = build_date_group_filter(date_field, date_group.lower())
                if date_filter is not None:
                    query = base_query.where(date_filter)
                else:
                    query = base_query
                query = query.order_by(sorting(sorting_by), Conversation.id.desc())
                total = query.count()
                query = query.limit(limit).offset(offset)
                result = query.all()

                ungrouped_ids = [conv.id for conv in result]
                mg_counts_ungrouped = dict(
                    session.query(
                        ConversationMessageGroup.conversation_id,
                        func.count(ConversationMessageGroup.id)
                    ).filter(
                        ConversationMessageGroup.conversation_id.in_(ungrouped_ids)
                    ).group_by(ConversationMessageGroup.conversation_id).all()
                ) if ungrouped_ids else {}

                selected_conversation_id = None
                existing_selection = session.query(SelectedConversations).filter(
                    SelectedConversations.user_id == user_id
                ).first()
                if existing_selection:
                    selected_conversation_id = existing_selection.conversation_id

                return {
                    "date_group": date_group.lower(),
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "selected_conversation_id": selected_conversation_id,
                    "conversations": [
                        {
                            "folder_id": None,
                            **serialize(ConversationList.from_orm(i)),
                            "participants_count": len(i.participants),
                            "messages_count": mg_counts_ungrouped.get(i.id, 0),
                            "users_count": sum(1 for p in i.participants if p.entity_name == ParticipantTypes.user.value),
                        } for i in result
                    ],
                }, 200

            selected_conversation_id = None
            existing_selection = session.query(SelectedConversations).filter(
                SelectedConversations.user_id == user_id
            ).first()
            if existing_selection:
                selected_conversation_id = existing_selection.conversation_id

            folder_query = session.query(ConversationFolder).outerjoin(Conversation).filter(
                or_(
                    ConversationFolder.owner_id == user_id,
                    Conversation.id.in_(distinct_conversation_subquery)
                )
            )

            search_q = q
            if search_q:
                search_q = f"%{search_q.lower()}%"
                folder_query = folder_query.filter(
                    or_(
                        ConversationFolder.name.ilike(search_q),
                        Conversation.id.in_(
                            session.query(Conversation.id).filter(
                                Conversation.name.ilike(search_q)
                            )
                        )
                    )
                )

            total_folders = folder_query.count()
            folders = folder_query.order_by(
                desc(ConversationFolder.position),
                ConversationFolder.created_at
            ).all()

            folder_data = []
            if folders:
                folder_ids = [f.id for f in folders]
                all_conversations = session.query(Conversation).filter(
                    Conversation.folder_id.in_(folder_ids),
                    Conversation.id.in_(distinct_conversation_subquery)
                ).order_by(sorting(sorting_by)).all()

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
                    if search_q:
                        conversations = [c for c in conversations if search_q.strip('%').lower() in c.name.lower()]

                    folder_item = serialize(FolderList.model_validate(folder))
                    folder_item["total"] = len(conversations)
                    paginated_conversations = conversations[:limit] if grouped else conversations
                    folder_item["conversations"] = [
                       {
                           "folder_id": folder.id,
                           "participants_count": len(conversation.participants),
                           "messages_count": mg_counts_folder.get(conversation.id, 0),
                           "users_count": sum(1 for p in conversation.participants if p.entity_name == ParticipantTypes.user.value),
                           **serialize(ConversationList.from_orm(conversation)),
                       } for conversation in paginated_conversations
                    ]
                    folder_data.append(folder_item)

            if grouped:
                Pin = rpc.timeout(2).social_get_pin_model()
                pinned_conv_ids_query = session.query(Pin.entity_id).filter(
                    Pin.entity == 'conversation',
                    Pin.project_id == project_id
                ).order_by(desc(Pin.updated_at))

                pinned_conv_ids = [row[0] for row in pinned_conv_ids_query.all()]

                pinned_data = {"name": "pinned", "total": 0, "conversations": []}
                if pinned_conv_ids:
                    pinned_query = session.query(Conversation).where(
                        Conversation.id.in_(pinned_conv_ids),
                        Conversation.id.in_(distinct_conversation_subquery)
                    )
                    if q:
                        pinned_query = pinned_query.where(Conversation.name.ilike(f'%{q}%'))
                    if sources:
                        pinned_query = pinned_query.where(Conversation.source.in_(sources))

                    pinned_conversations_all = pinned_query.all()
                    pinned_conv_map = {c.id: c for c in pinned_conversations_all}
                    pinned_conversations_ordered = [
                        pinned_conv_map[cid] for cid in pinned_conv_ids
                        if cid in pinned_conv_map
                    ]

                    pinned_data["total"] = len(pinned_conversations_ordered)
                    pinned_data["conversations"] = pinned_conversations_ordered

                date_groups_data = []
                all_date_group_conv_ids = []

                for group_name in DATE_GROUP_ORDER:
                    group_filter = build_date_group_filter(date_field, group_name)
                    if group_filter is not None:
                        group_query = base_query.where(group_filter)
                        if pinned_conv_ids:
                            group_query = group_query.where(~Conversation.id.in_(pinned_conv_ids))
                        group_query = group_query.order_by(sorting(sorting_by), Conversation.id.desc())
                        group_total = group_query.count()
                        group_conversations = group_query.limit(limit).all()

                        all_date_group_conv_ids.extend([c.id for c in group_conversations])

                        date_groups_data.append({
                            "name": group_name,
                            "total": group_total,
                            "conversations": group_conversations,
                        })

                all_conv_ids = all_date_group_conv_ids + [c.id for c in pinned_data["conversations"]]
                mg_counts = dict(
                    session.query(
                        ConversationMessageGroup.conversation_id,
                        func.count(ConversationMessageGroup.id)
                    ).filter(
                        ConversationMessageGroup.conversation_id.in_(all_conv_ids)
                    ).group_by(ConversationMessageGroup.conversation_id).all()
                ) if all_conv_ids else {}

                def serialize_conversation(conv, folder_id=None):
                    return {
                        "folder_id": folder_id,
                        **serialize(ConversationList.from_orm(conv)),
                        "participants_count": len(conv.participants),
                        "messages_count": mg_counts.get(conv.id, 0),
                        "users_count": sum(1 for p in conv.participants if p.entity_name == ParticipantTypes.user.value),
                    }

                pinned_data["conversations"] = [
                    serialize_conversation(c) for c in pinned_data["conversations"]
                ]

                for group_data in date_groups_data:
                    group_data["conversations"] = [
                        serialize_conversation(c) for c in group_data["conversations"]
                    ]

                return {
                    "total_folders": total_folders,
                    "folders": folder_data,
                    "pinned": pinned_data,
                    "date_groups": date_groups_data,
                    "selected_conversation_id": selected_conversation_id,
                }, 200

            # Backward compatible response (grouped=false)
            date_groups_counts = {}
            for group_name in DATE_GROUP_ORDER:
                group_filter = build_date_group_filter(date_field, group_name)
                if group_filter is not None:
                    count = base_query.where(group_filter).count()
                    date_groups_counts[group_name] = count

            query = base_query.order_by(sorting(sorting_by), Conversation.id.desc())
            total = query.count()
            query = query.limit(limit).offset(offset)
            result = query.all()

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
                "date_groups": date_groups_counts,
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

    @register_openapi(
        name="Create Folder",
        description="Create a new named folder to organize conversations in the sidebar",
        mcp_description="""
        USE to create a new folder for organizing conversations.

        DO NOT USE to move a conversation into a folder → use update_conversation with folder_id.
        DO NOT USE to rename or reorder an existing folder → use update_folder.

        Examples:
        1. Create folder at top: { 'name': 'Sprint 12' } → placed at top automatically.
        2. Create folder at specific position: { 'name': 'Archive', 'position': 1000 } → placed at position 1000.
        """,
        tags=["elitea_core/chat"],
        mcp_tool=True,
        request_body=FolderCreate,
        available_to_users=True,
    )
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

    @register_openapi(
        name="Update Folder",
        description="Update a folder's name or reorder it in the sidebar using position with automatic collision detection and rebalancing",
        mcp_description="""
        USE to rename a folder or reorder it in the sidebar (e.g., after a drag-and-drop operation in the UI).

        DO NOT USE to delete a folder → use the folder DELETE endpoint.
        DO NOT USE to move conversations between folders → use update_conversation with folder_id.

        Reorder guidance: provide both position and neighbor_above_id/neighbor_below_id when available for most
        accurate placement. The backend will rebalance all positions automatically if needed.

        Examples:
        1. Rename: { 'name': 'Q3 Reviews' }
        2. Move to top (drag-and-drop): { 'position': 9999999, 'neighbor_above_id': null, 'neighbor_below_id': 3 }
        3. Move between two folders: { 'position': 500, 'neighbor_above_id': 7, 'neighbor_below_id': 2 }
        """,
        tags=["elitea_core/chat"],
        mcp_tool=True,
        request_body=FolderUpdate,
        available_to_users=True,
    )
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

    @register_openapi(
        name="Patch Folder",
        description="Update folder pin status.",
        tags=["elitea_core/chat"],
        parameters=[
            {"name": "folder_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Folder ID."},
        ],
        available_to_users=True,
    )
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

    @register_openapi(
        name="Delete Folder",
        description="Delete a folder and unassign conversations from it.",
        tags=["elitea_core/chat"],
        parameters=[
            {"name": "folder_id", "in": "path", "required": True, "schema": {"type": "integer"}, "description": "Folder ID."},
        ],
        available_to_users=True,
    )
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
