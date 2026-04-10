import urllib.parse
from flask import request
from werkzeug.datastructures import FileStorage
from tools import api_tools, auth, db, config as c, MinioClient, rpc_tools, serialize, register_openapi

from ...utils.attachments import process_uploaded_files, handle_chunked_upload, parse_filepath
from ...utils.internal_tools import get_default_attachment_bucket
from ...models.conversation import Conversation
from ...models.message_items.base import MessageItem
from ...models.message_items.attachment import AttachmentMessageItem
from ...models.message_group import ConversationMessageGroup
from ...models.pd.message import MessageGroupDetail
from ...utils.sio_utils import get_chat_room
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.sio_utils import SioEvents


class PromptLibAPI(api_tools.APIModeHandler):
    # @auth.decorators.check_api({
    #     "permissions": ["models.chat.attachments.list"],
    #     "recommended_roles": {
    #         c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
    #         c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
    #     }})
    # @api_tools.endpoint_metrics
    # def get(self, project_id: int, conversation_id: int, **kwargs):
    #     with (db.get_session(project_id) as session):
    #         conversation: Conversation = session.query(Conversation).filter(
    #             Conversation.id == conversation_id
    #         ).first()

    #         if conversation is None:
    #             return {"error": f"Conversation {conversation.id} not found"}, 404

    #         ret = []
    #         for msg_group in conversation.message_groups:
    #             for msg_item in msg_group.message_items:
    #                 if msg_item.item_type == AttachmentMessageItem.__mapper_args__['polymorphic_identity']:
    #                     ret.append(AttachmentMessageItemBase.from_orm(msg_item).model_dump())

    #         return ret, 200

    @register_openapi(
        name="Create Attachments",
        description="Upload file attachments to a conversation.",
        mcp_tool=True
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.attachments.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, conversation_id: int, **kwargs):
        # Check if this is a chunked upload request
        file_id = request.form.get("file_id")
        chunk_index = request.form.get("chunk_index")
        total_chunks = request.form.get("total_chunks")
        file_name = request.form.get("file_name")
        overwrite_attachments = bool(request.form.get("overwrite_attachments", 0, type=int))
        user_id = auth.current_user().get('id')

        # If chunk parameters are present, handle as chunked upload
        if all([file_id, chunk_index is not None, total_chunks, file_name]):
            chunk_file = request.files.get("file")
            if not chunk_file:
                return {"error": "No chunk file provided"}, 400

            return handle_chunked_upload(
                project_id=project_id,
                conversation_id=conversation_id,
                file_id=file_id,
                chunk_index=chunk_index,
                total_chunks=total_chunks,
                file_name=file_name,
                chunk_file=chunk_file,
                overwrite_attachments=overwrite_attachments,
                user_id=user_id,
            )

        # Otherwise, handle as regular file upload
        form_files: list[FileStorage] = request.files.getlist("file")

        if not form_files:
            return {"error": "No files provided"}, 400

        return process_uploaded_files(
            project_id=project_id,
            conversation_id=conversation_id,
            form_files=form_files,
            overwrite_attachments=overwrite_attachments,
            user_id=user_id,
        )

    @auth.decorators.check_api({
        "permissions": ["models.chat.attachments.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, conversation_id: int, **kwargs):
        # Get filename(s) - supports both single and multiple filenames
        # These can be either simple filenames or full filepaths (/{bucket}/{filename})
        filenames_param = request.args.getlist('filename')
        # Get keep_in_storage flag from request parameters (supports true/false and 1/0)
        keep_in_storage_param = request.args.get('keep_in_storage', 'false').lower()
        keep_in_storage = keep_in_storage_param in ('true', '1')

        if not filenames_param:
            return {"error": "Filename parameter is required"}, 400

        # Decode all filenames and remove duplicates while preserving order
        filenames_decoded = list(dict.fromkeys([urllib.parse.unquote(fn) for fn in filenames_param]))

        # Determine bucket and extract filenames
        # If filepath format (/{bucket}/{filename}), extract bucket from first file
        # Otherwise use default attachment bucket
        first_file = filenames_decoded[0]
        if first_file.startswith('/'):
            try:
                bucket_name, _ = parse_filepath(first_file)
                # Extract just filenames from filepaths
                filenames_to_delete = []
                for fp in filenames_decoded:
                    _, fn = parse_filepath(fp)
                    filenames_to_delete.append(fn)
            except ValueError as e:
                return {"error": str(e)}, 400
        else:
            # Simple filenames - use default bucket
            bucket_name = get_default_attachment_bucket(project_id)
            filenames_to_delete = filenames_decoded

        with db.get_session(project_id) as session:
            # Verify conversation exists
            conversation: Conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()

            if conversation is None:
                return {"error": f"Conversation {conversation_id} not found"}, 400

            mc = MinioClient.from_project_id(project_id)

            # Read filenames from bucket and validate all files exist (only if we need to delete from storage)
            if not keep_in_storage:
                try:
                    bucket_files = mc.list_files(bucket_name)
                    existing_filenames = {bf['name'] for bf in bucket_files}
                except Exception as e:
                    return {"error": f"Failed to list bucket files: {str(e)}"}, 400

                # Check if all requested files exist
                missing_files = []
                for filename in filenames_to_delete:
                    if filename not in existing_filenames:
                        missing_files.append(filename)

                if missing_files:
                    return {"error": f"Files not found in bucket: {', '.join(missing_files)}"}, 400

            # Delete all files from storage (only if keep_in_storage is False)
            deleted_files = []
            failed_files = []
            if not keep_in_storage:
                for filename in filenames_to_delete:
                    try:
                        mc.remove_file(bucket_name, filename)
                        deleted_files.append(filename)
                    except Exception as e:
                        failed_files.append({"filename": filename, "error": str(e)})
            else:
                # If keeping files in storage, mark all as "deleted" for database cleanup
                deleted_files = filenames_to_delete

            # Clean up database records for successfully deleted files first
            if deleted_files:
                try:
                    # Get all attachment items that match the deleted files from this bucket
                    # We need to delete the parent MessageItem records to properly cascade
                    attachment_items = session.query(AttachmentMessageItem).filter(
                        AttachmentMessageItem.bucket == bucket_name,
                        AttachmentMessageItem.name.in_(deleted_files)
                    ).all()

                    # Collect unique message group IDs that will be affected
                    affected_message_group_ids = set()
                    for item in attachment_items:
                        affected_message_group_ids.add(item.message_group_id)

                    # Delete the parent MessageItem records, which will cascade to AttachmentMessageItem
                    message_item_ids = [item.id for item in attachment_items]
                    if message_item_ids:
                        session.query(MessageItem).filter(
                            MessageItem.id.in_(message_item_ids)
                        ).delete(synchronize_session=False)

                    session.commit()

                    # Emit SIO events for all affected message groups
                    if affected_message_group_ids:
                        for msg_group_id in affected_message_group_ids:
                            # Refresh the message group from database after deletion
                            msg_group: ConversationMessageGroup = session.query(ConversationMessageGroup).filter(
                                ConversationMessageGroup.id == msg_group_id
                            ).first()

                            if msg_group:
                                room = get_chat_room(msg_group.conversation.uuid)
                                response_payload = serialize(MessageGroupDetail.model_validate(msg_group))
                                self.module.context.sio.emit(
                                    event=SioEvents.chat_message_sync,
                                    data=response_payload,
                                    room=room,
                                )

                except Exception as e:
                    session.rollback()
                    if keep_in_storage:
                        return {"error": f"Failed to remove attachment records from database: {str(e)}"}, 500
                    else:
                        return {"error": f"Files deleted from storage but failed to remove attachment records from database: {str(e)}"}, 500

            # If any files failed to delete from storage, return error after database cleanup
            if failed_files and not keep_in_storage:
                return {"error": "Failed to delete some files from storage", "failed_files": failed_files, "deleted_files": deleted_files}, 400

        return {}, 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
