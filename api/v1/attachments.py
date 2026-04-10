import urllib.parse
from flask import request
from werkzeug.datastructures import FileStorage
from pylon.core.tools import log
from tools import api_tools, auth, db, config as c, VaultClient, MinioClient, serialize, this

from ...utils.file_utils import sanitize_filename
from ...models.conversation import Conversation
from ...models.message_items.base import MessageItem
from ...models.message_items.attachment import AttachmentMessageItem
from ...models.message_group import ConversationMessageGroup
from ...models.participants import Participant
from ...models.pd.attachment import AttachmentMessageItemBase, AttachmentMessageItemCreated
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

    @auth.decorators.check_api({
        "permissions": ["models.chat.attachments.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, conversation_id: int, **kwargs):
        form_files: list[FileStorage] = request.files.getlist("file")
        overwrite_attachments = bool(request.form.get("overwrite_attachments", 0, type=int))

        if not form_files:
            return {"error": "No files provided"}, 400

        with db.get_session(project_id) as session:
            conversation: Conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()

            if conversation is None:
                return {"error": f"Conversation {conversation_id} not found"}, 404

            if conversation.attachment_participant_id:
                attachment_participant = session.query(Participant).filter(
                    Participant.id == conversation.attachment_participant_id
                ).first()
                if attachment_participant is None:
                    return {"error": f"Attachment storage with id {conversation.attachment_participant_id} not found"}, 400
                toolkit_details = this.module.get_toolkit_by_id(
                    project_id=attachment_participant.entity_meta['project_id'],
                    toolkit_id=attachment_participant.entity_meta['id']
                )
                bucket_name = toolkit_details.get('settings', {})['bucket']
            else:
                return {"error": f"Conversation {conversation.uuid} has no attachment storage"}, 400

            # Validate file upload count and total size
            vault_client = VaultClient()
            secrets = vault_client.get_secrets()
            chat_max_upload_count = int(secrets.get('chat_max_upload_count', 10))
            chat_max_upload_size_mb = int(secrets.get('chat_max_upload_size_mb', 150))

            # Check upload count limit
            if len(form_files) > chat_max_upload_count:
                return {"error": f"Number of files in request exceeds the limit of {chat_max_upload_count}"}, 400

            # Calculate file sizes and total upload size
            file_sizes = {}
            total_upload_size = 0
            for form_file in form_files:
                size = form_file.seek(0, 2)
                form_file.seek(0)
                file_sizes[form_file.filename] = size
                total_upload_size += size

            # Check total upload size limit
            if total_upload_size > chat_max_upload_size_mb * 1024 * 1024:
                return {"error": f"Total upload size exceeds the limit of {chat_max_upload_size_mb} MB"}, 400

            mc = MinioClient.from_project_id(project_id)

            bucket_files = []
            try:
                bucket_files = mc.list_files(bucket_name)
            except Exception:
                pass
            else:
                if not overwrite_attachments:
                    for bf in bucket_files:
                        for form_file in form_files:
                            if bf['name'] == form_file.filename:
                                return {"error": f"File with name {form_file.filename} already attached to conversation {conversation.uuid}"}, 400

            # Create bucket if it doesn't exist
            if bucket_name not in mc.list_bucket():
                chat_bucket_retention_days = secrets.get('chat_bucket_retention_days', None)
                if chat_bucket_retention_days is not None:
                    chat_bucket_retention_days = int(chat_bucket_retention_days)
                mc.create_bucket(
                    bucket=bucket_name,
                    bucket_type='local',
                    retention_days=chat_bucket_retention_days
                )

            attachments_info: list[dict] = []
            # Only track existing filenames for collision detection if NOT overwriting
            existing_filenames = [] if overwrite_attachments else [bf['name'] for bf in bucket_files]
            
            for form_file in form_files:
                original_filename = form_file.filename
                
                # Sanitize filename to prevent regex errors during indexing
                sanitized_filename, was_modified = sanitize_filename(
                    original_filename, 
                    existing_filenames
                )
                
                if was_modified:
                    log.info(f"Sanitized filename: '{original_filename}' -> '{sanitized_filename}'")
                
                # Upload with sanitized filename (will overwrite if file exists and overwrite_attachments=True)
                mc.upload_file(bucket_name, form_file, sanitized_filename)
                
                # Track newly uploaded filename only if not overwriting
                if not overwrite_attachments:
                    existing_filenames.append(sanitized_filename)
                
                # Store sanitized filename (matches what's in MinIO)
                attachments_info.append(
                    AttachmentMessageItemCreated(
                        filepath=f"/{bucket_name}/{sanitized_filename}",
                        file_size=file_sizes[original_filename],
                    ).model_dump()
                )

        return attachments_info, 201

    @auth.decorators.check_api({
        "permissions": ["models.chat.attachments.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, conversation_id: int, **kwargs):
        # Get filename(s) - supports both single and multiple filenames
        filenames_param = request.args.getlist('filename')
        # Get keep_in_storage flag from request parameters (supports true/false and 1/0)
        keep_in_storage_param = request.args.get('keep_in_storage', 'false').lower()
        keep_in_storage = keep_in_storage_param in ('true', '1')

        if not filenames_param:
            return {"error": "Filename parameter is required"}, 400

        # Decode all filenames and remove duplicates while preserving order
        filenames_to_delete = list(dict.fromkeys([urllib.parse.unquote(fn) for fn in filenames_param]))

        with db.get_session(project_id) as session:
            # Check if attachment storage is already configured
            conversation: Conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()

            if conversation is None:
                return {"error": f"Conversation {conversation_id} not found"}, 400

            if conversation.attachment_participant_id:
                attachment_participant = session.query(Participant).filter(
                    Participant.id == conversation.attachment_participant_id
                ).first()
                if attachment_participant is None:
                    return {"error": f"Attachment storage with id {conversation.attachment_participant_id} not found"}, 400
                toolkit_details = this.module.get_toolkit_by_id(
                    project_id=attachment_participant.entity_meta['project_id'],
                    toolkit_id=attachment_participant.entity_meta['id']
                )
                bucket_name = toolkit_details.get('settings', {})['bucket']
            else:
                return {"error": f"Conversation {conversation.uuid} has no attachment storage"}, 400

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
    module_name_override = "chat"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
