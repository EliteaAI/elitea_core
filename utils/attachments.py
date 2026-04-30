import mimetypes
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from sqlalchemy.orm.attributes import flag_modified

from pylon.core.tools import log
from pydantic import ValidationError
from werkzeug.datastructures import FileStorage
from tools import db, rpc_tools, VaultClient, auth, this

from .file_utils import (
    sanitize_filename,
    save_chunk,
    are_all_chunks_received,
    merge_chunks,
    cleanup_chunks,
    CHUNKS_TEMP_DIR,
)
from .internal_tools import get_default_attachment_bucket, ATTACHMENT_TOOLKIT_NAME
from ..models.conversation import Conversation
from ..models.message_items.attachment import AttachmentMessageItem
from ..models.message_items.text import TextMessageItem
from ..models.pd.attachment import AttachmentMessageItemCreated, ChunkUploadPayload


AWAIT_TASK_TIMEOUT_MAX = 600  # 10 minutes for  document indexing
DEFAULT_CUT_OFF=0.1
DEFAULT_SEARCH_TOP=10


def parse_filepath(filepath: str) -> tuple[str, str]:
    """Parse /{bucket}/{filename} into (bucket, filename)."""
    path = filepath.lstrip('/')
    parts = path.split('/', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid filepath format: {filepath}. Expected /{'{bucket}'}/{'{filename}'}")
    return parts[0], parts[1]


def process_single_attachment_file(
    session,
    project_id: int,
    msg_group,
    filepath: str,
    order_index: int,
    user_id: int = None,
    collection_suffix: str | None = "attach",
    prompt: str = None,
    **kwargs,
) -> tuple['AttachmentMessageItem', bool]:
    """Process a single attachment file and create an AttachmentMessageItem.

    Returns (AttachmentMessageItem, needs_content_extraction).
    """
    try:
        bucket_name, filename = parse_filepath(filepath)

        file_processor = FileToAIProcessorCollector()
        processed_data = file_processor.process_file(
            bucket_name=bucket_name,
            filename=filename,
            project_id=project_id,
            conversation_id=msg_group.conversation.uuid,
            user_id=user_id,
            collection_suffix=collection_suffix,
            filepath=filepath,
            prompt=prompt,
            **kwargs,
        )

        attachment_msg = AttachmentMessageItem(
            message_group=msg_group,
            item_type=AttachmentMessageItem.__mapper_args__['polymorphic_identity'],
            order_index=order_index,
            name=filename,
            bucket=bucket_name,
            attachment_type=processed_data["type"],
            content=processed_data["content"],
        )

        needs_content_extraction = processed_data.get("needs_content_extraction", False)
        return attachment_msg, needs_content_extraction

    except NotSupportableProcessorExtension:
        log.error(f"Unsupported file type for {filepath}")
        raise
    except Exception as e:
        log.error(f"Failed to process file {filepath}: {e}")
        raise RuntimeError(f"Failed to process file {filepath}") from None


class ProcessorContext:
    """Context object containing all parameters that processors might need."""

    def __init__(
        self,
        bucket_name: str,
        filename: str,
        project_id: int = None,
        conversation_id: str = None,
        user_id: int = None,
        llm_settings: dict = None,
        collection_suffix: str = "attach",
        filepath: str = None,
        prompt: str = None,
        **kwargs
    ):
        self.bucket_name = bucket_name
        self.filename = filename
        self.project_id = project_id
        self.conversation_id = conversation_id
        self.user_id = user_id
        self.llm_settings = llm_settings
        self.collection_suffix = collection_suffix
        self.filepath = filepath
        self.prompt = prompt
        self.additional_params = kwargs


class NotSupportableFileExtension(Exception):
    pass


class NotSupportableProcessorExtension(Exception):
    pass


class BaseFileToAIProcessor(ABC):
    def __init__(self, filename: str, mime_type: str = None):
        self._file_name = Path(filename)
        # remove the dot from the suffix
        self._file_ext = self._file_name.suffix.lstrip('.')
        self.mime_type = mime_type

    @abstractmethod
    def process(self, context: ProcessorContext) -> dict:
        pass

    def get_ai_payload_from_file(self, context: ProcessorContext) -> dict:
        try:
            return self.process(context)
        except FileNotFoundError:
            return {"error": f"The file {self._file_name} does not exist."}
        except Exception as e:
            return {"error": f"An error occurred while processing the file: {e}"}


class ImageToModelProcessor(BaseFileToAIProcessor):

    def process(self, context: ProcessorContext) -> dict:
        description_parts = [f"Image file: {context.filename}"]

        if context.filepath:
            description_parts.append(f"filepath: {context.filepath}")

        if context.prompt:
            description_parts.append(f"Context: {context.prompt}")

        image_url = context.additional_params.get('image_url')
        if image_url:
            url = image_url
            # Base64 image data is fully embedded inline in the adjacent image_url chunk.
            # No tool call is needed or should be made to re-read this image.
            description_parts.extend([
                "",
                "NOTE: This image is ALREADY EMBEDDED as base64 in this message.",
                "Analyze the image directly from the provided image_url data.",
                "Do NOT call any file reading tool to re-read this image.",
            ])
        elif context.filepath:
            url = f"filepath:{context.filepath}"
            # The image will be embedded as base64 inline at invocation time if S3 download succeeds.
            # If inline image_url data is present, prefer it over making a tool call.
            # If inline data is unavailable (download failed or history turn), read_file is the fallback.
            description_parts.extend([
                "",
                "NOTE: This image will be embedded as base64 inline if successfully loaded.",
                "If the adjacent image_url chunk contains base64 data, analyze it directly.",
                "If inline image data is not available, use the filepath above with an appropriate file reading tool as a fallback.",
            ])
        else:
            raise ValueError(
                "ImageToModelProcessor requires either 'image_url' in kwargs "
                "or a 'filepath' in context"
            )

        description_text = "\n".join(description_parts)

        return {
            "type": "image",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {"url": url},
                },
                {
                    "type": "text",
                    "text": description_text,
                }
            ]
        }


class TextToModelProcessor(BaseFileToAIProcessor):
    """Processor for text/code files.  Expects pre-fetched text via additional_params['text']."""

    def process(self, context: ProcessorContext) -> dict:
        text = context.additional_params.get('text')
        if text is None:
            raise ValueError(
                "TextToModelProcessor requires 'text' in additional_params"
            )

        # Build description header with metadata
        description_parts = [f"File: {context.filename}"]

        if context.filepath:
            description_parts.append(f"filepath: {context.filepath}")

        if context.prompt:
            description_parts.append(f"Context: {context.prompt}")

        description_parts.append("")  # Empty line separator
        description_parts.append(text)  # Actual file content

        full_text = "\n".join(description_parts)

        return {
            "type": "text",
            "content": [
                {
                    "type": "text",
                    "text": full_text,
                }
            ]
        }


class DocumentToModelProcessor(BaseFileToAIProcessor):
    def process(self, context: ProcessorContext) -> dict:
        description_parts = []

        if context.filepath:
            try:
                bucket_name, filename = parse_filepath(context.filepath)
                description_parts.append(f"Bucket: {bucket_name}")
                description_parts.append(f"Filename: {filename}")
            except Exception as e:
                log.warning(f"Failed to parse filepath {context.filepath}: {e}")

            description_parts.append(f"filepath: {context.filepath}")

        if context.prompt:
            description_parts.append(f"Context: {context.prompt}")

        # Always request content extraction - SDK tool decides whether to return content or size limit error
        # If content is embedded in next chunk, prioritize using it over making tool calls
        description_parts.extend([
            "",
            "NOTE: File content may be EMBEDDED in the next message chunk.",
            "If embedded content is provided below, please review it first - the full text is already included.",
            "File reading tools are available if needed for specific operations (search, partial access), but prefer embedded content when available."
        ])

        description_text = "\n".join(description_parts)

        return {
            "type": "document",
            "content": [
                {
                    "type": "text",
                    "text": description_text,
                }
            ],
            "needs_content_extraction": True
        }


class FileToAIProcessorCollector:
    @staticmethod
    def get_processor(filename: str) -> BaseFileToAIProcessor:
        mime_type, _ = mimetypes.guess_type(filename)
        file_ext = Path(filename).suffix

        if mime_type == 'image/svg+xml' or file_ext.lower() == '.svg':
            return DocumentToModelProcessor(filename, mime_type)
        # images
        if mime_type and mime_type.startswith('image'):
            return ImageToModelProcessor(filename, mime_type)

        # code files - check if extension is in code_types
        if file_ext:
            try:
                code_types = this.module.get_index_types().get("code_types", {})
                if file_ext in code_types:
                    return DocumentToModelProcessor(filename, code_types[file_ext])
            except Exception as e:
                log.error(f"Failed to get code types: {e}")

        # documents
        if file_ext:
            try:
                supported_docs = this.module.get_supported_index_documents()
                if file_ext in supported_docs:
                    return DocumentToModelProcessor(filename, supported_docs[file_ext])
            except Exception as e:
                log.error(f"Failed to get supported document types: {e}")

        raise NotSupportableProcessorExtension(f"No processor available for MIME type: {mime_type}, extension: {file_ext}")

    def process_file(self, filename: str, **context_params) -> dict:
        """Process a file using the appropriate processor.

        Processors are self-contained — each fetches its own data.
        Processor-specific kwargs (e.g. image_url_base64) flow
        through to ProcessorContext.additional_params.
        """
        processor = self.get_processor(filename)
        context = ProcessorContext(
            filename=filename,
            **context_params
        )
        return processor.get_ai_payload_from_file(context)


_FILEPATH_PREFIX = 'filepath:'


def update_attachment_thumbnails(session, user_msg_group, image_thumbnails: dict) -> int:
    """Replace ``filepath:`` image URLs with thumbnail data URLs in user attachments.

    Walks attachment message items of *user_msg_group*, finds ``image_url``
    chunks whose URL matches a key in *image_thumbnails*, and swaps the URL
    to the JPEG thumbnail so the frontend can display a preview.

    Returns the number of attachments updated.
    """
    if not image_thumbnails or user_msg_group is None:
        return 0

    updated = 0
    for item in user_msg_group.message_items:
        if not isinstance(item, AttachmentMessageItem):
            continue
        if item.attachment_type != 'image':
            continue

        content = item.content
        if not isinstance(content, list):
            continue

        changed = False
        for chunk in content:
            if not isinstance(chunk, dict):
                continue
            if chunk.get('type') != 'image_url':
                continue

            url = (chunk.get('image_url') or {}).get('url', '')
            if not url.startswith(_FILEPATH_PREFIX):
                continue

            filepath = url[len(_FILEPATH_PREFIX):]
            thumbnail_url = image_thumbnails.get(filepath)
            if thumbnail_url:
                chunk['image_url']['url'] = thumbnail_url
                changed = True

        if changed:
            flag_modified(item, 'content')
            session.add(item)
            updated += 1

    return updated


def read_file_content(
    project_id: int,
    llm_settings: dict,
    filepaths: list[str],
    sid: Optional[str] = None,
    stream_id: Optional[str] = None,
    message_id: Optional[str] = None,
    question_id: Optional[str] = None,
    await_task_timeout: int = AWAIT_TASK_TIMEOUT_MAX,
) -> dict:
    """
    Read file content using the artifact toolkit for attachments.
    
    Uses the project's default attachment bucket and auto-configured toolkit.
    Reads multiple files in a single batch operation.

    Returns:
        Dictionary with file contents keyed by filepath
    """
    if await_task_timeout <= 0:
        raise ValueError("await_task_timeout must be greater than 0")

    # Get default bucket
    bucket_name = get_default_attachment_bucket(project_id)

    toolkit_settings = {
        'bucket': bucket_name,
    }

    toolkit_config = {
        'type': 'artifact',
        'toolkit_name': ATTACHMENT_TOOLKIT_NAME,
        'toolkit_id': None,  # Auto-injected, no DB entity
        'settings': toolkit_settings
    }

    data = {
        'toolkit_config': toolkit_config,
        'tool_name': 'read_multiple_files',
        'tool_params': {
            'file_paths': filepaths,
            'skip_size_check': False,
        },
        'project_id': project_id,
        'llm_settings': llm_settings,
        'stream_id': stream_id,
        'message_id': message_id,
        'question_id': question_id,
    }
    
    log.debug(f"Reading {len(filepaths)} files from bucket {bucket_name}")
    result = this.module.test_toolkit_tool_sio(
        sid=sid,
        data=data,
        sio_event="chat_predict_attachment",
        start_event_content={
            'question_id': str(question_id),
        },
        await_task_timeout=await_task_timeout
    )

    # Check if read operation completed successfully
    if result is ...:
        raise RuntimeError(
            f"File read timed out after {await_task_timeout} seconds for bucket={bucket_name}"
        )

    # Return success result
    return result.get('result', {})


def is_multimodal_content(content) -> bool:
    """
    Check if content is multimodal (list of content chunks).
    
    Args:
        content: Content to check
        
    Returns:
        bool: True if content is multimodal format
    """
    return (
        isinstance(content, list) and
        len(content) > 0 and
        all(
            isinstance(item, dict) and 'type' in item
            for item in content
        )
    )


def process_multimodal_content(content: list, msg_group, session) -> list:
    """Process multimodal LLM response content into message items.

    LLM responses are text-only.  Any non-text chunk is stored as a
    text fallback so nothing is silently dropped.
    """
    message_items = []

    for order_index, chunk in enumerate(content):
        chunk_type = chunk.get('type')

        if chunk_type == 'text':
            text_content = chunk.get('text', '')
        else:
            log.warning(f"Unexpected content chunk type in LLM response: {chunk_type}")
            text_content = str(chunk)

        text_msg = TextMessageItem(
            content=text_content,
            message_group=msg_group,
            order_index=order_index,
        )
        message_items.append(text_msg)
        session.add(text_msg)

    return message_items


def process_uploaded_files(
    project_id: int,
    conversation_id: int,
    form_files: List[FileStorage],
    overwrite_attachments: bool,
    user_id: int = None,
) -> Tuple[Dict[str, Any], int]:
    """
    Process and upload files via artifact registration system.
    
    Uses the project's default attachment bucket (configurable via vault secret
    'default_attachment_bucket', defaults to 'attachments').

    Args:
        project_id: Project ID
        conversation_id: Conversation ID
        form_files: List of FileStorage objects to upload
        overwrite_attachments: Whether to overwrite existing files
        user_id: User ID (unused, kept for API compatibility)

    Returns:
        Tuple of (response_dict, status_code)
    """
    with db.get_session(project_id) as session:
        conversation: Conversation = session.query(Conversation).filter(
            Conversation.id == conversation_id
        ).first()

        if conversation is None:
            return {"error": f"Conversation {conversation_id} not found"}, 404

        # Use the project's default attachment bucket
        bucket_name = get_default_attachment_bucket(project_id)

        # Get vault configuration for size limits and bucket retention
        # Use project-scoped vault so per-project overrides take effect,
        # falling back to global admin defaults via get_all_secrets().
        vault_client = VaultClient(project_id)
        secrets = vault_client.get_all_secrets()
        chat_max_upload_size_mb = int(secrets.get('chat_max_upload_size_mb', 150))
        chat_max_file_upload_size_mb = int(secrets.get('chat_max_file_upload_size_mb', 150))
        # Default 3 MB accounts for ~33% base64 encoding overhead (3 MB → ~4 MB base64),
        # keeping images under Anthropic's 5 MB base64-encoded limit.
        chat_max_image_upload_size_mb = int(secrets.get('chat_max_image_upload_size_mb', 3))
        chat_bucket_retention_days = int(secrets.get('chat_bucket_retention_days', 365))

        # Validate per-file size limits and calculate total
        file_sizes = {}
        total_upload_size = 0
        for form_file in form_files:
            size = form_file.seek(0, 2)
            form_file.seek(0)
            file_sizes[form_file.filename] = size
            total_upload_size += size
            mime_type = mimetypes.guess_type(form_file.filename or '')[0] or ''
            is_image = mime_type.startswith('image') and not (form_file.filename or '').lower().endswith('.svg')
            if is_image:
                max_size_mb = chat_max_image_upload_size_mb
            else:
                max_size_mb = chat_max_file_upload_size_mb
            if size > max_size_mb * 1024 * 1024:
                file_type = "Image" if is_image else "File"
                return {"error": f"{file_type} \"{form_file.filename}\" exceeds the {max_size_mb} MB limit"}, 400

        # Check total upload size limit
        if total_upload_size > chat_max_upload_size_mb * 1024 * 1024:
            return {"error": f"Total upload size exceeds the limit of {chat_max_upload_size_mb} MB"}, 400

        attachments_info: List[Dict] = []
        # Track existing filenames for collision detection
        existing_filenames: List[str] = []

        for form_file in form_files:
            original_filename = form_file.filename

            # Sanitize filename to prevent regex errors during indexing
            sanitized_filename, was_modified = sanitize_filename(
                original_filename,
                existing_filenames
            )

            if was_modified:
                log.info(f"Sanitized filename: '{original_filename}' -> '{sanitized_filename}'")

            # Prefix with conversation UUID for folder-based isolation
            # This isolates each conversation's attachments: /attachments/{conversation_uuid}/filename
            folder_prefixed_filename = f"{conversation.uuid}/{sanitized_filename}"

            # Read file data for artifact registration
            form_file.seek(0)
            file_data = form_file.read()

            # Register artifact via RPC (handles bucket creation, duplicate checking, upload)
            try:
                artifact_result = rpc_tools.RpcMixin().rpc.timeout(10).artifacts_upload(
                    project_id=project_id,
                    bucket=bucket_name,
                    filename=folder_prefixed_filename,
                    file_data=file_data,
                    create_if_not_exists=True,
                    bucket_retention_days=chat_bucket_retention_days,
                    check_duplicates=True,
                    overwrite=overwrite_attachments
                )
                filepath = artifact_result['filepath']
            except RuntimeError as e:
                # Handle duplicate file error
                if "already exists" in str(e):
                    return {"error": str(e)}, 400
                raise
            except Exception as e:
                log.error(f"Failed to register artifact for {sanitized_filename}: {e}")
                return {"error": f"Failed to upload {sanitized_filename}: {str(e)}"}, 500
            finally:
                # Free memory
                del file_data

            # Track newly uploaded filename for collision detection
            existing_filenames.append(sanitized_filename)

            # Store sanitized filename with filepath
            attachments_info.append(
                AttachmentMessageItemCreated(
                    filepath=filepath,
                    file_size=file_sizes[original_filename],
                ).model_dump()
            )

    return attachments_info, 201


def handle_chunked_upload(
    project_id: int,
    conversation_id: int,
    file_id: str,
    chunk_index: str,
    total_chunks: str,
    file_name: str,
    chunk_file: FileStorage,
    overwrite_attachments: bool = False,
    user_id: int = None,
) -> Tuple[Dict[str, Any], int]:
    """
    Handle chunked file upload - save chunk and merge when complete.

    Args:
        project_id: Project ID
        conversation_id: Conversation ID
        file_id: Unique identifier for the file upload session
        chunk_index: Index of the current chunk (0-based)
        total_chunks: Total number of chunks for this file
        file_name: Original filename
        chunk_file: FileStorage object containing the chunk data
        overwrite_attachments: Whether to overwrite existing files
        user_id: User ID for attachment participant sync

    Returns:
        Tuple of (response_dict, status_code)
    """
    # Validate chunk payload
    try:
        chunk_payload = ChunkUploadPayload(
            file_id=file_id,
            chunk_index=int(chunk_index),
            total_chunks=int(total_chunks),
            file_name=file_name,
        )
    except (ValidationError, ValueError) as e:
        return {"error": f"Invalid chunk parameters: {str(e)}"}, 400

    try:
        # Save the chunk
        save_chunk(
            file_id=chunk_payload.file_id,
            chunk_index=chunk_payload.chunk_index,
            chunk_data=chunk_file.stream,
        )
        log.info(
            f"Saved chunk {chunk_payload.chunk_index + 1}/{chunk_payload.total_chunks} "
            f"for file_id={chunk_payload.file_id}, file_name={chunk_payload.file_name}"
        )

        # Check if all chunks have been received
        if not are_all_chunks_received(chunk_payload.file_id, chunk_payload.total_chunks):
            # Return progress response - not all chunks received yet
            return {
                "status": "chunk_received",
                "file_id": chunk_payload.file_id,
                "chunk_index": chunk_payload.chunk_index,
                "total_chunks": chunk_payload.total_chunks,
                "message": f"Chunk {chunk_payload.chunk_index + 1}/{chunk_payload.total_chunks} received",
            }, 202

        # All chunks received - merge them
        log.info(f"All chunks received for file_id={chunk_payload.file_id}, merging...")

        # Create merged file path in temp directory
        merged_dir = Path(CHUNKS_TEMP_DIR) / "merged"
        merged_dir.mkdir(parents=True, exist_ok=True)
        merged_file_path = merged_dir / f"{chunk_payload.file_id}_{chunk_payload.file_name}"

        try:
            file_size = merge_chunks(
                file_id=chunk_payload.file_id,
                total_chunks=chunk_payload.total_chunks,
                output_path=merged_file_path,
            )
            log.info(f"Merged file created: {merged_file_path}, size: {file_size} bytes")

            # Create a FileStorage-like object from the merged file
            with open(merged_file_path, 'rb') as f:
                file_storage = FileStorage(
                    stream=f,
                    filename=chunk_payload.file_name,
                    content_type='application/octet-stream',
                )

                # Process the merged file using the regular upload logic
                result, status_code = process_uploaded_files(
                    project_id=project_id,
                    conversation_id=conversation_id,
                    form_files=[file_storage],
                    overwrite_attachments=overwrite_attachments,
                    user_id=user_id,
                )

            return result, status_code

        finally:
            # Cleanup: remove chunks and merged file
            cleanup_chunks(chunk_payload.file_id)
            if merged_file_path.exists():
                merged_file_path.unlink()

    except Exception as e:
        log.error(f"Error handling chunked upload: {str(e)}")
        # Cleanup on error
        cleanup_chunks(file_id)
        return {"error": f"Failed to process chunked upload: {str(e)}"}, 500

