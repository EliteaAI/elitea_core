"""Public (unauthenticated) API for downloading attachments from shared conversations.

  GET /elitea_core/shared_chat_attachment/prompt_lib/<token>/<int:group_id>/<filename>

The token must be valid (not expired/revoked). The requested attachment must belong to
a message group that is visible under the token's scope (respects message_group_ids
for partial-scope tokens). The file is streamed from MinioClient storage.
"""
from datetime import datetime, timezone
import urllib.parse

from flask import Response, request
from itsdangerous import BadSignature, TimestampSigner
from pylon.core.tools import log

from flask import current_app
from tools import MinioClient, api_tools, db, register_openapi

from ...models.all import ConversationShareToken, ConversationShareTokenIndex
from ...models.message_group import ConversationMessageGroup
from ...models.message_items.attachment import AttachmentMessageItem
from ...models.pd.shared_chat_link import ShareScope
from ...utils.constants import PROMPT_LIB_MODE

_UNLOCK_COOKIE_PREFIX = 'share_unlocked_'
_COOKIE_MAX_AGE = 3600

# Extensions that must never be served inline from the app origin (stored XSS risk).
_FORCE_DOWNLOAD_EXTENSIONS = frozenset(['svg', 'svgz', 'html', 'htm', 'xhtml', 'xml', 'xsl'])


def _get_signer() -> TimestampSigner:
    return TimestampSigner(current_app.config['SECRET_KEY'], salt='share_unlock')


def _is_session_unlocked(token: str) -> bool:
    value = request.cookies.get(_UNLOCK_COOKIE_PREFIX + token)
    if not value:
        return False
    try:
        payload = _get_signer().unsign(value, max_age=_COOKIE_MAX_AGE).decode()
        return payload == token
    except BadSignature:
        return False


def _escape_like(value: str) -> str:
    """Escape LIKE metacharacters so a literal filename suffix match is safe."""
    return value.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')


class SharedConversationAttachmentAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Download Shared Conversation Attachment",
        description=(
            "Download an attachment from a shared conversation. "
            "No authentication required; validates the share token."
        ),
        tags=["elitea_core/chat"],
        available_to_users=True,
    )
    def get(self, token: str, group_id: int, filename: str, **kwargs):
        filename = urllib.parse.unquote(filename)

        # Look up project via the global token index
        try:
            with db.get_session(None) as pub_session:
                index = pub_session.get(ConversationShareTokenIndex, token)
        except Exception:
            log.exception("Failed to query share token index for token %s...", token[:8])
            return {"error": "This link is no longer available."}, 404

        if index is None:
            return {"error": "This link is no longer available."}, 404

        project_id = index.project_id

        with db.get_session(project_id) as session:
            share = (
                session.query(ConversationShareToken)
                .filter_by(token=token)
                .first()
            )
            if share is None or share.is_revoked:
                return {"error": "This link is no longer available."}, 404
            if share.expires_at and share.expires_at < datetime.now(timezone.utc).replace(tzinfo=None):
                return {"error": "This link has expired."}, 410

            # Password gate — must be unlocked before accessing attachments
            if share.password_hash and not _is_session_unlocked(token):
                return {"error": "Password required."}, 401

            # Validate that group_id is accessible under this token's scope
            try:
                scope_enum = ShareScope(share.scope)
            except ValueError:
                scope_enum = ShareScope.all

            # messages_only scope explicitly excludes attachments
            if scope_enum == ShareScope.messages_only:
                return {"error": "Attachment not found."}, 404

            group = session.get(ConversationMessageGroup, group_id)
            if group is None or group.conversation_id != share.conversation_id:
                return {"error": "Attachment not found."}, 404

            # Partial scope: deny if group not in the explicit allow-list.
            # Fail-closed: no allow-list means no groups are accessible.
            if scope_enum == ShareScope.partial:
                allowed = share.message_group_ids or []
                if group_id not in allowed:
                    return {"error": "Attachment not found."}, 404

            # Escape LIKE metacharacters to prevent wildcard injection
            safe_suffix = _escape_like(filename)
            attachment = (
                session.query(AttachmentMessageItem)
                .filter_by(message_group_id=group_id)
                .filter(AttachmentMessageItem.name.like(f"%{safe_suffix}", escape='\\'))
                .first()
            )
            if attachment is None:
                return {"error": "Attachment not found."}, 404

            bucket = attachment.bucket
            stored_name = attachment.name
            attachment_type = attachment.attachment_type or 'document'

        # Determine content type — SVG and other active-content types are forced
        # to attachment + octet-stream to prevent stored-XSS execution.
        ext = stored_name.rsplit('.', 1)[-1].lower() if '.' in stored_name else ''
        safe_image_ext_map = {
            'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
            'gif': 'image/gif', 'webp': 'image/webp', 'bmp': 'image/bmp',
        }
        force_download = ext in _FORCE_DOWNLOAD_EXTENSIONS
        if force_download:
            content_type = 'application/octet-stream'
        elif attachment_type == 'image':
            content_type = safe_image_ext_map.get(ext, 'image/png')
        elif attachment_type == 'text':
            content_type = 'text/plain; charset=utf-8'
        else:
            content_type = 'application/octet-stream'

        display_name = filename.split('/')[-1] if '/' in filename else filename
        safe_name = urllib.parse.quote(display_name)
        disposition = 'attachment' if force_download else 'inline'

        # Download from storage
        try:
            mc = MinioClient.from_project_id(project_id)
            file_data = mc.download_file(bucket, stored_name)
        except Exception:
            log.exception(
                "Failed to download shared attachment %s/%s for token %s...",
                bucket, stored_name, token[:8],
            )
            return {"error": "Failed to retrieve attachment."}, 500

        if not isinstance(file_data, (bytes, bytearray)):
            file_data = bytes(file_data)

        response = Response(
            file_data,
            content_type=content_type,
            direct_passthrough=True,
        )
        response.headers['Content-Disposition'] = (
            f"{disposition}; filename*=UTF-8''{safe_name}"
        )
        response.headers['Content-Length'] = len(file_data)
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<string:token>/<int:group_id>/<path:filename>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: SharedConversationAttachmentAPI,
    }
