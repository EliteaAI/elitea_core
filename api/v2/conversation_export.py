import json
from datetime import datetime

from flask import Response, request, send_file
from pylon.core.tools import log
from sqlalchemy.orm import selectinload
from tools import api_tools, auth, db, config as c, register_openapi

from ...models.conversation import Conversation
from ...utils.chat_crypto import (
    CHAT_ENVELOPE,
    ENVELOPE_MIMETYPE,
    ENVELOPE_SUFFIX,
    chat_master_key,
    iter_file_chunks,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.conversation_access import check_conversation_access, NOT_FOUND
from ...utils.conversation_export import (
    build_export_payload,
    build_export_zip,
    collect_attachment_sizes,
    load_conversation_export_data,
    sanitize_export_name,
    strip_internal_refs,
)
from ...utils.export_import_utils import content_disposition_attachment

_EXPOSED_HEADERS = 'Content-Disposition, X-Export-Missing-Files'


def _unique_attachments(groups: list[dict]) -> list[dict]:
    seen = {}
    for group in groups:
        for item in group['items']:
            if item['item_type'] == 'attachment_message':
                seen.setdefault(item['filepath'], item)
    return list(seen.values())


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Export Conversation",
        description=(
            "Export a conversation as JSON, or as a ZIP archive with its attachments and canvases. "
            "With summary=true returns attachment count and size instead of a file."
        ),
        parameters=[
            {"name": "include_attachments", "in": "query", "required": False,
             "schema": {"type": "boolean", "default": False},
             "description": "Return a ZIP with attachments and canvases instead of a JSON file."},
            {"name": "summary", "in": "query", "required": False,
             "schema": {"type": "boolean", "default": False},
             "description": "Return export summary (messages and attachments count, total size)."},
            {"name": "file_name", "in": "query", "required": False,
             "schema": {"type": "string"},
             "description": "Base file name for the export, without extension."},
        ],
        tags=["elitea_core/chat"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, conversation_id: int, **kwargs):
        current_user = auth.current_user()
        include_attachments = request.args.get('include_attachments', 'false').lower() == 'true'
        summary_only = request.args.get('summary', 'false').lower() == 'true'

        with db.get_session(project_id) as session:
            conversation = session.query(Conversation).options(
                selectinload(Conversation.participants)
            ).filter(Conversation.id == conversation_id).first()
            if conversation is None:
                return NOT_FOUND
            denied = check_conversation_access(project_id, conversation, current_user['id'])
            if denied:
                return denied
            conversation_dict, participants, groups = load_conversation_export_data(
                session, project_id, conversation
            )

        attachments = _unique_attachments(groups)
        sizes = collect_attachment_sizes(project_id, attachments)

        if summary_only:
            return {
                'messages_count': len(groups),
                'attachments_count': len(attachments),
                'total_size': sum(sizes.values()) if sizes else None,
            }, 200

        for group in groups:
            for item in group['items']:
                if item['item_type'] == 'attachment_message' and item['filepath'] in sizes:
                    item['file_size'] = sizes[item['filepath']]

        payload = build_export_payload(
            conversation=conversation_dict,
            participants=participants,
            groups=groups,
            include_attachments=include_attachments,
            exported_by={'id': current_user['id'], 'name': current_user.get('name')},
            project_id=project_id,
        )
        file_name = sanitize_export_name(
            request.args.get('file_name')
            or f"{conversation_dict['name']}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        )

        master_key = chat_master_key()
        if master_key is None:
            log.warning(
                "Chat export: SECRETS_MASTER_KEY is not set, exporting unencrypted (file will not be importable)"
            )

        if not include_attachments:
            body = json.dumps(strip_internal_refs(payload), ensure_ascii=False, indent=2).encode('utf-8')
            if master_key is None:
                return Response(
                    body,
                    mimetype='application/json; charset=utf-8',
                    headers={
                        'Content-Disposition': content_disposition_attachment(f'{file_name}.json'),
                        'Access-Control-Expose-Headers': _EXPOSED_HEADERS,
                    },
                )
            # direct_passthrough: stream as is, so after-request hooks don't buffer/decode the binary body
            return Response(
                CHAT_ENVELOPE.iter_encrypt([body], master_key),
                direct_passthrough=True,
                mimetype=ENVELOPE_MIMETYPE,
                headers={
                    'Content-Disposition': content_disposition_attachment(f'{file_name}.json{ENVELOPE_SUFFIX}'),
                    'Access-Control-Expose-Headers': _EXPOSED_HEADERS,
                },
            )

        try:
            zip_file, missing_count = build_export_zip(project_id, payload, groups, file_name)
        except Exception:
            log.exception("Chat export: failed to build archive for conversation %s", conversation_id)
            return {'error': 'Failed to export chat'}, 500

        log.info(
            "Chat exported: project=%s conversation=%s messages=%s files=%s missing=%s",
            project_id, conversation_id, len(groups), len(attachments), missing_count,
        )
        if master_key is None:
            response = send_file(
                zip_file,
                mimetype='application/zip',
                download_name=f'{file_name}.zip',
                as_attachment=True,
            )
        else:
            def _encrypted_stream():
                try:
                    yield from CHAT_ENVELOPE.iter_encrypt(iter_file_chunks(zip_file), master_key)
                finally:
                    zip_file.close()

            response = Response(
                _encrypted_stream(),
                direct_passthrough=True,
                mimetype=ENVELOPE_MIMETYPE,
                headers={'Content-Disposition': content_disposition_attachment(f'{file_name}.zip{ENVELOPE_SUFFIX}')},
            )
        response.headers['X-Export-Missing-Files'] = str(missing_count)
        response.headers['Access-Control-Expose-Headers'] = _EXPOSED_HEADERS
        return response


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:conversation_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
