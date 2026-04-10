import json
from io import BytesIO
from datetime import date

from flask import request, send_file, Response
from pydantic.v1 import ValidationError
from pylon.core.tools import log
from tools import api_tools, auth, config as c

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.export_import import (
    export_application,
    export_application_md,
    create_zip_archive
)
from ...utils.export_import import _slugify


def _generate_export_filename(result, file_extension="zip"):
    """
    Generate a meaningful filename for exported entities.
    
    Args:
        result: Export result containing applications
        file_extension: File extension (default: "zip")
    
    Returns:
        String filename based on the entity names
    """
    try:
        # Get original applications being exported
        original_apps = []
        if 'files' in result:  # MD export result
            # Extract application names from filenames
            for file_info in result['files']:
                if file_info.get('is_original', False):
                    filename = file_info['filename']
                    # Extract name from filename pattern: "name.agent.md" or "name.version.agent.md"
                    name_part = filename.split('.')[0]
                    
                    # Extract entity type from filename or file_info
                    entity_type = 'agent'  # default
                    parts = filename.split('.')
                    for part in parts:
                        if part in ['agent', 'pipeline']:
                            entity_type = part
                            break
                    # Also check file_info for entity type if available
                    if 'entity_type' in file_info:
                        entity_type = file_info['entity_type']
                    elif 'type' in file_info:
                        entity_type = file_info['type']
                    
                    original_apps.append(f"{name_part}.{entity_type}")
        elif 'applications' in result:  # JSON export result
            for app in result['applications']:
                if app.get('original_exported', False) and app.get('name'):
                    name = _slugify(app.get('name', ''))
                    # Determine entity type
                    entity_type = app.get('entity_type', app.get('type', 'agent'))
                    original_apps.append(f"{name}.{entity_type}")
        
        # Remove duplicates while preserving order
        original_apps = list(dict.fromkeys(original_apps))
        
        if original_apps:
            # Use only the first (main) exported agent name, ignore nested dependencies
            return f"{original_apps[0]}.{file_extension}"
    except Exception:
        # Fallback to generic name if anything goes wrong
        pass
    
    # Fallback to original generic naming
    return f'elitea_export_{date.today()}.{file_extension}'


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.export_import.export"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, application_ids: str, **kwargs):
        application_ids = [int(id_str) for id_str in application_ids.split(",")]
        forked = 'fork' in request.args
        follow_version_ids = request.args.get('follow_version_ids', type=str)
        if follow_version_ids:
            follow_version_ids = [int(id_str) for id_str in follow_version_ids.split(",")]

        # Check export format (json or md)
        export_format = request.args.get('format', 'json').lower()

        if export_format == 'md':
            # Markdown export
            try:
                result = export_application_md(
                    project_id=project_id,
                    user_id=auth.current_user()['id'],
                    application_ids=application_ids,
                    follow_version_ids=follow_version_ids
                )
            except Exception as e:
                log.error(f"MD export failed: {e}")
                return {'errors': {'applications': str(e)}}, 400

            if not result.get('ok'):
                return {'errors': {'applications': result.get('msg', 'Export failed')}}, 400

            files = result.get('files', [])
            if not files:
                return {'errors': {'applications': 'No applications to export'}}, 400

            # Single file - return as markdown
            if len(files) == 1 and not result.get('has_dependencies'):
                return Response(
                    files[0]['content'],
                    mimetype='text/markdown; charset=utf-8',
                    headers={
                        'Content-Disposition': f'attachment; filename="{files[0]["filename"]}"',
                        'Access-Control-Expose-Headers': 'Content-Disposition'
                    }
                )

            # Multiple files or has dependencies - return as ZIP
            zip_buffer = create_zip_archive(files)
            filename = _generate_export_filename(result, "zip")
            response = send_file(
                zip_buffer,
                mimetype='application/zip',
                download_name=filename,
                as_attachment=True
            )
            response.headers['Access-Control-Expose-Headers'] = 'Content-Disposition'
            return response

        # Default: JSON export
        try:
            result = export_application(
                project_id=project_id,
                user_id=auth.current_user()['id'],
                application_ids=application_ids,
                forked=forked,
                follow_version_ids=follow_version_ids
            )
        except ValidationError as e:
            return e.errors(), 400

        if not result['ok']:
            return {'errors': {'applications': result['msg']}}, 400

        result.pop('ok')

        if 'as_file' in request.args:
            file = BytesIO()
            data = json.dumps(result, ensure_ascii=False, indent=4)
            file.write(data.encode('utf-8'))
            file.seek(0)
            return send_file(file, download_name=f'elitea_agents_{date.today()}.json', as_attachment=False)
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:application_ids>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
