from pylon.core.tools import log
from flask import request
from sqlalchemy import and_, or_, text
from sqlalchemy.exc import DataError
import psycopg2.errors
from tools import api_tools, auth, config as c, db, context
from ...models.elitea_tools import EliteATool
from alembic import op

from ...models.pd.tool import ToolDetails

TOOLKIT_NAME_FIELDS_PER_TOOLKIT = {'artifact': 'bucket', 'memory': 'namespace', 'github': 'repository',
                                   'confluence': 'space', 'service_now': 'name', 'gitlab': 'repository',
                                   'gitlab_org': 'name', 'zephyr': 'base_url', 'testrail': 'name',
                                   'qtest': 'qtest_project_id', 'ado_plans': 'name', 'ado_boards': 'name',
                                   'ado_wiki': 'name', 'rally': 'name', 'sql': 'database_name',
                                   'sonar': 'sonar_project_name', 'google_places': 'results_count', 'elastic': 'url',
                                   'keycloak': 'base_url', 'pandas': 'bucket_name', 'ocr': 'artifacts_folder',
                                   'pptx': 'bucket_name'}

class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.migration_toolkit_names.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self):
        payload = dict(request.json)
        # if payload contains 'project_id' int value > 0, only process that project_id
        # if 'project_id' value equals 0, process all projects
        # Otherwise, return 400 error
        project_id = payload.get('project_id')
        if project_id is not None:
            try:
                project_id = int(project_id)
                if project_id < 0:
                    return {"error": "Invalid project_id. Must be >= 0."}, 400
            except ValueError:
                return {"error": "Invalid project_id. Must be an integer."}, 400

        #
        project_ids = [project_id] if project_id else [
            i['id'] for i in self.module.context.rpc_manager.call.project_list(
                filter_={'create_success': True}
            )
        ]
        #
        log.info(f"Verifying and migrating toolkit names for projects {project_ids}")

        # Track migration results
        migrated_projects = []
        failed_projects = []

        #
        for pid in project_ids:
            try:
                # elitea_tools migration
                with db.get_session(pid) as session_db:

                    elitea_tools = session_db.query(EliteATool).filter(
                        or_(EliteATool.name.is_(None), EliteATool.name == '')
                    ).distinct().all()
                    log.debug(f"Found {len(elitea_tools)} tools (with missing name) for project_id={pid}")
                    # get alias for each tool per id
                    for tool in elitea_tools:
                        log.debug(f'Processing tool ID {tool.id} of type {tool.type} with settings: {tool.settings}')
                        # handle application type separately
                        if tool.type == 'application':
                            log.debug(f'Fixing tool ID {tool.id} using fix_name method')
                            tool_details = ToolDetails.from_orm(tool)
                            tool_details.fix_name(pid)
                            tool.name = tool_details.name
                            log.debug(f'Updated tool ID {tool.id} name to: {tool.name}')
                        else:
                            # create new_toolkit_name based on a toolkit type and specific field in settings and max_length in metadata
                            toolkit_name = tool.settings.get(TOOLKIT_NAME_FIELDS_PER_TOOLKIT.get(tool.type, ''), 'Unset')
                            # Truncate toolkit_name to fit database column constraint (max 128 chars)
                            toolkit_name = toolkit_name[:128] if isinstance(toolkit_name, str) else str(toolkit_name)[:128]
                            log.debug(f'Processing tool ID {tool.id} with generated toolkit_name: {toolkit_name}')
                            tool.name = toolkit_name

                    session_db.commit()
                    session_db.execute(text(f"ALTER TABLE p_{pid}.elitea_tools ALTER COLUMN name SET NOT NULL"))
                    session_db.commit()
                    migrated_projects.append(pid)
                    log.info(f"Successfully migrated toolkit names for project_id={pid}")

            except DataError as e:
                # Check if it's a StringDataRightTruncation error
                if isinstance(e.orig, psycopg2.errors.StringDataRightTruncation):
                    log.error(f"StringDataRightTruncation error for project_id={pid}: {str(e)}")
                    failed_projects.append({"project_id": pid, "error": "Value too long for type character varying(128)"})
                else:
                    log.error(f"DataError for project_id={pid}: {str(e)}")
                    failed_projects.append({"project_id": pid, "error": str(e)})
            except Exception as e:
                log.error(f"Unexpected error for project_id={pid}: {str(e)}")
                failed_projects.append({"project_id": pid, "error": str(e)})

        # Build response
        response = {
            "message": "Toolkit names migration completed",
            "migrated_projects": migrated_projects,
            "failed_projects": failed_projects,
            "summary": {
                "total_projects": len(project_ids),
                "migrated_count": len(migrated_projects),
                "failed_count": len(failed_projects)
            }
        }

        status_code = 200 if len(failed_projects) == 0 else 207  # 207 Multi-Status for partial success

        return response, status_code

class API(api_tools.APIBase):
    url_params = api_tools.with_modes([''])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI
    }
