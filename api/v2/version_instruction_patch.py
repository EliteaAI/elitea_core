"""Atomic, versioned instruction edits for the internal Elitea MCP."""

from flask import request
from pydantic import ValidationError

from tools import api_tools, auth, config as c, db, register_openapi

from ...models.all import Application, ApplicationVersion
from ...models.pd.version import (
    ApplicationVersionInstructionsPatchModel,
    ApplicationVersionUpdateModel,
)
from ...utils.application_utils import applications_update_version, VersionNotUpdatableError
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.create_utils import clone_persisted_application_version
from ...utils.mcp_versioning import (
    InstructionsPatchConflictError,
    apply_instructions_patch,
    build_mcp_backup_version_name,
    instructions_sha256,
)


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name=(
            "Safely edit an agent or pipeline's instructions using an exact text "
            "replacement and an automatic backup version"
        ),
        description=(
            'Atomically creates a backup version and patches instructions. '
            'Rejects stale or ambiguous edits without changing the source version.'
        ),
        request_body=ApplicationVersionInstructionsPatchModel,
        mcp_description="""
        USE for every internal-MCP change to an existing agent prompt or pipeline YAML.
        This is the only safe instruction-edit tool; do not send instructions to update_version.

        REQUIRED workflow:
        1. Call get_version_details immediately before this tool.
        2. Copy response.instructions_sha256 into expected_instructions_sha256.
        3. Prefer a small exact replacement: old_text must occur exactly once; replacement is the new text.
        4. Use replace_all=true only for an intentional complete rewrite; omit old_text in that case.

        Success is one transaction: a full backup version is created first, then the draft is
        updated. The response includes mcp_backup_version and the new instructions_sha256.
        Conflict means nothing changed: read the version again and retry. Never try to continue
        or reconstruct a partially emitted tool call.

        Exact replacement example:
        { 'expected_instructions_sha256': '<hash from get_version_details>',
          'old_text': 'Always answer in prose.',
          'replacement': 'Answer with concise bullet points.' }

        Complete rewrite example:
        { 'expected_instructions_sha256': '<hash from get_version_details>',
          'replace_all': true,
          'replacement': 'You are a concise research assistant.' }
        """,
        tags=["elitea_core/applications"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, application_id: int, version_id: int, **kwargs):
        try:
            patch_data = ApplicationVersionInstructionsPatchModel.model_validate(
                request.get_json(silent=True) or {}
            )
        except ValidationError as exc:
            return exc.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        author_id = auth.current_user().get('id')
        try:
            with db.with_project_schema_session(project_id) as session:
                source_version = session.query(ApplicationVersion).filter(
                    ApplicationVersion.id == version_id,
                    ApplicationVersion.application_id == application_id,
                ).one_or_none()
                application = session.query(Application).filter(
                    Application.id == application_id,
                ).one_or_none()
                if not source_version or not application:
                    return {'error': 'Application version not found'}, 404
                if source_version.status in ('published', 'embedded'):
                    raise VersionNotUpdatableError(
                        f'Version id {version_id} is {source_version.status} and can not be updated'
                    )

                updated_instructions = apply_instructions_patch(
                    source_version.instructions,
                    expected_sha256=patch_data.expected_instructions_sha256,
                    old_text=patch_data.old_text,
                    replacement=patch_data.replacement,
                    replace_all=patch_data.replace_all,
                )
                update_data = ApplicationVersionUpdateModel.model_validate({
                    'id': version_id,
                    'application_id': application_id,
                    'project_id': project_id,
                    'author_id': author_id,
                    'name': source_version.name,
                    'agent_type': source_version.agent_type,
                    'instructions': updated_instructions,
                })

                backup_version = clone_persisted_application_version(
                    source_version=source_version,
                    application=application,
                    new_version_name=build_mcp_backup_version_name(version_id),
                    author_id=author_id,
                    session=session,
                )
                result = applications_update_version(update_data, session, commit=False)
                if not result['updated']:
                    session.rollback()
                    return result['msg'], 400
                session.commit()

                response = result['data']
                response['mcp_backup_version'] = {
                    'id': backup_version.id,
                    'name': backup_version.name,
                }
                response['instructions_sha256'] = instructions_sha256(updated_instructions)
                return response, 201
        except InstructionsPatchConflictError as exc:
            return {'error': str(exc)}, 409
        except VersionNotUpdatableError as exc:
            return {'error': str(exc)}, 409
        except ValidationError as exc:
            return exc.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:application_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
