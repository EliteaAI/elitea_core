"""Apply accepted "Enhance with AI" edits to a new version (ENH-6b, §6.1.1).

Fork is the default apply mode, and for a published or embedded version it is the only one. The
source version is never touched: the run that produced the proposal stays reproducible against the
exact text it scored, which is what makes an A/B comparison of the enhancement meaningful.

The order below is deliberate. Instructions are patched *before* the clone is created, so a stale
hash or an anchor that no longer matches leaves no orphan version behind — a failed apply that
still litters the version list is worse than one that changes nothing.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, auth, config as c, db, register_openapi

from ...models.all import Application, ApplicationVersion
from ...models.pd.enhance_from_eval import InstructionsForkRequest
from ...models.pd.version import ApplicationVersionUpdateModel
from ...utils.application_utils import applications_update_version
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.create_utils import clone_persisted_application_version
from ...utils.mcp_versioning import (
    InstructionsPatchConflictError,
    apply_instructions_patch_batch,
    build_enhance_fork_version_name,
    instructions_sha256,
)


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Apply instruction edits to a new forked version",
        description=(
            "Applies a batch of exact-replacement instruction edits to a new draft version cloned "
            "from the given one, leaving the source version unchanged. All or nothing: a stale "
            "hash or an anchor that does not match exactly once rejects the whole batch and "
            "creates no version."
        ),
        request_body=InstructionsForkRequest,
        tags=["elitea_core/applications"],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int, application_id: int, version_id: int, **kwargs):
        try:
            fork_data = InstructionsForkRequest.model_validate(
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

                # No status check, unlike the in-place patch path: forking a published version is
                # the entire reason this endpoint exists.
                updated_instructions = apply_instructions_patch_batch(
                    source_version.instructions,
                    expected_sha256=fork_data.expected_instructions_sha256,
                    patches=[patch.model_dump() for patch in fork_data.patches],
                )

                name = self._resolve_fork_name(
                    session, application_id, version_id, fork_data.new_version_name,
                )
                fork_version = clone_persisted_application_version(
                    source_version=source_version,
                    application=application,
                    new_version_name=name,
                    author_id=author_id,
                    session=session,
                )
                session.flush()

                update_data = ApplicationVersionUpdateModel.model_validate({
                    'id': fork_version.id,
                    'application_id': application_id,
                    'project_id': project_id,
                    'author_id': author_id,
                    'name': fork_version.name,
                    'agent_type': fork_version.agent_type,
                    'instructions': updated_instructions,
                })
                result = applications_update_version(update_data, session, commit=False)
                if not result['updated']:
                    session.rollback()
                    return result['msg'], 400
                session.commit()

                response = result['data']
                response['forked_from_version_id'] = version_id
                response['instructions_sha256'] = instructions_sha256(updated_instructions)
                return response, 201
        except InstructionsPatchConflictError as exc:
            return {'error': str(exc)}, 409
        except ValidationError as exc:
            return exc.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

    @staticmethod
    def _resolve_fork_name(session, application_id: int, version_id: int, requested):
        """Pick a name no existing version of this agent is already using.

        Version names are unique per application, so a user who accepts two proposals and keeps the
        suggested name both times would otherwise get an integrity error instead of a second fork.
        """
        taken = {
            name for (name,) in session.query(ApplicationVersion.name).filter(
                ApplicationVersion.application_id == application_id,
            ).all()
        }
        candidate = (requested or '').strip() or build_enhance_fork_version_name(version_id)
        if candidate not in taken:
            return candidate

        for suffix in range(2, 100):
            # The 128-char column limit is enforced by the request model; a suffix can push past it.
            numbered = f'{candidate[:120]}-{suffix}'
            if numbered not in taken:
                return numbered
        raise InstructionsPatchConflictError(
            f'Could not find an unused version name based on "{candidate}".'
        )


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:application_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
