from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.skill import SkillUpdateModel, SkillUpdateRelationModel
from ...models.pd.skill_version import (
    SkillVersionCreateModel,
    SkillVersionUpdateModel,
)
from ...utils.skill_utils import (
    get_skill_details,
    update_skill,
    delete_skill,
    create_skill_version,
    update_skill_version,
    delete_skill_version,
    get_skill_version_by_id,
    attach_skill_to_agent,
    detach_skill_from_agent,
    SkillError,
)
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Retrieve full metadata and version configuration of a specific skill",
        description="Returns the full details of the specified skill. If a version_id path segment is provided, that version's details are included (404 if the version id does not exist); otherwise the default ('base') version is included.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "path", "required": False, "schema": {"type": "integer"}, "description": "Optional numeric version id to load details for"},
        ],
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        # When a version id is addressed in the path, it must exist.
        if version_id is not None:
            version = get_skill_version_by_id(
                project_id=project_id,
                skill_id=skill_id,
                version_id=version_id,
            )
            if not version:
                return {"error": f"Skill version '{version_id}' not found"}, 404

        result = get_skill_details(
            project_id=project_id,
            skill_id=skill_id,
            version_id=version_id,
        )

        if not result.get('data'):
            return {"error": "Skill not found"}, 404

        return result['data'], 200

    @register_openapi(
        name="Create a new version for an existing skill",
        description="Creates a new (non-base) version for the skill. Version name must be unique within the skill and must not be 'base'.",
        request_body=SkillVersionCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, skill_id: int, **kwargs):
        raw = dict(request.json)
        raw['author_id'] = auth.current_user().get("id")

        try:
            version_data = SkillVersionCreateModel.model_validate(raw)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        if version_data.name == 'base':
            return {"error": "Version name 'base' is reserved; use a different name"}, 400

        try:
            detail = create_skill_version(
                project_id=project_id,
                skill_id=skill_id,
                version_data=version_data,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status

        return detail, 201

    @register_openapi(
        name="Update a skill's metadata or a specific skill version",
        description="Without a version_id path segment, updates the skill metadata (name, description, meta) and optionally its default version content. With a version_id segment, updates that specific version (resolved by numeric id).",
        request_body=SkillUpdateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "path", "required": False, "schema": {"type": "integer"}, "description": "Optional numeric version id to update a specific version"},
        ],
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        # Update a specific version addressed by id.
        if version_id is not None:
            version = get_skill_version_by_id(
                project_id=project_id,
                skill_id=skill_id,
                version_id=version_id,
            )
            if not version:
                return {"error": f"Skill version '{version_id}' not found"}, 404

            try:
                update_data = SkillVersionUpdateModel.model_validate(dict(request.json))
            except ValidationError as e:
                return e.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                ), 400

            try:
                detail = update_skill_version(
                    project_id=project_id,
                    skill_id=skill_id,
                    version_id=version.id,
                    update_data=update_data,
                )
            except SkillError as exc:
                return {"error": str(exc)}, exc.http_status

            return detail, 200

        # Update skill metadata (and optionally the default version).
        payload = dict(request.json)
        payload['project_id'] = project_id
        payload['user_id'] = auth.current_user().get("id")

        try:
            update_data = SkillUpdateModel.model_validate(payload)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        try:
            detail = update_skill(
                project_id=project_id,
                skill_id=skill_id,
                update_data=update_data,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status

        return detail, 200

    @register_openapi(
        name="Link or unlink a skill to an agent version",
        description="Toggles the relation between a skill and an agent (application) version, mirroring the Link Agent to Toolkit flow. When has_relation is True the given skill version is attached (skill_version_id required); when False the skill is detached.",
        request_body=SkillUpdateRelationModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/skills"],
        mcp_tool=False,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def patch(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        # PATCH does not address a specific version in the path.
        if version_id is not None:
            return {"error": "version_id path segment is not supported for PATCH"}, 400

        try:
            relation_data = SkillUpdateRelationModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        if relation_data.has_relation:
            try:
                data = attach_skill_to_agent(
                    project_id=project_id,
                    entity_version_id=relation_data.entity_version_id,
                    skill_id=skill_id,
                    skill_version_id=relation_data.skill_version_id,
                    entity_type=relation_data.entity_type,
                )
            except SkillError as exc:
                return {"error": str(exc)}, exc.http_status
            return data, 201

        try:
            detach_skill_from_agent(
                project_id=project_id,
                entity_version_id=relation_data.entity_version_id,
                skill_id=skill_id,
                entity_type=relation_data.entity_type,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status
        return {'ok': True}, 200

    @register_openapi(
        name="Delete a skill or a specific skill version",
        description="Without a version_id path segment, permanently deletes the skill and all of its versions (agent attachments cascade-removed). With a version_id segment, deletes that specific version (cannot delete the only version or a version still attached to agents). Irreversible.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "path", "required": False, "schema": {"type": "integer"}, "description": "Optional numeric version id to delete a specific version"},
        ],
        tags=["elitea_core/skills"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        # Delete a specific version addressed by id.
        if version_id is not None:
            version = get_skill_version_by_id(
                project_id=project_id,
                skill_id=skill_id,
                version_id=version_id,
            )
            if not version:
                return {"error": f"Skill version '{version_id}' not found"}, 404

            try:
                delete_skill_version(
                    project_id=project_id,
                    skill_id=skill_id,
                    version_id=version.id,
                )
            except SkillError as exc:
                return {"error": str(exc)}, exc.http_status

            return '', 204

        # Delete the entire skill.
        try:
            delete_skill(
                project_id=project_id,
                skill_id=skill_id,
            )
        except SkillError as exc:
            return {"error": str(exc)}, exc.http_status

        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>',
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
