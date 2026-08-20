"""The platform eval-dimension catalog as seen from inside a project (§16.1).

``GET`` lists the active registry entries, each annotated with ``local_dimension_id`` — the id
of this project's own copy, or ``None`` if the project has never attached it. ``POST`` attaches
one: it copies the registry definition into the project schema and returns the **local**
dimension, whose id the caller then binds through the ordinary binding endpoint.

Attaching twice is a no-op that returns the same local row, so a double click cannot fork the
projection or orphan a binding.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.eval_platform_dimension import EvalPlatformAttachModel
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @property
    def _rpc(self):
        return self.module.context.rpc_manager.call

    @register_openapi(
        name="List the platform eval dimension catalog",
        description="Active platform-wide dimensions, each annotated with local_dimension_id if this project already attached it.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dimension.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        return self._rpc.elitea_core_platform_catalog_list(project_id=project_id), 200

    @register_openapi(
        name="Attach a platform eval dimension to a project",
        description="Copies the platform definition into the project and returns the local dimension. Idempotent.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dimension.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        try:
            body = EvalPlatformAttachModel.model_validate(request.json or {})
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            dimension = self._rpc.elitea_core_platform_catalog_materialize(
                project_id=project_id, dimension_uuid=body.uuid,
            )
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return dimension, 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
