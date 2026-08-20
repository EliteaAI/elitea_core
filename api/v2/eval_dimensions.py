"""Eval **dimension** definitions — collection endpoint (list + create) for EVAL-P1-B1.

Dimensions are author-gated at the ``editor`` role per the EVAL-H3 RBAC decision
(EDITOR persona authors dimensions/datasets/suites). Platform-tier seeds are
read-only here and surfaced only on list.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDimensionCreateModel,
    EvalDimensionDetailModel,
)
from ...utils.evaluation_library_utils import (
    list_dimensions,
    create_dimension,
    EvalLibraryError,
)
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List eval dimension definitions in a project",
        description="Returns dimension definitions visible to the project (project + agent_adhoc tiers, plus read-only platform-tier seeds unless include_platform=false).",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "include_platform", "in": "query", "schema": {"type": "boolean", "default": True}, "description": "Include read-only platform-tier seeds"},
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
        include_platform = request.args.get("include_platform", default="true").lower() != "false"
        with db.get_session(project_id) as session:
            rows = list_dimensions(project_id, include_platform=include_platform, session=session)
            return [
                EvalDimensionDetailModel.model_validate(r).model_dump(mode='json')
                for r in rows
            ], 200

    @register_openapi(
        name="Create an eval dimension definition",
        description="Creates a project- or agent_adhoc-tier dimension definition. Platform-tier authoring is not exposed here (admin console only).",
        request_body=EvalDimensionCreateModel,
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
            data = EvalDimensionCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        owner_id = auth.current_user().get("id")
        try:
            dimension = create_dimension(project_id, data, owner_id=owner_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalDimensionDetailModel.model_validate(dimension).model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
