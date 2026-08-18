"""Eval **dimension** definition — item endpoint (get + update + delete) for EVAL-P1-B1.

Read is viewer-visible; mutation is editor-gated per EVAL-H3. Platform-tier
definitions are immutable from the project library (403).
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDimensionUpdateModel,
    EvalDimensionDetailModel,
)
from ...utils.evaluation_library_utils import (
    get_dimension,
    update_dimension,
    delete_dimension,
    EvalLibraryError,
)
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get an eval dimension definition",
        description="Returns a single dimension definition by id.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dimension_id", "in": "path", "schema": {"type": "integer"}},
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
    def get(self, project_id: int, dimension_id: int, **kwargs):
        dimension = get_dimension(project_id, dimension_id)
        if not dimension:
            return {"error": f"Eval dimension with id {dimension_id} not found"}, 404
        return EvalDimensionDetailModel.model_validate(dimension).model_dump(mode='json'), 200

    @register_openapi(
        name="Update an eval dimension definition",
        description="Updates a project- or agent_adhoc-tier dimension. Tier is immutable post-create; platform-tier definitions cannot be modified from the project library.",
        request_body=EvalDimensionUpdateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dimension_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dimension.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, dimension_id: int, **kwargs):
        try:
            data = EvalDimensionUpdateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            dimension = update_dimension(project_id, dimension_id, data)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalDimensionDetailModel.model_validate(dimension).model_dump(mode='json'), 200

    @register_openapi(
        name="Delete an eval dimension definition",
        description="Deletes a project- or agent_adhoc-tier dimension. Platform-tier definitions cannot be deleted from the project library. Irreversible.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dimension_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dimension.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, dimension_id: int, **kwargs):
        try:
            delete_dimension(project_id, dimension_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status
        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:dimension_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
