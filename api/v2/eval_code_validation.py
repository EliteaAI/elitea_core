"""Eval **code-validation** definition — item endpoint (get + update + delete) for EVAL-P1-B1.

Read is viewer-visible; update/delete require the ``admin`` role (MAINTAINER) per
EVAL-H3 — code-validation authoring is gated stricter than dimensions. A changed
code body is re-screened by the Layer-1 AST pre-screen before it is stored.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.evaluation import (
    EvalCodeValidationUpdateModel,
    EvalCodeValidationDetailModel,
)
from ...utils.evaluation_library_utils import (
    get_code_validation,
    update_code_validation,
    delete_code_validation,
    EvalLibraryError,
)
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get an eval code-validation definition",
        description="Returns a single code-validation definition by id.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "code_validation_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.code_validation.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, code_validation_id: int, **kwargs):
        code_validation = get_code_validation(project_id, code_validation_id)
        if not code_validation:
            return {"error": f"Code validation with id {code_validation_id} not found"}, 404
        return EvalCodeValidationDetailModel.model_validate(code_validation).model_dump(mode='json'), 200

    @register_openapi(
        name="Update an eval code-validation definition",
        description="Updates a project-tier code-validation. If the code body changes it is re-run through the Layer-1 AST safety pre-screen before being stored.",
        request_body=EvalCodeValidationUpdateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "code_validation_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.code_validation.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, code_validation_id: int, **kwargs):
        try:
            data = EvalCodeValidationUpdateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            code_validation = update_code_validation(project_id, code_validation_id, data)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalCodeValidationDetailModel.model_validate(code_validation).model_dump(mode='json'), 200

    @register_openapi(
        name="Delete an eval code-validation definition",
        description="Deletes a project-tier code-validation definition. Irreversible.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "code_validation_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.code_validation.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, code_validation_id: int, **kwargs):
        try:
            delete_code_validation(project_id, code_validation_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status
        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:code_validation_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
