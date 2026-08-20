"""Eval **code-validation** definitions — collection endpoint (list + create) for EVAL-P1-B1.

Code validations are project-tier only (§19.6) and gated stricter than dimensions:
authoring requires the ``admin`` role (MAINTAINER persona) per EVAL-H3 — ``editor``
is denied. Every body passes the Layer-1 AST pre-screen before it can be stored.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalCodeValidationCreateModel,
    EvalCodeValidationDetailModel,
)
from ...utils.evaluation_library_utils import (
    list_code_validations,
    create_code_validation,
    EvalLibraryError,
)
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List eval code-validation definitions in a project",
        description="Returns the project's code-validation definitions (Code engine, project-tier only).",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
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
    def get(self, project_id: int, **kwargs):
        with db.get_session(project_id) as session:
            rows = list_code_validations(project_id, session=session)
            return [
                EvalCodeValidationDetailModel.model_validate(r).model_dump(mode='json')
                for r in rows
            ], 200

    @register_openapi(
        name="Create an eval code-validation definition",
        description="Creates a project-tier code-validation. The code body must pass the Layer-1 AST safety pre-screen (dangerous imports/builtins/dunder traversal are rejected) before it is stored.",
        request_body=EvalCodeValidationCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.code_validation.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        try:
            data = EvalCodeValidationCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        owner_id = auth.current_user().get("id")
        try:
            code_validation = create_code_validation(project_id, data, owner_id=owner_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalCodeValidationDetailModel.model_validate(code_validation).model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
