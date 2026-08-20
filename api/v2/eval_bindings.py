"""Eval **bindings** — collection endpoint (list + add + reorder) for EVAL-P1-B2.

A binding applies one library item (dimension / code-validation / platform key) within a
suite, with per-agent weight/target/engine/evidence, pinned to a concrete ApplicationVersion
(§16.3). Read is viewer-visible; add/reorder are editor-gated per EVAL-H3.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalBindingCreateModel,
    EvalBindingDetailModel,
    EvalBindingReorderModel,
)
from ...utils.evaluation_suite_utils import (
    list_bindings,
    add_binding,
    reorder_bindings,
)
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List bindings in an eval suite",
        description="Returns the suite's bindings ordered by order_index.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "suite_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.suite.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, suite_id: int, **kwargs):
        with db.get_session(project_id) as session:
            try:
                rows = list_bindings(project_id, suite_id, session=session)
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
            return [
                EvalBindingDetailModel.model_validate(r).model_dump(mode='json')
                for r in rows
            ], 200

    @register_openapi(
        name="Add a binding to an eval suite",
        description="Adds a binding for exactly one library item (dimension_id | code_validation_id | platform_key). If application_version_id is set it must belong to the suite's agent.",
        request_body=EvalBindingCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "suite_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.suite.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, suite_id: int, **kwargs):
        try:
            data = EvalBindingCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            binding = add_binding(project_id, suite_id, data)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalBindingDetailModel.model_validate(binding).model_dump(mode='json'), 201

    @register_openapi(
        name="Reorder bindings in an eval suite",
        description="Sets each binding's order_index to its position in binding_ids. The list must contain exactly the suite's current binding ids.",
        request_body=EvalBindingReorderModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "suite_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.suite.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, suite_id: int, **kwargs):
        try:
            data = EvalBindingReorderModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            rows = reorder_bindings(project_id, suite_id, data.binding_ids)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return [
            EvalBindingDetailModel.model_validate(r).model_dump(mode='json')
            for r in rows
        ], 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:suite_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
