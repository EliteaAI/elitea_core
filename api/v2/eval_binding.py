"""Eval **binding** — item endpoint (get + update + delete) for EVAL-P1-B2.

Read is viewer-visible; mutation is editor-gated per EVAL-H3. Only binding knobs
(weight/target/engine/evidence/order/version-pin) are editable — the bound source
(dimension/code/platform) is immutable post-create (§16.2).
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.evaluation import (
    EvalBindingUpdateModel,
    EvalBindingDetailModel,
)
from ...utils.evaluation_suite_utils import (
    get_binding,
    update_binding,
    delete_binding,
)
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get an eval binding",
        description="Returns a single binding within a suite.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "suite_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "binding_id", "in": "path", "schema": {"type": "integer"}},
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
    def get(self, project_id: int, suite_id: int, binding_id: int, **kwargs):
        binding = get_binding(project_id, suite_id, binding_id)
        if not binding:
            return {"error": f"Eval binding with id {binding_id} not found"}, 404
        return EvalBindingDetailModel.model_validate(binding).model_dump(mode='json'), 200

    @register_openapi(
        name="Update an eval binding",
        description="Updates binding knobs (weight, target, engine, evidence_scope, order_index, version pin). The bound source is immutable.",
        request_body=EvalBindingUpdateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "suite_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "binding_id", "in": "path", "schema": {"type": "integer"}},
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
    def put(self, project_id: int, suite_id: int, binding_id: int, **kwargs):
        try:
            data = EvalBindingUpdateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            binding = update_binding(project_id, suite_id, binding_id, data)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalBindingDetailModel.model_validate(binding).model_dump(mode='json'), 200

    @register_openapi(
        name="Delete an eval binding",
        description="Removes a binding from a suite. Irreversible.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "suite_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "binding_id", "in": "path", "schema": {"type": "integer"}},
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
    def delete(self, project_id: int, suite_id: int, binding_id: int, **kwargs):
        try:
            delete_binding(project_id, suite_id, binding_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status
        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:suite_id>/<int:binding_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
