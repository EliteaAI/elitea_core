"""Eval **suite** — item endpoint (get + update + delete) for EVAL-P1-B2.

Read is viewer-visible; mutation is editor-gated per EVAL-H3. Deleting a suite cascades to
its bindings (§16.2). ``judge_model`` override and ``baseline_run_id`` are editable here.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.evaluation import (
    EvalSuiteUpdateModel,
    EvalSuiteDetailModel,
)
from ...utils.evaluation_suite_utils import (
    get_suite,
    update_suite,
    delete_suite,
)
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get an eval suite",
        description="Returns a single eval suite with its bindings.",
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
        suite = get_suite(project_id, suite_id)
        if not suite:
            return {"error": f"Eval suite with id {suite_id} not found"}, 404
        return EvalSuiteDetailModel.model_validate(suite).model_dump(mode='json'), 200

    @register_openapi(
        name="Update an eval suite",
        description="Updates suite-level fields (name, dataset, judge_model override, baseline_run_id, trigger_config). The agent (application_id) is immutable post-create.",
        request_body=EvalSuiteUpdateModel,
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
            data = EvalSuiteUpdateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            suite = update_suite(project_id, suite_id, data)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalSuiteDetailModel.model_validate(suite).model_dump(mode='json'), 200

    @register_openapi(
        name="Delete an eval suite",
        description="Deletes a suite and cascades to its bindings. Irreversible.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "suite_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.suite.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, suite_id: int, **kwargs):
        try:
            delete_suite(project_id, suite_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status
        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:suite_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
