"""Eval **suites** — collection endpoint (list + create/bootstrap) for EVAL-P1-B2.

A suite is a named binding set on an agent (§13, §16.2). Read is viewer-visible; authoring
is editor-gated per EVAL-H3 (EDITOR persona authors suites). ``?bootstrap=true`` returns the
app's idempotent 'Default suite', creating it if absent (§13 default-suite bootstrap).
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalSuiteCreateModel,
    EvalSuiteDetailModel,
)
from ...utils.evaluation_suite_utils import (
    list_suites,
    create_suite,
    bootstrap_default_suite,
)
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List eval suites in a project",
        description="Returns the project's eval suites, optionally filtered to a single agent via application_id.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "application_id", "in": "query", "schema": {"type": "integer"}, "description": "Filter to one agent"},
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
    def get(self, project_id: int, **kwargs):
        application_id = request.args.get("application_id", type=int)
        with db.get_session(project_id) as session:
            rows = list_suites(project_id, application_id=application_id, session=session)
            return [
                EvalSuiteDetailModel.model_validate(r).model_dump(mode='json')
                for r in rows
            ], 200

    @register_openapi(
        name="Create an eval suite",
        description="Creates a suite on an agent. With bootstrap=true the app's idempotent 'Default suite' is returned (created if absent) and the request body is ignored except for application_id.",
        request_body=EvalSuiteCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "bootstrap", "in": "query", "schema": {"type": "boolean", "default": False}, "description": "Return/create the agent's Default suite idempotently"},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.suite.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        try:
            data = EvalSuiteCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        owner_id = auth.current_user().get("id")
        bootstrap = request.args.get("bootstrap", default="false").lower() == "true"
        try:
            if bootstrap:
                suite = bootstrap_default_suite(project_id, data.application_id, owner_id=owner_id)
            else:
                suite = create_suite(project_id, data, owner_id=owner_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalSuiteDetailModel.model_validate(suite).model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
