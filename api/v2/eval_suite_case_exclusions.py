"""Eval suite **case exclusions** — collection endpoint (get + replace) for #6350.

A shared dataset belongs to the agent that authored it, so a borrowing suite cannot edit its
cases. Excluding a case here drops it from this suite's runs only; the origin dataset is
untouched and every other suite still runs the full set.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import EvalSuiteCaseExclusionsModel
from ...utils.evaluation_suite_utils import list_case_exclusions, set_case_exclusions
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List a suite's excluded dataset cases",
        description=(
            "Returns the ids of the suite's dataset cases that are excluded from its runs. "
            "The origin dataset is unaffected; other suites on the same dataset run the full set."
        ),
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
                case_ids = list_case_exclusions(project_id, suite_id, session=session)
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
            return {"case_ids": case_ids}, 200

    @register_openapi(
        name="Replace a suite's excluded dataset cases",
        description=(
            "Replaces the exclusion set with case_ids; an empty list clears it. Every id must "
            "belong to the suite's own dataset. Excluding every case makes the suite unrunnable "
            "until at least one is restored."
        ),
        request_body=EvalSuiteCaseExclusionsModel,
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
            data = EvalSuiteCaseExclusionsModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            case_ids = set_case_exclusions(project_id, suite_id, data.case_ids)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return {"case_ids": case_ids}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:suite_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
