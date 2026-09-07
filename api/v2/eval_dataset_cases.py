"""Eval dataset **cases** — nested collection endpoint (list + add) for EVAL-P1-B3 (§17.4).

Cases carry ``input`` + optional ``variables`` + optional ``expected_output`` (§17.1). Read is
viewer-visible; adding a case is editor-gated (dataset content mutation). New cases append to
the end of the ordered set.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDatasetCaseCreateModel,
    EvalDatasetCaseDetailModel,
)
from ...utils.evaluation_dataset_utils import (
    add_case,
    list_cases,
    DEFAULT_CASE_LIMIT,
    MAX_CASE_LIMIT,
)
from ...utils.evaluation_suite_utils import list_case_exclusions
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List cases in an eval dataset",
        description="Returns one page of a dataset's cases ordered by order_index, in a {total, limit, offset, cases} envelope.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dataset_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "limit", "in": "query", "schema": {"type": "integer"},
             "description": f"Cases per page (default {DEFAULT_CASE_LIMIT}, max {MAX_CASE_LIMIT})."},
            {"name": "offset", "in": "query", "schema": {"type": "integer"},
             "description": "Cases to skip."},
            {"name": "suite_id", "in": "query", "schema": {"type": "integer"}, "required": False,
             "description": "Annotates each case with excluded (#6350) — whether that suite "
                             "drops the case from its runs. Omit for the plain dataset view. "
                             "An unknown suite_id is a 404."},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dataset.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, dataset_id: int, **kwargs):
        try:
            limit = int(request.args.get("limit", DEFAULT_CASE_LIMIT))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return {"error": "limit and offset must be integers"}, 400
        agent_id = request.args.get('agent_id', type=int)
        suite_id = request.args.get('suite_id', type=int)
        with db.get_session(project_id) as session:
            try:
                page = list_cases(
                    project_id, dataset_id, agent_id=agent_id, session=session,
                    limit=limit, offset=offset,
                )
                # Goes through list_case_exclusions rather than reading the table directly so an
                # unknown suite_id 404s here exactly as it does on the exclusions endpoint: an
                # empty set is indistinguishable from "that suite excludes nothing", and a typo'd
                # id would render every case as included.
                excluded = (
                    set(list_case_exclusions(project_id, suite_id, session=session))
                    if suite_id is not None else set()
                )
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
            cases = []
            for c_ in page["cases"]:
                item = EvalDatasetCaseDetailModel.model_validate(c_).model_dump(mode='json')
                item["excluded"] = item["id"] in excluded
                cases.append(item)
            return {
                "total": page["total"],
                "limit": page["limit"],
                "offset": page["offset"],
                "cases": cases,
            }, 200

    @register_openapi(
        name="Add a case to an eval dataset",
        description="Appends a manually authored case to the dataset (§17.1).",
        request_body=EvalDatasetCaseCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dataset_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dataset.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, dataset_id: int, **kwargs):
        try:
            data = EvalDatasetCaseCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        agent_id = request.args.get('agent_id', type=int)
        try:
            case = add_case(project_id, dataset_id, data, agent_id=agent_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalDatasetCaseDetailModel.model_validate(case).model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:dataset_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
