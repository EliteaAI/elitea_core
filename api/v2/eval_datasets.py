"""Eval **datasets** — collection endpoint (list + create) for EVAL-P1-B3 (§17.1, §17.3).

A dataset is a project-scoped set of golden cases. Read is viewer-visible; authoring is
editor-gated per EVAL-H3. The list shape carries case counters (§17.3) rather than the full
case set.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDatasetCreateModel,
    EvalDatasetSummaryModel,
    EvalDatasetDetailModel,
)
from ...utils.evaluation_dataset_utils import list_datasets, create_dataset, can_edit_dataset
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List eval datasets in a project",
        description=(
            "Returns the project's eval datasets with case counters (total + with expected output). "
            "Pass agent_id (#6350) to scope the list to that agent's own datasets plus any dataset "
            "another agent has opted to share project-wide; omit it for the unfiltered project view."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "agent_id", "in": "query", "schema": {"type": "integer"}, "required": False},
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
    def get(self, project_id: int, **kwargs):
        agent_id = request.args.get('agent_id', type=int)
        with db.get_session(project_id) as session:
            rows = list_datasets(project_id, agent_id=agent_id, session=session)
            payload = []
            for r in rows:
                item = EvalDatasetSummaryModel.model_validate(r).model_dump(mode='json')
                item['can_edit'] = can_edit_dataset(item.get('agent_id'), agent_id)
                payload.append(item)
            return payload, 200

    @register_openapi(
        name="Create an eval dataset",
        description="Creates an empty dataset (cases are added via import, promote, or the cases API).",
        request_body=EvalDatasetCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dataset.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        try:
            data = EvalDatasetCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        owner_id = auth.current_user().get("id")
        try:
            dataset = create_dataset(project_id, data, owner_id=owner_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalDatasetDetailModel.model_validate(dataset).model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
