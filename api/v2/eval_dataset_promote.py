"""Eval dataset **promote** — promote-from-conversations for EVAL-P1-B3 (§17.2, §8.3, E2E-06).

Turns a stored conversation into golden cases via the verified turn-extraction contract
(EVAL-H7): each user turn → a case ``input``; the agent reply → ``expected_output`` when
``include_expected``. Editor-gated (dataset content mutation).
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDatasetPromoteModel,
    EvalDatasetCaseDetailModel,
)
from ...utils.evaluation_dataset_utils import promote_from_conversation
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Promote a conversation into an eval dataset",
        description="Extracts (input, output) turn pairs from a conversation (§8.3) and appends them as conversation-sourced cases. include_expected controls whether the agent reply is stored as expected_output.",
        request_body=EvalDatasetPromoteModel,
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
            data = EvalDatasetPromoteModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            report = promote_from_conversation(
                project_id, dataset_id, data.conversation_id, include_expected=data.include_expected,
            )
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return {
            "accepted": report["accepted"],
            "cases": [
                EvalDatasetCaseDetailModel.model_validate(c_).model_dump(mode='json')
                for c_ in report["cases"]
            ],
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:dataset_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
