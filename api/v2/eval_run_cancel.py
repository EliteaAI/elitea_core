"""Eval **run cancel** — stop a queued or in-flight run (§14.2 durability).

A separate module from :mod:`eval_run` because ``APIBase`` maps one handler method per HTTP verb
and this is a verb on the run, not a write of the run itself.

A 50-case run can take hours (agent execution plus judge and code dispatches per case), so without
this the only recourse for a run started by mistake — wrong dataset, wrong version pin, wrong judge
model — was to let it burn through the whole dataset. Gated on the ``run.create`` permission:
whoever may spend the budget may stop spending it.
"""

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.evaluation import EvalRunDetailModel
from ...utils.evaluation_run_utils import request_cancel
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Cancel an eval run",
        description=(
            "Requests that a run stop. A run still 'created' is cancelled immediately; a 'running' "
            "run stops at its next case boundary and keeps the cases it already scored as a partial "
            "scorecard, so the status stays 'running' until the worker writes the terminal row. "
            "Returns 409 if the run already finished, errored, or was cancelled."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.run.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, run_id: int, **kwargs):
        try:
            run = request_cancel(project_id, run_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status
        return EvalRunDetailModel.model_validate(run).model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:run_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
