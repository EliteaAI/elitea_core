"""Eval **case executions** — what the agent did on each case of a run (#6809 P1, design §3.1).

GET returns one page of the run's ``eval_case_execution`` rows: per case, the agent outcome,
whether a trajectory was recorded (and why not), its counters and — for a single case, or on
request — the normalized trajectory, plus the cases' agent and judge usage (#6716). Drives the Trajectory tab of the case drill-down. Read-only and viewer-visible, like the results endpoint.
"""

from flask import request  # pylint: disable=E0401

from tools import api_tools, config as c, db, auth, register_openapi

from ...utils.evaluation_result_utils import get_case_executions
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Read eval run case executions",
        description="Returns one page of a run's per-case agent executions: outcome status, trajectory state and reason, the trajectory counters, the normalized trajectory (LLM and tool steps) when one case is asked for or include_trajectory is set, and the page's agent and judge token and cost rows, with total, limit and offset. Empty for runs that did not execute the agent.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "case_index", "in": "query", "schema": {"type": "integer"},
             "description": "Only this case (its position in the run's frozen case list)."},
            {"name": "dataset_case_id", "in": "query", "schema": {"type": "integer"},
             "description": "Only this case (its dataset case id)."},
            {"name": "include_trajectory", "in": "query", "schema": {"type": "boolean"},
             "description": "Include the step lists. Defaults to true when case_index or dataset_case_id picks one case, false otherwise."},
            {"name": "limit", "in": "query", "schema": {"type": "integer"},
             "description": "Page size, by case_index: at most 5000 without trajectories, 10 with them."},
            {"name": "offset", "in": "query", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.run.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, run_id: int, **kwargs):
        filters = {}
        for name in ("case_index", "dataset_case_id", "limit", "offset"):
            value = request.args.get(name)
            try:
                filters[name] = int(value) if value not in (None, "") else None
            except ValueError:
                return {"error": f"{name} must be an integer"}, 400
        raw = request.args.get("include_trajectory")
        include_trajectory = None if raw in (None, "") else raw.lower() not in ("false", "0", "no")
        filters["offset"] = filters["offset"] or 0
        with db.get_session(project_id) as session:
            try:
                data = get_case_executions(
                    project_id, run_id, session=session,
                    include_trajectory=include_trajectory, **filters,
                )
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
        return data, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:run_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
