"""Eval **run compare** — the run-over-run performance diff (performance design §3.4, §21.7).

GET compares a run with a reference run: p50/p95 latency, the final-answer time, agent tokens and
cost, tool calls and the cases that newly timed out, over the cases both runs measured. The
reference is ``reference_run_id`` when given, else the suite's baseline, else the previous finished
run on the same dataset. Computed on read; nothing is stored. Viewer-visible, like the results.
"""

from flask import request  # pylint: disable=E0401

from tools import api_tools, config as c, db, auth, register_openapi

from ...utils.evaluation_run_compare import compare_run_performance
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Compare eval run performance",
        description="Compares a run with a reference run, case by case aligned on dataset_case_id: p50/p95 latency, p50 final-answer time, agent tokens, agent cost and tool calls over the cases both runs measured, cases that newly timed out or no longer do, and the cases left out (changed input, missing on one side, unpriced). Latency changes within ±15 % or 2 s read as within_noise. The reference defaults to the suite baseline, then the previous finished run on the same dataset; reference and diff are null when there is none.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "reference_run_id", "in": "query", "schema": {"type": "integer"},
             "description": "Compare with this run instead of the default reference."},
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
        raw = request.args.get("reference_run_id")
        try:
            reference_run_id = int(raw) if raw not in (None, "") else None
        except ValueError:
            return {"error": "reference_run_id must be an integer"}, 400
        with db.get_session(project_id) as session:
            try:
                data = compare_run_performance(project_id, run_id, reference_run_id, session=session)
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
