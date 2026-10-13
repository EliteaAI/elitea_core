"""Eval suite **pre-run estimate** — what a run of this suite would likely cost (design Q-S6).

``cases × per-case usage of the last finished run of this suite on the same version``, agent plus
judge, as a low / expected / high range. Only the same suite + version counts as history (#6716);
without it the answer is ``available: false`` ("No estimate (first run)"). The caller's remaining
monthly budget comes along, so the UI can warn before launch when the estimate is over it.
"""

from flask import request  # pylint: disable=E0401
from pylon.core.tools import log  # pylint: disable=E0611,E0401

from tools import api_tools, config as c, auth, register_openapi

from ...utils.evaluation_run_utils import suite_estimate_inputs
from ...utils.evaluation_usage import binding_budget, estimate_exceeds_budget, estimate_run
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE
from .project_budget import _budget_state
from .user_budget import _user_budget_state


def _remaining_budget(project_id: int, user_id):
    """The caller's tightest remaining budget, or None when unlimited or unreadable. The estimate
    stays useful without it, so a failed budget read is logged and left out."""
    try:
        project = _budget_state(project_id)
        member = _user_budget_state(project_id, user_id) if user_id is not None else None
    except Exception:  # pylint: disable=W0703
        log.exception("eval estimate: budget read failed for project %s", project_id)
        return None
    return binding_budget(project, member)


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Estimate an eval suite run",
        description="Estimates what a run of this suite would use: case count × per-case tokens and cost of the last finished run of the same suite and version (agent + judge), as a low / expected / high range, plus the caller's remaining monthly budget. available=false when this suite has not finished a run on that version.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "suite_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "application_version_id", "in": "query", "schema": {"type": "integer"},
             "description": "Version the run would use; defaults to the suite's pinned version."},
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
        value = request.args.get("application_version_id")
        try:
            version_id = int(value) if value not in (None, "") else None
        except ValueError:
            return {"error": "application_version_id must be an integer"}, 400
        try:
            inputs = suite_estimate_inputs(project_id, suite_id, application_version_id=version_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        estimate = estimate_run(inputs["history_rows"], inputs["cases"])
        budget = _remaining_budget(project_id, auth.current_user().get("id"))
        return {
            "available": estimate is not None,
            "application_version_id": inputs["application_version_id"],
            "cases": inputs["cases"],
            "history_run_id": inputs["history_run_id"],
            "history_finished_at": inputs["history_finished_at"],
            "estimate": estimate,
            "budget": budget,
            "exceeds_budget": estimate_exceeds_budget(estimate, budget),
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:suite_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
