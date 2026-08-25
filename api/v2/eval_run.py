"""Eval **run** — item endpoint (status / progress / detail) for EVAL-P1-B4.

The poll target behind a started run (§14.2): GET returns the run's status, progress feed, and
headline for the detail screen (screen #6, E2E-09/E2E-11). Read-only and viewer-visible; runs are
started via the collection endpoint (:mod:`eval_runs`).

The frozen snapshot carries every case's input/output/expected_output, so it is opt-in via
``?include=snapshot`` — this endpoint is polled while a run is in flight and would otherwise ship
the whole case set on every tick.
"""

from flask import request  # pylint: disable=E0401

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import EvalRunDetailModel
from ...utils.evaluation_run_utils import get_run, delete_run
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get an eval run",
        description="Returns a single run: status, progress ({done,total}), headline_score and error. Poll this after starting a run to drive the progress feed. The frozen snapshot is omitted unless include=snapshot is passed.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "include", "in": "query", "schema": {"type": "string"},
             "description": "Comma-separated extras. Pass 'snapshot' to embed the frozen case set."},
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
        include = {
            part.strip() for part in (request.args.get("include") or "").split(",") if part.strip()
        }
        with db.get_session(project_id) as session:
            try:
                run = get_run(project_id, run_id, session=session)
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
            detail = EvalRunDetailModel.model_validate(run)
            # Excluded during the dump rather than popped afterwards: serialising the whole frozen
            # case set only to discard it is the expensive half of the work, and this endpoint is
            # the one the client falls back to polling.
            exclude = set() if "snapshot" in include else {"snapshot"}
            return detail.model_dump(mode='json', exclude=exclude), 200

    @register_openapi(
        name="Delete an eval run",
        description=(
            "Hard-deletes a run and all its per-case/per-dimension results and human-score audit "
            "rows. Does not affect the dataset, suite, or dimension definitions the run referenced. "
            "Irreversible — the caller is expected to have confirmed with the user before calling."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.run.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, run_id: int, **kwargs):
        try:
            delete_run(project_id, run_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status
        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:run_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
