"""Eval **runs** — collection endpoint (list + start) for EVAL-P1-B4.

Thin REST surface over the H5 engine (§7#6, §14.2): POST starts a run — offline-batch over a
stored dataset, or on-demand over a stored conversation — and returns ``202`` immediately while
a daemon thread runs it to completion; GET lists runs for the project (screen #6 feed). Read is
viewer-visible; starting a run is editor-gated per EVAL-H3. No orchestration logic lives here —
that is H5; this handler only validates, delegates to the run wrapper, and launches the worker.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalRunCreateModel,
    EvalRunSummaryModel,
)
from ...utils.evaluation_run_utils import (
    list_runs,
    create_batch_run,
    create_on_demand_run,
    launch_run,
    mark_run_unstarted,
)
from ...utils.evaluation_run_orchestration import TRIGGER_ON_DEMAND
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.publish_utils import get_validation_llm_settings
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List eval runs in a project",
        description="Returns the project's eval runs (newest first), optionally filtered by application_id and/or suite_id. Drives the run history / progress feed (screen #6).",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "application_id", "in": "query", "schema": {"type": "integer"}, "description": "Filter to one agent"},
            {"name": "suite_id", "in": "query", "schema": {"type": "integer"}, "description": "Filter to one suite"},
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
    def get(self, project_id: int, **kwargs):
        application_id = request.args.get("application_id", type=int)
        suite_id = request.args.get("suite_id", type=int)
        with db.get_session(project_id) as session:
            rows = list_runs(project_id, application_id=application_id, suite_id=suite_id, session=session)
            return [
                EvalRunSummaryModel.model_validate(r).model_dump(mode='json')
                for r in rows
            ], 200

    @register_openapi(
        name="Start an eval run",
        description="Starts a run and returns 202 while it executes in the background (poll GET /<run_id> for status/progress). trigger_type=offline_batch scores a stored dataset; on_demand scores a stored conversation's turns (reference-free only). Judge model and application version can be overridden.",
        request_body=EvalRunCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
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
    def post(self, project_id: int, **kwargs):
        try:
            data = EvalRunCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        owner_id = auth.current_user().get("id")
        try:
            if data.trigger_type == TRIGGER_ON_DEMAND:
                run = create_on_demand_run(
                    project_id, data.suite_id, data.conversation_id,
                    application_version_id=data.application_version_id,
                    judge_model=data.judge_model, owner_id=owner_id,
                )
            else:
                run = create_batch_run(
                    project_id, data.suite_id,
                    dataset_id=data.dataset_id,
                    application_version_id=data.application_version_id,
                    judge_model=data.judge_model, owner_id=owner_id,
                )
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        # Resolve the judge model with precedence: per-run override (§18.7) > the suite's frozen
        # judge_model (baked into the snapshot at create) > the project's low-tier default. Without
        # this final fallback an AI binding on a suite with no judge_model fails at execute time
        # ("llm_settings with model_name is required"). get_validation_llm_settings is only invoked
        # (RPC) when neither of the higher-precedence sources is set — `or` short-circuits.
        snapshot_judge = (run.snapshot or {}).get('suite', {}).get('judge_model')
        judge_settings = (
            data.judge_model
            or snapshot_judge
            or get_validation_llm_settings(project_id, run.application_version_id)
        )
        # A rejected submission means no pool slot (or maintenance mode) — the run is not queued
        # anywhere, so resolve the row here rather than answering 202 for work that will never
        # start and leaving it in `created` forever.
        task_id = launch_run(project_id, run.id, eval_task_node=self.module.eval_task_node,
                             judge_llm_settings=judge_settings)
        if task_id is None:
            reason = ('Could not start: too many evaluation runs are already in progress. '
                      'Wait for one to finish and start this run again.')
            run = mark_run_unstarted(project_id, run.id, reason)
            return {"error": reason,
                    "run": EvalRunSummaryModel.model_validate(run).model_dump(mode='json')}, 503
        return EvalRunSummaryModel.model_validate(run).model_dump(mode='json'), 202


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
