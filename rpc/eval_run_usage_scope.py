from pylon.core.tools import web, log

from tools import db  # pylint: disable=E0401

from ..models.evaluation import EvalRun


class RPC:
    @web.rpc("elitea_core_eval_run_usage_scope", "eval_run_usage_scope")
    def eval_run_usage_scope(self, project_id: int, eval_run_id: int) -> dict | None:
        """What usage analytics needs to scope its rows to one eval run, or None if not found."""
        try:
            with db.get_session(project_id) as session:
                run = session.query(
                    EvalRun.meta, EvalRun.started_at, EvalRun.finished_at, EvalRun.created_at,
                ).filter(EvalRun.id == eval_run_id).first()
                if run is None:
                    return None
                # Naive UTC timestamps, sent as ISO strings so the payload stays serialisable
                started_at = run.started_at or run.created_at
                return {
                    "platform_run_id": (run.meta or {}).get("platform_run_id"),
                    "started_at": started_at.isoformat() if started_at else None,
                    "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                }
        except Exception:  # pylint: disable=broad-except
            log.exception("eval run usage scope lookup failed: project %s run %s", project_id, eval_run_id)
            raise
