"""Evaluation-run usage attribution (#6677).

Twin contract: mirrors pylon_indexer's
indexer_worker.utils.usage_tool_events.{ENTITY_KWARGS_KEY,ROOT_ENTITY_KWARGS_KEY,
ENTITY_TYPE_EVALUATION} — pylon_main doesn't import indexer_worker at runtime, same
pattern as run_id.py's PREDICT_RUN_ID_KWARGS_KEY twin.
"""

from typing import Optional

ENTITY_KWARGS_KEY = "_elitea_entity"
ROOT_ENTITY_KWARGS_KEY = "_elitea_root_entity"
ENTITY_TYPE_APPLICATION = "application"
ENTITY_TYPE_EVALUATION = "evaluation"


def evaluation_attribution(
    eval_run_id: Optional[int],
    application_id: Optional[int],
    application_version_id: Optional[int],
) -> Optional[dict]:
    """`{'entity': {...}, 'root': {...}}` for a judge/eval-agent predict, or None.

    Leaf marks the call as evaluation spend (entity_id = the eval run, not the
    application, so entity_type/entity_id stay a consistent pair); root keeps the
    real application identity so "spend on evaluation for this agent" stays queryable.
    """
    if not application_id or not eval_run_id:
        return None
    return {
        "entity": {
            "type": ENTITY_TYPE_EVALUATION,
            "id": eval_run_id,
        },
        "root": {
            "type": ENTITY_TYPE_APPLICATION,
            "id": application_id,
            "version_id": application_version_id,
        },
    }
