"""Platform run id — correlates one predict/eval run's LLM and tool usage (#6569).

Minted at the shared ``task_node.start_task`` seam in :meth:`module.Module.init`, so a
new dispatch path cannot forget to correlate its usage. The indexer worker lifts it onto
the gateway request as ``X-Elitea-Run-Id``.
"""

import uuid
from typing import Optional

# Twin contract: mirrors elitea_sdk.runtime.utils.utils.PREDICT_RUN_ID_KWARGS_KEY
# (pylon_main doesn't import elitea_sdk at runtime, unlike pylon_indexer — see
# models/indexer.py's twin-contract comment for the same pattern). Distinct
# from IndexerKeywords.RUN_ID / '_elitea_run_id', which identifies a vector
# *index* run, not a predict/eval run.
PREDICT_RUN_ID_KWARGS_KEY = "_elitea_predict_run_id"
PLATFORM_RUN_ID_META_KEY = "platform_run_id"

#: Namespace for ids derived from a dispatch's own stable identity (see below).
_RUN_ID_NAMESPACE = uuid.UUID("6f1d0e2a-6569-4f2a-9c1e-7a5b3d9c0e11")

# Every id below is emitted in canonical hyphenated form, not .hex: the usage plugin stores it
# in a Postgres ``uuid`` column (usage.models.usage_event.UsageEvent.run_id), which canonicalises
# on write, so an unhyphenated id would not compare equal to what a usage report reads back.


def derived_run_id(payload: dict) -> Optional[str]:
    """Run id derived from the dispatch's own stable identity, or None.

    One run can be dispatched several times: a HITL / toolkit-authorization / token-limit
    resume regenerates the payload from the persisted message instead of replaying the
    first dispatch's kwargs, so a freshly minted uuid would bill one interrupted run as
    two. Deriving from the identity that *is* stable across re-dispatch keeps them on one
    id without every resume path having to remember to propagate it.

      * chat / predict — the response message group, plus the execution generation so a
        retry of the same message stays a separate run;
      * voice TTS/ASR — the socket session, so a session's per-chunk dispatches group
        into one run instead of each chunk becoming its own.
    """
    message_id = payload.get("message_id")
    if message_id:
        generation = payload.get("execution_generation") or 0
        return str(uuid.uuid5(_RUN_ID_NAMESPACE, f"message:{message_id}:{generation}"))
    sid = payload.get("sid")
    if sid:
        return str(uuid.uuid5(_RUN_ID_NAMESPACE, f"sid:{sid}"))
    return None


def stamp_predict_run_id(call_kwargs: dict) -> str:
    """Stamp a platform run id onto a ``start_task`` call and return it.

    Precedence: an id already carried in the payload wins (sub-agent child dispatch,
    parent reconcile, eval run), then one derived from the dispatch's stable identity,
    then a fresh uuid.
    """
    payload = call_kwargs.setdefault("kwargs", {})
    run_id = payload.get(PREDICT_RUN_ID_KWARGS_KEY)
    if not run_id:
        run_id = derived_run_id(payload) or str(uuid.uuid4())
        payload[PREDICT_RUN_ID_KWARGS_KEY] = run_id
    call_kwargs.setdefault("meta", {}).setdefault(PLATFORM_RUN_ID_META_KEY, run_id)
    return run_id
