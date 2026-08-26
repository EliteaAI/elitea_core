import threading
import time
from datetime import datetime, UTC

from sqlalchemy import text

from pylon.core.tools import web, log
from tools import db, rpc_tools, VaultClient

from ..models.elitea_tools import EliteATool
from ..models.indexer import EmbeddingStore
from ..models.enums.all import IndexDataStatus
from ..utils.application_tools import (
    TASK_LOST,
    UNTRACKED_RECLAIM_AGE_FACTOR,
    _expand_toolkit_settings,
    _get_pgvector_engine,
    get_session_for_schema,
    reclaim_toolkit_index_meta,
    should_reclaim_index_meta,
)
from ..utils.utils import make_yield_to_hub, end_ambient_transaction
from ..utils.maintenance_gate import is_maintenance_active


# Re-entrancy guard: skip overlapping ticks if a previous run is still in flight.
_reclaim_lock = threading.Lock()

DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC = 7200

# Outlives a Redis reconnect (retry_interval 3s) and any transient event-bus blackout,
# so a task must miss two independent liveness queries before its row is rewritten.
# Paid at most once per tick, and only when the first pass found suspects.
LIVENESS_CONFIRM_DELAY_SEC = 20


def _resolve_task_liveness(task_node, task_id: str, verdicts: dict) -> str:
    if task_id in verdicts:
        return verdicts[task_id]
    # Our node caches 'running' announcements forever; a task whose runner died hard
    # never retracts one. Evict the cache entry so get_task_status must re-query the
    # network — only a node that still holds the task can answer. (A peer's stale
    # cache can still answer 'running' for a dead task; that keeps the row as-is,
    # which is safe — the UI's stale affordances remain the escape hatch.)
    with task_node.lock:
        task_node.global_task_state.pop(task_id, None)
    try:
        verdict = task_node.get_task_status(task_id)
    except RuntimeError:
        verdict = TASK_LOST
    except Exception as e:
        log.warning(f"Task liveness query failed for task {task_id}: {e}")
        verdict = 'unknown'
    verdicts[task_id] = verdict
    return verdict


def _find_registered_task(module, project_id: int, toolkit_id: int, index_name: str):
    """Reverse lookup in the in-process run registry: gives an untracked row a
    probeable task_id when this pylon saw the run's events. A miss is NOT evidence
    of death — the registry dies with the process, and a pylon restart is itself a
    primary way runs get abandoned.
    """
    registry_key = (project_id, toolkit_id, index_name)
    with module.active_index_tasks_lock:
        for task_key, entries in module.active_index_tasks.items():
            if registry_key in entries:
                return task_key
    return None


def _resolve_pgvector_connection(project_id: int, ref, author_id, memo: dict):
    """Expand ONLY the pgvector credential reference, never the whole toolkit config.

    Full config expansion fails the toolkit when ANY sibling field is broken (renamed
    embedding model, foreign private credential), which would make such toolkits
    permanently unsweepable — and it costs an author RPC plus model validation the
    sweep never reads.

    Memoized per tick only: nothing fires an event on a configuration `data` edit,
    so a cross-tick cache could hold a rotated-away connection string.
    """
    if not isinstance(ref, dict):
        return None
    if 'connection_string' in ref:
        return ref.get('connection_string')
    key = (project_id, ref.get('elitea_title'), author_id if ref.get('private') else None)
    if key not in memo:
        try:
            expanded = _expand_toolkit_settings(
                {'elitea_title': ref.get('elitea_title'), 'private': bool(ref.get('private'))},
                project_id,
                author_id,
            )
            memo[key] = (expanded or {}).get('connection_string')
        except Exception as e:
            log.warning(f"reclaim: pgvector configuration unresolvable in project {project_id}: {e}")
            memo[key] = None
    return memo[key]


def _schemas_with_embeddings(connection_string: str) -> set:
    """Which schemas in this database already hold an embeddings table.

    Lets the sweep skip never-indexed toolkits without get_session_for_schema, whose
    first touch CREATEs the schema — a write side effect a housekeeping read must
    not have on customer-owned databases.
    """
    engine = _get_pgvector_engine(connection_string)
    with engine.connect() as connection:
        rows = connection.execute(text(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_name = 'langchain_pg_embedding'"
        )).fetchall()
    return {row[0] for row in rows}


def _scan_toolkit_suspects(module, project_id: int, toolkit_id: int, connection_string: str,
                           timeout: int, reclaim_untracked: bool, probe_verdicts: dict) -> list:
    schema = str(toolkit_id)
    with get_session_for_schema(connection_string, schema) as session:
        rows = session.query(
            EmbeddingStore.id,
            EmbeddingStore.cmetadata,
        ).filter(
            # Containment instead of ->> equality: only @> can use the schema's
            # GIN jsonb_path_ops index; the astext form forces a full scan of a
            # table that holds one row per embedded chunk.
            EmbeddingStore.cmetadata.contains({
                'type': 'index_meta',
                'state': IndexDataStatus.in_progress.value,
            }),
        ).all()
    # The liveness probe can block up to the arbiter's query_wait per lost task,
    # so it runs on detached row data with no schema session held open.
    suspects = []
    for _, cmetadata in rows:
        probed = dict(cmetadata)
        if not probed.get('task_id'):
            registered = _find_registered_task(module, project_id, toolkit_id, probed.get('collection'))
            if registered:
                probed['task_id'] = registered
        if should_reclaim_index_meta(
            probed, time.time(), timeout,
            lambda task_id: _resolve_task_liveness(module.task_node, task_id, probe_verdicts),
            reclaim_untracked,
        ):
            suspects.append({
                'project_id': project_id,
                'toolkit_id': toolkit_id,
                'connection_string': connection_string,
                'schema': schema,
                'cmetadata': cmetadata,
                'probed_task_id': probed.get('task_id'),
                'timeout': timeout,
            })
    return suspects


def _collect_project_suspects(module, project_id: int, yield_to_hub, reclaim_untracked: bool,
                              conn_memo: dict, probe_verdicts: dict) -> list:
    secrets = VaultClient(project_id).get_secrets()
    # Same per-project threshold the index_meta GET uses for its read-time 'stale'
    # flag, so the persisted state and the flag can never disagree about the cutoff.
    timeout = int(secrets.get('task_disconnected_timeout_sec', DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC))
    with db.get_session(project_id) as project_session:
        toolkits = [
            (row.id, row.author_id, row.ref) for row in project_session.query(
                EliteATool.id,
                EliteATool.author_id,
                EliteATool.settings['pgvector_configuration'].label('ref'),
            ).filter(
                EliteATool.settings['pgvector_configuration'].isnot(None)
            ).all()
        ]
    by_connection = {}
    for toolkit_id, author_id, ref in toolkits:
        yield_to_hub()
        connection_string = _resolve_pgvector_connection(project_id, ref, author_id, conn_memo)
        if connection_string:
            by_connection.setdefault(connection_string, []).append(toolkit_id)
    suspects = []
    for connection_string, toolkit_ids in by_connection.items():
        yield_to_hub()
        end_ambient_transaction()
        try:
            populated_schemas = _schemas_with_embeddings(connection_string)
        except Exception as e:
            log.error(f"reclaim: cannot inspect pgvector database for project {project_id}: {e}")
            continue
        for toolkit_id in toolkit_ids:
            if str(toolkit_id) not in populated_schemas:
                continue
            yield_to_hub()
            end_ambient_transaction()
            try:
                suspects.extend(_scan_toolkit_suspects(
                    module, project_id, toolkit_id, connection_string, timeout,
                    reclaim_untracked, probe_verdicts,
                ))
            except Exception as e:
                log.error(
                    f"reclaim_interrupted_indexes: toolkit {toolkit_id} "
                    f"in project {project_id} failed: {e}"
                )
    return suspects


class RPC:
    @web.rpc("elitea_core_reclaim_interrupted_indexes")
    def reclaim_interrupted_indexes(self, **kwargs):
        # Skip the tick while maintenance mode is on: task nodes may be tearing down,
        # and a mass "unknown task" answer would misread every live run as dead.
        if is_maintenance_active():
            log.info("reclaim_interrupted_indexes: maintenance mode active, skipping tick")
            return None
        #
        if not _reclaim_lock.acquire(blocking=False):
            log.warning("reclaim_interrupted_indexes: previous tick still running, skipping")
            return None

        tick_started = time.monotonic()
        suspect_count = 0
        reclaimed_total = 0
        try:
            reclaim_cfg = (self.descriptor.config.get('scheduler') or {}).get('index_reclaim') or {}
            reclaim_untracked = bool(reclaim_cfg.get('reclaim_untracked', False))
            yield_to_hub = make_yield_to_hub(self.context.web_runtime)

            all_project_ids = [
                project_['id'] for project_ in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                    filter_={'create_success': True}
                )
            ]

            conn_memo = {}
            first_pass_verdicts = {}
            suspects = []
            for project_id in all_project_ids:
                # Yield between projects so a long tick does not starve the gevent hub.
                yield_to_hub()
                end_ambient_transaction()
                try:
                    suspects.extend(_collect_project_suspects(
                        self, project_id, yield_to_hub, reclaim_untracked,
                        conn_memo, first_pass_verdicts,
                    ))
                except Exception as e:
                    log.error(f"reclaim_interrupted_indexes: project {project_id} failed: {e}")
            suspect_count = len(suspects)
            if not suspects:
                return None

            # A lost-task verdict from a single broadcast can be a transient bus
            # blackout rather than a dead task; only a suspect that fails a second,
            # independent probe is rewritten.
            time.sleep(LIVENESS_CONFIRM_DELAY_SEC)
            second_pass_verdicts = {}
            for suspect in suspects:
                probed = dict(suspect['cmetadata'])
                probed['task_id'] = suspect['probed_task_id']
                if not should_reclaim_index_meta(
                    probed, time.time(), suspect['timeout'],
                    lambda task_id: _resolve_task_liveness(self.task_node, task_id, second_pass_verdicts),
                    reclaim_untracked,
                ):
                    log.info(
                        f"reclaim: pass-2 downgraded suspect index={probed.get('collection')} "
                        f"(project={suspect['project_id']}, toolkit={suspect['toolkit_id']}) — "
                        f"task answered on the confirm probe"
                    )
                    continue
                cmetadata = suspect['cmetadata']
                min_updated_age = (
                    suspect['timeout'] if suspect['probed_task_id']
                    else UNTRACKED_RECLAIM_AGE_FACTOR * suspect['timeout']
                )
                if reclaim_toolkit_index_meta(
                    suspect['connection_string'],
                    suspect['schema'],
                    cmetadata.get('collection'),
                    expected_task_id=cmetadata.get('task_id'),
                    expected_created_on=cmetadata.get('created_on'),
                    min_updated_age=min_updated_age,
                ):
                    reclaimed_total += 1
                    log.info(
                        f"Reclaimed interrupted index run: project={suspect['project_id']}, "
                        f"toolkit={suspect['toolkit_id']}, index={cmetadata.get('collection')}, "
                        f"task_id={cmetadata.get('task_id')}"
                    )
            return None
        finally:
            log.info(
                f"reclaim_interrupted_indexes tick finished at {datetime.now(UTC).isoformat()} "
                f"(total {time.monotonic() - tick_started:.3f}s, "
                f"suspects {suspect_count}, reclaimed {reclaimed_total})"
            )
            _reclaim_lock.release()
