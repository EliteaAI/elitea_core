import threading
import time
from datetime import datetime, UTC

from sqlalchemy import text

from pylon.core.tools import web, log
from tools import db, rpc_tools

from ..models.elitea_tools import EliteATool
from ..models.indexer import EmbeddingStore
from ..models.enums.all import IndexDataStatus
from ..utils.application_tools import (
    RECLAIM_HARD_CEILING_FACTOR,
    TASK_LOST,
    UNTRACKED_RECLAIM_AGE_FACTOR,
    _expand_toolkit_settings,
    _get_pgvector_engine,
    get_session_for_schema,
    read_task_disconnected_timeout,
    reclaim_toolkit_index_meta,
    should_reclaim_index_meta,
)
from ..utils.utils import make_yield_to_hub, end_ambient_transaction
from ..utils.maintenance_gate import is_maintenance_active


_tick_in_progress = threading.Lock()

# Long enough to outlive a Redis reconnect, so a transient event-bus blackout cannot
# masquerade as a dead task across both liveness probes.
LIVENESS_CONFIRM_DELAY_SEC = 20

# Bounds the probing phase only; discovery and the writes are not budgeted. The tick
# runs off the scheduler thread, so a long one delays nothing but the next tick, which
# the lock skips — it costs throughput, not correctness.
PROBE_BUDGET_SEC = 240

SWEEP_STATEMENT_TIMEOUT_MS = 30000


def _max_confirms_per_tick(task_node) -> int:
    """Every probe pays the node's query_wait, because the liveness check evicts the
    cached entry to force a real query, and each candidate is probed once per pass."""
    probe_cost = max(1, getattr(task_node, 'query_wait', 5))
    return max(1, int((PROBE_BUDGET_SEC - LIVENESS_CONFIRM_DELAY_SEC) // (2 * probe_cost)))


def _memoize(produce):
    """Defer a value until something needs it, then hold it for the tick."""
    cached = []

    def read():
        if not cached:
            cached.append(produce())
        return cached[0]

    return read


def _restore_task_state(task_node, task_id: str, evicted) -> None:
    """Put back what the probe evicted when no fresh answer replaced it, so a
    background job cannot cost the task views an entry they had."""
    if evicted is None:
        return
    with task_node.lock:
        task_node.global_task_state.setdefault(task_id, evicted)


def _resolve_task_liveness(task_node, task_id: str, verdicts: dict) -> str:
    if task_id in verdicts:
        return verdicts[task_id]
    # Cached 'running' announcements are never retracted when a runner dies, so the
    # entry has to go before asking, or the answer is our own stale copy — but the
    # cache is shared with the admin task views, so a transient silence must not cost
    # them the entry.
    with task_node.lock:
        evicted = task_node.global_task_state.pop(task_id, None)
    try:
        verdict = task_node.get_task_status(task_id)
    except RuntimeError:
        verdict = TASK_LOST
        _restore_task_state(task_node, task_id, evicted)
    except Exception as e:
        log.warning(f"Task liveness query failed for task {task_id}: {e}")
        verdict = 'unknown'
        _restore_task_state(task_node, task_id, evicted)
    verdicts[task_id] = verdict
    return verdict


def _find_registered_task(module, project_id: int, toolkit_id: int, index_name: str):
    """Task id for a row that carries none, if this pylon saw the run start.

    A miss is not evidence of death: the registry dies with the process, and a
    restart is itself a way runs get abandoned.
    """
    wanted = (str(project_id), str(toolkit_id), str(index_name))
    with module.active_index_tasks_lock:
        for task_key, entries in module.active_index_tasks.items():
            if any(tuple(str(part) for part in key) == wanted for key in entries):
                return task_key
    return None


def _read_abandoned_at(candidate) -> float:
    """When the run stopped reporting progress. Immutable while a row is stuck, which
    is what makes oldest-first ordering starvation-free. Coerced because the value can
    reach the row from an event payload as a string, and a mixed-type sort would escape
    the RPC."""
    try:
        return float(candidate['cmetadata'].get('updated_on') or 0)
    except (TypeError, ValueError):
        return 0.0


def _forget_registered_task(module, project_id: int, toolkit_id: int, index_name: str):
    """Nothing else will: the registry is evicted by terminal events, and a reclaimed
    run emits none."""
    wanted = (str(project_id), str(toolkit_id), str(index_name))
    with module.active_index_tasks_lock:
        for task_key in list(module.active_index_tasks):
            entries = module.active_index_tasks[task_key]
            for key in [k for k in entries if tuple(str(part) for part in k) == wanted]:
                entries.pop(key, None)
            if not entries:
                module.active_index_tasks.pop(task_key, None)


def _resolve_pgvector_connection(project_id: int, ref, author_id, memo: dict):
    """Expand the pgvector credential alone, never the whole toolkit config.

    Whole-config expansion fails on any broken sibling field, which would leave such
    toolkits permanently unsweepable.

    The memo must not outlive the tick: no event fires on a configuration edit, so a
    longer-lived cache can hand out a rotated-away connection string.
    """
    if not isinstance(ref, dict):
        return None
    key = (project_id, ref.get('elitea_title'), author_id if ref.get('private') else None)
    if key not in memo:
        try:
            # Passed through unmodified so the reference's own shape check runs: an
            # inline connection_string is not a stored credential, and honouring one
            # would let whoever wrote the toolkit choose where this sweep connects.
            expanded = _expand_toolkit_settings(ref, project_id, author_id) or {}
            configuration_type = expanded.get('configuration_type')
            if configuration_type and configuration_type != 'pgvector':
                raise ValueError(f"expected a pgvector credential, got '{configuration_type}'")
            memo[key] = expanded.get('connection_string')
        except Exception as e:
            log.warning(f"reclaim: pgvector configuration unresolvable in project {project_id}: {e}")
            memo[key] = None
    return memo[key]


def _bound_query_time(executor):
    """Cap this transaction's queries without touching the shared engine.

    SET LOCAL dies with the transaction, so it is safe through a pooler and leaves
    the request path's long-running deletes alone.
    """
    executor.execute(text(f'SET LOCAL statement_timeout = {SWEEP_STATEMENT_TIMEOUT_MS}'))


def _schemas_with_embeddings(connection_string: str) -> set:
    """Schemas in this database that already hold an embeddings table.

    Narrows how often the sweep reaches get_session_for_schema, which creates whatever
    is missing on first touch. It does not eliminate that: a schema holding the
    embeddings table but not its companion collection table still gets DDL from a
    service account in the customer's audit log. Passing create_if_missing through
    would close it properly.
    """
    engine = _get_pgvector_engine(connection_string)
    with engine.begin() as connection:
        _bound_query_time(connection)
        rows = connection.execute(text(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_name = 'langchain_pg_embedding'"
        )).fetchall()
    return {row[0] for row in rows}


def _fetch_in_progress_rows(connection_string: str, schema: str) -> list:
    with get_session_for_schema(connection_string, schema) as session:
        _bound_query_time(session)
        rows = session.query(
            EmbeddingStore.id,
            EmbeddingStore.cmetadata,
        ).filter(
            # Only @> can use the schema's GIN jsonb_path_ops index; ->> equality
            # scans every embedded chunk.
            EmbeddingStore.cmetadata.contains({
                'type': 'index_meta',
                'state': IndexDataStatus.in_progress.value,
            }),
        ).all()
    return [cmetadata for _, cmetadata in rows]


def _scan_toolkit_candidates(module, project_id: int, toolkit_id: int, connection_string: str,
                             read_timeout, reclaim_untracked: bool) -> list:
    """Rows that would be reclaimed if their task turned out to be dead.

    Assuming the worst costs nothing and defers every network round-trip until the
    caller has ordered the whole fleet's candidates and decided how many it can afford
    this tick — probing here would spend the tick's budget in discovery order.
    """
    schema = str(toolkit_id)
    rows = _fetch_in_progress_rows(connection_string, schema)
    if not rows:
        return []
    timeout = read_timeout()
    candidates = []
    for cmetadata in rows:
        # On a pgvector credential shared between projects the schema name — the
        # toolkit id — is not unique, so a row that names a different project belongs
        # to that project's sweep. A row with no project_id predates the stamp.
        row_project_id = cmetadata.get('project_id')
        if row_project_id is not None and str(row_project_id) != str(project_id):
            continue
        probed = dict(cmetadata)
        if not probed.get('task_id'):
            registered = _find_registered_task(module, project_id, toolkit_id, probed.get('collection'))
            if registered:
                probed['task_id'] = registered
        if should_reclaim_index_meta(
            probed, time.time(), timeout, lambda _task_id: TASK_LOST, reclaim_untracked,
        ):
            candidates.append({
                'project_id': project_id,
                'toolkit_id': toolkit_id,
                'connection_string': connection_string,
                'schema': schema,
                'cmetadata': cmetadata,
                'probed_task_id': probed.get('task_id'),
                'timeout': timeout,
            })
    return candidates


def _confirm_dead(module, candidate, reclaim_untracked: bool, hard_ceiling_factor: float,
                  verdicts: dict) -> bool:
    probed = dict(candidate['cmetadata'])
    probed['task_id'] = candidate['probed_task_id']
    return should_reclaim_index_meta(
        probed, time.time(), candidate['timeout'],
        lambda task_id: _resolve_task_liveness(module.task_node, task_id, verdicts),
        reclaim_untracked, hard_ceiling_factor,
    )


def _collect_project_candidates(module, project_id: int, yield_to_hub, reclaim_untracked: bool,
                                conn_memo: dict, schema_memo: dict) -> list:
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
    if not toolkits:
        return []
    # Resolved on the first in-progress row rather than up front: nearly every project
    # has nothing stuck on nearly every tick, and under a managed vault this read is an
    # approle login plus a KV fetch landing on the request path's Vault.
    read_timeout = _memoize(lambda: read_task_disconnected_timeout(project_id))
    by_connection = {}
    for toolkit_id, author_id, ref in toolkits:
        yield_to_hub()
        connection_string = _resolve_pgvector_connection(project_id, ref, author_id, conn_memo)
        if connection_string:
            by_connection.setdefault(connection_string, []).append(toolkit_id)
    candidates = []
    for connection_string, toolkit_ids in by_connection.items():
        yield_to_hub()
        end_ambient_transaction()
        if connection_string not in schema_memo:
            # Keyed by database, not project: in the default deployment every project
            # resolves the same DSN, so this is one query per tick rather than one per
            # project — and a failure is remembered too, or an unreachable host is
            # retried once per project at the full connect timeout.
            try:
                schema_memo[connection_string] = _schemas_with_embeddings(connection_string)
            except Exception as e:
                log.error(f"reclaim: cannot inspect pgvector database for project {project_id}: {e}")
                schema_memo[connection_string] = None
        populated_schemas = schema_memo[connection_string]
        if populated_schemas is None:
            continue
        for toolkit_id in toolkit_ids:
            if str(toolkit_id) not in populated_schemas:
                continue
            yield_to_hub()
            end_ambient_transaction()
            try:
                candidates.extend(_scan_toolkit_candidates(
                    module, project_id, toolkit_id, connection_string, read_timeout,
                    reclaim_untracked,
                ))
            except Exception as e:
                log.error(
                    f"reclaim_interrupted_indexes: toolkit {toolkit_id} "
                    f"in project {project_id} failed: {e}"
                )
    return candidates


class RPC:
    @web.rpc("elitea_core_reclaim_interrupted_indexes")
    def reclaim_interrupted_indexes(self, **kwargs):
        """Hand the tick to its own thread and return.

        A locally-registered RPC runs inline on the scheduler's single thread, which
        walks every cron job serially — so the probe waits in here would delay every
        other job on the platform for as long as the sweep takes.
        """
        # Tearing-down task nodes answer 'unknown' for live runs, so every row would
        # look dead at once.
        if is_maintenance_active():
            log.info("reclaim_interrupted_indexes: maintenance mode active, skipping tick")
            return None
        if not _tick_in_progress.acquire(blocking=False):
            log.warning("reclaim_interrupted_indexes: previous tick still running, skipping")
            return None
        try:
            threading.Thread(target=_run_tick, args=(self,), name="index-reclaim-tick", daemon=True).start()
        except Exception:
            # The release lives in the thread's finally, which never runs if the thread
            # never starts — and thread exhaustion is exactly when reclaim is wanted.
            _tick_in_progress.release()
            raise
        return None


def _run_tick(module):
    try:
        _sweep(module)
    except Exception:  # pylint: disable=W0703
        log.exception("reclaim_interrupted_indexes: tick failed")
    finally:
        _tick_in_progress.release()


def _sweep(module):
    tick_started = time.monotonic()
    candidate_count = 0
    reclaimed_total = 0
    try:
        reclaim_cfg = (module.descriptor.config.get('scheduler') or {}).get('index_reclaim') or {}
        reclaim_untracked = bool(reclaim_cfg.get('reclaim_untracked', False))
        hard_ceiling_factor = float(
            reclaim_cfg.get('hard_ceiling_factor', RECLAIM_HARD_CEILING_FACTOR)
        )
        max_confirms = _max_confirms_per_tick(module.task_node)
        yield_to_hub = make_yield_to_hub(module.context.web_runtime)

        try:
            all_project_ids = [
                project_['id'] for project_ in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                    filter_={'create_success': True}
                )
            ]
        except Exception as e:
            # Escaping the RPC costs the schedule row its last_run update, which
            # silently promotes this sweep to firing every poll period.
            log.warning(f"reclaim_interrupted_indexes: project list unavailable, skipping tick: {e}")
            return None

        conn_memo = {}
        schema_memo = {}
        candidates = []
        for project_id in all_project_ids:
            yield_to_hub()
            end_ambient_transaction()
            try:
                candidates.extend(_collect_project_candidates(
                    module, project_id, yield_to_hub, reclaim_untracked, conn_memo, schema_memo,
                ))
            except Exception as e:
                log.error(f"reclaim_interrupted_indexes: project {project_id} failed: {e}")
        candidate_count = len(candidates)
        if not candidates:
            return None

        candidates.sort(key=_read_abandoned_at)
        # Only a candidate inside the ceiling costs a probe, so the rest must not
        # spend the budget.
        now = time.time()
        free, needs_probe = [], []
        for candidate in candidates:
            within_ceiling = (
                candidate['probed_task_id']
                and now - _read_abandoned_at(candidate) <= hard_ceiling_factor * candidate['timeout']
            )
            (needs_probe if within_ceiling else free).append(candidate)
        deferred = max(0, len(needs_probe) - max_confirms)
        candidates = free + needs_probe[:max_confirms]
        if deferred:
            log.warning(
                f"reclaim: {len(needs_probe)} candidates need a liveness probe, more than the "
                f"{max_confirms} this tick can afford; {deferred} deferred, oldest first, "
                f"~{-(-deferred // max_confirms)} more tick(s) to drain"
            )

        first_pass_verdicts = {}
        confirmed = [
            candidate for candidate in candidates
            if _confirm_dead(module, candidate, reclaim_untracked, hard_ceiling_factor, first_pass_verdicts)
        ]
        if not confirmed:
            return None

        time.sleep(LIVENESS_CONFIRM_DELAY_SEC)
        second_pass_verdicts = {}
        for suspect in confirmed:
            if not _confirm_dead(module, suspect, reclaim_untracked, hard_ceiling_factor, second_pass_verdicts):
                log.info(
                    f"reclaim: pass-2 downgraded suspect "
                    f"index={suspect['cmetadata'].get('collection')} "
                    f"(project={suspect['project_id']}, toolkit={suspect['toolkit_id']}) — "
                    f"task answered on the confirm probe"
                )
                continue
            cmetadata = suspect['cmetadata']
            min_updated_age = (
                suspect['timeout'] if suspect['probed_task_id']
                else UNTRACKED_RECLAIM_AGE_FACTOR * suspect['timeout']
            )
            try:
                reclaimed = reclaim_toolkit_index_meta(
                    suspect['connection_string'],
                    suspect['schema'],
                    cmetadata.get('collection'),
                    expected_task_id=cmetadata.get('task_id'),
                    expected_created_on=cmetadata.get('created_on'),
                    min_updated_age=min_updated_age,
                )
            except Exception as e:
                log.error(
                    f"reclaim: write failed for index={cmetadata.get('collection')} "
                    f"(project={suspect['project_id']}, toolkit={suspect['toolkit_id']}): {e}"
                )
                continue
            if reclaimed:
                reclaimed_total += 1
                _forget_registered_task(
                    module, suspect['project_id'], suspect['toolkit_id'], cmetadata.get('collection'),
                )
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
            f"candidates {candidate_count}, reclaimed {reclaimed_total})"
        )
