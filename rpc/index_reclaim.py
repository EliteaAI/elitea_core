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


def _resolve_task_liveness(task_node, task_id: str, verdicts: dict) -> str:
    if task_id in verdicts:
        return verdicts[task_id]
    # Cached 'running' announcements are never retracted when a runner dies, so the
    # entry has to go before asking, or the answer is our own stale copy.
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
    """Task id for a row that carries none, if this pylon saw the run start.

    A miss is not evidence of death: the registry dies with the process, and a
    restart is itself a way runs get abandoned.
    """
    registry_key = (project_id, toolkit_id, index_name)
    with module.active_index_tasks_lock:
        for task_key, entries in module.active_index_tasks.items():
            if registry_key in entries:
                return task_key
    return None


def _resolve_pgvector_connection(project_id: int, ref, author_id, memo: dict):
    """Expand the pgvector credential alone, never the whole toolkit config.

    Whole-config expansion fails on any broken sibling field, which would leave such
    toolkits permanently unsweepable.

    The memo must not outlive the tick: no event fires on a configuration edit, so a
    longer-lived cache can hand out a rotated-away connection string.
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
    """Schemas in this database that already hold an embeddings table.

    Skipping never-indexed toolkits keeps the sweep away from get_session_for_schema,
    which creates the schema on first touch — a write this read must not perform on a
    customer-owned database.
    """
    engine = _get_pgvector_engine(connection_string)
    with engine.connect() as connection:
        rows = connection.execute(text(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_name = 'langchain_pg_embedding'"
        )).fetchall()
    return {row[0] for row in rows}


def _fetch_in_progress_rows(connection_string: str, schema: str) -> list:
    with get_session_for_schema(connection_string, schema) as session:
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


def _scan_toolkit_suspects(module, project_id: int, toolkit_id: int, connection_string: str,
                           timeout: int, reclaim_untracked: bool, probe_verdicts: dict) -> list:
    schema = str(toolkit_id)
    rows_outside_session = _fetch_in_progress_rows(connection_string, schema)
    suspects = []
    for cmetadata in rows_outside_session:
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
    timeout = read_task_disconnected_timeout(project_id)
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
        # Tearing-down task nodes answer 'unknown' for live runs, so every row would
        # look dead at once.
        if is_maintenance_active():
            log.info("reclaim_interrupted_indexes: maintenance mode active, skipping tick")
            return None
        if not _tick_in_progress.acquire(blocking=False):
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
            _tick_in_progress.release()
