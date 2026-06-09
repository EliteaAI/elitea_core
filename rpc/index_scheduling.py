import threading
import time
from copy import deepcopy
from datetime import datetime, UTC

from pylon.core.tools import web, log
from sqlalchemy.orm.attributes import flag_modified
from tools import db, rpc_tools, this

try:
    import gevent  # pylint: disable=C0413
except ImportError:  # pragma: no cover - gevent absent in non-gevent deploys
    gevent = None

from ..models.elitea_tools import EliteATool
from ..models.indexer import EmbeddingStore
from ..models.enums import InitiatorType
from ..models.enums.indexer import IndexingSchedule
from ..models.pd.index import ToolkitIndexingSchedule
from ..utils.application_tools import get_session_for_schema, start_index_task, update_toolkit_index_meta_history_with_failed_state
from ..utils.cron_utils import is_cron_due
from ..utils.predict_utils import get_predict_base_url, get_system_user_token
from ..utils.index_scheduling import resolve_credentials, handle_failed_index_schedule


# Re-entrancy guard: skip overlapping ticks if a previous run is still in
# flight. The scheduler thread fires every minute regardless of prior state.
_check_index_scheduling_lock = threading.Lock()


def _collect_due_indexes_for_project(project_id: int, yield_to_hub=lambda: None):
    """
    Phase 1 (read-only): enumerate scheduled toolkit indexes in this project,
    decide which are cron-due right now, and return both the candidate count
    and the due plan.

    Returns:
        (candidates, due) where:
          - candidates: total schedules examined (incl. invalid / disabled /
            not-due rows)
          - due: list of plan dicts ready for the launch phase. Each item
            carries enough context (toolkit id/type, index_meta_id, user_id,
            creator_id, cron config, raw user_config) for the launch phase to
            re-acquire the toolkit row and proceed with credential resolution.
    """
    candidates = 0
    due = []
    with db.get_session(project_id) as session:
        toolkits = session.query(EliteATool).filter(
            EliteATool.meta['indexes_meta'].isnot(None)
        ).all()
        for toolkit in toolkits:
            yield_to_hub()
            indexes_meta = toolkit.meta['indexes_meta']
            log.debug(f'Indexes meta: {indexes_meta}')
            for index_meta_id, index_entry in indexes_meta.items():
                yield_to_hub()
                schedules = index_entry.get('schedules', {})
                log.debug(f'Schedules: {schedules}')
                for user_id, user_config in schedules.items():
                    yield_to_hub()
                    candidates += 1

                    try:
                        schedule_model = ToolkitIndexingSchedule.parse_obj(user_config)
                    except Exception as e:
                        log.error(
                            f"Invalid schedule configuration for project {project_id}, "
                            f"toolkit {toolkit.id} ({toolkit.type}), "
                            f"index_meta {index_meta_id}, user {user_id}: {e!r}"
                        )
                        continue

                    # Inline cron evaluation: avoid an RPC round-trip
                    # per scheduled toolkit/user every minute.
                    should_trigger_by_time = schedule_model.enabled and is_cron_due(
                        schedule_model.cron,
                        schedule_model.last_run,
                        schedule_model.timezone,
                    )
                    log.debug(
                        f'Should trigger by time: {should_trigger_by_time}, '
                        f'{index_meta_id}, user {user_id} in project {project_id}, '
                        f'toolkit {toolkit.type} {toolkit.id}'
                    )

                    if not should_trigger_by_time:
                        continue

                    due.append({
                        "project_id": project_id,
                        "toolkit_id": toolkit.id,
                        "toolkit_type": toolkit.type,
                        "index_meta_id": index_meta_id,
                        "user_id": user_id,
                        "creator_id": schedule_model.created_by,
                        "cron": schedule_model.cron,
                        "timezone": schedule_model.timezone,
                        "last_run": schedule_model.last_run,
                        "user_config_raw": user_config,
                    })

    return candidates, due


def _launch_due_index(task_node, item: dict, yield_to_hub=lambda: None):
    """
    Phase 2: process one cron-due index schedule — resolve credentials, expand
    settings, look up the index in its pgvector schema, and trigger an index
    task if the index is not already running. Updates last_run on success.

    Re-fetches the toolkit row in a fresh session so a long collection phase
    cannot serve stale data here.

    Cooperative yields surround each heavy operation (RPC calls,
    cross-schema DB lookup, task start) so a slow tick does not starve the
    gevent hub between launches.
    """
    project_id = item["project_id"]
    toolkit_id = item["toolkit_id"]
    index_meta_id = item["index_meta_id"]
    user_id = item["user_id"]
    creator_id = item["creator_id"]
    user_config = item["user_config_raw"]

    launch_start = time.monotonic()
    try:
        with db.get_session(project_id) as project_session:
            toolkit = project_session.query(EliteATool).get(toolkit_id)
            if not toolkit:
                log.warning(
                    f"Scheduled toolkit disappeared before launch: "
                    f"project={project_id}, toolkit_id={toolkit_id}"
                )
                return

            init_issue = None

            # Start with a copy of toolkit settings
            updated_settings = deepcopy(toolkit.settings)

            yield_to_hub()
            # Apply user-provided credentials if present
            should_trigger_by_credentials = resolve_credentials(
                project_settings=updated_settings,
                toolkit_type=toolkit.type,
                user_config=user_config,
                project_id=project_id,
            )

            if not init_issue and not should_trigger_by_credentials:
                init_issue = "toolkit credentials resolving issue"

            yield_to_hub()
            user_token = get_system_user_token(project_id)
            if not init_issue and not user_token:
                init_issue = "missing valid user token"

            if init_issue:
                handle_failed_index_schedule(
                    project_id, updated_settings, creator_id, toolkit,
                    index_meta_id, init_issue
                )
                return

            yield_to_hub()
            # Expand the updated settings
            settings_expanded = rpc_tools.RpcMixin().rpc.timeout(2).configurations_expand(
                project_id=project_id,
                settings=updated_settings,
                user_id=user_id,
                unsecret=True
            )
            connection_string = settings_expanded.get(
                'pgvector_configuration'
            ).get('connection_string')
            if not connection_string:
                log.warning(
                    f"Skipping indexing for toolkit {toolkit.id}, "
                    f"index {index_meta_id}, user {user_id} in project "
                    f"{project_id} due to missing connection string"
                )
                return

            log.debug(
                f"Checking index_meta for toolkit {toolkit.id}, "
                f"index {index_meta_id}, user {user_id} in project {project_id}"
            )

            yield_to_hub()
            with get_session_for_schema(connection_string, str(toolkit.id)) as session:
                index = session.query(
                    EmbeddingStore.id,
                    EmbeddingStore.cmetadata,
                ).filter(
                    EmbeddingStore.cmetadata['type'].astext == 'index_meta',
                    EmbeddingStore.cmetadata["collection"].astext == index_meta_id,
                ).first()

                if not index:
                    log.warning(f"Index {index_meta_id} not found in database")
                    return

                running_state = index.cmetadata.get('state')

                if running_state and running_state.lower() != 'in_progress':
                    log.debug(
                        f"Triggering scheduled indexing for project {project_id}, "
                        f"toolkit {toolkit.id}, index {index_meta_id}, "
                        f"user {user_id} with cron '{user_config.get('cron')}'"
                    )

                    toolkit_config = {
                        'id': toolkit.id,
                        'toolkit_name': toolkit.type,
                        'settings': settings_expanded
                    }

                    data = {
                        "tool_name": "index_data",
                        "project_id": project_id,
                        "toolkit_config": toolkit_config,
                        "tool_params": index.cmetadata.get('index_configuration'),
                        # "llm_model": TODO get default,
                        # "llm_params": TODO get default,
                        "user_id": creator_id,
                        "project_auth_token": user_token,
                        "deployment_url": get_predict_base_url(project_id)
                    }
                    yield_to_hub()
                    start_index_task(
                        task_node,
                        data,
                        None,
                        initiator=InitiatorType.schedule
                    )
                    yield_to_hub()

                    # Update last_run timestamp in toolkit meta
                    current_time = datetime.now(UTC).isoformat()
                    toolkit.meta['indexes_meta'][index_meta_id]['schedules'][user_id]['last_run'] = current_time
                    flag_modified(toolkit, 'meta')
                    project_session.commit()

                    launch_elapsed_ms = int((time.monotonic() - launch_start) * 1000)
                    log.debug(
                        f"Updated last_run for project {project_id}, "
                        f"toolkit {toolkit.id}, index {index_meta_id}, "
                        f"user {user_id} to {current_time}, "
                        f"took={launch_elapsed_ms}ms"
                    )
    except Exception as e:
        launch_elapsed_ms = int((time.monotonic() - launch_start) * 1000)
        log.error(
            f"Error occurred while scheduled indexing for project {project_id}, "
            f"toolkit {toolkit_id}, index {index_meta_id}, user {user_id}, "
            f"took={launch_elapsed_ms}ms: {e}"
        )


class RPC:
    @web.rpc("applications_check_index_scheduling")
    def check_index_scheduling(self, **kwargs):
        if not _check_index_scheduling_lock.acquire(blocking=False):
            log.warning(
                "check_index_scheduling: previous tick still running, "
                "skipping this minute"
            )
            return None

        try:
            # Cooperative yield only when gevent is the actual web runtime;
            # under flask/waitress/hypercorn this is a no-op.
            yield_to_hub = (
                (lambda: gevent.sleep(0))
                if (gevent is not None and self.context.web_runtime == "gevent")
                else (lambda: None)
            )

            all_project_ids = [
                project_['id'] for project_ in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                    filter_={'create_success': True}
                )
            ]

            # Phase 1 — collect all cron-due index schedules across projects,
            # timed end-to-end so we can spot scheduler-side slowdowns.
            collection_start = time.monotonic()
            total_candidates = 0
            due_plan: list[dict] = []
            for project_id in all_project_ids:
                # Yield between projects so a long scheduler tick does not starve
                # the gevent hub and stall request greenlets / EventNode.
                yield_to_hub()
                try:
                    candidates, due = _collect_due_indexes_for_project(
                        project_id, yield_to_hub=yield_to_hub
                    )
                    total_candidates += candidates
                    due_plan.extend(due)
                except Exception as e:
                    log.error(
                        f"Error collecting index schedules for project "
                        f"{project_id}: {e}"
                    )
            collection_elapsed_ms = int((time.monotonic() - collection_start) * 1000)

            # Compact summaries keep the launch plan readable in a single line.
            launch_plan_summary = [
                {
                    "project_id": item["project_id"],
                    "toolkit_id": item["toolkit_id"],
                    "toolkit_type": item["toolkit_type"],
                    "index_meta_id": item["index_meta_id"],
                    "user_id": item["user_id"],
                    "cron": item["cron"],
                    "timezone": item["timezone"],
                    "last_run": item["last_run"],
                }
                for item in due_plan
            ]
            log.info(
                f"check_index_scheduling: collection took "
                f"{collection_elapsed_ms}ms, examined {total_candidates} "
                f"candidate(s) across {len(all_project_ids)} project(s); "
                f"{len(due_plan)} schedule(s) will launch this tick: "
                f"{launch_plan_summary}"
            )

            # Phase 2 — execute each planned launch.
            launch_start = time.monotonic()
            for item in due_plan:
                yield_to_hub()
                _launch_due_index(self.task_node, item, yield_to_hub=yield_to_hub)
            if due_plan:
                launch_elapsed_ms = int((time.monotonic() - launch_start) * 1000)
                log.info(
                    f"check_index_scheduling: launch phase completed in "
                    f"{launch_elapsed_ms}ms for {len(due_plan)} schedule(s)"
                )

            return None
        finally:
            _check_index_scheduling_lock.release()
