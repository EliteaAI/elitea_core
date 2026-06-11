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


class RPC:
    @web.rpc("applications_check_index_scheduling")
    def check_index_scheduling(self, **kwargs):
        if not _check_index_scheduling_lock.acquire(blocking=False):
            log.warning(
                "check_index_scheduling: previous tick still running, "
                "skipping this minute"
            )
            return None

        tick_started = time.monotonic()
        log.info(
            f"check_index_scheduling tick started at {datetime.now(UTC).isoformat()}"
        )
        try:
            # Cooperative yield only when gevent is the actual web runtime;
            # under flask/waitress/hypercorn this is a no-op.
            yield_to_hub = (
                (lambda: gevent.sleep(0))
                if (gevent is not None and self.context.web_runtime == "gevent")
                else (lambda: None)
            )

            try:
                all_project_ids = [
                    project_['id'] for project_ in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                        filter_={'create_success': True}
                    )
                ]
            except Exception as exc:  # pylint: disable=W0703
                log.exception(
                    "check_index_scheduling: failed to enumerate projects: "
                    "exc_type=%s exc=%r",
                    type(exc).__name__, exc,
                )
                return None

            for project_id in all_project_ids:
                yield_to_hub()
                try:
                    _check_index_scheduling_project(
                        project_id, yield_to_hub, self.task_node,
                    )
                except Exception as exc:  # pylint: disable=W0703
                    log.exception(
                        "check_index_scheduling: skipped project due to error: "
                        "project_id=%s exc_type=%s exc=%r",
                        project_id, type(exc).__name__, exc,
                    )
            return None
        finally:
            log.info(
                f"check_index_scheduling tick finished at {datetime.now(UTC).isoformat()} "
                f"(total {time.monotonic() - tick_started:.3f}s)"
            )
            _check_index_scheduling_lock.release()


def _check_index_scheduling_project(project_id, yield_to_hub, task_node):
    with db.get_session(project_id) as project_session:
        toolkits = project_session.query(EliteATool).filter(
            EliteATool.meta['indexes_meta'].isnot(None)
        ).all()
        for toolkit in toolkits:
            yield_to_hub()
            try:
                _check_index_scheduling_toolkit(
                    project_id, project_session, toolkit, yield_to_hub, task_node,
                )
            except Exception as exc:  # pylint: disable=W0703
                log.exception(
                    "check_index_scheduling: skipped toolkit due to error: "
                    "project_id=%s toolkit_id=%s toolkit_type=%s "
                    "exc_type=%s exc=%r",
                    project_id, getattr(toolkit, 'id', '?'),
                    getattr(toolkit, 'type', '?'),
                    type(exc).__name__, exc,
                )


def _check_index_scheduling_toolkit(
    project_id, project_session, toolkit, yield_to_hub, task_node,
):
    indexes_meta = toolkit.meta['indexes_meta']
    for index_meta_id, index_entry in indexes_meta.items():
        yield_to_hub()
        schedules = index_entry.get('schedules', {}) if isinstance(index_entry, dict) else {}
        for user_id, user_config in schedules.items():
            yield_to_hub()
            try:
                _run_one_index_schedule(
                    project_id=project_id,
                    project_session=project_session,
                    toolkit=toolkit,
                    index_meta_id=index_meta_id,
                    user_id=user_id,
                    user_config=user_config,
                    task_node=task_node,
                )
            except Exception as exc:  # pylint: disable=W0703
                log.exception(
                    "check_index_scheduling: skipped schedule due to error: "
                    "project_id=%s toolkit_id=%s index_meta=%s user_id=%s "
                    "exc_type=%s exc=%r",
                    project_id, toolkit.id, index_meta_id, user_id,
                    type(exc).__name__, exc,
                )


def _run_one_index_schedule(
    *, project_id, project_session, toolkit,
    index_meta_id, user_id, user_config, task_node,
):
    try:
        schedule_model = ToolkitIndexingSchedule.parse_obj(user_config)
        creator_id = schedule_model.created_by
    except Exception as e:  # pylint: disable=W0703
        log.error(
            f"Invalid schedule configuration for project {project_id}, "
            f"toolkit {toolkit.id} ({toolkit.type}), index_meta {index_meta_id}, "
            f"user {user_id}: {e!r}"
        )
        return

    should_trigger_by_time = schedule_model.enabled and is_cron_due(
        schedule_model.cron,
        schedule_model.last_run,
        schedule_model.timezone,
    )
    if not should_trigger_by_time:
        return

    updated_settings = deepcopy(toolkit.settings)

    try:
        should_trigger_by_credentials = resolve_credentials(
            project_settings=updated_settings,
            toolkit_type=toolkit.type,
            user_config=user_config,
            project_id=project_id,
        )
    except Exception as e:  # pylint: disable=W0703
        log.exception(
            "Failed to resolve credentials: project_id=%s toolkit_id=%s "
            "index_meta=%s user_id=%s exc=%r",
            project_id, toolkit.id, index_meta_id, user_id, e,
        )
        should_trigger_by_credentials = False

    init_issue = None
    if not should_trigger_by_credentials:
        init_issue = "toolkit credentials resolving issue"

    try:
        user_token = get_system_user_token(project_id)
    except Exception as e:  # pylint: disable=W0703
        log.exception(
            "Failed to obtain system user token: project_id=%s toolkit_id=%s "
            "index_meta=%s user_id=%s exc=%r",
            project_id, toolkit.id, index_meta_id, user_id, e,
        )
        user_token = None

    if not init_issue and not user_token:
        init_issue = "missing valid user token"

    if init_issue:
        try:
            handle_failed_index_schedule(
                project_id, updated_settings, creator_id, toolkit, index_meta_id, init_issue
            )
        except Exception as e:  # pylint: disable=W0703
            log.exception(
                "handle_failed_index_schedule raised: project_id=%s "
                "toolkit_id=%s index_meta=%s user_id=%s init_issue=%s exc=%r",
                project_id, toolkit.id, index_meta_id, user_id, init_issue, e,
            )
        return

    try:
        settings_expanded = rpc_tools.RpcMixin().rpc.timeout(2).configurations_expand(
            project_id=project_id,
            settings=updated_settings,
            user_id=user_id,
            unsecret=True
        )
    except Exception as e:  # pylint: disable=W0703
        log.exception(
            "configurations_expand RPC failed: project_id=%s toolkit_id=%s "
            "index_meta=%s user_id=%s exc=%r",
            project_id, toolkit.id, index_meta_id, user_id, e,
        )
        return

    pgvector_cfg = settings_expanded.get('pgvector_configuration') if isinstance(settings_expanded, dict) else None
    connection_string = pgvector_cfg.get('connection_string') if isinstance(pgvector_cfg, dict) else None
    if not connection_string:
        log.warning(
            f"Skipping indexing for toolkit {toolkit.id}, "
            f"index {index_meta_id}, user {user_id} "
            f"in project {project_id} due to missing connection string"
        )
        return

    try:
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
            if not running_state or running_state.lower() == 'in_progress':
                return

            trigger_started = time.monotonic()
            log.info(
                f"Index trigger started at {datetime.now(UTC).isoformat()} "
                f"for toolkit {toolkit.id}, index {index_meta_id}, "
                f"user {user_id}, project {project_id}, "
                f"cron '{user_config.get('cron')}'"
            )

            toolkit_config = {
                'id': toolkit.id,
                'toolkit_name': toolkit.type,
                'settings': settings_expanded,
            }
            data = {
                "tool_name": "index_data",
                "project_id": project_id,
                "toolkit_config": toolkit_config,
                "tool_params": index.cmetadata.get('index_configuration'),
                "user_id": creator_id,
                "project_auth_token": user_token,
                "deployment_url": get_predict_base_url(project_id),
            }
            start_index_task(
                task_node,
                data,
                None,
                initiator=InitiatorType.schedule,
            )

            current_time = datetime.now(UTC).isoformat()
            toolkit.meta['indexes_meta'][index_meta_id]['schedules'][user_id]['last_run'] = current_time
            flag_modified(toolkit, 'meta')
            project_session.commit()

            log.info(
                f"Index trigger finished at {current_time} "
                f"for toolkit {toolkit.id}, index {index_meta_id}, "
                f"user {user_id}, project {project_id} "
                f"(dispatched in {time.monotonic() - trigger_started:.3f}s)"
            )
    except Exception as e:  # pylint: disable=W0703
        # Aborted tx must not poison the next iteration's commit.
        try:
            project_session.rollback()
        except Exception:  # pylint: disable=W0703
            pass
        log.exception(
            "Error occurred while scheduled indexing: project_id=%s "
            "toolkit_id=%s index_meta=%s user_id=%s exc=%r",
            project_id, toolkit.id, index_meta_id, user_id, e,
        )
