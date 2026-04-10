from copy import deepcopy
from datetime import datetime, UTC

from pylon.core.tools import web, log
from sqlalchemy.orm.attributes import flag_modified
from tools import db, rpc_tools, this

from ..models.elitea_tools import EliteATool
from ..models.indexer import EmbeddingStore
from ..models.enums import InitiatorType
from ..models.enums.indexer import IndexingSchedule
from ..models.pd.index import ToolkitIndexingSchedule
from ..utils.application_tools import get_session_for_schema, start_index_task, update_toolkit_index_meta_history_with_failed_state
from ..utils.predict_utils import get_predict_base_url, get_system_user_token
from ..utils.index_scheduling import resolve_credentials, handle_failed_index_schedule


class RPC:
    @web.rpc("applications_check_index_scheduling")
    def check_index_scheduling(self, **kwargs):
        all_project_ids = [
            project_['id'] for project_ in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                filter_={'create_success': True}
            )
        ]

        for project_id in all_project_ids:
            with db.get_session(project_id) as project_session:
                for toolkit in project_session.query(EliteATool).filter(
                    EliteATool.meta['indexes_meta'].isnot(None)
                ).all():
                    indexes_meta = toolkit.meta['indexes_meta']
                    log.debug(f'Indexes meta: {indexes_meta}')
                    for index_meta_id, index_entry in indexes_meta.items():
                        schedules = index_entry.get('schedules', {})
                        log.debug(f'Schedules: {schedules}')
                        for user_id, user_config in schedules.items():
                            init_issue = None

                            # Convert stored dict to ToolkitIndexingSchedule model
                            try:
                                schedule_model = ToolkitIndexingSchedule.parse_obj(user_config)
                                creator_id = schedule_model.created_by
                            except Exception as e:
                                # If schedule configuration is invalid, log error and skip this schedule
                                log.error(
                                    f"Invalid schedule configuration for project {project_id}, "
                                    f"toolkit {toolkit.id} ({toolkit.type}), index_meta {index_meta_id}, "
                                    f"user {user_id}: {e!r}"
                                )
                                continue

                            should_trigger_by_time = schedule_model.enabled and rpc_tools.RpcMixin().rpc.timeout(3).scheduling_time_to_run(
                                schedule_model.cron,
                                schedule_model.last_run,
                                schedule_model.timezone,
                            )
                            log.debug(
                                f'Should trigger by time: {should_trigger_by_time}, {index_meta_id}, '
                                f'user {user_id} in project {project_id}, toolkit {toolkit.type} {toolkit.id}'
                            )

                            if not should_trigger_by_time:
                                continue

                            # Start with a copy of toolkit settings
                            updated_settings = deepcopy(toolkit.settings)

                            # Apply user-provided credentials if present
                            should_trigger_by_credentials = resolve_credentials(
                                project_settings=updated_settings,
                                toolkit_type=toolkit.type,
                                user_config=user_config,
                                project_id=project_id,
                            )

                            if not init_issue and not should_trigger_by_credentials:
                                init_issue = "toolkit credentials resolving issue"

                            user_token = get_system_user_token(project_id)
                            if not init_issue and not user_token:
                                init_issue = "missing valid user token"

                            if init_issue:
                                handle_failed_index_schedule(
                                    project_id, updated_settings, creator_id, toolkit, index_meta_id, init_issue
                                )
                                continue

                            # Expand the updated settings
                            settings_expanded = rpc_tools.RpcMixin().rpc.timeout(2).configurations_expand(
                                project_id=project_id,
                                settings=updated_settings,
                                user_id=user_id,
                                unsecret=True
                            )
                            connection_string = settings_expanded.get('pgvector_configuration').get('connection_string')
                            if not connection_string:
                                log.warning(f"Skipping indexing for toolkit {toolkit.id}, "
                                            f"index {index_meta_id}, user {user_id} "
                                            f"in project {project_id} due to missing connection string")
                                continue

                            log.debug(f"Checking index_meta for toolkit {toolkit.id}, "
                                      f"index {index_meta_id}, user {user_id} in project {project_id}")

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
                                        continue

                                    running_state = index.cmetadata.get('state')

                                    if running_state and running_state.lower() != 'in_progress':
                                        log.debug(
                                            f"Triggering scheduled indexing for toolkit {toolkit.id}, "
                                            f"index {index_meta_id}, user {user_id} "
                                            f"with cron '{user_config.get('cron')}'"
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
                                        start_index_task(
                                            self.task_node,
                                            data,
                                            None,
                                            initiator=InitiatorType.schedule
                                        )

                                        # Update last_run timestamp in toolkit meta
                                        current_time = datetime.now(UTC).isoformat()
                                        toolkit.meta['indexes_meta'][index_meta_id]['schedules'][user_id]['last_run'] = current_time
                                        flag_modified(toolkit, 'meta')
                                        project_session.commit()

                                        log.debug(f"Updated last_run for user {user_id} to {current_time}")
                            except Exception as e:
                                log.error(f"Error occurred while scheduled indexing for user {user_id}: {e}")
        return None
