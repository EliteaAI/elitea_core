import threading
import time
from copy import deepcopy
from datetime import datetime, UTC

from pylon.core.tools import web, log
from tools import VaultClient, db, rpc_tools, this

from ..models.elitea_tools import EliteATool
from ..models.indexer import EmbeddingStore
from ..models.enums import InitiatorType
from ..models.enums.indexer import IndexingSchedule
from ..models.pd.index import MISSING_SINCE_KEY, ToolkitIndexingSchedule
from ..utils.application_tools import (DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC, get_pending_index_run_heartbeat,
                                      get_session_for_schema, resolve_index_staleness,
                                      should_trigger_scheduled_index, start_index_task,
                                      update_toolkit_index_meta_history_with_failed_state)
from ..utils.utils import make_yield_to_hub, end_ambient_transaction
from ..utils.admission import (index_schedule_refusal_code, index_schedule_refusal_text,
                               record_index_schedule_refusal, record_index_schedule_would_deny)
from ..utils.credential_slots import has_free_form_secret, has_secret_template, mark_free_form_secret
from ..utils.cron_utils import is_cron_due
from ..utils.exceptions import AdmissionDenied
from ..utils.governance import is_auditing, is_governed
from ..utils.mandate_admission import audit_only_refusal, index_schedule_trigger_ref, toolkit_mandate_request
from ..utils.schedule_runs import OUTAGE_REFUSALS
from ..utils.trigger_principal import (OPERATION_OF, covering_toolkit_mandate, latest_toolkit_mandates, principal_of,
                                       resave_needed)
from ..utils.predict_utils import get_predict_base_url, get_system_user_token
from ..utils.index_scheduling import (
    as_credential_reference,
    resolve_credentials,
    resolve_index_llm_settings,
    handle_failed_index_schedule,
    index_log_context,
    stamp_schedule_last_run,
    stamp_schedule_retry_since,
    handle_missing_index_row,
    clear_index_missing_since,
    clear_schedule_retry_since,
    retry_escalation_due,
    describe_grace,
    handle_schedule_expiry,
)
from ..utils.maintenance_gate import is_maintenance_active


# Re-entrancy guard: skip overlapping ticks if a previous run is still in
# flight. The scheduler thread fires every minute regardless of prior state.
_check_index_scheduling_lock = threading.Lock()
_UNREADABLE = object()   # the toolkit mandates of a project could not be read this tick


def _read_toolkit_mandates(project_id):
    """``latest_toolkit_mandates`` of the project in a short session of its own (#7305), so a failed read never leaves
    the tick's project session in an aborted transaction; ``_UNREADABLE`` when it failed."""
    try:
        with db.get_session(project_id) as session:
            return latest_toolkit_mandates(session, project_id)
    except Exception as e:  # pylint: disable=W0703
        log.error(f"check_index_scheduling: the toolkit mandates of project {project_id} could not be read "
                  f"({type(e).__name__})")
        return _UNREADABLE


class RPC:
    @web.rpc("applications_check_index_scheduling")
    def check_index_scheduling(self, **kwargs):
        # Skip the tick while maintenance mode is on so no new index tasks
        # are dispatched to task nodes that are rejecting/tearing down work.
        if is_maintenance_active():
            log.info(
                "check_index_scheduling: maintenance mode active, skipping tick"
            )
            return None
        #
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
        # Every per-schedule diagnostic below is debug-level, which is off in production, so
        # a tick that silently stops part-way through the project list leaves no trace. These
        # counters are reported once per tick at info level: 'projects' short of the total or a
        # 'last_project' well below the highest project id is the starvation signal.
        stats = {
            'projects': 0, 'toolkits': 0, 'due': 0, 'expired': 0,
            'dispatched': 0, 'failed': 0, 'pruned': 0, 'last_project': None,
        }
        try:
            yield_to_hub = make_yield_to_hub(self.context.web_runtime)

            all_project_ids = [
                project_['id'] for project_ in rpc_tools.RpcMixin().rpc.timeout(3).project_list(
                    filter_={'create_success': True}
                )
            ]

            stats['total_projects'] = len(all_project_ids)

            for project_id in all_project_ids:
                stats['projects'] += 1
                stats['last_project'] = project_id
                # Yield between projects so a long scheduler tick does not starve
                # the gevent hub and stall request greenlets / EventNode.
                yield_to_hub()
                # Nested inline RPCs read on the scheduler thread's ambient session and
                # nothing commits it until the tick ends, so it holds AccessShareLock on
                # every table it touched for the whole tick, blocking DDL/migrations.
                end_ambient_transaction()
                # the project's toolkit mandates (#7305): read once, at its first due schedule under enforce or audit
                # only; never when governance is off
                toolkit_mandates = None
                with db.get_session(project_id) as project_session:
                    for toolkit in project_session.query(EliteATool).filter(
                        EliteATool.meta['indexes_meta'].isnot(None)
                    ).all():
                        stats['toolkits'] += 1
                        # Cooperative yield per toolkit: parse_obj + local-inline
                        # scheduling_time_to_run RPC are pure Python and accumulate
                        # CPU time between the outer DB I/O yields.
                        yield_to_hub()
                        # A single due toolkit can run for seconds (config expand, remote
                        # pgvector connect, dispatch), so bound the ambient tx here too.
                        end_ambient_transaction()
                        indexes_meta = toolkit.meta['indexes_meta']
                        # Names only, never the entry bodies: an f-string is built even when
                        # debug is off, so dumping every index_configuration here cost ~3 KiB
                        # of throwaway string per toolkit per minute and leaked credential
                        # titles into the log.
                        log.debug(
                            f'[idx project={project_id} toolkit={toolkit.id}] '
                            f'indexes: {sorted(indexes_meta)}'
                        )
                        for index_meta_id, index_entry in indexes_meta.items():
                            yield_to_hub()
                            schedules = index_entry.get('schedules', {})
                            # Owners only — a schedule body carries the credential title.
                            log.debug(
                                f'[idx project={project_id} toolkit={toolkit.id} '
                                f'index={index_meta_id}] schedule owners: {sorted(schedules)}'
                            )
                            for user_id, user_config in schedules.items():
                                yield_to_hub()
                                init_issue = None
                                ctx = index_log_context(
                                    project_id, toolkit.id, index_meta_id, user_id
                                )
                                # JSON serialises int keys as strings; team/shared schedules
                                # are stored under user_id=-1 (see index_meta PATCH endpoint).
                                is_team_schedule = str(user_id) == "-1"

                                # Convert stored dict to ToolkitIndexingSchedule model
                                try:
                                    schedule_model = ToolkitIndexingSchedule.parse_obj(user_config)
                                    creator_id = schedule_model.created_by
                                except Exception as e:
                                    # If schedule configuration is invalid, log error and skip this schedule
                                    log.error(
                                        f"{ctx} invalid schedule configuration for toolkit type "
                                        f"'{toolkit.type}': {e!r}"
                                    )
                                    stats['failed'] += 1
                                    continue

                                # Before the due check, so a warning does not depend on a
                                # firing landing inside its window. Cheap for the common
                                # case: no notification and no write until a threshold is
                                # crossed for the first time.
                                if handle_schedule_expiry(
                                    project_session, toolkit, index_meta_id, user_id, ctx,
                                    schedule_model, project_id,
                                    # Raw, not schedule_model.expires_at: this is the baseline
                                    # every expiry write is conditional on, and parsing has
                                    # already normalized the model's copy.
                                    observed_expires_at=user_config.get('expires_at'),
                                ):
                                    stats['expired'] += 1
                                    continue

                                # Inline cron evaluation: avoid an RPC round-trip
                                # per scheduled toolkit/user every minute.
                                should_trigger_by_time = schedule_model.enabled and is_cron_due(
                                    schedule_model.cron,
                                    schedule_model.last_run,
                                    schedule_model.timezone,
                                )
                                log.debug(
                                    f'{ctx} should trigger by time: {should_trigger_by_time} '
                                    f'(toolkit type {toolkit.type})'
                                )

                                if not should_trigger_by_time:
                                    continue

                                stats['due'] += 1

                                # Governed (#7305): a scheduled index run runs only under the toolkit mandate of
                                # the schedule's owner (personal key: that user, while they saved it last; team
                                # key: who saved it last). Without one it is refused as before: before the system
                                # token is fetched or any task is dispatched; the refusal is the index history
                                # entry and the slot is consumed, so it is not retried every minute. The deny is
                                # audited once the refusal was concluded, not on a slot that stays due. With one,
                                # the dispatch carries the mandate's admission request. Audit only: the same
                                # coverage is looked at, and a firing enforce would refuse leaves one would-deny.
                                admission, would_deny, refused_by, resave = None, None, None, False
                                governed = is_governed(self)
                                if governed or is_auditing(self):
                                    if toolkit_mandates is None:
                                        toolkit_mandates = _read_toolkit_mandates(project_id)
                                    if toolkit_mandates is _UNREADABLE:
                                        if governed:   # unknown, not refused: retried on the next scan
                                            stats['failed'] += 1
                                            continue
                                    else:
                                        mandate = covering_toolkit_mandate(
                                            toolkit_mandates, toolkit.id, user_id, creator_id)
                                        trigger_ref = index_schedule_trigger_ref(toolkit.id, index_meta_id, user_id)
                                        code = index_schedule_refusal_code(
                                            principal_of("index_schedule", schedule_key=user_id)) if mandate is None \
                                            else audit_only_refusal(mandate, OPERATION_OF["index_schedule"])
                                        resave = mandate is None and resave_needed(
                                            toolkit_mandates, toolkit.id, user_id, creator_id)
                                        if governed and mandate is not None and code is not None:
                                            refused_by = mandate.id   # its row refuses already: nothing is resolved
                                        elif governed and mandate is not None:
                                            admission = toolkit_mandate_request(
                                                project_id=project_id, toolkit_id=toolkit.id, mandate=mandate,
                                                trigger_ref=trigger_ref)
                                        elif not governed:
                                            would_deny = code
                                if governed and admission is None:
                                    try:
                                        if handle_failed_index_schedule(
                                            project_id, deepcopy(toolkit.settings), creator_id, toolkit,
                                            index_meta_id, index_schedule_refusal_text(code, resave),
                                            expand_user_id=creator_id,
                                        ):
                                            record_index_schedule_refusal(
                                                self, code, project_id=project_id, toolkit_id=toolkit.id,
                                                mandate_ref=refused_by)
                                            stamp_schedule_last_run(
                                                project_session, toolkit, index_meta_id, user_id, ctx,
                                            )
                                    except Exception as e:
                                        log.exception(
                                            f"{ctx} failed to record the governed refusal of a scheduled index run: "
                                            f"{e!r}"
                                        )
                                    stats['failed'] += 1
                                    continue

                                # Settings resolution reaches into other projects' vaults and
                                # configurations, so it can raise for reasons specific to this one
                                # schedule. The tick walks every project in one sequential pass,
                                # so an escape here silently starves every later project.
                                try:
                                    # Start with a copy of toolkit settings
                                    updated_settings = deepcopy(toolkit.settings)

                                    # Apply user-provided credentials if present
                                    credentials_ok, credentials_issue, credentials_retryable = resolve_credentials(
                                        project_settings=updated_settings,
                                        toolkit_type=toolkit.type,
                                        user_config=user_config,
                                        project_id=project_id,
                                        is_team_schedule=is_team_schedule,
                                        creator_id=creator_id,
                                        toolkit_id=toolkit.id,
                                        index_name=index_meta_id,
                                        user_id=user_id,
                                    )

                                    if admission is not None and credentials_ok:
                                        # a governed run carries the schedule's credential as a reference (#7305)
                                        as_credential_reference(updated_settings, toolkit.type)

                                    # The specific reason, not a generic label: init_issue is
                                    # written to the index history and shown to whoever owns
                                    # the schedule, and "missing" vs "not found" vs "lookup
                                    # failed" need different actions from them.
                                    init_issue_retryable = False
                                    if not init_issue and not credentials_ok:
                                        init_issue = credentials_issue
                                        init_issue_retryable = credentials_retryable

                                    llm_settings = None
                                    if not init_issue:
                                        llm_settings, init_issue, init_issue_retryable = (
                                            resolve_index_llm_settings(project_id)
                                        )

                                    if init_issue_retryable and retry_escalation_due(
                                        schedule_model.retry_since
                                    ):
                                        # Outlived a blip, so it is an outage. Demote it
                                        # rather than reporting here, so the terminal path
                                        # reports and stamps exactly as it always does.
                                        # The owner reads this in a notification, and a
                                        # raw timedelta renders as a clock time.
                                        init_issue = (
                                            f"{init_issue}; still failing for over "
                                            f"{describe_grace()}"
                                        )
                                        init_issue_retryable = False
                                    elif (credentials_ok and not init_issue_retryable
                                          and schedule_model.retry_since and admission is None):
                                        # (a mandate run keeps the stamp until it concludes: an admission outage
                                        # is measured by it too, #7305)
                                        # The lookups recovered. Clear here rather than on a
                                        # cursor write: this tick may still hit contention and
                                        # never reach one, leaving a stale stamp to escalate
                                        # the next unrelated blip with no grace.
                                        clear_schedule_retry_since(
                                            project_session, toolkit, index_meta_id,
                                            user_id, ctx,
                                        )

                                    user_token = get_system_user_token(project_id)
                                    if not init_issue and not user_token:
                                        init_issue = "missing valid user token"

                                    if init_issue:
                                        if init_issue_retryable:
                                            # Record the outage without reporting it: the
                                            # cursor stays put here, so notifying would repeat
                                            # every tick. The stamp is what lets a later tick
                                            # tell an outage from a blip.
                                            if not schedule_model.retry_since:
                                                stamp_schedule_retry_since(
                                                    project_session, toolkit, index_meta_id,
                                                    user_id, ctx,
                                                )
                                            log.warning(
                                                f"{ctx} {init_issue}; retrying on the next scan. "
                                                f"This index is not being updated while the "
                                                f"lookup keeps failing; reporting it "
                                                f"if it outlives {describe_grace()}"
                                            )
                                        else:
                                            recorded = handle_failed_index_schedule(
                                                project_id, updated_settings, creator_id, toolkit,
                                                index_meta_id, init_issue,
                                                expand_user_id=creator_id,
                                            )
                                            if recorded:
                                                stamp_schedule_last_run(
                                                    project_session, toolkit, index_meta_id,
                                                    user_id, ctx,
                                                )
                                        stats['failed'] += 1
                                        continue

                                    # Expand the updated settings. For team schedules (user_id=-1)
                                    # use the schedule creator to avoid get_personal_project_id(-1),
                                    # which raises when any nested config is private=True.
                                    # int(): schedules are keyed by user id in JSON, so user_id
                                    # is a string here, and configurations_expand feeds it to
                                    # get_personal_project_id which needs a real int.
                                    expand_user_id = creator_id if is_team_schedule else int(user_id)
                                    settings_expanded = rpc_tools.RpcMixin().rpc.timeout(2).configurations_expand(
                                        project_id=project_id,
                                        settings=updated_settings,
                                        user_id=expand_user_id,
                                        unsecret=True
                                    )
                                    connection_string = (settings_expanded.get('pgvector_configuration') or {}).get(
                                        'connection_string'
                                    )
                                except Exception as e:
                                    # exception(), not error(): these come from vault reads and
                                    # cross-project config expansion, where the repr alone does
                                    # not say which nested call failed.
                                    log.exception(
                                        f"{ctx} failed to resolve settings for scheduled indexing "
                                        f"of toolkit type '{toolkit.type}': {e!r}"
                                    )
                                    stats['failed'] += 1
                                    continue

                                if not connection_string:
                                    log.warning(
                                        f"{ctx} skipping indexing due to missing connection string"
                                    )
                                    stats['failed'] += 1
                                    continue

                                log.debug(f"{ctx} checking index_meta")
                                entry_before_lookup = deepcopy(
                                    toolkit.meta.get('indexes_meta', {}).get(index_meta_id)
                                )

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
                                            if handle_missing_index_row(
                                                project_session, toolkit, index_meta_id,
                                                entry_before_lookup, ctx,
                                            ):
                                                stats['pruned'] += 1
                                                break
                                            stamp_schedule_last_run(
                                                project_session, toolkit, index_meta_id,
                                                user_id, ctx,
                                            )
                                            stats['failed'] += 1
                                            continue

                                        if MISSING_SINCE_KEY in (entry_before_lookup or {}):
                                            clear_index_missing_since(
                                                project_session, toolkit, index_meta_id, ctx,
                                            )

                                        running_state = index.cmetadata.get('state')

                                        is_in_progress = bool(running_state) and running_state.lower() == 'in_progress'
                                        stale_retry = False
                                        if is_in_progress:
                                            # A worker that died without a terminal write leaves the row
                                            # in_progress forever, and skipping it here starves the schedule
                                            # silently. resolve_index_staleness is the rule the index list
                                            # GET applies too, so both surfaces agree on when a run stopped
                                            # counting as alive.
                                            secrets = VaultClient(project_id).get_secrets()
                                            task_disconnected_timeout = int(
                                                secrets.get('task_disconnected_timeout_sec',
                                                            DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC)
                                            )
                                            # No heartbeat_horizon: superseding is a
                                            # CONTROL decision and must agree with the
                                            # dispatch guard, or the scheduler kills a
                                            # worker and is refused the dispatch it
                                            # killed it for.
                                            stale_retry = resolve_index_staleness(
                                                running_state.lower(),
                                                index.cmetadata.get('updated_on', 0),
                                                task_disconnected_timeout,
                                                get_pending_index_run_heartbeat(session, index_meta_id),
                                            )
                                            if stale_retry:
                                                log.warning(
                                                    f"{ctx} retrying stale in_progress index: no update "
                                                    f"since updated_on="
                                                    f"{index.cmetadata.get('updated_on')}, "
                                                    f"timeout={task_disconnected_timeout}s"
                                                )
                                                # Best-effort supersede: staleness is a heuristic, and the
                                                # retry is about to overwrite this run's row either way — a
                                                # straggler that is somehow still alive must not keep writing
                                                # the collection alongside the new run. A dead task makes
                                                # this a no-op.
                                                stale_task_id = index.cmetadata.get('task_id')
                                                if stale_task_id:
                                                    try:
                                                        self.task_node.stop_task(stale_task_id)
                                                    except Exception as stop_error:
                                                        log.warning(
                                                            f"{ctx} could not stop stale task "
                                                            f"{stale_task_id} before retry: {stop_error}"
                                                        )
                                            else:
                                                log.debug(
                                                    f"{ctx} in_progress and fresh, skipping"
                                                )

                                        if should_trigger_scheduled_index(running_state, stale_retry):
                                            trigger_started = time.monotonic()
                                            log.info(
                                                f"{ctx} index trigger started at "
                                                f"{datetime.now(UTC).isoformat()} for toolkit type "
                                                f"'{toolkit.type}', cron '{user_config.get('cron')}'"
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
                                                "llm_model": llm_settings['model_name'],
                                                "llm_settings": llm_settings,
                                                # a mandate run acts for the mandate's owner (#7305)
                                                "user_id": creator_id if admission is None else mandate.owner_user_id,
                                                "project_auth_token": user_token,
                                                "deployment_url": get_predict_base_url(project_id)
                                            }
                                            if admission is not None:
                                                # reference-only (#7229): a {{secret.*}} outside a credential
                                                # configuration is refused at dispatch, as for on-demand indexing
                                                mark_free_form_secret(data, has_free_form_secret(
                                                    self, {'project_id': project_id, 'tools': [toolkit_config]})
                                                    or has_secret_template(data.get('tool_params')))
                                            try:
                                                start_index_task(
                                                    self.task_node,
                                                    data,
                                                    None,
                                                    initiator=InitiatorType.schedule,
                                                    admission=admission,
                                                )
                                            except AdmissionDenied as denied:
                                                # refused by admission (audited there): ends the slot as a refusal
                                                # without a mandate does, with the readable reason. A platform outage
                                                # keeps the slot and is retried next minute, reported only once it
                                                # outlives the grace (as a credential lookup outage is)
                                                text = index_schedule_refusal_text(denied.reason_code)
                                                if denied.reason_code in OUTAGE_REFUSALS:
                                                    if not retry_escalation_due(schedule_model.retry_since):
                                                        if not schedule_model.retry_since:
                                                            stamp_schedule_retry_since(
                                                                project_session, toolkit, index_meta_id, user_id, ctx)
                                                        log.warning(f"{ctx} admission unavailable "
                                                                    f"({denied.reason_code}); retrying on the next scan")
                                                        stats['failed'] += 1
                                                        continue
                                                    text = f"{text}; still failing for over {describe_grace()}"
                                                if handle_failed_index_schedule(
                                                    project_id, updated_settings, creator_id, toolkit,
                                                    index_meta_id, text,
                                                    expand_user_id=creator_id,
                                                ):
                                                    stamp_schedule_last_run(
                                                        project_session, toolkit, index_meta_id, user_id, ctx,
                                                    )
                                                stats['failed'] += 1
                                                continue
                                            stats['dispatched'] += 1
                                            if would_deny is not None:   # audit only: the run went on
                                                try:
                                                    record_index_schedule_would_deny(
                                                        self, would_deny, project_id=project_id, toolkit_id=toolkit.id,
                                                        trigger_ref=trigger_ref)
                                                except Exception as e:  # pylint: disable=W0703
                                                    log.error(f"{ctx} the audit-only evaluation failed "
                                                              f"({type(e).__name__})")

                                            current_time = datetime.now(UTC).isoformat()
                                            stamp_schedule_last_run(
                                                project_session, toolkit, index_meta_id,
                                                user_id, ctx, when=current_time,
                                            )

                                            log.info(
                                                f"{ctx} index trigger finished at {current_time} "
                                                f"(dispatched in {time.monotonic() - trigger_started:.3f}s)"
                                            )
                                except Exception as e:
                                    # Catch-all around the pgvector connect, the staleness
                                    # checks and the dispatch: the frame that raised is the
                                    # only thing that distinguishes them, so keep the traceback.
                                    log.exception(f"{ctx} error occurred while scheduled indexing: {e!r}")
                                    stats['failed'] += 1
            return None
        except Exception as e:
            # Projects are walked in one sequential pass, so an escape from the loop leaves
            # every project after the failing one unscanned. Never let that be silent.
            log.exception(f"check_index_scheduling tick aborted before scanning all projects: {e!r}")
            return None
        finally:
            log.info(
                f"check_index_scheduling tick finished at {datetime.now(UTC).isoformat()} "
                f"(total {time.monotonic() - tick_started:.3f}s): "
                f"projects={stats['projects']}/{stats.get('total_projects', '?')} "
                f"last_project={stats['last_project']} toolkits={stats['toolkits']} "
                f"due={stats['due']} dispatched={stats['dispatched']} failed={stats['failed']} "
                f"expired={stats['expired']} pruned={stats['pruned']}"
            )
            _check_index_scheduling_lock.release()
