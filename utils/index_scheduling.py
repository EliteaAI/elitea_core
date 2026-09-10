from datetime import datetime, timedelta, UTC

from pylon.core.tools import web, log
from sqlalchemy.orm.attributes import flag_modified
from tools import db, VaultClient, rpc_tools, this
from ..models.enums import InitiatorType
from ..utils.application_tools import (
    IndexMetaLockTimeoutError,
    update_toolkit_index_meta_history_with_failed_state,
)


def index_log_context(project_id=None, toolkit_id=None, index_name=None, user_id=None) -> str:
    """Build a uniform ``[idx project=.. toolkit=.. index=.. user=..]`` log prefix.

    An index name is only unique inside one toolkit's schema and a toolkit id only inside
    one project, so a message naming just one of them cannot be attributed in production.
    Emitting the same shape everywhere also makes the whole run greppable as one string.
    """
    parts = []
    if project_id is not None:
        parts.append(f"project={project_id}")
    if toolkit_id is not None:
        parts.append(f"toolkit={toolkit_id}")
    if index_name is not None:
        parts.append(f"index={index_name}")
    if user_id is not None:
        parts.append(f"user={user_id}")
    return f"[idx {' '.join(parts)}]"


# How long a retryable credential-lookup failure stays log-only before it is reported as
# an ordinary failure. Longer than any restart or hot reload, so those cannot be mistaken
# for an outage; shorter than the 24h floor the API enforces on schedules, so a daily
# schedule that breaks in the morning is still reported the same day.
RETRYABLE_REPORT_GRACE = timedelta(hours=1)


def describe_grace(delta: timedelta = RETRYABLE_REPORT_GRACE) -> str:
    """Render the grace as a duration for the schedule's owner, who reads it in a
    notification and in the index history.

    ``str(timedelta(hours=1))`` is ``'1:00:00'``, which reads as a clock time.
    """
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "a second" if seconds == 1 else f"{seconds} seconds"
    minutes = seconds // 60
    if minutes % 60 == 0:
        hours = minutes // 60
        return "an hour" if hours == 1 else f"{hours} hours"
    return "a minute" if minutes == 1 else f"{minutes} minutes"


def retry_escalation_due(retry_since_iso: str | None, now: datetime | None = None) -> bool:
    """True once a run of retryable failures has outlived a blip.

    False for None — no run in progress — and for anything unreadable or in the future.
    False is the safe answer to all three: the caller stays silent, which is today's
    behaviour, rather than reporting on every tick, which is #6583.
    """
    if not retry_since_iso:
        return False
    try:
        started = datetime.fromisoformat(retry_since_iso)
    except Exception:
        return False
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return (now or datetime.now(UTC)) - started >= RETRYABLE_REPORT_GRACE


def stamp_schedule_retry_since(project_session, toolkit, index_meta_id, user_id, ctx,
                               when: str | None = None) -> str | None:
    """Record when the current run of retryable failures began. Does NOT move the cursor.

    Written once per outage, not per tick: its age has to measure the outage, and
    re-stamping would pin it at one tick interval so it never outlives any grace.
    """
    current_time = when or datetime.now(UTC).isoformat()
    if _write_schedule_fields(
        project_session, toolkit, index_meta_id, user_id, ctx,
        {'retry_since': current_time},
        "this outage will not be reported until a later tick manages to record it",
    ):
        return current_time
    return None


def clear_schedule_retry_since(project_session, toolkit, index_meta_id, user_id, ctx) -> None:
    """End the retry run without concluding anything about the schedule.

    The lookup recovering is not itself a conclusion — the tick may still hit contention
    and never reach a cursor write — so a stale stamp would survive and escalate the next
    unrelated blip with no grace at all.
    """
    _write_schedule_fields(
        project_session, toolkit, index_meta_id, user_id, ctx,
        {'retry_since': None},
        "the stale stamp may escalate the next unrelated failure with no grace",
    )


def stamp_schedule_last_run(project_session, toolkit, index_meta_id, user_id, ctx,
                            when: str | None = None,
                            end_outage: bool = True) -> str | None:
    """Advance a schedule's ``last_run`` cron cursor. Returns the stamp written, or None.

    Only call this once the tick has concluded something about the schedule. A lock, a live
    run or an exception concluded nothing, and those paths need the cursor to stay put so
    the next scan retries within a minute rather than a cron period.

    Concluding usually ends any retry run, so the clear rides along on this write rather
    than costing a second one. ``end_outage=False`` keeps the stamp: a failure that was
    concluded but could not be reported has not ended the outage, and clearing it would
    restart the grace clock every period so the owner would never be told.
    """
    current_time = when or datetime.now(UTC).isoformat()
    if _write_schedule_fields(
        project_session, toolkit, index_meta_id, user_id, ctx,
        {'last_run': current_time, 'retry_since': None} if end_outage
        else {'last_run': current_time},
        "the schedule stays due and will be retried on every tick until this write succeeds",
    ):
        return current_time
    return None


def _write_schedule_fields(project_session, toolkit, index_meta_id, user_id, ctx,
                           fields: dict, consequence: str) -> bool:
    """Write keys into the live schedule entry, delete-wins. True when the write landed.

    ``consequence`` is what a failed write means for this particular caller, and it differs
    enough between them that a shared sentence would be wrong for at least one: a dropped
    cursor write leaves the schedule due every tick, a dropped outage stamp means the
    outage is never reported, and a dropped clear can escalate the next unrelated failure
    with no grace.

    Mutates through ``toolkit.meta`` after ``refresh()``: refresh rebinds it to a new dict,
    so writing into the dicts the tick loop captured earlier is silently dropped.
    """
    try:
        # Re-read the row to avoid clobbering a concurrent deletion (delete wins:
        # if the schedule was removed mid-tick, skip).
        project_session.refresh(toolkit)
        live_schedules = (
            toolkit.meta
            .get('indexes_meta', {})
            .get(index_meta_id, {})
            .get('schedules', {})
        )
        if user_id not in live_schedules:
            log.info(f"{ctx} schedule was deleted mid-tick, skipping {sorted(fields)} update")
            return False
        live_schedules[user_id].update(fields)
        flag_modified(toolkit, 'meta')
        project_session.commit()
        return True
    except Exception as exc:  # pylint: disable=W0703
        # A session left in a failed transaction would take every later schedule in this
        # tick down with it, so absorb the failure here rather than in the caller.
        try:
            project_session.rollback()
        except Exception:  # pylint: disable=W0703
            pass
        log.exception(f"{ctx} failed to write {sorted(fields)}: {exc!r}; {consequence}")
        return False


# Settings slots that never hold toolkit credentials, so they must not be mistaken
# for the credential slot when the toolkit type does not match its settings key.
_NON_CREDENTIAL_CONFIG_KEYS = frozenset({
    'pgvector_configuration',
    'index_configuration',
    'embedding_configuration',
})


def resolve_credential_config_key(project_settings: dict, toolkit_type: str) -> str | None:
    """Find the settings key holding the toolkit's credential reference.

    The naive ``{toolkit_type}_configuration`` guess breaks for toolkits whose type is
    narrower than their credential family — an ``ado_wiki`` toolkit stores its credential
    under ``ado_configuration`` — so fall back to the only remaining credential-shaped
    slot in the settings when the direct guess misses.
    """
    direct_key = f"{toolkit_type}_configuration"
    if direct_key in project_settings:
        return direct_key
    candidates = [
        key for key, value in project_settings.items()
        if key.endswith('_configuration')
        and key not in _NON_CREDENTIAL_CONFIG_KEYS
        and isinstance(value, dict)
    ]
    if len(candidates) == 1:
        return candidates[0]
    return None


def resolve_credentials(project_settings: dict, toolkit_type: str,
                                user_config: dict, project_id: int,
                                is_team_schedule: bool = False,
                                creator_id: int | None = None,
                                toolkit_id: int | None = None,
                                index_name: str | None = None,
                                user_id=None) -> tuple[bool, str | None, bool]:
    """Apply user-provided credentials to project settings.

    Extracts credentials from user_config, validates them, and loads project-level configuration
    to replace in project_settings dict (modifies in place).

    Returns ``(ok, issue, retryable)``. ``retryable`` is True only when the *lookup* raised,
    where the credential itself may be fine; every other failure is a property of the stored
    schedule or the credential catalogue and cannot resolve itself.

    ``issue`` is None on success and otherwise a short reason naming
    the distinct failure: the schedule carrying no credentials, a malformed credentials block,
    no slot to put them in, a credential that does not exist, and a lookup that itself failed
    all need different actions from the schedule's owner, and the caller writes this string to
    the index history where they will read it.

    Args:
        project_settings (dict): Project settings dict to modify (updated in place)
        toolkit_type (str): Type of the toolkit (e.g., 'github', 'pgvector')
        user_config (dict): User configuration that may contain 'credentials' key
        project_id (int): Project ID for configuration lookup
        is_team_schedule (bool): True when the schedule is stored under user_id=-1 (team/shared).
            Team schedules omit per-user credentials — the project-level configuration in
            project_settings is authoritative and no override is required.
        creator_id (int | None): Schedule author. Required to resolve a credential marked
            ``private``, which lives in that user's personal project rather than project_id.
        toolkit_id, index_name, user_id: Log context only — they identify which schedule this
            call belongs to. Without them a failure here cannot be attributed to a toolkit.

    Returns:
        tuple[bool, str | None]: (True, None) if no credentials or successfully applied,
            (False, reason) if validation/loading failed
    """
    ctx = index_log_context(project_id, toolkit_id, index_name, user_id)
    log.debug(f"{ctx} resolve_credentials started for toolkit_type='{toolkit_type}'")
    config_key = resolve_credential_config_key(project_settings, toolkit_type)

    # Extract credentials from user_config
    user_credentials = user_config.get('credentials')

    if config_key is None:
        if not user_credentials:
            log.debug(
                f"{ctx} no credential slot in settings for toolkit_type='{toolkit_type}' and no "
                f"credentials on the schedule, nothing to replace"
            )
            return True, None, False
        # The schedule names a credential but there is nowhere to put it: running anyway
        # would silently index with whatever credential the toolkit was last saved with.
        log.warning(
            f"{ctx} schedule supplies credentials but no credential slot could be resolved in "
            f"settings for toolkit_type='{toolkit_type}'; "
            f"settings keys={sorted(project_settings.keys())}"
        )
        return False, (
            f"toolkit settings have no credential field for toolkit type '{toolkit_type}' "
            f"to apply the schedule's credentials to"
        ), False

    # The credential row's own type follows the settings slot, not the toolkit type:
    # an `ado_wiki` toolkit references a credential of type `ado`.
    config_type = config_key[: -len('_configuration')]
    if not user_credentials:
        if is_team_schedule:
            # Team/shared schedules never carry per-user credentials — the project-level
            # configuration already sitting in project_settings is authoritative.
            log.debug(
                f"{ctx} team schedule with no per-user credentials override for "
                f"toolkit_type='{toolkit_type}'; using project-level configuration as-is"
            )
            return True, None, False
        log.warning(f"{ctx} no credentials provided in schedule for toolkit_type='{toolkit_type}'")
        return False, "schedule has no credentials selected", False

    # Validate credentials is a dict
    if not isinstance(user_credentials, dict):
        log.warning(
            f"{ctx} credentials is not a dict (type={type(user_credentials).__name__}), "
            f"cannot apply credentials"
        )
        return False, "schedule credentials are malformed", False

    # Config key exists - validate elitea_title
    config_title = user_credentials.get('elitea_title') or user_credentials.get('alita_title')
    if not config_title:
        log.warning(
            f"{ctx} credentials missing 'elitea_title', cannot apply for type '{toolkit_type}'"
        )
        return False, "schedule credentials do not name a credential", False

    # A credential the author marked private lives in their personal project, not in
    # project_id, so the project-scoped lookup can never find it. This mirrors the
    # resolution order in configurations.expand_configuration; keeping the two in step
    # matters because that function re-resolves the same payload downstream.
    is_private = bool(user_credentials.get('private'))
    if is_private and creator_id is None:
        log.warning(
            f"{ctx} credential '{config_title}' for toolkit_type='{toolkit_type}' is private but "
            f"the schedule carries no creator; cannot resolve a personal configuration"
        )
        return False, (
            f"credential '{config_title}' is private but the schedule has no author to "
            f"resolve it for"
        ), False

    try:
        if is_private:
            personal_configurations = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_filtered_personal(
                user_id=creator_id,
                include_shared=True,
                filter_fields={
                    'type': config_type,
                    'elitea_title': config_title
                }
            )
            user_configuration = personal_configurations[0] if personal_configurations else None
        else:
            user_configuration = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_first_filtered_project(
                project_id=project_id,
                filter_fields={
                    'type': config_type,
                    'elitea_title': config_title
                }
            )

        if not user_configuration:
            log.warning(
                f"{ctx} configuration not found: type='{config_type}', title='{config_title}', "
                f"private={is_private}, creator_id={creator_id}"
            )
            return False, (
                f"credential '{config_title}' of type '{config_type}' no longer exists"
                + (" in the author's personal configurations" if is_private else "")
            ), False

        # ConfigurationDetails carries no `private` flag, so the substituted payload would
        # read as project-level and send the downstream configurations_expand back to
        # project_id — the same dead end this function just worked around.
        user_configuration['private'] = is_private

        # Replace configuration in project_settings (in place)
        project_settings[config_key] = user_configuration

        # Success - single debug log
        log.debug(
            f"{ctx} configuration '{config_title}' (id={user_configuration.get('id')}, "
            f"private={is_private}) is being used to run the toolkit index"
        )
        return True, None, False

    except Exception as e:
        # Distinct from "no longer exists": the credential may be fine and the RPC simply
        # timed out, so a 3s blip must not cost a daily schedule its whole day.
        log.exception(
            f"{ctx} error loading configuration '{config_title}' of type '{config_type}' "
            f"(private={is_private}, creator_id={creator_id}): {e!r}"
        )
        return False, f"could not look up credential '{config_title}': {e.__class__.__name__}", True


# handle_failed_index_schedule's three outcomes. Truthiness is the "did the tick conclude
# something" test the caller gates its cursor write on; CONCLUDED_UNREPORTABLE is additionally
# distinguishable because a failure that was never reported must not end the outage.
CONCLUDED_REPORTED = True
CONCLUDED_UNREPORTABLE = "unreportable"
NOT_CONCLUDED = False


def handle_failed_index_schedule(
    project_id, updated_settings, user_id, toolkit, index_meta_id, init_issue,
    expand_user_id=None
):
    """Handle failed index scheduling: update history and notify status.

    Returns one of the three ``CONCLUDED_*`` / ``NOT_CONCLUDED`` values above. Truthy means
    the tick concluded something about the schedule itself and the caller should advance
    ``last_run``; ``NOT_CONCLUDED`` means a lock or a live run concluded nothing and the
    schedule must be retried on the next scan.

    ``expand_user_id`` is the user_id used when expanding configurations. For team schedules
    (``user_id == -1``) callers must pass the schedule's creator so ``get_personal_project_id``
    is never invoked with ``-1``. Defaults to ``user_id`` when not provided.

    ``user_id`` itself can be ``None``: since #6526 relaxed ``created_by`` to optional, a
    legacy schedule with no recorded author reaches this function via ``creator_id=None``.
    ``notify_index_data_status`` already treats a falsy ``user_id`` as "cannot notify, log
    and return" — this function must not crash before that guard gets a chance to run.
    """
    ctx = index_log_context(project_id, toolkit.id, index_meta_id, user_id)
    log.info(
        f"{ctx} skipping scheduled run of toolkit type '{toolkit.type}' due to: {init_issue}"
    )
    try:
        pgv_settings_expanded = rpc_tools.RpcMixin().rpc.timeout(2).configurations_expand(
            project_id=project_id,
            settings=updated_settings.get('pgvector_configuration', {}),
            user_id=expand_user_id if expand_user_id is not None else user_id,
            unsecret=True
        )
    except Exception as e:  # pylint: disable=W0703
        # This reports failures that are often the configurations plugin being down, and it
        # reaches pgvector through that same plugin. Escaping here lands in the tick's
        # settings catch-all, which does not advance the cursor, so every broken schedule
        # would re-enter this 2s RPC on every tick — enough of them push the tick past 60s
        # and the re-entrancy guard then starves every schedule on the platform.
        # Concluded but unreportable. The caller consumes the slot so the tick does not
        # re-enter this call, but must NOT end the outage: clearing it here would restart
        # the grace clock every period and the owner would never be told.
        log.exception(f"{ctx} could not expand pgvector settings to record the failure: {e!r}")
        return CONCLUDED_UNREPORTABLE
    try:
        outcome = update_toolkit_index_meta_history_with_failed_state(
            pgv_settings_expanded.get('connection_string'),
            toolkit.id,
            index_meta_id,
            init_issue,
            initiator=InitiatorType.schedule,
        )
    except IndexMetaLockTimeoutError as e:
        # The row is locked by a live run's promote/registration — do not notify from an
        # unknown state and do not abort the rest of the tick.
        log.warning(f"{ctx} {e}; retrying next scan")
        return False
    if outcome.get('skipped_live_run'):
        # The notification is gated on the writer's locked-read outcome, never on a
        # separate unlocked pre-check: a live registered run means this start failure
        # must not flip the shared row or alarm over the run in flight.
        log.info(f"{ctx} live run registered; skipping failure notification")
        return False
    if not outcome.get('flipped'):
        # The writer found no index_meta row, so this schedule names an index that does not
        # exist in this project — the signature of a schedule that arrived with a copied
        # toolkit, or outlived its index. There is nothing to report a failure ON, and the
        # author cannot act on it: no screen lists these, and the project may not even be
        # theirs. Leave the log line as the only trace.
        log.warning(
            f"{ctx} schedule names an index with no metadata in this project; "
            f"skipping failure notification"
        )
        # Absent, not busy. A manual run would create the row, but paying a vault read and
        # a pgvector round trip every 60s to notice that is not worth it.
        return True
    # The row is flipped and committed by now, so the conclusion is already reached and a
    # failed notification does not un-reach it. Escaping here would leave the caller unable
    # to advance the cursor, and the schedule would re-flip and re-append history on every
    # tick — #6583 again, conditional on the notify path breaking.
    try:
        this.module.notify_index_data_status({
            'id': None,
            'index_name': index_meta_id,
            'state': 'failed',
            'error': init_issue,
            'reindex': outcome.get('reindex', False),
            'indexed': outcome.get('indexed', 0),
            'updated': outcome.get('updated', 0),
            'indexed_chunks': outcome.get('indexed_chunks', 0),
            'toolkit_id': toolkit.id,
            'project_id': project_id,
            # int(None) raises; a missing author must fall through to notify's own
            # "cannot notify without a user_id" guard, not crash here.
            'user_id': int(user_id) if user_id is not None else None,
            'initiator': InitiatorType.schedule
        })
    except Exception as e:  # pylint: disable=W0703
        log.exception(f"{ctx} failure recorded but could not be notified: {e!r}")
    # Debug, not info: a permanently broken schedule reaches this once per cron period for
    # as long as it stays broken, and the info line at the top of this function already
    # carries the reason. The two early returns above log their own outcome, so nothing is
    # left unexplained at info level.
    log.debug(f"{ctx} failure notified on index history")
    return True
