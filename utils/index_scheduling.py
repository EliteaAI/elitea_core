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
                                user_id=None) -> bool:
    """Apply user-provided credentials to project settings.

    Extracts credentials from user_config, validates them, and loads project-level configuration
    to replace in project_settings dict (modifies in place). Returns True if no credentials
    to apply or successfully applied, False if validation/loading failed.

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
        bool: True if no credentials or successfully applied, False if validation/loading failed
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
            return True
        # The schedule names a credential but there is nowhere to put it: running anyway
        # would silently index with whatever credential the toolkit was last saved with.
        log.warning(
            f"{ctx} schedule supplies credentials but no credential slot could be resolved in "
            f"settings for toolkit_type='{toolkit_type}'; "
            f"settings keys={sorted(project_settings.keys())}"
        )
        return False

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
            return True
        log.warning(f"{ctx} no credentials provided in schedule for toolkit_type='{toolkit_type}'")
        return False

    # Validate credentials is a dict
    if not isinstance(user_credentials, dict):
        log.warning(
            f"{ctx} credentials is not a dict (type={type(user_credentials).__name__}), "
            f"cannot apply credentials"
        )
        return False

    # Config key exists - validate elitea_title
    config_title = user_credentials.get('elitea_title') or user_credentials.get('alita_title')
    if not config_title:
        log.warning(
            f"{ctx} credentials missing 'elitea_title', cannot apply for type '{toolkit_type}'"
        )
        return False

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
        return False

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
            return False

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
        return True

    except Exception as e:
        log.warning(
            f"{ctx} error loading configuration '{config_title}' of type '{config_type}' "
            f"(private={is_private}, creator_id={creator_id}): {e!r}"
        )
        return False


def handle_failed_index_schedule(
    project_id, updated_settings, user_id, toolkit, index_meta_id, init_issue,
    expand_user_id=None
):
    """Handle failed index scheduling: update history and notify status.

    ``expand_user_id`` is the user_id used when expanding configurations. For team schedules
    (``user_id == -1``) callers must pass the schedule's creator so ``get_personal_project_id``
    is never invoked with ``-1``. Defaults to ``user_id`` when not provided.
    """
    ctx = index_log_context(project_id, toolkit.id, index_meta_id, user_id)
    log.info(
        f"{ctx} skipping scheduled run of toolkit type '{toolkit.type}' due to: {init_issue}"
    )
    pgv_settings_expanded = rpc_tools.RpcMixin().rpc.timeout(2).configurations_expand(
        project_id=project_id,
        settings=updated_settings.get('pgvector_configuration', {}),
        user_id=expand_user_id if expand_user_id is not None else user_id,
        unsecret=True
    )
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
        # unknown state and do not abort the rest of the tick; last_run never advances on
        # this path, so the next scheduler scan retries within a minute.
        log.warning(f"{ctx} {e}; retrying next scan")
        return
    if outcome.get('skipped_live_run'):
        # The notification is gated on the writer's locked-read outcome, never on a
        # separate unlocked pre-check: a live registered run means this start failure
        # must not flip the shared row or alarm over the run in flight.
        log.info(f"{ctx} live run registered; skipping failure notification")
        return
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
        'user_id': int(user_id),
        'initiator': InitiatorType.schedule
    })
    # Debug, not info: a permanently broken schedule reaches this every minute forever, and
    # the info line at the top of this function already carries the reason. The two early
    # returns above log their own outcome, so nothing is left unexplained at info level.
    log.debug(f"{ctx} failure notified on index history")