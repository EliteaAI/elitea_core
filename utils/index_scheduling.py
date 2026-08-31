from pylon.core.tools import web, log
from sqlalchemy.orm.attributes import flag_modified
from tools import db, VaultClient, rpc_tools, this
from ..models.enums import InitiatorType
from ..utils.application_tools import (
    IndexMetaLockTimeoutError,
    update_toolkit_index_meta_history_with_failed_state,
)


def resolve_credentials(project_settings: dict, toolkit_type: str,
                                user_config: dict, project_id: int,
                                is_team_schedule: bool = False,
                                creator_id: int | None = None) -> bool:
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

    Returns:
        bool: True if no credentials or successfully applied, False if validation/loading failed
    """
    log.debug(
        f"Starting resolve_credentials for toolkit_type='{toolkit_type}', project_id={project_id}"
    )
    # Build configuration key: {type}_configuration
    config_key = f"{toolkit_type}_configuration"

    # Check if settings contains appropriate config key
    if config_key not in project_settings:
        log.debug(
            f"Configuration key '{config_key}' not in settings, no credentials replacement needed"
        )
        return True

    # Extract credentials from user_config
    user_credentials = user_config.get('credentials')
    if not user_credentials:
        if is_team_schedule:
            # Team/shared schedules never carry per-user credentials — the project-level
            # configuration already sitting in project_settings is authoritative.
            log.debug(
                f"Team schedule with no per-user credentials override for "
                f"toolkit_type='{toolkit_type}', project_id={project_id}; "
                f"using project-level configuration as-is"
            )
            return True
        log.warning(f"No credentials provided in user_config for toolkit_type='{toolkit_type}', project_id={project_id}")
        return False

    # Validate credentials is a dict
    if not isinstance(user_credentials, dict):
        log.warning(
            f"Credentials is not a dict (type={type(user_credentials).__name__}), "
            f"cannot apply credentials"
        )
        return False

    # Config key exists - validate elitea_title
    config_title = user_credentials.get('elitea_title')
    if not config_title:
        log.warning(
            f"Credentials missing 'elitea_title', "
            f"cannot apply for type '{toolkit_type}'"
        )
        return False

    # A credential the author marked private lives in their personal project, not in
    # project_id, so the project-scoped lookup can never find it. This mirrors the
    # resolution order in configurations.expand_configuration; keeping the two in step
    # matters because that function re-resolves the same payload downstream.
    is_private = bool(user_credentials.get('private'))
    if is_private and creator_id is None:
        log.warning(
            f"Credential '{config_title}' for toolkit_type='{toolkit_type}' is private but "
            f"the schedule carries no creator; cannot resolve a personal configuration"
        )
        return False

    try:
        if is_private:
            personal_configurations = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_filtered_personal(
                user_id=creator_id,
                include_shared=True,
                filter_fields={
                    'type': toolkit_type,
                    'elitea_title': config_title
                }
            )
            user_configuration = personal_configurations[0] if personal_configurations else None
        else:
            user_configuration = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_first_filtered_project(
                project_id=project_id,
                filter_fields={
                    'type': toolkit_type,
                    'elitea_title': config_title
                }
            )

        if not user_configuration:
            log.warning(
                f"Configuration not found: type='{toolkit_type}', title='{config_title}', "
                f"private={is_private}, project_id={project_id}, creator_id={creator_id}"
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
            f"Project-level configuration '{config_title}' (id={user_configuration.get('id')}) "
            f"are using to run toolkit index"
        )
        return True

    except Exception as e:
        log.debug(
            f"Failed to apply credentials '{config_title}' for toolkit '{toolkit_type}': "
            f"project_id={project_id}, error={str(e)}"
        )
        log.warning(
            f"Error loading user configuration: {e}"
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
    log.debug(
        f"Skip running by schedule due to {init_issue}: {index_meta_id}, "
        f"user {user_id} in project {project_id}, toolkit {toolkit.type} {toolkit.id}"
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
        log.warning(f"[handle_failed_index_schedule] {e}; retrying next scan")
        return
    if outcome.get('skipped_live_run'):
        # The notification is gated on the writer's locked-read outcome, never on a
        # separate unlocked pre-check: a live registered run means this start failure
        # must not flip the shared row or alarm over the run in flight.
        log.info(f"[handle_failed_index_schedule] Live run registered for {index_meta_id}; "
                 f"skipping failure notification")
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
    log.debug(f"[handle_failed_index_schedule] End: project_id={project_id}, toolkit_id={toolkit.id}, index_meta_id={index_meta_id}, user_id={user_id}")