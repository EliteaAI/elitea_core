from pylon.core.tools import web, log
from sqlalchemy.orm.attributes import flag_modified
from tools import db, VaultClient, rpc_tools, this
from ..models.enums import InitiatorType
from ..utils.application_tools import update_toolkit_index_meta_history_with_failed_state


def resolve_credentials(project_settings: dict, toolkit_type: str,
                                user_config: dict, project_id: int) -> bool:
    """Apply user-provided credentials to project settings.

    Extracts credentials from user_config, validates them, and loads project-level configuration
    to replace in project_settings dict (modifies in place). Returns True if no credentials
    to apply or successfully applied, False if validation/loading failed.

    Args:
        project_settings (dict): Project settings dict to modify (updated in place)
        toolkit_type (str): Type of the toolkit (e.g., 'github', 'pgvector')
        user_config (dict): User configuration that may contain 'credentials' key
        project_id (int): Project ID for configuration lookup

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

    # Credentials valid - load and apply project-level configuration
    try:
        user_configuration = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_first_filtered_project(
            project_id=project_id,
            filter_fields={
                'type': toolkit_type,
                'elitea_title': config_title
            }
        )

        if not user_configuration:
            log.warning(
                f"Project-level configuration not found: "
                f"type='{toolkit_type}', title='{config_title}', project_id={project_id}"
            )
            return False

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
    project_id, updated_settings, user_id, toolkit, index_meta_id, init_issue
):
    """Handle failed index scheduling: update history and notify status."""
    log.debug(
        f"Skip running by schedule due to {init_issue}: {index_meta_id}, "
        f"user {user_id} in project {project_id}, toolkit {toolkit.type} {toolkit.id}"
    )
    pgv_settings_expanded = rpc_tools.RpcMixin().rpc.timeout(2).configurations_expand(
        project_id=project_id,
        settings=updated_settings.get('pgvector_configuration', {}),
        user_id=user_id,
        unsecret=True
    )
    update_toolkit_index_meta_history_with_failed_state(
        pgv_settings_expanded.get('connection_string'),
        toolkit.id,
        index_meta_id,
        init_issue
    )
    this.module.notify_index_data_status({
        'id': None,
        'index_name': index_meta_id,
        'state': 'failed',
        'error': init_issue,
        'reindex': False,
        'indexed': 0,
        'updated': 0,
        'toolkit_id': toolkit.id,
        'project_id': project_id,
        'user_id': int(user_id),
        'initiator': InitiatorType.schedule
    })
    log.debug(f"[handle_failed_index_schedule] End: project_id={project_id}, toolkit_id={toolkit.id}, index_meta_id={index_meta_id}, user_id={user_id}")