from tools import VaultClient


def get_context_manager_feature_flag(project_id: int, *kwargs) -> bool:
    """
    Retrieve the context manager feature flag status for a given project. The method
    determines whether the feature is enabled by querying stored secrets and optionally
    examines a conversation's metadata.

    :param project_id: The unique identifier for the project.
    :return: A boolean value indicating the status of the context manager feature flag.
    """
    vault_client = VaultClient.from_project(project_id)
    secrets = vault_client.get_secrets()
    context_management_enabled = secrets.get('context_manager', 'true').lower() == 'true'
    return context_management_enabled


def get_legacy_meta_tool_calls_storage_flag(*args, **kwargs) -> bool:
    """Whether the platform stores tool_calls / thinking_steps in message-group meta.

    Platform-wide config flag (config.yml `legacy_meta_tool_calls_storage`), same mechanism as
    `ai_project_id`. False (default) → trace steps read/write via the message_trace_step table
    only. True → legacy pre-epic behavior (meta only, no table). See Epic #5724 / TS-2.

    Accepts and ignores positional args so existing per-project call sites keep working.
    """
    from tools import elitea_config  # pylint: disable=C0415,E0401
    value = elitea_config.get('legacy_meta_tool_calls_storage', False)
    if isinstance(value, str):
        return value.lower() == 'true'
    return bool(value)

