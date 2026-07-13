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

