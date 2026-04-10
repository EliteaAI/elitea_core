from tools import VaultClient


def get_pgvector_connection_string(project_id: int) -> str | None:
    """
    Get pgvector connection string from vault secrets
    :param project_id: project id
    :return: pgvector connection string
    """
    vc = VaultClient(project_id)
    project_secrets: dict = vc.get_secrets()

    if 'pgvector_project_connstr' in project_secrets:
        return project_secrets['pgvector_project_connstr']

    return None
