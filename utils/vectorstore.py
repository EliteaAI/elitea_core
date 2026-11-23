from hvac.exceptions import Forbidden, InvalidPath
from pylon.core.tools import log
from tools import VaultClient


def get_pgvector_connection_string(project_id: int, *, default_on_hidden_failure: bool = False) -> str | None:
    """
    Get pgvector connection string from vault secrets
    :param project_id: project id
    :param default_on_hidden_failure: answer None when the hidden secrets cannot be read (checkpoint deletes)
    :return: pgvector connection string
    """
    vc = VaultClient(project_id)
    project_secrets: dict = vc.get_secrets()
    if 'pgvector_project_connstr' in project_secrets:
        return project_secrets['pgvector_project_connstr']

    # A hidden path that was never written, or that the project's policy predates, holds no connstr the project can
    # reach; any other failure must raise, so dispatch leaves the key out instead of sending a None the worker trusts
    try:
        hidden_secrets: dict = vc.get_project_hidden_secrets()
    except (InvalidPath, Forbidden):
        log.debug('No readable hidden secrets path for project_id=%s', project_id)
        return None
    except Exception:  # noqa: BLE001 -- a delete still reaches the default store, as before hidden secrets were read
        if not default_on_hidden_failure:
            raise
        log.warning('Hidden secrets not readable for project_id=%s; using the default store', project_id, exc_info=True)
        return None
    return hidden_secrets.get('pgvector_project_connstr')
