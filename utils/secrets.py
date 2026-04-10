from tools import VaultClient


def check_secret_header(received_secret: str, project_id: int) -> bool:
    secrets = VaultClient(project_id).get_all_secrets()
    expected_secret = str(secrets.get("secrets_header_value", "secret"))
    if not received_secret or received_secret != expected_secret:
        return False
    return True
