from pylon.core.tools import log
from tools import rpc_tools


def get_service_prompt(key: str) -> str:
    """Return the content of a public service prompt identified by ``key``.

    Service prompts are stored in the Public project as ``service_prompt``
    configurations (seeded by ``configurations.service_prompt_seed``). The key
    lives under the record's ``data.key`` field, so we fetch the public
    service prompts and match on it.

    Returns an empty string when the prompt is missing or the lookup fails, so
    callers can surface a clear "service prompt is not configured" error.
    """
    try:
        configs = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_filtered_public(
            filter_fields={"type": "service_prompt"}
        )
        for cfg in configs or []:
            if cfg.get("data", {}).get("key") == key:
                return cfg["data"].get("prompt", "")
    except Exception:
        log.warning("get_service_prompt: failed to fetch service prompt '%s' from configurations", key)
    return ""
