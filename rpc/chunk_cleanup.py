from pylon.core.tools import web, log

from ..utils.file_utils import cleanup_stale_chunks


class RPC:
    @web.rpc("elitea_core_cleanup_stale_chunks", "cleanup_stale_chunks")
    def cleanup_stale_chunks_rpc(self, max_age_seconds: int = 43200, **kwargs) -> dict[str, int] | None:
        try:
            return cleanup_stale_chunks(max_age_seconds=max_age_seconds)
        except Exception as e:
            log.error("Error during stale chunks cleanup: %s", str(e))
