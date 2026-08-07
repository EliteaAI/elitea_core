from pylon.core.tools import web, log

from ..utils.application_tools import reap_idle_pgvector_engines


class RPC:
    @web.rpc("elitea_core_reap_pgvector_engines", "reap_pgvector_engines")
    def reap_pgvector_engines_rpc(self, **kwargs) -> int | None:
        try:
            reaped = reap_idle_pgvector_engines()
            if reaped:
                log.info("pgvector engine cache: reaped %s idle engine(s)", reaped)
            return reaped
        except Exception:
            log.exception("Error during pgvector engine cache reap")
