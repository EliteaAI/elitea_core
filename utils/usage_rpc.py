"""
Shared access to the usage plugin's elitea-mode analytics RPCs.

Analytics endpoints call these instead of importing usage's SOURCE_ELITEA constant directly,
per the RPC-based inter-plugin communication convention.
"""

import cachetools

from pylon.core.tools import log

from tools import rpc_tools

ELITEA_SPEND_SOURCE = "elitea"

_SPEND_SOURCE_CACHE = cachetools.TTLCache(maxsize=1, ttl=60)
_MODELS_CACHE = cachetools.TTLCache(maxsize=2048, ttl=60)


@cachetools.cached(cache=_SPEND_SOURCE_CACHE)
def _usage_get_spend_source_rpc():
    return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_spend_source()


def usage_spend_source():
    try:
        return _usage_get_spend_source_rpc()
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_spend_source unreachable")
        return None


def is_elitea_mode():
    return usage_spend_source() == ELITEA_SPEND_SOURCE


@cachetools.cached(cache=_MODELS_CACHE)
def _configurations_get_models_rpc(project_id, section="llm", include_shared=True):
    return rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_models(
        project_id=project_id, section=section, include_shared=include_shared,
    )


def configurations_get_models_cached(project_id, section="llm", include_shared=True):
    try:
        return _configurations_get_models_rpc(project_id, section, include_shared)
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: configurations_get_models unreachable")
        return None


def usage_get_cost_kpis(project_id, start_date=None, end_date=None, all_projects=False):
    try:
        return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_cost_kpis(
            project_id=project_id, start_date=start_date, end_date=end_date,
            all_projects=all_projects,
        )
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_cost_kpis unreachable")
        return None


def usage_get_model_breakdown(project_id, start_date=None, end_date=None, all_projects=False):
    try:
        return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_model_breakdown(
            project_id=project_id, start_date=start_date, end_date=end_date,
            all_projects=all_projects,
        )
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_model_breakdown unreachable")
        return None


def usage_get_user_breakdown(
        project_id, start_date=None, end_date=None, all_projects=False, user_id=None,
):
    try:
        return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_user_breakdown(
            project_id=project_id, start_date=start_date, end_date=end_date,
            all_projects=all_projects, user_id=user_id,
        )
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_user_breakdown unreachable")
        return None


def usage_get_daily_trend(project_id, start_date=None, end_date=None, all_projects=False):
    try:
        return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_daily_trend(
            project_id=project_id, start_date=start_date, end_date=end_date,
            all_projects=all_projects,
        )
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_daily_trend unreachable")
        return None
