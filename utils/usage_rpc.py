"""
Shared access to the usage plugin's elitea-mode analytics RPCs.

Analytics endpoints call these instead of importing usage's SOURCE_ELITEA constant directly,
per the RPC-based inter-plugin communication convention.
"""

import threading

import cachetools

from pylon.core.tools import log

from tools import rpc_tools

ELITEA_SPEND_SOURCE = "elitea"

_SPEND_SOURCE_CACHE = cachetools.TTLCache(maxsize=1, ttl=60)
_MODELS_CACHE = cachetools.TTLCache(maxsize=2048, ttl=60)
# Keyed on (project_id, start_date, end_date, all_projects[, user_id]) — one entry
# per requested window, hence the larger maxsize.
_COST_KPIS_CACHE = cachetools.TTLCache(maxsize=2048, ttl=60)
_MODEL_BREAKDOWN_CACHE = cachetools.TTLCache(maxsize=2048, ttl=60)
_USER_BREAKDOWN_CACHE = cachetools.TTLCache(maxsize=2048, ttl=60)
_DAILY_TREND_CACHE = cachetools.TTLCache(maxsize=2048, ttl=60)

# analytics_costs.py fetches several of these concurrently, and TTLCache itself is
# not thread-safe: an unguarded eviction race would raise, and the wrappers below
# would report that as "RPC unreachable".
_CACHE_LOCK = threading.RLock()


@cachetools.cached(cache=_SPEND_SOURCE_CACHE, lock=_CACHE_LOCK)
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


@cachetools.cached(cache=_MODELS_CACHE, lock=_CACHE_LOCK)
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


@cachetools.cached(cache=_COST_KPIS_CACHE, lock=_CACHE_LOCK)
def _usage_get_cost_kpis_rpc(project_id, start_date=None, end_date=None, all_projects=False):
    return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_cost_kpis(
        project_id=project_id, start_date=start_date, end_date=end_date,
        all_projects=all_projects,
    )


def usage_get_cost_kpis(project_id, start_date=None, end_date=None, all_projects=False):
    try:
        return _usage_get_cost_kpis_rpc(project_id, start_date, end_date, all_projects)
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_cost_kpis unreachable")
        return None


@cachetools.cached(cache=_MODEL_BREAKDOWN_CACHE, lock=_CACHE_LOCK)
def _usage_get_model_breakdown_rpc(project_id, start_date=None, end_date=None, all_projects=False):
    return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_model_breakdown(
        project_id=project_id, start_date=start_date, end_date=end_date,
        all_projects=all_projects,
    )


def usage_get_model_breakdown(project_id, start_date=None, end_date=None, all_projects=False):
    try:
        return _usage_get_model_breakdown_rpc(project_id, start_date, end_date, all_projects)
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_model_breakdown unreachable")
        return None


@cachetools.cached(cache=_USER_BREAKDOWN_CACHE, lock=_CACHE_LOCK)
def _usage_get_user_breakdown_rpc(
        project_id, start_date=None, end_date=None, all_projects=False, user_id=None,
):
    return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_user_breakdown(
        project_id=project_id, start_date=start_date, end_date=end_date,
        all_projects=all_projects, user_id=user_id,
    )


def usage_get_user_breakdown(
        project_id, start_date=None, end_date=None, all_projects=False, user_id=None,
):
    try:
        return _usage_get_user_breakdown_rpc(
            project_id, start_date, end_date, all_projects, user_id,
        )
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_user_breakdown unreachable")
        return None


@cachetools.cached(cache=_DAILY_TREND_CACHE, lock=_CACHE_LOCK)
def _usage_get_daily_trend_rpc(project_id, start_date=None, end_date=None, all_projects=False):
    return rpc_tools.RpcMixin().rpc.timeout(5).usage_get_daily_trend(
        project_id=project_id, start_date=start_date, end_date=end_date,
        all_projects=all_projects,
    )


def usage_get_daily_trend(project_id, start_date=None, end_date=None, all_projects=False):
    try:
        return _usage_get_daily_trend_rpc(project_id, start_date, end_date, all_projects)
    except Exception:  # pylint: disable=W0703
        log.warning("analytics: usage_get_daily_trend unreachable")
        return None
