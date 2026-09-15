#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" Effective budget limits — the one ladder every enforcement path resolves through

Transcribed from runtime_interface_litellm's budgets.py so the two inference planes cannot
drift apart on money. Storage stays in this plugin's own models; the ladder now does too.
"""

import time

from pylon.core.tools import web, log

from tools import context

# Distinguishes "not read yet" from an explicit null limit
_UNSET = object()

PERSONAL_PROJECTS_TTL = 300.0

_personal_projects_cache = None


def _personal_project_ids(strict=False):
    """Cached set of personal project ids. strict=True raises instead of guessing "team"."""
    global _personal_projects_cache  # pylint: disable=W0603
    #
    cache = _personal_projects_cache
    #
    if cache is None or time.monotonic() - cache[0] > PERSONAL_PROJECTS_TTL:
        try:
            ids = set(context.rpc_manager.timeout(10).projects_get_personal_project_ids())
        except:  # pylint: disable=W0702
            log.exception("Failed to list personal projects")
            #
            if strict:
                raise
            #
            return set()
        #
        cache = (time.monotonic(), ids)
        _personal_projects_cache = cache
    #
    return cache[1]


def _to_micro(value):
    """USD float to micro-USD int; None stays None so "unlimited" never becomes 0."""
    if value is None:
        return None
    #
    return int(round(float(value) * 1_000_000))


class RPC:
    """ RPC """

    @web.rpc("elitea_core_is_personal_project", "is_personal_project")
    def is_personal_project(self, project_id, strict=False, **kwargs):
        """True when the project is a user's auto-created personal project.

        strict=True raises instead of answering "team" from a failed lookup.
        """
        return int(project_id) in _personal_project_ids(strict=strict)

    @web.rpc("elitea_core_get_budget_default_limit", "get_budget_default_limit")
    def get_budget_default_limit(self, scope, project_id, **kwargs):
        """Configured default limit (USD) for a scope, or None when defaults are off.

        Personal projects get their own default because they are auto-created per user
        and would otherwise all be unlimited.
        """
        defaults = (self.descriptor.config.get("cost_budgets") or {}).get("defaults") or {}
        #
        if not defaults.get("enabled", False):
            return None
        #
        if scope == "user":
            return defaults.get("user_monthly_limit", None)
        #
        if self.is_personal_project(project_id):
            return defaults.get("personal_project_monthly_limit", None)
        #
        return defaults.get("project_monthly_limit", None)

    @web.rpc("elitea_core_get_effective_project_limit", "get_effective_project_limit")
    def get_effective_project_limit(self, project_id, project_budget=_UNSET, **kwargs):
        """Effective monthly limit (USD) for a project, or None if unlimited.

        An explicit row with enabled=false means "deliberately exempt" and is honoured as
        unlimited; a project nobody has budgeted falls back to the configured default.
        """
        if project_budget is _UNSET:
            project_budget = self._read_project_budget(project_id)
        #
        if project_budget is not None:
            if not project_budget.get("enabled", True):
                return None
            #
            if project_budget.get("monthly_limit") is not None:
                return project_budget["monthly_limit"]
        #
        return self.get_budget_default_limit("project", project_id)

    @web.rpc("elitea_core_get_effective_member_default", "get_effective_member_default")
    def get_effective_member_default(
            self, project_id, project_budget=_UNSET, exempt=False, **kwargs
    ):
        """The project's own member default, or the platform default when it has none.

        The project's `enabled` flag is not consulted: it marks the project's *own* limit
        exempt, while a member default is a separately-set value in its own right.
        """
        if project_budget is _UNSET:
            project_budget = self._read_project_budget(project_id)
        #
        if project_budget and project_budget.get("member_default_limit") is not None:
            return project_budget["member_default_limit"]
        #
        return None if exempt else self.get_budget_default_limit("user", project_id)

    @web.rpc("elitea_core_get_effective_member_limit", "get_effective_member_limit")
    def get_effective_member_limit(
            self, project_id, user_id, project_budget=_UNSET, member_budget=_UNSET, **kwargs
    ):
        """Effective monthly per-member limit within a project, or None if unlimited.

        Three tiers: the member's own row, the project's member default, the platform default.

        A row with enabled=false exempts the member from the *platform* default only — a limit
        an admin set on this project still applies, so "set a limit for everyone here" cannot
        be silently undone by a member row nobody meant to opt out.

        A personal project has one member, its owner, so its project budget already IS that
        member's budget; a second limit there could only duplicate or silently override it.
        """
        if self.is_personal_project(project_id):
            return None
        #
        if member_budget is _UNSET:
            member_budget = self._read_user_budget(project_id, user_id)
        #
        exempt = member_budget is not None and not member_budget.get("enabled", True)
        #
        if member_budget is not None and not exempt \
                and member_budget.get("monthly_limit") is not None:
            return member_budget["monthly_limit"]
        #
        return self.get_effective_member_default(
            project_id, project_budget=project_budget, exempt=exempt,
        )

    @web.rpc("elitea_core_get_effective_budget_limits", "get_effective_budget_limits")
    def get_effective_budget_limits(self, project_id, user_id=None, **kwargs):
        """Both effective limits in micro-USD. None = unlimited.

        One call, one project-row read: the gate needs both scopes per decision and must not
        pay two RPCs plus two duplicate reads of the same row for them.

        Raises when a read fails: this is the enforcement path, where "no row" and "could not
        read the row" must not resolve to the same limit. The caller reports the answer as
        unknown, which fails closed in enforce mode.
        """
        personal = self.is_personal_project(project_id, strict=True)
        #
        project_budget = self.get_project_budget(project_id)
        member_budget = _UNSET if user_id is None else self.get_user_budget(project_id, user_id)
        #
        project_limit = self.get_effective_project_limit(
            project_id, project_budget=project_budget,
        )
        #
        member_limit = None
        #
        if user_id is not None:
            member_limit = self.get_effective_member_limit(
                project_id, user_id,
                project_budget=project_budget, member_budget=member_budget,
            )
        #
        return {
            "project_limit_micro": _to_micro(project_limit),
            "member_limit_micro": _to_micro(member_limit),
            "project_limit": project_limit,
            "member_limit": member_limit,
            # False when nothing limits this caller, so the gate can skip Redis entirely
            "enabled": project_limit is not None or member_limit is not None,
            "is_personal_project": personal,
        }

    @web.rpc("elitea_core_get_effective_project_limits", "get_effective_project_limits")
    def get_effective_project_limits(self, project_ids, **kwargs):
        """Effective limits for many projects, keyed by project id.

        Reads every stored row in one query: the admin pages list whole environments, where a
        per-project lookup is thousands of calls. Must stay identical to the single-project
        resolver above, which the request path uses.
        """
        try:
            budgets = self.list_project_budgets() or {}
        except:  # pylint: disable=W0702
            log.exception("Failed to list project budgets")
            return {project_id: None for project_id in project_ids}
        #
        # Iterate the requested ids, not the budget map: a project with no stored row still
        # has to fall through to the configured default
        return {
            project_id: self.get_effective_project_limit(
                project_id,
                project_budget=budgets.get(project_id, budgets.get(str(project_id))),
            )
            for project_id in project_ids
        }

    @web.rpc("elitea_core_get_effective_member_limits", "get_effective_member_limits")
    def get_effective_member_limits(self, project_id, user_ids, **kwargs):
        """Effective per-member limits within a project, keyed by user id.

        The project row holds the member default every unset member falls back to, so it is
        read once here rather than per member — the member list can be a whole project.
        """
        project_budget = self._read_project_budget(project_id)
        #
        return {
            user_id: self.get_effective_member_limit(
                project_id, user_id, project_budget=project_budget,
            )
            for user_id in user_ids
        }

    @web.rpc("elitea_core_read_project_budget_row", "_read_project_budget")
    def _read_project_budget(self, project_id, **kwargs):
        """The stored project row as a dict, or None. Never raises — money math degrades."""
        try:
            return self.get_project_budget(project_id)
        except:  # pylint: disable=W0702
            log.exception("Failed to read the budget row for project %s", project_id)
            return None

    @web.rpc("elitea_core_read_user_budget_row", "_read_user_budget")
    def _read_user_budget(self, project_id, user_id, **kwargs):
        """The stored member row as a dict, or None. Never raises."""
        try:
            return self.get_user_budget(project_id, user_id)
        except:  # pylint: disable=W0702
            log.exception(
                "Failed to read the budget row for project %s user %s", project_id, user_id,
            )
            return None
