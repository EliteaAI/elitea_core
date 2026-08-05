#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
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

""" Config resolution for the next-input-suggestion feature (guardrail-shaped). """

from tools import this


def _next_input_suggestion_config() -> dict:
    """Live config (read each call so admin changes need no reload)."""
    return this.descriptor.config.get('next_input_suggestion_guardrail', {}) or {}


def get_next_input_suggestion_min_response_chars() -> int:
    return int(_next_input_suggestion_config().get('min_response_chars', 150))


def get_next_input_suggestion_timeout_seconds() -> int:
    return int(_next_input_suggestion_config().get('timeout_seconds', 15))


def is_next_input_suggestion_enabled_for_project(project_id: int) -> bool:
    """Toggle-off means off for everyone; toggle-on means on for everyone except
    projects explicitly listed in the disallow-list."""
    cfg = _next_input_suggestion_config()
    if not cfg.get('is_enabled', False):
        return False
    disallowed = {
        int(x) for x in cfg.get('disallowed_project_ids', []) or []
        if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit())
    }
    return project_id not in disallowed
