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

DEFAULT_MIN_RESPONSE_CHARS = 150
DEFAULT_TIMEOUT_SECONDS = 15


def next_input_suggestion_config(project_id: int) -> dict:
    """Live config (read each call so admin changes need no reload).

    Toggle-off means off for everyone; toggle-on means on for everyone except
    projects explicitly listed in the disallow-list.
    """
    cfg = this.descriptor.config.get('next_input_suggestion_guardrail', {}) or {}
    disallowed = {
        int(x) for x in cfg.get('disallowed_project_ids', []) or []
        if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit())
    }
    enabled = bool(cfg.get('is_enabled', False)) and project_id not in disallowed
    return {
        'enabled': enabled,
        'min_response_chars': int(cfg.get('min_response_chars', DEFAULT_MIN_RESPONSE_CHARS)),
        'timeout_seconds': int(cfg.get('timeout_seconds', DEFAULT_TIMEOUT_SECONDS)),
    }
