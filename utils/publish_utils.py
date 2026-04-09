"""Publishing utilities for agent version cross-project publish/unpublish.

This module composes existing functions — it does NOT duplicate them.
`create_application()`, `create_version()` remain the authoritative implementations.

Also contains publish validation: deterministic checks (#4049) + AI quality
checks (#3774) — HMAC tokens, rule-based checks, AI pipeline, merging.
"""

import base64
import hashlib
import hmac
import json
import re
import time
from copy import deepcopy
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError
from pylon.core.tools import log
from sqlalchemy.orm import selectinload
from tools import db, this, rpc_tools

from ..models.all import Application, ApplicationVersion
from ..models.elitea_tools import EliteATool, EntityToolMapping
from ..models.enums.all import AgentTypes, NotificationEventTypes, PublishStatus, ToolEntityTypes
from ..models.pd.application import ApplicationImportModel
from ..models.pd.version import ApplicationVersionForkCreateModel
from ..models.pd.publish import PublishAIResult
from .create_utils import create_application, create_version
from .utils import get_public_project_id


# §12 — fields carried into the public snapshot
_VERSION_ALLOWLIST = frozenset({
    'name',
    'instructions',
    'agent_type',
    'variables',
    'welcome_message',
    'conversation_starters',
    'tags',
})

# Keys preserved from version.meta (everything else is stripped)
_META_ALLOWLIST = frozenset({
    'internal_tools',
    'step_limit',
})


# Maximum sub-agent nesting depth (configurable)
_MAX_SUB_AGENT_DEPTH = 3

# ---------------------------------------------------------------------------
# Validation constants
# ---------------------------------------------------------------------------

DEFAULT_VALIDATION_TIMEOUT = 60
DEFAULT_VALIDATION_TOKEN_TTL = 300  # 5 minutes

GENERIC_NAME_BLOCKLIST = frozenset([
    "test agent", "my agent", "new agent", "agent", "test",
    "untitled", "demo", "example", "sample",
])

GENERIC_TAG_SET = frozenset({"agent", "assistant", "ai", "bot", "helper"})

GENERIC_VERSION_BLOCKLIST = frozenset({
    "v1", "v2", "v3", "1", "2", "test", "draft",
})

PLACEHOLDER_RE = re.compile(
    r'TODO|TBD|Lorem|FIXME|\[REPLACE\]|placeholder|insert here',
    re.IGNORECASE,
)

SECRET_RE = re.compile(
    r'sk-[a-zA-Z0-9]{20,}'
    r'|password\s*='
    r'|Bearer\s+[a-zA-Z0-9]{10,}'
    r'|api[_\-]?key\s*[:=]'
    r'|secret[_\-]?key\s*[:=]',
    re.IGNORECASE,
)

ACTION_VERB_RE = re.compile(
    r'\b(helps?|analyzes?|generates?|creates?|manages?|monitors?'
    r'|provides?|assists?|automates?|processes?)\b',
    re.IGNORECASE,
)

SEMVER_HINT_RE = re.compile(r'^v?\d+\.\d+')

DEFAULT_VALIDATION_PROMPT = """\
You are an Agent Publishing Validator for a public AI-agent marketplace.

Evaluate the agent definition provided by the user and report quality issues.

EVALUATION CRITERIA (AI-focused — deterministic identity checks are done separately):
1. Instructions quality — Clear, specific, actionable? Safety concerns?
2. Variables — Meaningful names? Sensible defaults?
3. Welcome message — Introduces the agent and guides the user?
4. Conversation starters — Relevant, diverse, helpful?
5. Sub-agent instructions — Same quality bar as the parent.

SEVERITY GUIDE:
- critical_issues: instructions missing/incoherent/harmful, safety problems
- warnings: quality gaps (vague instructions, poor starters)
- recommendations: nice-to-have improvements

OUTPUT FORMAT — return a single JSON object with exactly these four keys:

{"summary": "One-sentence overall assessment", "critical_issues": [{"field": "instructions", "issue": "what is wrong", "fix": "how to fix", "context": null}], "warnings": [{"field": "welcome_message", "issue": "what is wrong", "fix": "how to fix", "context": null}], "recommendations": [{"field": "conversation_starters", "suggestion": "what to improve", "context": null}]}

FIELD RULES:
- critical_issues/warnings items MUST have keys: field, issue, fix, context
- recommendations items MUST have keys: field, suggestion, context
- context: null for parent agent, "sub-agent: <name> (<tool_name>)" for sub-agents
- Max 20 items per list; use empty list [] when no items to report
- summary must always be present and non-empty
- Be concise; only report genuine issues — do NOT fabricate problems
- If everything looks good, return empty lists and a positive summary

CRITICAL: Your entire response must be ONLY the raw JSON object.
No explanatory text, no markdown formatting, no code fences.
Start your response with { and end with }.
"""


# ---------------------------------------------------------------------------
# Pre-validation errors (shared base for early-exit failures)
# ---------------------------------------------------------------------------

class PublishPreValidationError(Exception):
    """Base for errors that short-circuit validation to FAIL."""

    def to_validation_result(self) -> dict:
        raise NotImplementedError


class SubAgentTreeError(PublishPreValidationError):
    """Structural problem in the sub-agent tree (cycle or depth exceeded)."""

    def __init__(self, message: str, error_code: str, fix: str):
        super().__init__(message)
        self.error_code = error_code
        self.fix = fix

    def to_validation_result(self) -> dict:
        """Convert to a FAIL validation result with a single critical issue."""
        return {
            'status': 'FAIL',
            'critical_issues': [{
                'field': 'sub_agents',
                'issue': str(self),
                'fix': self.fix,
                'source': 'deterministic',
                'context': None,
            }],
            'warnings': [],
            'recommendations': [],
            'summary': str(self),
            'counts': {'critical': 1, 'warnings': 0, 'suggestions': 0},
            'ai_validation_available': False,
            'validation_token': None,
        }


class PreValidationError(PublishPreValidationError):
    """Early-exit failure detected before heavy validation."""

    def __init__(self, rule: str, message: str):
        super().__init__(message)
        self.rule = rule

    def to_validation_result(self) -> dict:
        return {
            'status': 'FAIL',
            'issues': [{
                'severity': 'critical',
                'rule': self.rule,
                'message': str(self),
            }],
            'counts': {'critical': 1, 'warning': 0, 'info': 0},
            'validation_token': None,
        }


# ---------------------------------------------------------------------------
# Sub-agent tree model
# ---------------------------------------------------------------------------

class SubAgentNode(BaseModel):
    """One node in the sub-agent tree discovered during publish."""
    app_id: int
    version_id: int
    tool_name: str
    depth: int
    children: List['SubAgentNode'] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Pipeline guard
# ---------------------------------------------------------------------------

def check_not_pipeline(
    version_id: int,
    session,
) -> Optional[Tuple[dict, int]]:
    """Return an error tuple if the version is a pipeline, else None."""
    ver = session.query(ApplicationVersion).get(version_id)
    if ver and ver.agent_type == AgentTypes.pipeline.value:
        return {
            "error": "pipeline_not_publishable",
            "msg": "Pipeline agents cannot be published",
        }, 400
    return None


# ---------------------------------------------------------------------------
# Sub-agent tree collection
# ---------------------------------------------------------------------------

def collect_sub_agent_tree(
    project_id: int,
    version_id: int,
    max_depth: int = _MAX_SUB_AGENT_DEPTH,
    session=None,
) -> List[SubAgentNode]:
    """Recursively discover sub-agents referenced by a version's tools.

    Returns a list of top-level ``SubAgentNode`` objects, each of which may
    contain nested children.  Raises ``SubAgentTreeError`` on circular
    references or depth violations, ``ValueError`` on missing sub-agents.
    """
    visited: set = set()
    return _collect_sub_agents_recursive(
        project_id, version_id, max_depth, depth=0, visited=visited,
        session=session,
    )


def _collect_sub_agents_recursive(
    project_id: int,
    version_id: int,
    max_depth: int,
    depth: int,
    visited: set,
    session=None,
) -> List[SubAgentNode]:
    """Internal recursive helper for ``collect_sub_agent_tree``."""
    if session is not None:
        return _collect_sub_agents_in_session(
            project_id, version_id, max_depth, depth, visited,
            session,
        )
    with db.get_session(project_id) as session:
        return _collect_sub_agents_in_session(
            project_id, version_id, max_depth, depth, visited,
            session,
        )


def _collect_sub_agents_in_session(
    project_id: int,
    version_id: int,
    max_depth: int,
    depth: int,
    visited: set,
    session,
) -> List[SubAgentNode]:
    """Core logic for sub-agent tree collection within a session."""
    version = (
        session.query(ApplicationVersion)
        .filter(ApplicationVersion.id == version_id)
        .options(
            selectinload(ApplicationVersion.tools),
        )
        .first()
    )
    if version is None:
        raise ValueError(f"Version {version_id} not found in project {project_id}")

    # Collect application-type tools (sub-agents)
    sub_agent_tools = [
        t for t in version.tools if t.type == 'application'
    ]
    if not sub_agent_tools:
        return []

    nodes: List[SubAgentNode] = []
    for tool in sub_agent_tools:
        child_app_id = (tool.settings or {}).get('application_id')
        child_ver_id = (tool.settings or {}).get('application_version_id')
        tool_name = tool.name or f'application_{child_app_id}'

        if child_app_id is None or child_ver_id is None:
            raise ValueError(
                f"Sub-agent tool '{tool_name}' has incomplete settings "
                f"(application_id={child_app_id}, application_version_id={child_ver_id})"
            )

        # Cycle detection
        key = (child_app_id, child_ver_id)
        if key in visited:
            raise SubAgentTreeError(
                f"Circular sub-agent dependency detected involving "
                f"application {child_app_id} version {child_ver_id}",
                error_code='cycle_detected',
                fix='Remove the circular sub-agent reference before publishing',
            )

        # Depth check
        next_depth = depth + 1
        if next_depth > max_depth:
            raise SubAgentTreeError(
                f"Sub-agent nesting exceeds maximum depth of {max_depth}",
                error_code='depth_exceeded',
                fix=f'Reduce sub-agent nesting to at most {max_depth} levels',
            )

        # Validate sub-agent exists
        child_version = (
            session.query(ApplicationVersion)
            .filter(ApplicationVersion.id == child_ver_id)
            .first()
        )
        if child_version is None:
            raise ValueError(
                f"Sub-agent version {child_ver_id} "
                f"(referenced by tool '{tool_name}') not found"
            )

        # Skip pipeline-type sub-agents — pipelines must never be
        # published or validated as sub-agents
        if child_version.agent_type == AgentTypes.pipeline.value:
            log.info(
                "Skipping pipeline sub-agent '%s' (ver=%d) from "
                "sub-agent tree — pipelines are excluded",
                tool_name, child_ver_id,
            )
            continue

        visited.add(key)
        children = _collect_sub_agents_recursive(
            project_id, child_ver_id, max_depth,
            depth=next_depth, visited=visited,
            session=session,
        )
        nodes.append(SubAgentNode(
            app_id=child_app_id,
            version_id=child_ver_id,
            tool_name=tool_name,
            depth=next_depth,
            children=children,
        ))

    return nodes


# ---------------------------------------------------------------------------
# Snapshot creation
# ---------------------------------------------------------------------------

def create_publish_snapshot(
    project_id: int,
    version_id: int,
    user_id: int,
) -> dict:
    """Export a version and strip fields that must not appear in the public project.

    Returns a sanitised dict suitable for feeding into ``publish_first_version``
    or ``publish_additional_version``.
    Runs its own DB session (read-only).
    """
    with db.get_session(project_id) as session:
        version = (
            session.query(ApplicationVersion)
            .filter(ApplicationVersion.id == version_id)
            .first()
        )
        if version is None:
            raise ValueError(f"Version {version_id} not found in project {project_id}")

        application = version.application

        # Serialise version via the ORM helper (includes tools/vars/tags)
        version_dict = version.to_dict()

        # Serialise app-level fields
        app_dict = application.to_json()

    # -- Build sanitised version data --
    sanitised_version = {k: deepcopy(version_dict[k]) for k in _VERSION_ALLOWLIST if k in version_dict}

    # Preserve allowed meta keys
    raw_meta = version_dict.get('meta') or {}
    sanitised_meta = {k: deepcopy(raw_meta[k]) for k in _META_ALLOWLIST if k in raw_meta}
    sanitised_version['meta'] = sanitised_meta

    # Log stripped content for auditing
    stripped_tools_count = len(version_dict.get('tools', []))
    stripped_pipeline = bool(version_dict.get('pipeline_settings'))
    if stripped_tools_count:
        log.info("[PUBLISH] Stripped %d tool(s) from version %d", stripped_tools_count, version_id)
    if stripped_pipeline:
        log.info("[PUBLISH] Stripped pipeline_settings from version %d", version_id)

    return {
        'application': {
            'name': app_dict.get('name', ''),
            'description': app_dict.get('description', ''),
            'icon': app_dict.get('icon'),
        },
        'version': sanitised_version,
        'source': {
            'project_id': project_id,
            'application_id': application.id,
            'version_id': version_id,
            'author_id': user_id,
        },
    }


# ---------------------------------------------------------------------------
# Twin detection
# ---------------------------------------------------------------------------

def find_public_twin(
    public_project_id: int,
    source_project_id: int,
    source_app_id: int,
) -> Optional[Application]:
    """Find the public Application that mirrors a private one.

    Uses the ``shared_owner_id`` / ``shared_id`` unique constraint.
    Returns the ORM object **inside an active session** — caller must use the
    returned session context or call within a ``db.get_session`` block.
    """
    with db.get_session(public_project_id) as session:
        twin = (
            session.query(Application)
            .filter(
                Application.shared_owner_id == source_project_id,
                Application.shared_id == source_app_id,
            )
            .first()
        )
        if twin is not None:
            # Eagerly read the id so we can use it after session closes
            twin_id = twin.id
    # Return just the id; caller re-queries as needed
    if twin is not None:
        return twin_id
    return None


# ---------------------------------------------------------------------------
# First publish (Application + base + named version)
# ---------------------------------------------------------------------------

def publish_first_version(
    public_project_id: int,
    snapshot: dict,
    version_name: str,
    user_id: int,
) -> dict:
    """Create a new Application in the public project with a base version and the published version.

    Returns ``{'application_id': int, 'version_id': int}``.
    """
    src = snapshot['source']
    app_info = snapshot['application']
    ver_info = snapshot['version']

    # Build the base version (required by Application.get_default_version())
    base_version = _build_base_version_dict(ver_info, user_id, public_project_id)

    # Build the published version
    published_version = _build_published_version_dict(
        ver_info, version_name, user_id, public_project_id, src,
    )

    import_payload = {
        'name': app_info['name'],
        'description': app_info.get('description', ''),
        'icon': app_info.get('icon'),
        'owner_id': public_project_id,
        'shared_owner_id': src['project_id'],
        'shared_id': src['application_id'],
        'versions': [base_version, published_version],
        'project_id': public_project_id,
        'user_id': user_id,
    }

    model = ApplicationImportModel.model_validate(import_payload)

    with db.get_session(public_project_id) as session:
        application = create_application(model, session, public_project_id)
        # Initialise adoption counters
        application.meta = application.meta or {}
        application.meta['adoption'] = {
            'conversation_count': 0,
            'project_count': 0,
            'project_ids': [],
        }
        session.flush()

        # Identify the published version (not the base) and mark it published
        pub_ver = next(
            (v for v in application.versions if v.name == version_name),
            None,
        )
        if pub_ver is not None:
            pub_ver.status = PublishStatus.published
        result = {
            'application_id': application.id,
            'version_id': pub_ver.id if pub_ver else None,
        }
        session.commit()

    return result


# ---------------------------------------------------------------------------
# Additional version on existing public Application
# ---------------------------------------------------------------------------

def publish_additional_version(
    public_project_id: int,
    public_app_id: int,
    snapshot: dict,
    version_name: str,
    user_id: int,
) -> dict:
    """Add a new published version to an existing public Application.

    Returns ``{'application_id': int, 'version_id': int}``.
    """
    src = snapshot['source']
    ver_info = snapshot['version']

    published_version_dict = _build_published_version_dict(
        ver_info, version_name, user_id, public_project_id, src,
    )

    model = ApplicationVersionForkCreateModel.model_validate(published_version_dict)

    with db.get_session(public_project_id) as session:
        application = session.query(Application).get(public_app_id)
        if application is None:
            raise ValueError(f"Public application {public_app_id} not found")

        new_version = create_version(model, application=application, session=session)
        new_version.status = PublishStatus.published
        result = {
            'application_id': application.id,
            'version_id': new_version.id,
        }
        session.commit()

    return result


# ---------------------------------------------------------------------------
# Unpublish (delete from public project)
# ---------------------------------------------------------------------------

def delete_public_version(
    public_project_id: int,
    public_version_id: int,
) -> dict:
    """Delete a published version from the public project.

    Cascade-deletes any embedded sub-agents owned by this version first.
    If no non-base versions remain on the parent application after deletion,
    removes the application shell as well (prevents zombie shells).
    Returns source linkage from ``version.meta`` so the caller can sync the
    private version and optionally send a notification.
    """
    # Cascade-delete embedded sub-agents before removing the parent version
    cascade_delete_sub_agents(public_project_id, public_version_id)

    with db.get_session(public_project_id) as session:
        version = session.query(ApplicationVersion).get(public_version_id)
        if version is None:
            raise ValueError(f"Public version {public_version_id} not found")
        if version.status != PublishStatus.published:
            return {'not_published': True}

        app_id = version.application_id
        source_meta = {
            'source_project_id': (version.meta or {}).get('source_project_id'),
            'source_version_id': (version.meta or {}).get('source_version_id'),
            'source_application_id': (version.meta or {}).get('source_application_id'),
            'source_author_id': (version.meta or {}).get('source_author_id'),
        }

        session.delete(version)
        session.flush()

        # Remove the application shell if no non-base versions remain
        remaining = (
            session.query(ApplicationVersion)
            .filter(
                ApplicationVersion.application_id == app_id,
                ApplicationVersion.name != 'base',
            )
            .count()
        )
        if remaining == 0:
            app = session.query(Application).get(app_id)
            if app is not None:
                session.delete(app)
                log.info(
                    "[PUBLISH] Removed empty public app shell %d after unpublish",
                    app_id,
                )

        session.commit()

    return {'not_published': False, **source_meta}


# ---------------------------------------------------------------------------
# Source version status sync
# ---------------------------------------------------------------------------

def sync_source_version_status(
    source_project_id: int,
    source_version_id: int,
    new_status: PublishStatus,
) -> bool:
    """Update the author's private version status after publish/unpublish.

    Returns True if the update was applied, False if version not found.
    """
    with db.get_session(source_project_id) as session:
        version = session.query(ApplicationVersion).get(source_version_id)
        if version is None:
            log.warning(
                "[PUBLISH] Source version %d not found in project %d — skipping sync",
                source_version_id,
                source_project_id,
            )
            return False
        version.status = new_status
        session.commit()
    return True


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def check_version_name_unique(
    public_project_id: int,
    public_app_id: int,
    name: str,
) -> bool:
    """Return True if no version with this name exists on the public app."""
    with db.get_session(public_project_id) as session:
        exists = (
            session.query(ApplicationVersion)
            .filter(
                ApplicationVersion.application_id == public_app_id,
                ApplicationVersion.name == name,
            )
            .first()
        )
    return exists is None


def version_name_exists(
    project_id: int,
    application_id: int,
    version_name: str,
    session=None,
) -> bool:
    """Check whether a version with the given name already exists."""
    def _query(s):
        return s.query(
            s.query(ApplicationVersion)
            .filter(
                ApplicationVersion.application_id == application_id,
                ApplicationVersion.name == version_name,
            )
            .exists()
        ).scalar()

    if session is not None:
        return _query(session)
    with db.get_session(project_id) as s:
        return _query(s)


def check_publish_limit(
    public_project_id: int,
    public_app_id: int,
    limit: int,
) -> Tuple[bool, int]:
    """Return ``(allowed, current_count)`` where *allowed* is True if under limit."""
    with db.get_session(public_project_id) as session:
        count = (
            session.query(ApplicationVersion)
            .filter(
                ApplicationVersion.application_id == public_app_id,
                ApplicationVersion.status == PublishStatus.published,
            )
            .count()
        )
    return count < limit, count


def find_public_version_by_source(
    public_project_id: int,
    public_app_id: int,
    source_version_id: int,
) -> Optional[int]:
    """Find the public version whose meta links back to a specific source version.

    Returns the public version id, or None.
    """
    with db.get_session(public_project_id) as session:
        versions = (
            session.query(ApplicationVersion)
            .filter(ApplicationVersion.application_id == public_app_id)
            .all()
        )
        for v in versions:
            meta = v.meta or {}
            if meta.get('source_version_id') == source_version_id:
                return v.id
    return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_base_version_dict(ver_info: dict, user_id: int, project_id: int) -> dict:
    """Build a minimal 'base' version dict required by ``Application.get_default_version()``."""
    return {
        'name': 'base',
        'author_id': user_id,
        'project_id': project_id,
        'user_id': user_id,
        'instructions': ver_info.get('instructions', ''),
        'agent_type': ver_info.get('agent_type', 'openai'),
        'llm_settings': {'model_name': '', 'integration_uid': ''},
        'meta': {},
        'status': PublishStatus.draft.value,
    }


def _build_published_version_dict(
    ver_info: dict,
    version_name: str,
    user_id: int,
    project_id: int,
    source: dict,
) -> dict:
    """Build the published version dict from the sanitised snapshot."""
    meta = deepcopy(ver_info.get('meta', {}))
    # Store source linkage for unpublish / sync
    meta['source_project_id'] = source['project_id']
    meta['source_application_id'] = source['application_id']
    meta['source_version_id'] = source['version_id']
    meta['source_author_id'] = source.get('author_id')
    meta['published_by'] = user_id

    return {
        'name': version_name,
        'author_id': user_id,
        'project_id': project_id,
        'user_id': user_id,
        'instructions': ver_info.get('instructions', ''),
        'agent_type': ver_info.get('agent_type', 'openai'),
        'variables': ver_info.get('variables', []),
        'tags': ver_info.get('tags', []),
        'welcome_message': ver_info.get('welcome_message', ''),
        'conversation_starters': ver_info.get('conversation_starters', []),
        'meta': meta,
        'status': PublishStatus.published.value,
    }


# ---------------------------------------------------------------------------
# Sub-agent embedding (Phase 2)
# ---------------------------------------------------------------------------

def create_embedded_agent(
    public_project_id: int,
    snapshot: dict,
    parent_pub_app_id: int,
    parent_pub_ver_id: int,
    user_id: int,
) -> dict:
    """Create an embedded sub-agent copy in the public project.

    Similar to ``publish_first_version`` but sets status to ``embedded``
    and records parent linkage in version meta.

    Returns ``{'application_id': int, 'version_id': int}``.
    """
    src = snapshot['source']
    app_info = snapshot['application']
    ver_info = snapshot['version']

    base_version = _build_base_version_dict(ver_info, user_id, public_project_id)

    # Build the embedded version
    meta = deepcopy(ver_info.get('meta', {}))
    meta['source_project_id'] = src['project_id']
    meta['source_application_id'] = src['application_id']
    meta['source_version_id'] = src['version_id']
    meta['source_author_id'] = src.get('author_id')
    meta['parent_published_app_id'] = parent_pub_app_id
    meta['parent_published_version_id'] = parent_pub_ver_id

    embedded_version = {
        'name': 'embedded',
        'author_id': user_id,
        'project_id': public_project_id,
        'user_id': user_id,
        'instructions': ver_info.get('instructions', ''),
        'agent_type': ver_info.get('agent_type', 'openai'),
        'variables': ver_info.get('variables', []),
        'meta': meta,
        'status': PublishStatus.embedded.value,
    }

    import_payload = {
        'name': app_info['name'],
        'description': app_info.get('description', ''),
        'icon': app_info.get('icon'),
        'owner_id': public_project_id,
        'shared_owner_id': None,   # NULL avoids UniqueConstraint collision
        'shared_id': None,         # when multiple parents embed the same source
        'versions': [base_version, embedded_version],
        'project_id': public_project_id,
        'user_id': user_id,
    }

    model = ApplicationImportModel.model_validate(import_payload)

    with db.get_session(public_project_id) as session:
        application = create_application(model, session, public_project_id)
        session.flush()

        emb_ver = next(
            (v for v in application.versions if v.name == 'embedded'),
            None,
        )
        if emb_ver is not None:
            emb_ver.status = PublishStatus.embedded

        result = {
            'application_id': application.id,
            'version_id': emb_ver.id if emb_ver else None,
        }
        session.commit()

    return result


def link_sub_agent_to_version(
    public_project_id: int,
    parent_ver_id: int,
    parent_app_id: int,
    sub_agent_app_id: int,
    sub_agent_ver_id: int,
    tool_name: str,
    user_id: int,
) -> None:
    """Create an EliteATool + EntityToolMapping linking a parent version to a sub-agent copy."""
    with db.get_session(public_project_id) as session:
        tool = EliteATool(
            name=tool_name,
            type='application',
            author_id=user_id,
            settings={
                'application_id': sub_agent_app_id,
                'application_version_id': sub_agent_ver_id,
            },
        )
        session.add(tool)
        session.flush()

        mapping = EntityToolMapping(
            tool_id=tool.id,
            entity_version_id=parent_ver_id,
            entity_id=parent_app_id,
            entity_type=ToolEntityTypes.agent,
        )
        session.add(mapping)
        session.commit()


def _flatten_tree(nodes: List[SubAgentNode]) -> List[SubAgentNode]:
    """Return all nodes in the tree sorted deepest-first (bottom-up)."""
    flat: List[SubAgentNode] = []
    for node in nodes:
        flat.extend(_flatten_tree(node.children))
        flat.append(node)
    return flat


def publish_sub_agents(
    source_project_id: int,
    public_project_id: int,
    source_version_id: int,
    parent_pub_app_id: int,
    parent_pub_ver_id: int,
    user_id: int,
    pre_validated_tree: Optional[List[SubAgentNode]] = None,
) -> Optional[dict]:
    """Discover, snapshot, and embed all sub-agents for a published parent version.

    Returns None on success, or a dict ``{'error': …, 'msg': …}`` on
    partial failure so the caller can report what went wrong.
    """
    if pre_validated_tree is not None:
        tree = pre_validated_tree
    else:
        try:
            tree = collect_sub_agent_tree(source_project_id, source_version_id)
        except ValueError as exc:
            return {'error': 'sub_agent_validation', 'msg': str(exc)}

    if not tree:
        return None  # No sub-agents — nothing to do

    # Flatten with deepest nodes first for bottom-up creation
    flat_nodes = _flatten_tree(tree)

    # Maps (source_app_id, source_ver_id) → (pub_app_id, pub_ver_id)
    id_map: Dict[tuple, tuple] = {}
    created_names: List[str] = []

    # Phase 1: Create all embedded agent copies (bottom-up)
    for node in flat_nodes:
        key = (node.app_id, node.version_id)
        if key in id_map:
            continue  # Same sub-agent already created (shared across branches)
        try:
            snapshot = create_publish_snapshot(
                source_project_id, node.version_id, user_id,
            )
            result = create_embedded_agent(
                public_project_id, snapshot,
                parent_pub_app_id, parent_pub_ver_id, user_id,
            )
            id_map[key] = (result['application_id'], result['version_id'])
            created_names.append(node.tool_name)
        except Exception as exc:
            log.error(
                "[PUBLISH] Failed to create embedded sub-agent %s (app=%d, ver=%d): %s",
                node.tool_name, node.app_id, node.version_id, exc,
            )
            return {
                'error': 'partial_publish',
                'msg': (
                    f"Parent published successfully. "
                    f"Sub-agent '{node.tool_name}' failed: {exc}. "
                    f"Already embedded: {created_names or 'none'}"
                ),
            }

    # Phase 2: Create tool links (bottom-up — sub-agents first, then parent)
    try:
        # Link sub-agent internal tool chains (B' → D', etc.)
        for node in flat_nodes:
            if not node.children:
                continue
            src_key = (node.app_id, node.version_id)
            pub_app_id, pub_ver_id = id_map[src_key]
            for child in node.children:
                child_key = (child.app_id, child.version_id)
                child_pub_app_id, child_pub_ver_id = id_map[child_key]
                link_sub_agent_to_version(
                    public_project_id,
                    pub_ver_id, pub_app_id,
                    child_pub_app_id, child_pub_ver_id,
                    child.tool_name, user_id,
                )

        # Link parent version to its direct sub-agent copies
        for node in tree:
            src_key = (node.app_id, node.version_id)
            pub_app_id, pub_ver_id = id_map[src_key]
            link_sub_agent_to_version(
                public_project_id,
                parent_pub_ver_id, parent_pub_app_id,
                pub_app_id, pub_ver_id,
                node.tool_name, user_id,
            )
    except Exception as exc:
        log.error("[PUBLISH] Failed to link sub-agent tools: %s", exc)
        return {
            'error': 'partial_publish',
            'msg': (
                f"Parent and sub-agents published but tool linking failed: {exc}. "
                f"Embedded: {created_names}"
            ),
        }

    log.info(
        "[PUBLISH] Successfully embedded %d sub-agent(s) for parent version %d",
        len(id_map), parent_pub_ver_id,
    )
    return None


def cascade_delete_sub_agents(
    public_project_id: int,
    parent_pub_ver_id: int,
) -> int:
    """Delete all embedded sub-agent copies owned by a parent published version.

    Returns the number of sub-agent applications deleted.
    """
    deleted_count = 0
    with db.get_session(public_project_id) as session:
        # Find all embedded versions referencing this parent version
        embedded_versions = (
            session.query(ApplicationVersion)
            .filter(
                ApplicationVersion.status == PublishStatus.embedded,
                ApplicationVersion.meta['parent_published_version_id'].astext
                == str(parent_pub_ver_id),
            )
            .all()
        )

        app_ids_to_check: set = set()
        for ver in embedded_versions:
            app_ids_to_check.add(ver.application_id)
            session.delete(ver)

        session.flush()

        # Clean up applications that have no remaining versions (except base)
        for app_id in app_ids_to_check:
            remaining = (
                session.query(ApplicationVersion)
                .filter(
                    ApplicationVersion.application_id == app_id,
                    ApplicationVersion.name != 'base',
                )
                .count()
            )
            if remaining == 0:
                app = session.query(Application).get(app_id)
                if app is not None:
                    session.delete(app)
                    deleted_count += 1

        session.commit()

    if deleted_count:
        log.info(
            "[PUBLISH] Cascade-deleted %d embedded sub-agent(s) for parent version %d",
            deleted_count, parent_pub_ver_id,
        )
    return deleted_count


# ---------------------------------------------------------------------------
# High-level orchestration (called by API handlers)
# ---------------------------------------------------------------------------

def admin_publish(
    project_id: int,
    version_id: int,
    source_app_id: int,
    version_name: str,
    user_id: int,
    max_versions: int,
) -> Tuple[dict, int]:
    """Snapshot the version directly into a shell — no clone created."""
    return _publish_impl(
        project_id, version_id, source_app_id,
        version_name, user_id,
        public_project_id=project_id,
        max_versions=max_versions,
        clone_source=False,
    )


def user_publish(
    project_id: int,
    version_id: int,
    source_app_id: int,
    version_name: str,
    user_id: int,
    public_project_id: int,
    max_versions: int,
) -> Tuple[dict, int]:
    """Clone the source version, then snapshot the clone into the public project."""
    return _publish_impl(
        project_id, version_id, source_app_id,
        version_name, user_id,
        public_project_id=public_project_id,
        max_versions=max_versions,
        clone_source=True,
    )


def _publish_impl(
    project_id: int,
    version_id: int,
    source_app_id: int,
    version_name: str,
    user_id: int,
    public_project_id: int,
    max_versions: int,
    clone_source: bool,
) -> Tuple[dict, int]:
    """Shared publish logic for both user and admin flows.

    When *clone_source* is True (user flow), clones the source version
    first, then snapshots the clone.  The clone is marked ``published``.

    When *clone_source* is False (admin flow), snapshots the source
    version directly — no clone is created, the original stays ``draft``.
    """
    # Single source-project session for all read-only pre-checks
    with db.get_session(project_id) as source_session:
        # Reject pipeline-type agents (#4525)
        pipeline_err = check_not_pipeline(version_id, source_session)
        if pipeline_err is not None:
            return pipeline_err

        # 0. Pre-validate sub-agent tree (before any mutations)
        try:
            sub_agent_tree = collect_sub_agent_tree(
                project_id, version_id, session=source_session,
            )
        except SubAgentTreeError as exc:
            return {"error": exc.error_code, "msg": str(exc)}, 400
        except ValueError as exc:
            return {"error": "sub_agent_validation", "msg": str(exc)}, 400

        # 1. Reject if version_name already taken (clone only)
        if clone_source and version_name_exists(
            project_id, source_app_id, version_name,
            session=source_session,
        ):
            return {
                "error": "version_name_exists_in_source",
                "msg": f"Version name '{version_name}' already exists in this application",
            }, 400

    # 2. Cheap public-side checks before heavy clone/snapshot
    public_app_id = find_public_twin(public_project_id, project_id, source_app_id)
    if public_app_id is not None:
        if not check_version_name_unique(public_project_id, public_app_id, version_name):
            return {
                "error": "version_name_exists",
                "msg": f"Version name '{version_name}' already exists on this agent",
            }, 400

        allowed, current_count = check_publish_limit(
            public_project_id, public_app_id, max_versions,
        )
        if not allowed:
            return {
                "error": "limit_reached",
                "msg": f"Maximum {max_versions} published versions reached (current: {current_count})",
            }, 400

    # 3. Clone or use source directly
    if clone_source:
        clone_result = this.module.clone_version(
            project_id, source_app_id, version_id, version_name, user_id,
        )
        if isinstance(clone_result, dict) and 'error' in clone_result:
            return {"error": "clone_failed", "msg": clone_result['error']}, 500
        snapshot_version_id = clone_result['id']
    else:
        snapshot_version_id = version_id

    # 4. Create sanitised snapshot
    snapshot = create_publish_snapshot(project_id, snapshot_version_id, user_id)

    # 5. Publish into the public project
    if public_app_id is not None:
        result = publish_additional_version(
            public_project_id, public_app_id, snapshot, version_name, user_id,
        )
    else:
        result = publish_first_version(
            public_project_id, snapshot, version_name, user_id,
        )

    # 6. Mark clone as published (user flow only, cross-project)
    if clone_source and project_id != public_project_id:
        sync_source_version_status(project_id, snapshot_version_id, PublishStatus.published)

    # 7. Embed sub-agents
    sub_err = publish_sub_agents(
        source_project_id=project_id,
        public_project_id=public_project_id,
        source_version_id=snapshot_version_id,
        parent_pub_app_id=result['application_id'],
        parent_pub_ver_id=result['version_id'],
        user_id=user_id,
        pre_validated_tree=sub_agent_tree,
    )

    response = {
        "msg": "Successfully published",
        "public_agent_id": result['application_id'],
        "public_version_id": result['version_id'],
        "version_name": version_name,
        "source_version_id": snapshot_version_id,
    }

    if sub_err is not None:
        return {**sub_err, **response}, 207

    return response, 200


def admin_unpublish(
    project_id: int,
    version_id: int,
    reason: Optional[str],
    actor_id: int,
) -> Tuple[dict, int]:
    """Unpublish a version that lives in the public project (admin flow)."""
    result = delete_public_version(project_id, version_id)
    if result.get('not_published'):
        return {"error": "not_published", "msg": "Version is not currently published"}, 409

    # Sync source version back to draft and notify the original author
    source_project_id = result.get('source_project_id')
    source_version_id = result.get('source_version_id')
    source_application_id = result.get('source_application_id')
    source_author_id = result.get('source_author_id')

    if source_project_id and source_version_id and source_project_id != project_id:
        sync_source_version_status(source_project_id, source_version_id, PublishStatus.draft)

        if source_author_id:
            notify_author_unpublished(
                source_project_id, source_author_id, version_id, reason,
                source_version_id=source_version_id,
                source_application_id=source_application_id,
            )

    return {"msg": "Successfully unpublished", "status": "deleted"}, 200


def user_unpublish(
    project_id: int,
    version_id: int,
    user_id: int,
    public_project_id: int,
    reason: Optional[str],
) -> Tuple[dict, int]:
    """Find the public twin version, delete it, and revert source to draft."""
    # Resolve source application id
    with db.get_session(project_id) as session:
        version = session.query(ApplicationVersion).get(version_id)
        if not version:
            return {"error": f"Version {version_id} not found"}, 404
        source_app_id = version.application_id

    # Find public application twin
    public_app_id = find_public_twin(public_project_id, project_id, source_app_id)
    if public_app_id is None:
        return {"error": "not_published", "msg": "No published agent found for this application"}, 404

    # Find the public version mapped to this source version
    public_version_id = find_public_version_by_source(
        public_project_id, public_app_id, version_id,
    )
    if public_version_id is None:
        return {"error": "not_published", "msg": "This version is not published"}, 404

    # Delete the public version
    result = delete_public_version(public_project_id, public_version_id)
    if result.get('not_published'):
        return {"error": "not_published", "msg": "Version is not currently published"}, 409

    # Revert source version to draft
    sync_source_version_status(project_id, version_id, PublishStatus.draft)

    return {"msg": "Successfully unpublished", "status": "deleted"}, 200


def notify_author_unpublished(
    source_project_id: int,
    author_id: int,
    public_version_id: int,
    reason: Optional[str],
    *,
    source_version_id: Optional[int] = None,
    source_application_id: Optional[int] = None,
) -> None:
    """Fire a notification event when an admin unpublishes a user's agent."""
    try:
        meta = {
            'public_version_id': public_version_id,
            'source_version_id': source_version_id,
            'reason': reason or '',
            'status': PublishStatus.draft.value,
        }
        if source_application_id is not None:
            meta['source_application_id'] = source_application_id

        event_manager = rpc_tools.EventManagerMixin().event_manager
        event_manager.fire_event(
            'notifications_stream',
            {
                'event_type': NotificationEventTypes.agent_unpublished,
                'project_id': source_project_id,
                'user_id': author_id,
                'meta': meta,
            },
        )
    except Exception as e:
        log.warning("[UNPUBLISH] Failed to fire notification: %s", e)


# ===========================================================================
# Publish Validation — deterministic checks (#4049) + AI quality checks (#3774)
# ===========================================================================

# ---------------------------------------------------------------------------
# HMAC Token helpers
# ---------------------------------------------------------------------------

def compute_content_hash(snapshot: dict, sub_snapshots: list) -> str:
    """SHA-256 of agent content for change detection."""
    payload = {
        'instructions': snapshot.get('version', {}).get('instructions', ''),
        'variables': snapshot.get('version', {}).get('variables', []),
        'sub_agents': [
            {
                'tool_name': s.get('tool_name', ''),
                'instructions': s.get('instructions', ''),
            }
            for s in sub_snapshots
        ],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode()).hexdigest()


def generate_validation_token(
    version_id: int, content_hash: str, secret_key: str,
) -> str:
    """Create HMAC-signed validation token."""
    payload = f"{version_id}:{content_hash}:{int(time.time())}"
    sig = hmac.new(
        secret_key.encode(), payload.encode(), hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(sig).decode() + ":" + payload


def verify_validation_token(
    token: str,
    version_id: int,
    content_hash: str,
    secret_key: str,
    ttl: int,
) -> tuple:
    """Verify token. Returns (is_valid, error_message)."""
    try:
        sig_b64, tok_version, tok_hash, tok_ts = token.split(":", 3)
    except (ValueError, AttributeError):
        return False, "Invalid validation token."

    # Recompute expected signature
    payload = f"{tok_version}:{tok_hash}:{tok_ts}"
    expected_sig = hmac.new(
        secret_key.encode(), payload.encode(), hashlib.sha256,
    ).digest()
    try:
        actual_sig = base64.urlsafe_b64decode(sig_b64)
    except Exception:
        return False, "Invalid validation token."

    if not hmac.compare_digest(expected_sig, actual_sig):
        return False, "Invalid validation token."

    if str(version_id) != tok_version:
        return False, "Invalid validation token."

    try:
        ts = int(tok_ts)
    except ValueError:
        return False, "Invalid validation token."

    if time.time() - ts > ttl:
        return False, "Validation token expired. Please re-validate before publishing."

    if content_hash != tok_hash:
        return False, "Agent was modified since validation. Please re-validate."

    return True, ""


# ---------------------------------------------------------------------------
# Agent expansion for AI input
# ---------------------------------------------------------------------------

def build_validation_input(
    project_id: int, version_id: int, user_id: int, version_name: str,
) -> tuple:
    """Build cleaned agent JSON for validation.

    Returns (json_str, parent_snapshot, sub_agent_snapshots_list).
    """
    parent_snapshot = create_publish_snapshot(project_id, version_id, user_id)

    # Enrich snapshot with icon_meta from raw version meta
    # (create_publish_snapshot strips it for the publish flow,
    #  but validation needs it to check icon presence)
    with db.get_session(project_id) as session:
        version = session.query(ApplicationVersion).get(version_id)
        if version:
            raw_meta = version.meta or {}
            parent_snapshot['version'].setdefault('meta', {})
            parent_snapshot['version']['meta']['icon_meta'] = (
                raw_meta.get('icon_meta')
            )

    try:
        sub_agent_tree = collect_sub_agent_tree(project_id, version_id)
    except SubAgentTreeError:
        raise  # Structural problems are hard blockers — cannot validate
    except Exception:
        log.warning("Failed to collect sub-agent tree for validation, continuing without")
        sub_agent_tree = []

    sub_agent_snapshots = []
    for node in _flatten_tree(sub_agent_tree):
        try:
            sub_snap = create_publish_snapshot(project_id, node.version_id, user_id)
        except Exception:
            log.warning("Failed to snapshot sub-agent %s, skipping", node.tool_name)
            continue
        sub_agent_snapshots.append({
            'tool_name': node.tool_name,
            'depth': node.depth,
            'name': sub_snap['application']['name'],
            'description': sub_snap['application']['description'],
            'instructions': sub_snap['version'].get('instructions', ''),
            'variables': sub_snap['version'].get('variables', []),
            'tags': sub_snap['version'].get('tags', []),
            'welcome_message': sub_snap['version'].get('welcome_message', ''),
            'conversation_starters': sub_snap['version'].get('conversation_starters', []),
        })

    validation_input = {
        'agent': {
            'name': parent_snapshot['application']['name'],
            'description': parent_snapshot['application']['description'],
            'icon_meta': (
                parent_snapshot['version']
                .get('meta', {}).get('icon_meta')
            ),
            'instructions': parent_snapshot['version'].get('instructions', ''),
            'variables': parent_snapshot['version'].get('variables', []),
            'tags': parent_snapshot['version'].get('tags', []),
            'welcome_message': parent_snapshot['version'].get('welcome_message', ''),
            'conversation_starters': parent_snapshot['version'].get('conversation_starters', []),
        },
        'sub_agents': sub_agent_snapshots,
        'version_name': version_name,
    }
    return json.dumps(validation_input, indent=2), parent_snapshot, sub_agent_snapshots


# ---------------------------------------------------------------------------
# Deterministic checks (#4049) — Chain-of-Responsibility pattern
# ---------------------------------------------------------------------------

class ValidationResult:
    """Accumulates validation findings across all checkers."""

    __slots__ = ('critical', 'warnings', 'recommendations')

    def __init__(self):
        self.critical = []
        self.warnings = []
        self.recommendations = []

    @staticmethod
    def _display_ctx(context):
        """Extract the human-readable label from *context*.

        *context* may be a plain string (sub-agent label), a dict
        carrying routing data + optional 'label' key, or None.
        """
        if context is None:
            return None
        if isinstance(context, str):
            return context
        return context.get('label')

    def issue(self, severity, field, issue, fix, context=None):
        """Add a finding. *severity* is 'critical' or 'warnings'."""
        getattr(self, severity).append({
            "field": field, "issue": issue, "fix": fix,
            "source": "deterministic",
            "context": self._display_ctx(context),
        })

    def recommend(self, field, suggestion, context=None):
        self.recommendations.append({
            "field": field, "suggestion": suggestion,
            "source": "deterministic",
            "context": self._display_ctx(context),
        })

    def to_dict(self):
        return {
            "critical_issues": self.critical,
            "warnings": self.warnings,
            "recommendations": self.recommendations,
        }


class BaseChecker:
    """Single-responsibility deterministic checker."""

    def check(self, data, result, *, context=None):
        raise NotImplementedError


class NameChecker(BaseChecker):
    """Agent/sub-agent name quality."""

    def check(self, data, result, *, context=None):
        name = (data.get('name') or '').strip()
        if not name:
            result.issue(
                'critical', 'name',
                'Agent name is missing',
                'Provide a descriptive name', context,
            )
            return
        if len(name) < 3 or len(name) > 32:
            result.issue(
                'warnings', 'name',
                'Name length should be 3-32 characters',
                f'Current length is {len(name)}', context,
            )
        if name.lower() in GENERIC_NAME_BLOCKLIST:
            result.issue(
                'warnings', 'name',
                'Name is too generic for a public marketplace',
                'Choose a more descriptive, unique name', context,
            )
        if PLACEHOLDER_RE.search(name):
            result.issue(
                'critical', 'name',
                'Name contains placeholder text',
                'Replace placeholder with actual name', context,
            )
        # Sub-agent name uniqueness: check if this name appears more than once
        # (list includes parent name + all sub-agent names; mutable ref shared
        #  across iterations — once reported, duplicates are removed so the
        #  warning is emitted only for the first occurrence)
        all_sub_names = data.get('_all_sub_agent_names') or []
        if all_sub_names:
            name_lower = name.strip().lower()
            count = all_sub_names.count(name_lower)
            if count > 1:
                result.issue(
                    'warnings', 'name',
                    f"Sub-agent name '{name}' is not unique "
                    f"({count} occurrences found)",
                    'Ensure all sub-agent names are different',
                    context,
                )
                # Remove all occurrences so the next sub-agent with the
                # same name does not produce a duplicate warning
                while name_lower in all_sub_names:
                    all_sub_names.remove(name_lower)


class DescriptionChecker(BaseChecker):
    """Agent/sub-agent description quality.

    *min_length* and *short_severity* allow different thresholds
    for parent agents vs sub-agents.
    """

    def __init__(self, min_length=50, short_severity='critical',
                 check_verbs=True):
        self.min_length = min_length
        self.short_severity = short_severity
        self.check_verbs = check_verbs

    def check(self, data, result, *, context=None):
        desc = (data.get('description') or '').strip()
        if not desc:
            result.issue(
                'critical', 'description',
                'Description is missing',
                f'Add a description of at least {self.min_length} characters',
                context,
            )
            return
        if len(desc) < self.min_length:
            result.issue(
                self.short_severity, 'description',
                f'Description is too short (min {self.min_length} chars)',
                f'Expand description (currently {len(desc)} chars)',
                context,
            )
        if self.check_verbs and not ACTION_VERB_RE.search(desc):
            result.issue(
                'warnings', 'description',
                'Description lacks action verbs describing purpose',
                "Add verbs like 'helps', 'analyzes', 'generates'",
                context,
            )
        if PLACEHOLDER_RE.search(desc):
            result.issue(
                'critical', 'description',
                'Description contains placeholder text',
                'Replace placeholder text with actual description',
                context,
            )


class IconChecker(BaseChecker):
    """Checks for a custom icon via version meta.icon_meta."""

    def check(self, data, result, *, context=None):
        icon_meta = data.get('icon_meta')
        if not icon_meta or not isinstance(icon_meta, dict):
            result.issue(
                'warnings', 'icon',
                'No custom icon set',
                'Add a custom icon to improve marketplace visibility',
                context,
            )


class TagsChecker(BaseChecker):
    """Tag presence and quality."""

    def check(self, data, result, *, context=None):
        tags = data.get('tags') or []
        if not tags:
            result.issue(
                'critical', 'tags',
                'No tags defined',
                'Add at least one relevant tag', context,
            )
            return
        tag_set = {
            (t.lower() if isinstance(t, str) else str(t).lower())
            for t in tags
        }
        if tag_set and tag_set <= GENERIC_TAG_SET:
            result.issue(
                'warnings', 'tags',
                'All tags are generic',
                'Add domain-specific tags', context,
            )
        if len(tags) > 2:
            result.recommend(
                'tags',
                'Recommend 1-2 tags for optimal discoverability',
                context,
            )


class InstructionsChecker(BaseChecker):
    """Agent/sub-agent instructions quality."""

    def __init__(self, min_length=100):
        self.min_length = min_length

    def check(self, data, result, *, context=None):
        instructions = (data.get('instructions') or '').strip()
        if not instructions:
            result.issue(
                'critical', 'instructions',
                'Instructions are missing',
                f'Add detailed instructions '
                f'(min {self.min_length} characters)', context,
            )
            return
        if len(instructions) < self.min_length:
            result.issue(
                'critical', 'instructions',
                f'Instructions are too short '
                f'(min {self.min_length} chars)',
                f'Expand instructions '
                f'(currently {len(instructions)} chars)', context,
            )
        if PLACEHOLDER_RE.search(instructions):
            result.issue(
                'critical', 'instructions',
                'Instructions contain placeholder text',
                'Replace placeholder text with actual instructions',
                context,
            )


class VariablesChecker(BaseChecker):
    """Checks variable values for leaked secrets."""

    def check(self, data, result, *, context=None):
        for var in (data.get('variables') or []):
            var_name = (
                var.get('name', '') if isinstance(var, dict) else ''
            )
            var_value = (
                var.get('value', '') if isinstance(var, dict) else ''
            )
            if isinstance(var_value, str) and SECRET_RE.search(var_value):
                result.issue(
                    'critical', 'variables',
                    f"Variable '{var_name}' may contain a secret "
                    f"or API key",
                    'Remove secrets from variable values '
                    '— use environment variables or vault',
                    context,
                )


class VersionNameChecker(BaseChecker):
    """Version name format, uniqueness, and quality.

    Reads ``public_project_id`` and ``public_app_id`` from the
    *context* dict passed via ``ValidationChain.run()``.
    """

    def check(self, data, result, *, context=None):
        ctx = context if isinstance(context, dict) else {}
        public_project_id = ctx.get('public_project_id')
        public_app_id = ctx.get('public_app_id')

        vn = (data.get('version_name') or '').strip()
        if not vn:
            result.issue(
                'critical', 'version_name',
                'Version name is required',
                'Provide a version name',
            )
            return
        if not re.match(r'^[a-zA-Z0-9._-]{1,50}$', vn):
            result.issue(
                'critical', 'version_name',
                'Invalid version name format',
                'Use only letters, digits, dots, hyphens, '
                'underscores (max 50 chars)',
            )
        elif public_app_id is not None and public_project_id is not None:
            if not check_version_name_unique(
                public_project_id, public_app_id, vn,
            ):
                result.issue(
                    'critical', 'version_name',
                    'Version name already exists',
                    'Choose a different version name',
                )
        if vn.lower() in GENERIC_VERSION_BLOCKLIST:
            result.issue(
                'warnings', 'version_name',
                'Version name is not descriptive',
                "Use a meaningful name like 'v1.0-initial-release'",
            )
        if not SEMVER_HINT_RE.match(vn):
            result.recommend(
                'version_name',
                'Consider semantic versioning (e.g. v1.0, v2.1)',
            )


class ValidationChain:
    """Runs a sequence of checkers against a data dict."""

    def __init__(self, checkers):
        self._checkers = list(checkers)

    def run(self, data, result, *, context=None):
        for checker in self._checkers:
            checker.check(data, result, context=context)


# Pre-built chains — parent has more checkers than sub-agents
_PARENT_CHAIN = ValidationChain([
    NameChecker(),
    DescriptionChecker(min_length=50),
    IconChecker(),
    TagsChecker(),
    InstructionsChecker(),
    VariablesChecker(),
    VersionNameChecker(),
])

_SUB_AGENT_CHAIN = ValidationChain([
    NameChecker(),
    DescriptionChecker(
        min_length=30, short_severity='warnings', check_verbs=False,
    ),
    InstructionsChecker(),
])


def run_deterministic_checks(
    snapshot: dict,
    version_name: str,
    public_project_id: int,
    public_app_id,
    sub_agent_snapshots: list,
) -> dict:
    """Fast rule-based checks using chain-of-responsibility."""
    result = ValidationResult()

    app = snapshot.get('application', {})
    ver = snapshot.get('version', {})

    # --- Parent agent ---
    meta = ver.get('meta') or {}
    parent_data = {
        'name': app.get('name'),
        'description': app.get('description'),
        'icon_meta': meta.get('icon_meta'),
        'tags': ver.get('tags'),
        'instructions': ver.get('instructions'),
        'variables': ver.get('variables'),
        'version_name': version_name,
    }
    parent_ctx = {
        'public_project_id': public_project_id,
        'public_app_id': public_app_id,
    }
    _PARENT_CHAIN.run(parent_data, result, context=parent_ctx)

    # --- Sub-agents (same chain, iterated) ---
    parent_name = (app.get('name') or '').strip().lower()
    all_sub_names = [parent_name] + [
        (sub.get('name') or '').strip().lower()
        for sub in sub_agent_snapshots
    ]
    for sub in sub_agent_snapshots:
        tool = sub.get('tool_name', 'unknown')
        name = sub.get('name', '')
        ctx = f"sub-agent: {name} ({tool})" if name else f"sub-agent: {tool}"
        sub_data = {
            'name': sub.get('name'),
            'description': sub.get('description'),
            'instructions': sub.get('instructions'),
            '_all_sub_agent_names': all_sub_names,
        }
        _SUB_AGENT_CHAIN.run(sub_data, result, context=ctx)

    return result.to_dict()


# ---------------------------------------------------------------------------
# AI validation error
# ---------------------------------------------------------------------------

class AIValidationError(Exception):
    """Raised when AI validation pipeline fails irrecoverably."""


# ---------------------------------------------------------------------------
# LLM settings resolution for validation
# ---------------------------------------------------------------------------

def get_validation_llm_settings(
    project_id: int, version_id: int,
) -> dict | None:
    """Resolve full LLM settings for the validation pipeline.

    Version llm_settings -> validate & resolve -> project default.
    """
    from .application_utils import validate_and_resolve_llm_settings

    llm_settings = None
    with db.get_session(project_id) as session:
        version = session.query(ApplicationVersion).get(version_id)
        if version and isinstance(version.llm_settings, dict):
            llm_settings = version.llm_settings

    if llm_settings and llm_settings.get('model_name'):
        resolved = validate_and_resolve_llm_settings(
            project_id, llm_settings, version_id=version_id,
        )
        if resolved and resolved.get('model_name'):
            return resolved

    # Fallback: project default model
    default = rpc_tools.RpcMixin().rpc.timeout(3) \
        .configurations_get_default_model(
            project_id=project_id,
            section='llm',
            include_shared=True,
        )
    if default and default.get('model_name'):
        return default

    return None


# ---------------------------------------------------------------------------
# AI validation (#3774)
# ---------------------------------------------------------------------------

def run_ai_validation(
    project_id: int,
    version_id: int,
    validation_input_json: str,
) -> dict:
    """Simple-agent LLM quality check.

    Raises AIValidationError on any failure — never returns None.
    """
    config = this.descriptor.config
    timeout = int(
        config.get('publish_validation_timeout',
                    DEFAULT_VALIDATION_TIMEOUT),
    )
    prompt = (
        config.get('publish_validation_prompt')
        or DEFAULT_VALIDATION_PROMPT
    )

    llm_settings = get_validation_llm_settings(
        project_id, version_id,
    )
    if not llm_settings or not llm_settings.get('model_name'):
        raise AIValidationError(
            "No LLM model available for AI validation. "
            "Configure a default model for the project.",
        )

    resolved_llm = dict(llm_settings)
    resolved_llm.pop('reasoning_effort', None)
    resolved_llm.pop('max_tokens', None)  # Use default instead of version's potentially small limit
    resolved_llm['temperature'] = 0.1  # deterministic output for reliable JSON

    version_details = {
        'agent_type': 'openai',
        'instructions': prompt,
        'llm_settings': resolved_llm,
        'tools': [],
        'meta': {'internal_tools': [], 'step_limit': 5},
    }

    uid = uuid4().hex[:12]
    data = {
        'project_id': project_id,
        'user_input': validation_input_json,
        'llm_settings': resolved_llm,
        'version_details': version_details,
        'chat_history': [],
        'tools': [],
        'internal_tools': [],
        'stream_id': f'publish_validate_{version_id}_{uid}',
        'message_id': f'publish_validate_{version_id}_{uid}',
    }

    try:
        result = this.module.predict_sio(
            sid=None,
            data=data,
            await_task_timeout=timeout,
            is_system_user=True,
        )
    except Exception as exc:
        raise AIValidationError(
            f"AI validation failed: {exc}",
        ) from exc

    # Handle task timeout — predict_sio returns {"task_id": ...}
    # when join_task doesn't complete within the timeout
    if isinstance(result, dict) and 'task_id' in result and 'result' not in result:
        task_id = result['task_id']
        try:
            this.module.stop_task(task_id)
        except Exception:
            pass
        raise AIValidationError(
            f"AI validation timed out after {timeout}s. "
            f"Try again or increase publish_validation_timeout.",
        )

    try:
        parsed = PublishAIResult.model_validate(result)
    except ValidationError as ex:
        log.error(f"AI validation result parsing failed: {ex}\nRaw result: {result}")
        _check_predict_error(result)
        raise AIValidationError(
            "AI validation returned unparseable result.",
        )
    return parsed.model_dump()


def _check_predict_error(result: dict | None) -> None:
    """Raise AIValidationError if predict_sio returned an error."""
    if not isinstance(result, dict):
        return
    for container in (result, result.get('result')):
        if isinstance(container, dict) and container.get('error'):
            error_text = str(container['error'])
            if len(error_text) > 500:
                error_text = error_text[:500] + '…'
            raise AIValidationError(
                f"AI validation error: {error_text}",
            )


# ---------------------------------------------------------------------------
# Result merging
# ---------------------------------------------------------------------------

def merge_validation_results(
    deterministic: dict, ai_result: dict,
) -> dict:
    """Merge deterministic + AI results. Compute status."""
    critical = list(deterministic.get('critical_issues', []))
    warnings = list(deterministic.get('warnings', []))
    recs = list(deterministic.get('recommendations', []))

    critical.extend(ai_result.get('critical_issues', []))
    warnings.extend(ai_result.get('warnings', []))
    recs.extend(ai_result.get('recommendations', []))
    summary = ai_result.get('summary', '')

    # Derive status
    if critical:
        status = 'FAIL'
    elif warnings:
        status = 'WARN'
    else:
        status = 'PASS'

    if not summary:
        if status == 'FAIL':
            summary = (
                f"Agent has {len(critical)} critical issue(s) "
                f"that must be fixed before publishing."
            )
        elif status == 'WARN':
            summary = (
                f"Agent meets requirements but has "
                f"{len(warnings)} warning(s) for improvement."
            )
        else:
            summary = "Agent meets all publishing requirements."

    return {
        "status": status,
        "critical_issues": critical,
        "warnings": warnings,
        "recommendations": recs,
        "summary": summary,
        "counts": {
            "critical": len(critical),
            "warnings": len(warnings),
            "suggestions": len(recs),
        },
        "ai_validation_available": True,
    }


# ---------------------------------------------------------------------------
# Validation orchestrators
# ---------------------------------------------------------------------------

def validate_for_publish(
    project_id: int,
    version_id: int,
    application_id: int,
    version_name: str,
    user_id: int,
) -> dict:
    """Full validation: deterministic + AI.

    Returns unified result + token (if not FAIL).
    Raises AIValidationError if the AI pipeline fails.
    """
    public_project_id = get_public_project_id()

    # Early checks and snapshot building — short-circuit to FAIL on error
    try:
        if version_name_exists(project_id, application_id, version_name):
            raise PreValidationError(
                'version_name_exists_in_source',
                f"Version name '{version_name}' already exists in this application",
            )

        validation_json, parent_snapshot, sub_snapshots = (
            build_validation_input(
                project_id, version_id, user_id, version_name,
            )
        )
    except PublishPreValidationError as exc:
        return exc.to_validation_result()

    # Resolve public app id for version-name uniqueness check
    public_app_id = find_public_twin(
        public_project_id, project_id,
        parent_snapshot['source']['application_id'],
    )

    # Deterministic
    det_result = run_deterministic_checks(
        parent_snapshot, version_name, public_project_id,
        public_app_id, sub_snapshots,
    )

    # AI (raises AIValidationError on failure)
    ai_result = run_ai_validation(
        project_id, version_id, validation_json,
    )

    # Merge
    merged = merge_validation_results(det_result, ai_result)

    # Token (only if not FAIL)
    if merged['status'] != 'FAIL':
        content_hash = compute_content_hash(
            parent_snapshot, sub_snapshots,
        )
        secret = this.module._publish_validation_secret
        merged['validation_token'] = generate_validation_token(
            version_id, content_hash, secret,
        )
    else:
        merged['validation_token'] = None

    return merged


def verify_token_for_publish(
    project_id: int,
    version_id: int,
    user_id: int,
    validation_token: str,
) -> tuple:
    """Verify a validation token.

    Returns (is_valid, error_message).
    Rebuilds content hash from current agent state for comparison.
    """
    try:
        parent_snapshot = create_publish_snapshot(
            project_id, version_id, user_id,
        )
        sub_agent_tree = collect_sub_agent_tree(
            project_id, version_id,
        )
    except Exception:
        return False, "Failed to verify agent state."

    sub_snapshots = []
    for node in _flatten_tree(sub_agent_tree):
        try:
            sub_snap = create_publish_snapshot(
                project_id, node.version_id, user_id,
            )
        except Exception:
            continue
        sub_snapshots.append({
            'tool_name': node.tool_name,
            'instructions': (
                sub_snap['version'].get('instructions', '')
            ),
        })

    content_hash = compute_content_hash(
        parent_snapshot, sub_snapshots,
    )
    secret = this.module._publish_validation_secret
    ttl = int(this.descriptor.config.get(
        'publish_validation_token_ttl', DEFAULT_VALIDATION_TOKEN_TTL,
    ))
    return verify_validation_token(
        validation_token, version_id, content_hash, secret, ttl,
    )
