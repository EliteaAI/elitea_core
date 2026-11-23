"""Which agent versions external MCP clients reach, and under which tool name (#1597; governed since #7341, C-1009).

A version is exposed when it carries the ``mcp`` tag; under enforce it also needs an active identity binding, so an
external client only sees what its owner had approved. The agent's default version keeps the agent's name as its tool
name and every other exposed version is ``<agent>_<version>``. The Elitea MCP server lists and calls by this rule and
the local MCP client reads it through the ``mcp_exposed_versions`` API, so both expose the same agent versions under
the same names.
"""
import re
from dataclasses import dataclass

from pylon.core.tools import log
from sqlalchemy import select

from ..models.agent_identity import AgentIdentityBinding

MCP_TAG = "mcp"


@dataclass(frozen=True)
class ExposedVersion:
    application_id: int
    application_name: str
    version_id: int
    version_name: str
    tool_name: str
    description: str
    rule: str


def tool_identifier(name: str) -> str:
    return re.sub(r'[^A-Za-z0-9_\-]', "_", name)


def is_named_after_agent(exposed: ExposedVersion) -> bool:
    return exposed.tool_name == tool_identifier(exposed.application_name)


def exposed_versions(applications, bound_by_application, governed, scoped_version_id=None) -> list:
    """The exposed versions of ``applications`` (each with ``versions`` carrying their ``tags``), by agent with the
    one named after the agent first. Tool names are claimed by the agents' own names first, oldest agent first, and
    then by the ``<agent>_<version>`` names, so tagging a version never takes a name another agent already serves. A
    session scoped to one version reaches that version alone, under its agent's name. Pure."""
    candidates = []
    for application in applications:
        default = application.get_default_version()
        default_id = default.id if default is not None else None
        bound = bound_by_application.get(application.id, set())
        for version in application.versions:
            if not any(tag.name == MCP_TAG for tag in version.tags) or (governed and version.id not in bound):
                continue
            if scoped_version_id not in (None, version.id):
                continue
            is_default = version.id == default_id
            named_after_agent = is_default or scoped_version_id is not None
            description = application.description or ""
            if not is_default:
                description = f"Version '{version.name}' of agent '{application.name}'. {description}".strip()
            candidates.append(ExposedVersion(
                application.id, application.name, version.id, version.name,
                tool_identifier(application.name if named_after_agent else f"{application.name}_{version.name}"),
                description, "bound" if governed else "tagged"))
    exposed, claimed = [], set()
    for candidate in sorted(candidates, key=lambda candidate: (
            not is_named_after_agent(candidate), candidate.application_id, candidate.version_id)):
        if candidate.tool_name in claimed:
            log.warning("Skipping agent version with colliding MCP tool name: %s (version id=%s) -> %s",
                        candidate.application_name, candidate.version_id, candidate.tool_name)
            continue
        claimed.add(candidate.tool_name)
        exposed.append(candidate)
    return sorted(exposed, key=lambda served: (
        served.application_id, not is_named_after_agent(served), served.version_id))


def bound_version_ids(session, project_id, application_ids):
    """``{application id: {version ids with an active binding}}``; one statement."""
    found = {}
    if not application_ids:
        return found
    rows = session.execute(select(AgentIdentityBinding.application_id, AgentIdentityBinding.version_id).where(
        AgentIdentityBinding.project_id == project_id, AgentIdentityBinding.application_id.in_(list(application_ids)),
        AgentIdentityBinding.state == "active"))
    for application_id, version_id in rows:
        found.setdefault(application_id, set()).add(version_id)
    return found


def read_exposed_versions(session, project_id, applications, governed, scoped_version_id=None) -> list:
    """``exposed_versions`` of the project's ``mcp``-tagged ``applications``, with the bindings read when governed."""
    bound = bound_version_ids(session, project_id, [application.id for application in applications]) if governed else {}
    return exposed_versions(applications, bound, governed, scoped_version_id)
