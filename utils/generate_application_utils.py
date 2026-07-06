import re
from typing import List, Tuple

from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.elitea_tools import EliteATool
from ..models.skill import Skill

_MAX_TOOLKITS = 20
_MAX_APPLICATIONS = 5
_MAX_SKILLS = 10


def _score_item(query_tokens: set, name: str, description: str, extra: str = "") -> int:
    text = " ".join(filter(None, [name, description, extra])).lower()
    return sum(1 for t in query_tokens if t in text)


def _tokenize(text: str) -> set:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def fetch_project_resources(
    project_id: int,
    user_description: str,
) -> Tuple[List[dict], List[dict], List[dict]]:
    """Fetch toolkits, agents, and skills from the project.

    Returns:
        Tuple of (toolkits, agents, skills) lists.
    """
    query_tokens = _tokenize(user_description)

    with db.with_project_schema_session(project_id) as session:
        toolkit_rows = session.query(
            EliteATool.id, EliteATool.type, EliteATool.name, EliteATool.description
        ).all()
        toolkits_scored = sorted(
            (
                (
                    _score_item(
                        query_tokens, r.name or r.type, r.description or "", r.type
                    ),
                    {
                        "id": r.id,
                        "type": r.type,
                        "name": r.name or r.type,
                        "description": r.description,
                    },
                )
                for r in toolkit_rows
            ),
            key=lambda x: x[0],
            reverse=True,
        )
        toolkits = [item for _, item in toolkits_scored[:_MAX_TOOLKITS]]

        agent_rows = (
            session.query(
                Application.id,
                Application.name,
                Application.description,
                ApplicationVersion.id.label("version_id"),
                ApplicationVersion.agent_type,
            )
            .outerjoin(
                ApplicationVersion,
                (ApplicationVersion.application_id == Application.id)
                & (ApplicationVersion.name == "base"),
            )
            .all()
        )
        agents_scored = sorted(
            (
                (
                    _score_item(query_tokens, r.name or "", r.description or ""),
                    {
                        "application_id": r.id,
                        "id": r.version_id,
                        "name": r.name,
                        "description": r.description,
                        "type": "pipeline" if r.agent_type == "pipeline" else "agent",
                    },
                )
                for r in agent_rows
            ),
            key=lambda x: x[0],
            reverse=True,
        )
        # NOTE(#5680): these are ranked SUGGESTIONS shown to the LLM, not auto-bindings — a
        # suggested container agent only becomes a real sub-agent if the user accepts it, and
        # that binding is now rejected at bind time (application_toolkit_change_relation) and
        # at chat resolution. If this endpoint ever gains an owner-application context, filter
        # it out here so an agent is never suggested as its own sub-agent.
        agents = [item for _, item in agents_scored[:_MAX_APPLICATIONS]]

        # Fetch skills
        skill_rows = session.query(Skill.id, Skill.name, Skill.description).all()
        skills_scored = sorted(
            (
                (
                    _score_item(query_tokens, r.name or "", r.description or ""),
                    {"id": r.id, "name": r.name, "description": r.description},
                )
                for r in skill_rows
            ),
            key=lambda x: x[0],
            reverse=True,
        )
        skills = [item for _, item in skills_scored[:_MAX_SKILLS]]

    return toolkits, agents, skills


def build_system_prompt(
    template: str, toolkits: list, agents: list, skills: list | None = None
) -> str:
    """Build the system prompt by formatting the template with available resources.

    Args:
        template: The prompt template with {toolkits}, {agents}, and optionally {skills} placeholders.
        toolkits: List of toolkit dictionaries.
        agents: List of agent dictionaries.
        skills: List of skill dictionaries (optional for backward compatibility).
    """
    if skills is None:
        skills = []

    toolkit_lines = (
        [
            f'- id={t["id"]}  type="{t["type"]}"  name="{t["name"]}"  {(t["description"] or "")[:120]}'
            for t in toolkits
        ]
        if toolkits
        else ["(none configured)"]
    )
    agent_lines = (
        [
            f'- application_id={a["application_id"]}  type="{a["type"]}"  name="{a["name"]}"  {(a["description"] or "")[:100]}'
            for a in agents
        ]
        if agents
        else ["(none)"]
    )
    skill_lines = (
        [
            f'- id={s["id"]}  name="{s["name"]}"  {(s["description"] or "")[:120]}'
            for s in skills
        ]
        if skills
        else ["(none)"]
    )
    return template.format(
        toolkits="\n".join(toolkit_lines),
        agents="\n".join(agent_lines),
        skills="\n".join(skill_lines),
    )
