import json
import re
from typing import List, Tuple, Optional

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


def _format_toolkit_lines(toolkits: list) -> list:
    """Format toolkit items for prompt."""
    if not toolkits:
        return ["(none configured)"]
    return [
        f'- id={t["id"]}  type="{t["type"]}"  name="{t["name"]}"  {(t["description"] or "")[:120]}'
        for t in toolkits
    ]


def _format_agent_lines(agents: list) -> list:
    """Format agent items for prompt."""
    if not agents:
        return ["(none)"]
    return [
        f'- application_id={a["application_id"]}  type="{a["type"]}"  name="{a["name"]}"  {(a["description"] or "")[:100]}'
        for a in agents
    ]


def _format_skill_lines(skills: list) -> list:
    """Format skill items for prompt."""
    if not skills:
        return ["(none)"]
    return [
        f'- id={s["id"]}  name="{s["name"]}"  {(s["description"] or "")[:120]}'
        for s in skills
    ]


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

    return template.format(
        toolkits="\n".join(_format_toolkit_lines(toolkits)),
        agents="\n".join(_format_agent_lines(agents)),
        skills="\n".join(_format_skill_lines(skills)),
    )


def fetch_application_for_edit(
    project_id: int,
    application_id: int,
    version_id: int
) -> Optional[dict]:
    """Fetch and serialize application version for LLM edit context.

    Returns dict with:
    - name, description, instructions, welcome_message, conversation_starters
    - llm_settings
    - attached_toolkits: [{id, type, name, description}, ...] - non-MCP, non-application tools
    - attached_mcp: [{id, type, name, description}, ...] - MCP servers
    - attached_agents: [{application_id, name, description, type}, ...] - subagents
    - attached_pipelines: [{application_id, name, description, type}, ...] - pipelines
    - attached_skills: [{id, name, description}, ...]
    - variables: [{name, value}, ...]

    Returns None if application/version not found.
    """
    with db.with_project_schema_session(project_id) as session:
        version = session.query(ApplicationVersion).filter(
            ApplicationVersion.id == version_id,
            ApplicationVersion.application_id == application_id
        ).first()

        if not version:
            return None

        application = version.application
        version_dict = version.to_dict()

        tools_list = version_dict.get("tools", [])
        skills_list = version_dict.get("skills", [])
        variables_list = version_dict.get("variables", [])

        # Separate tools by type for clearer LLM context
        attached_toolkits = []
        attached_mcp = []
        attached_agents = []
        attached_pipelines = []

        for t in tools_list:
            tool_type = t.get("type", "").lower()
            tool_info = {
                "id": t["id"],
                "type": t["type"],
                "name": t["name"],
                "description": t.get("description"),
            }
            if tool_type == "mcp":
                attached_mcp.append(tool_info)
            elif tool_type == "application":
                # Extract application_id from settings for subagents/pipelines
                settings = t.get("settings", {})
                app_id = settings.get("application_id")
                app_type = settings.get("agent_type", "agent")
                linked_app = session.get(Application, app_id) if app_id else None
                app_info = {
                    "application_id": app_id,
                    "name": linked_app.name if linked_app else t["name"],
                    "description": linked_app.description if linked_app else t.get("description"),
                    "type": app_type,
                }
                if app_type == "pipeline":
                    attached_pipelines.append(app_info)
                else:
                    attached_agents.append(app_info)
            else:
                attached_toolkits.append(tool_info)

        return {
            "name": application.name,
            "description": application.description or "",
            "instructions": version.instructions or "",
            "welcome_message": version.welcome_message or "",
            "conversation_starters": version.conversation_starters or [],
            "llm_settings": version.llm_settings or {},
            "attached_toolkits": attached_toolkits,
            "attached_mcp": attached_mcp,
            "attached_agents": attached_agents,
            "attached_pipelines": attached_pipelines,
            "attached_skills": [
                {
                    "id": s["skill_id"],
                    "name": s["name"],
                    "description": s.get("description")
                }
                for s in skills_list
            ],
            "variables": [
                {"name": v["name"], "value": v.get("value")}
                for v in variables_list
            ],
        }


def build_edit_system_prompt(
    template: str,
    current_config: dict,
    toolkits: list,
    agents: list,
    skills: list
) -> str:
    """Build the edit system prompt with current config and available resources."""
    return template.format(
        current_config=json.dumps(current_config, indent=2),
        toolkits="\n".join(_format_toolkit_lines(toolkits)),
        agents="\n".join(_format_agent_lines(agents)),
        skills="\n".join(_format_skill_lines(skills)),
    )
