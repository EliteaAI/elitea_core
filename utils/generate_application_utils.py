import re
from typing import List, Tuple

from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.elitea_tools import EliteATool


_MAX_TOOLKITS = 20
_MAX_APPLICATIONS = 5


def _score_item(query_tokens: set, name: str, description: str, extra: str = "") -> int:
    text = " ".join(filter(None, [name, description, extra])).lower()
    return sum(1 for t in query_tokens if t in text)


def _tokenize(text: str) -> set:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def fetch_project_resources(
    project_id: int,
    user_description: str,
) -> Tuple[List[dict], List[dict]]:
    query_tokens = _tokenize(user_description)

    with db.with_project_schema_session(project_id) as session:
        toolkit_rows = (
            session.query(EliteATool.id, EliteATool.type, EliteATool.name, EliteATool.description)
            .all()
        )
        toolkits_scored = sorted(
            (
                (
                    _score_item(query_tokens, r.name or r.type, r.description or "", r.type),
                    {"id": r.id, "type": r.type, "name": r.name or r.type, "description": r.description},
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
                        "id": r.id,
                        "name": r.name,
                        "description": r.description,
                        "type": r.agent_type or "agent",
                    },
                )
                for r in agent_rows
            ),
            key=lambda x: x[0],
            reverse=True,
        )
        agents = [item for _, item in agents_scored[:_MAX_APPLICATIONS]]

    return toolkits, agents


def build_system_prompt(template: str, toolkits: list, agents: list) -> str:
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
            f'- application_id={a["id"]}  type="{a["type"]}"  name="{a["name"]}"  {(a["description"] or "")[:100]}'
            for a in agents
        ]
        if agents
        else ["(none)"]
    )
    return template.format(
        toolkits="\n".join(toolkit_lines),
        agents="\n".join(agent_lines),
    )
