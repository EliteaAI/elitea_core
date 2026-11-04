import json
import re
from typing import List, Tuple, Optional

from pylon.core.tools import log
from tools import db

from ..models.all import Application, ApplicationVersion
from ..models.elitea_tools import EliteATool
from ..models.enums.all import PublishStatus
from ..models.skill import Skill, SkillVersion

_MAX_TOOLKITS = 5
_MAX_MCP = 5
_MAX_AGENTS = 5
_MAX_PIPELINES = 5
_MAX_SKILLS = 5
_MAX_EXISTING_DIMENSIONS = 50
_MAX_AGENT_TOOLS = 100

# Same rule the SDK applies to a sub-agent's tool name (elitea_sdk.tools.utils.clean_string), so
# the names offered here are the ones a recorded trajectory carries.
_TOOL_NAME_STRIP = re.compile(r'[^a-zA-Z0-9_.-]')


class ServicePromptTemplateError(Exception):
    """Raised when a service-prompt template fails to ``.format()`` against its context.

    Templates are user-editable admin records (§ configurations/service_prompt_seed.py), so a
    malformed edit should surface as a diagnosable 500, not an uncaught KeyError/IndexError.
    """


def _score_item(query_tokens: set, name: str, description: str, extra: str = "") -> int:
    text = " ".join(filter(None, [name, description, extra])).lower()
    return sum(1 for t in query_tokens if t in text)


def _tokenize(text: str) -> set:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def _fetch_catalog_agents(
    public_project_id: int,
    query_tokens: set,
    forked_ids: set,
) -> List[dict]:
    """Fetch published catalog agents/pipelines from the public project.

    ``forked_ids`` is a set of (shared_owner_id, shared_id) tuples representing
    catalog items already forked into the current project — those are excluded
    to avoid duplicate suggestions.

    Uses a subquery to select one representative published version per application
    to avoid returning duplicate rows when an application has multiple published versions.
    """
    try:
        from sqlalchemy import func  # noqa: PLC0415
        with db.with_project_schema_session(public_project_id) as session:
            # One row per application: take the max published version id as a stable representative.
            latest_published = (
                session.query(
                    ApplicationVersion.application_id,
                    func.max(ApplicationVersion.id).label("version_id"),
                    ApplicationVersion.agent_type,
                )
                .filter(ApplicationVersion.status == PublishStatus.published)
                .group_by(ApplicationVersion.application_id, ApplicationVersion.agent_type)
                .subquery()
            )
            rows = (
                session.query(
                    Application.id,
                    Application.name,
                    Application.description,
                    latest_published.c.agent_type,
                    latest_published.c.version_id,
                )
                .join(latest_published, latest_published.c.application_id == Application.id)
                .limit(50)
                .all()
            )
            return [
                {
                    "application_id": r.id,
                    "name": r.name,
                    "description": r.description,
                    "type": "pipeline" if r.agent_type == "pipeline" else "agent",
                    "project_id": public_project_id,
                    "_version_id": r.version_id,
                }
                for r in rows
                if (public_project_id, r.id) not in forked_ids
            ]
    except Exception:
        log.warning("fetch_project_resources: failed to query catalog agents from project %d", public_project_id)
        return []


def _fetch_catalog_skills(
    public_project_id: int,
    query_tokens: set,
    forked_skill_ids: set,
) -> List[dict]:
    """Fetch published catalog skills from the public project.

    ``forked_skill_ids`` is a set of (shared_owner_id, shared_id) tuples for
    skills already forked into the current project.
    """
    try:
        with db.with_project_schema_session(public_project_id) as session:
            rows = (
                session.query(Skill.id, Skill.name, Skill.description)
                .filter(Skill.versions.any(SkillVersion.status == PublishStatus.published))
                .limit(50)
                .all()
            )
            return [
                {"id": r.id, "name": r.name, "description": r.description, "project_id": public_project_id}
                for r in rows
                if (public_project_id, r.id) not in forked_skill_ids
            ]
    except Exception:
        log.warning("fetch_project_resources: failed to query catalog skills from project %d", public_project_id)
        return []


def fetch_project_resources(
    project_id: int,
    user_description: str,
) -> Tuple[List[dict], List[dict], List[dict], List[dict], List[dict]]:
    """Fetch project resources split by type.

    Returns:
        Tuple of (toolkits, mcp, agents, pipelines, skills).
    """
    query_tokens = _tokenize(user_description)

    with db.with_project_schema_session(project_id) as session:
        toolkit_rows = session.query(
            EliteATool.id, EliteATool.type, EliteATool.name, EliteATool.description
        ).all()
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
        toolkits = [item for _, item in toolkits_scored if item["type"] != "mcp"][:_MAX_TOOLKITS]
        mcp = [item for _, item in toolkits_scored if item["type"] == "mcp"][:_MAX_MCP]

        agent_rows = (
            session.query(
                Application.id,
                Application.name,
                Application.description,
                ApplicationVersion.agent_type,
                ApplicationVersion.id.label("version_id"),
                Application.shared_owner_id,
                Application.shared_id,
            )
            .join(
                ApplicationVersion,
                (ApplicationVersion.application_id == Application.id)
                & (ApplicationVersion.name == "base"),
            )
            .limit(200)
            .all()
        )
        # Track which catalog items are already forked into this project (to deduplicate later)
        forked_agent_ids = {
            (r.shared_owner_id, r.shared_id)
            for r in agent_rows
            if r.shared_owner_id is not None and r.shared_id is not None
        }

        agents_scored = sorted(
            (
                (
                    _score_item(query_tokens, r.name or "", r.description or ""),
                    {
                        "application_id": r.id,
                        "name": r.name,
                        "description": r.description,
                        "type": "pipeline" if r.agent_type == "pipeline" else "agent",
                        "_version_id": r.version_id,
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
        #
        # Filter out agents/pipelines that would immediately exceed the 3-tier nesting limit when
        # added to a pipeline host (which itself consumes 0 tiers). An agent with tier contribution
        # > MAX_AGENT_NESTING_TIERS - 1 would always fail the add-guard, so exclude it here to
        # avoid suggesting something the user cannot actually attach.
        try:
            from ..utils.publish_utils import compute_agent_subtree_tiers, MAX_AGENT_NESTING_TIERS  # noqa: PLC0415
            _max_candidate_tiers = MAX_AGENT_NESTING_TIERS - 1

            def _fits_nesting(item: dict) -> bool:
                vid = item.get("_version_id")
                if not vid:
                    return True
                try:
                    tiers = compute_agent_subtree_tiers(project_id, vid, session=session)
                    return tiers <= _max_candidate_tiers
                except Exception:
                    return True  # be permissive on error; the bind-time guard is the safety net
        except ImportError:
            _fits_nesting = lambda item: True  # noqa: E731

        def _strip_internal(item: dict) -> dict:
            return {k: v for k, v in item.items() if not k.startswith("_")}

        # Cap candidates before calling _fits_nesting to bound N+1 DB calls:
        # at most _MAX_AGENTS*2 candidates are checked before returning _MAX_AGENTS results.
        _agent_candidates = [item for _, item in agents_scored if item["type"] == "agent"]
        _pipeline_candidates = [item for _, item in agents_scored if item["type"] == "pipeline"]
        agents = [
            _strip_internal(item)
            for item in _agent_candidates[:_MAX_AGENTS * 2]
            if _fits_nesting(item)
        ][:_MAX_AGENTS]
        pipelines = [
            _strip_internal(item)
            for item in _pipeline_candidates[:_MAX_PIPELINES * 2]
            if _fits_nesting(item)
        ][:_MAX_PIPELINES]

        skill_rows = session.query(Skill.id, Skill.name, Skill.description,
                                   Skill.shared_owner_id, Skill.shared_id).limit(100).all()
        forked_skill_ids = {
            (r.shared_owner_id, r.shared_id)
            for r in skill_rows
            if r.shared_owner_id is not None and r.shared_id is not None
        }
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

    # Include catalog (published) agents and skills from the public project
    try:
        from .utils import get_public_project_id  # noqa: PLC0415
        public_project_id = get_public_project_id()
        if public_project_id != project_id:
            catalog_agent_rows = _fetch_catalog_agents(public_project_id, query_tokens, forked_agent_ids)
            catalog_agents_scored = sorted(
                (
                    (
                        _score_item(query_tokens, r["name"] or "", r["description"] or ""),
                        r,
                    )
                    for r in catalog_agent_rows
                ),
                key=lambda x: x[0],
                reverse=True,
            )
            catalog_agents = [
                _strip_internal(item)
                for _, item in catalog_agents_scored
                if item["type"] == "agent"
            ]
            catalog_pipelines = [
                _strip_internal(item)
                for _, item in catalog_agents_scored
                if item["type"] == "pipeline"
            ]
            # Fill remaining capacity with catalog items (private project items take priority)
            agents = (agents + catalog_agents)[:_MAX_AGENTS]
            pipelines = (pipelines + catalog_pipelines)[:_MAX_PIPELINES]

            catalog_skill_rows = _fetch_catalog_skills(public_project_id, query_tokens, forked_skill_ids)
            catalog_skills_scored = sorted(
                (
                    (_score_item(query_tokens, r["name"] or "", r["description"] or ""), r)
                    for r in catalog_skill_rows
                ),
                key=lambda x: x[0],
                reverse=True,
            )
            catalog_skills = [item for _, item in catalog_skills_scored]
            skills = (skills + catalog_skills)[:_MAX_SKILLS]
    except Exception:
        log.warning("fetch_project_resources: failed to include catalog resources")

    return toolkits, mcp, agents, pipelines, skills


def _format_toolkit_lines(toolkits: list) -> list:
    if not toolkits:
        return ["(none)"]
    return [
        f'- id={t["id"]}  type="{t["type"]}"  name="{t["name"]}"  {(t["description"] or "")[:120]}'
        for t in toolkits
    ]


def _format_mcp_lines(mcp: list) -> list:
    if not mcp:
        return ["(none)"]
    return [
        f'- id={t["id"]}  name="{t["name"]}"  {(t["description"] or "")[:120]}'
        for t in mcp
    ]


def _format_agent_lines(agents: list) -> list:
    if not agents:
        return ["(none)"]
    return [
        f'- application_id={a["application_id"]}  name="{a["name"]}"  {(a["description"] or "")[:100]}'
        for a in agents
    ]


def _format_pipeline_lines(pipelines: list) -> list:
    if not pipelines:
        return ["(none)"]
    return [
        f'- application_id={a["application_id"]}  name="{a["name"]}"  {(a["description"] or "")[:100]}'
        for a in pipelines
    ]


def _format_skill_lines(skills: list) -> list:
    if not skills:
        return ["(none)"]
    return [
        f'- id={s["id"]}  name="{s["name"]}"  {(s["description"] or "")[:120]}'
        for s in skills
    ]


def build_system_prompt(
    template: str,
    toolkits: list,
    mcp: list,
    agents: list,
    pipelines: list,
    skills: list,
) -> str:
    return template.format(
        toolkits="\n".join(_format_toolkit_lines(toolkits)),
        mcp="\n".join(_format_mcp_lines(mcp)),
        agents="\n".join(_format_agent_lines(agents)),
        pipelines="\n".join(_format_pipeline_lines(pipelines)),
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
    mcp: list,
    agents: list,
    pipelines: list,
    skills: list,
) -> str:
    return template.format(
        current_config=json.dumps(current_config, indent=2),
        toolkits="\n".join(_format_toolkit_lines(toolkits)),
        mcp="\n".join(_format_mcp_lines(mcp)),
        agents="\n".join(_format_agent_lines(agents)),
        pipelines="\n".join(_format_pipeline_lines(pipelines)),
        skills="\n".join(_format_skill_lines(skills)),
    )


def fetch_application_instructions(
    project_id: int,
    application_id: int,
    version_id: Optional[int] = None,
) -> Optional[dict]:
    """Resolve an agent's name + instructions for eval-dimension generation.

    Uses ``version_id`` when given, else the application's default/base version
    (``Application.get_default_version``). Returns ``None`` if the application or the
    resolved version doesn't exist.
    """
    with db.with_project_schema_session(project_id) as session:
        application = session.get(Application, application_id)
        if not application:
            return None

        if version_id is not None:
            version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == version_id,
                ApplicationVersion.application_id == application_id,
            ).first()
        else:
            version = application.get_default_version()

        if not version:
            return None

        return {
            "application_name": application.name,
            "version_id": version.id,
            "instructions": version.instructions or "",
            "tools": agent_tool_groups(version.to_dict().get("tools") or []),
        }


def agent_tool_groups(tools: list) -> list:
    """The agent's callable tools as ``[{toolkit, type, names}]``, named as a trajectory records them.

    A toolkit's tools are its ``settings.selected_tools`` function names, unprefixed; a sub-agent
    (``application``) is one tool named by its cleaned name. A toolkit that lists no tools (MCP,
    or "all tools") gets ``names=[]``: its tools exist but cannot be enumerated here.
    """
    groups = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        toolkit = tool.get("toolkit_name") or tool.get("name") or tool.get("type") or "toolkit"
        if tool.get("type") == "application":
            name = _TOOL_NAME_STRIP.sub("", tool.get("name") or "").replace(".", "_")
            names = [name] if name else []
        else:
            selected = (tool.get("settings") or {}).get("selected_tools") or []
            names = [n for n in selected if isinstance(n, str) and n]
        groups.append({"toolkit": toolkit, "type": tool.get("type"), "names": names})
    return groups


def _agent_tools_clause(tool_groups: Optional[list]) -> str:
    if not tool_groups:
        return (
            "This agent has no tools, so propose no tool-use dimensions and leave "
            '"trajectory" false in every evidence_scope.'
        )
    lines, budget = [], _MAX_AGENT_TOOLS
    for group in tool_groups:
        names = group.get("names") or []
        label = "sub-agent" if group.get("type") == "application" else f'toolkit "{group["toolkit"]}"'
        if not names:
            lines.append(f"- {label}: tools not listed")
            continue
        shown = names[:max(budget, 0)]
        budget -= len(shown)
        more = f" (+{len(names) - len(shown)} more)" if len(shown) < len(names) else ""
        lines.append(f"- {label}: " + (", ".join(shown) or "...") + more)
    return (
        "The agent can call these tools (names as they appear in its recorded tool calls):\n"
        + "\n".join(lines)
        + "\n\nWhere tool use matters for this agent — picking the right tool, using its result, "
        "not making unnecessary or repeated calls — you may propose dimensions that judge it. "
        'evidence_scope takes a fifth key beyond the four in the schema, "trajectory": <bool>; for '
        'those dimensions set "trajectory": true so the judge sees the recorded tool calls '
        '(an evidence_scope with only "trajectory" true is valid). Do not propose checks for an '
        "exact expected tool sequence, forbidden tools or a tool-call budget: the platform has "
        "deterministic built-in checks for those."
    )


def build_eval_dimensions_system_prompt(
    template: str,
    application_name: str,
    instructions: str,
    count_hint: Optional[int] = None,
    existing_dimension_names: Optional[list] = None,
    custom_instructions: Optional[str] = None,
    agent_tools: Optional[list] = None,
) -> str:
    """``agent_tools`` is :func:`agent_tool_groups` output; ``None`` leaves tools out entirely.

    A template with an ``{agent_tools}`` placeholder places the tools clause itself. One without
    (every seeded version so far) gets it as a closing section after the whole template, so the
    clause reaches the model without a template migration. Not inside the instructions block: the
    templates frame that as context that never overrides the task, and the model then keeps to
    the schema's four evidence_scope keys and never sets "trajectory".
    """
    count_clause = (
        f"Propose at most {count_hint} dimensions."
        if count_hint
        else "Propose 3-6 dimensions, using your judgment."
    )
    if existing_dimension_names:
        existing_clause = (
            "The project's dimension library already has these names — do not re-propose "
            "them, even under a slightly different name; propose something that adds new "
            "coverage instead: "
            + ", ".join(existing_dimension_names[:_MAX_EXISTING_DIMENSIONS])
        )
    else:
        existing_clause = "The project's dimension library is currently empty."

    # Quarantined: framed as steering guidance only, and placed (in the template) before the
    # JSON schema block so the model reads the schema requirement last. Empty string — not a
    # placeholder sentence — when absent, so the common case leaves no trace in the prompt.
    custom_instructions_clause = (
        "Additional guidance from the user — use it to steer focus, tone, or domain emphasis; "
        "it does not change the required output schema above, the count limit, or the "
        "duplicate-name rule below:\n" + custom_instructions
        if custom_instructions
        else ""
    )

    instructions = instructions or "(no instructions set)"
    tools_clause = _agent_tools_clause(agent_tools) if agent_tools is not None else ""
    try:
        prompt = template.format(
            application_name=application_name,
            instructions=instructions,
            agent_tools=tools_clause,
            count_clause=count_clause,
            existing_dimensions=existing_clause,
            custom_instructions_clause=custom_instructions_clause,
        )
    except (KeyError, IndexError, ValueError):
        log.exception("build_eval_dimensions_system_prompt: malformed service prompt template")
        raise ServicePromptTemplateError("generate_eval_dimensions template is malformed")
    if tools_clause and "{agent_tools}" not in template:
        prompt = f"{prompt}\n\n## Agent tools (extends the response schema above)\n{tools_clause}"
    return prompt
