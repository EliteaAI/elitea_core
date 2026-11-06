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


_MAX_QUERY_TOKENS = 12
# rows fetched per kind before Python re-scores them; ranking happens in SQL first, so this
# only needs to cover the top of the list plus what the nesting check may reject
_CANDIDATE_POOL = 50


def _score_item(query_tokens: set, name: str, description: str, extra: str = "") -> int:
    text = " ".join(filter(None, [name, description, extra])).lower()
    return sum(1 for t in query_tokens if t in text)


def _tokenize(text: str) -> set:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def _relevance(query_tokens: set, *columns):
    """SQL expression counting how many query tokens appear in any of ``columns``.

    Ordering by it before ``LIMIT`` keeps the best matches when a project has more rows than the
    pool; the Python ``_score_item`` pass afterwards only breaks ties inside that pool. Short
    tokens are skipped because they match almost everything.
    """
    from sqlalchemy import case, func, literal  # noqa: PLC0415

    tokens = sorted(t for t in query_tokens if len(t) >= 3)[:_MAX_QUERY_TOKENS]
    if not tokens:
        return literal(0)
    haystack = func.lower(func.concat(*[func.coalesce(col, "") for col in columns]))
    return sum(case((haystack.contains(t), 1), else_=0) for t in tokens)


def _ranked(query, query_tokens: set, *columns, limit: int = _CANDIDATE_POOL):
    return query.order_by(_relevance(query_tokens, *columns).desc()).limit(limit).all()


def _by_score(query_tokens: set, items: list, *fields) -> list:
    return [
        item for _, item in sorted(
            ((_score_item(query_tokens, *(item.get(f) or "" for f in fields)), item) for item in items),
            key=lambda x: x[0],
            reverse=True,
        )
    ]


def _strip_internal(item: dict) -> dict:
    return {k: v for k, v in item.items() if not k.startswith("_")}


def _nesting_filter(project_id: int, session):
    """Predicate rejecting agents that would exceed the nesting limit when attached to a pipeline.

    A pipeline host consumes no tier, so a candidate whose own subtree already uses every tier can
    never be attached; suggesting it only produces a bind-time error. Errors are permissive because
    the bind-time guard is the real safety net.
    """
    try:
        from ..utils.publish_utils import compute_agent_subtree_tiers, MAX_AGENT_NESTING_TIERS  # noqa: PLC0415
    except ImportError:
        return lambda item: True
    max_candidate_tiers = MAX_AGENT_NESTING_TIERS - 1

    def _fits(item: dict) -> bool:
        version_id = item.get("_version_id")
        if not version_id:
            return True
        try:
            return compute_agent_subtree_tiers(project_id, version_id, session=session) <= max_candidate_tiers
        except Exception:  # pylint: disable=W0703
            return True

    return _fits


def _first_fitting(items: list, fits, cap: int) -> list:
    """Up to ``cap`` items that pass ``fits``, checking lazily so at most a few walks run."""
    result = []
    for item in items:
        if len(result) >= cap:
            break
        if fits(item):
            result.append(_strip_internal(item))
    return result


def _forked_origins(session, model) -> set:
    """``(shared_owner_id, shared_id)`` of every catalog item already forked into this project.

    Read with its own query rather than from the ranked candidate rows, which are truncated and so
    would miss forks that did not make the pool.
    """
    return {
        (owner, shared)
        for owner, shared in session.query(model.shared_owner_id, model.shared_id)
        .filter(model.shared_owner_id.isnot(None), model.shared_id.isnot(None))
        .all()
    }


def _fetch_catalog_agents(
    public_project_id: int,
    query_tokens: set,
    forked_ids: set,
) -> List[dict]:
    """Published catalog agents and pipelines, one row per application, best matches first.

    Already-forked items (``forked_ids`` holds ``(shared_owner_id, shared_id)``) are excluded,
    and the nesting check runs against the catalog project, where their sub-agents live.
    """
    try:
        from sqlalchemy import func  # noqa: PLC0415
        with db.with_project_schema_session(public_project_id) as session:
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
            rows = _ranked(
                session.query(
                    Application.id,
                    Application.name,
                    Application.description,
                    latest_published.c.agent_type,
                    latest_published.c.version_id,
                ).join(latest_published, latest_published.c.application_id == Application.id),
                query_tokens, Application.name, Application.description,
            )
            items = _by_score(query_tokens, [
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
            ], "name", "description")
            fits = _nesting_filter(public_project_id, session)
            return (
                _first_fitting([i for i in items if i["type"] == "agent"], fits, _MAX_AGENTS)
                + _first_fitting([i for i in items if i["type"] == "pipeline"], fits, _MAX_PIPELINES)
            )
    except Exception:  # pylint: disable=W0703
        log.warning("fetch_project_resources: failed to query catalog agents from project %d", public_project_id)
        return []


def _fetch_catalog_skills(
    public_project_id: int,
    query_tokens: set,
    forked_skill_ids: set,
) -> List[dict]:
    """Published catalog skills, best matches first, excluding ones already forked here."""
    try:
        with db.with_project_schema_session(public_project_id) as session:
            rows = _ranked(
                session.query(Skill.id, Skill.name, Skill.description)
                .filter(Skill.versions.any(SkillVersion.status == PublishStatus.published)),
                query_tokens, Skill.name, Skill.description,
            )
            return _by_score(query_tokens, [
                {"id": r.id, "name": r.name, "description": r.description, "project_id": public_project_id}
                for r in rows
                if (public_project_id, r.id) not in forked_skill_ids
            ], "name", "description")[:_MAX_SKILLS]
    except Exception:  # pylint: disable=W0703
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
        toolkits_ranked = _by_score(query_tokens, [
            {"id": r.id, "type": r.type, "name": r.name or r.type, "description": r.description}
            for r in toolkit_rows
        ], "name", "description", "type")
        toolkits = [item for item in toolkits_ranked if item["type"] != "mcp"][:_MAX_TOOLKITS]
        mcp = [item for item in toolkits_ranked if item["type"] == "mcp"][:_MAX_MCP]

        forked_agent_ids = _forked_origins(session, Application)
        forked_skill_ids = _forked_origins(session, Skill)

        # NOTE(#5680): these are ranked SUGGESTIONS shown to the LLM, not auto-bindings — a
        # suggested container agent only becomes a real sub-agent if the user accepts it, and
        # that binding is rejected at bind time (application_toolkit_change_relation) and at chat
        # resolution. If this endpoint ever gains an owner-application context, filter it out
        # here so an agent is never suggested as its own sub-agent.
        base_versions = session.query(
            Application.id,
            Application.name,
            Application.description,
            ApplicationVersion.agent_type,
            ApplicationVersion.id.label("version_id"),
        ).join(
            ApplicationVersion,
            (ApplicationVersion.application_id == Application.id)
            & (ApplicationVersion.name == "base"),
        )
        fits = _nesting_filter(project_id, session)
        apps_by_type = {}
        for kind, is_pipeline in (("agent", False), ("pipeline", True)):
            type_filter = (
                ApplicationVersion.agent_type == "pipeline" if is_pipeline
                else ApplicationVersion.agent_type != "pipeline"
            )
            rows = _ranked(
                base_versions.filter(type_filter), query_tokens, Application.name, Application.description,
            )
            apps_by_type[kind] = _by_score(query_tokens, [
                {
                    "application_id": r.id,
                    "name": r.name,
                    "description": r.description,
                    "type": kind,
                    "_version_id": r.version_id,
                }
                for r in rows
            ], "name", "description")
        agents = _first_fitting(apps_by_type["agent"], fits, _MAX_AGENTS)
        pipelines = _first_fitting(apps_by_type["pipeline"], fits, _MAX_PIPELINES)

        skill_rows = _ranked(
            session.query(Skill.id, Skill.name, Skill.description),
            query_tokens, Skill.name, Skill.description,
        )
        skills = _by_score(query_tokens, [
            {"id": r.id, "name": r.name, "description": r.description} for r in skill_rows
        ], "name", "description")[:_MAX_SKILLS]

    # Fill remaining capacity with published catalog items; the project's own take priority
    try:
        from .utils import get_public_project_id  # noqa: PLC0415
        public_project_id = get_public_project_id()
        if public_project_id != project_id:
            catalog_apps = _fetch_catalog_agents(public_project_id, query_tokens, forked_agent_ids)
            agents = (agents + [a for a in catalog_apps if a["type"] == "agent"])[:_MAX_AGENTS]
            pipelines = (pipelines + [a for a in catalog_apps if a["type"] == "pipeline"])[:_MAX_PIPELINES]
            skills = (
                skills + _fetch_catalog_skills(public_project_id, query_tokens, forked_skill_ids)
            )[:_MAX_SKILLS]
    except Exception:  # pylint: disable=W0703
        log.warning("fetch_project_resources: failed to include catalog resources")

    return toolkits, mcp, agents, pipelines, skills


def _format_toolkit_lines(toolkits: list) -> list:
    if not toolkits:
        return ["(none)"]
    return [
        f'- id={t["id"]}  type="{t["type"]}"  name="{t["name"]}"  {(t.get("description") or "")[:120]}'
        for t in toolkits
    ]


def _format_mcp_lines(mcp: list) -> list:
    if not mcp:
        return ["(none)"]
    return [
        f'- id={t["id"]}  name="{t["name"]}"  {(t.get("description") or "")[:120]}'
        for t in mcp
    ]


def _format_app_lines(apps: list, project_id: int) -> list:
    """Agents and pipelines, each with the project it lives in: ids repeat across projects."""
    if not apps:
        return ["(none)"]
    return [
        f'- project_id={a.get("project_id") or project_id}  application_id={a["application_id"]}  '
        f'name="{a["name"]}"  {(a.get("description") or "")[:100]}'
        for a in apps
    ]


def _format_skill_lines(skills: list, project_id: int) -> list:
    if not skills:
        return ["(none)"]
    return [
        f'- project_id={s.get("project_id") or project_id}  id={s["id"]}  '
        f'name="{s["name"]}"  {(s.get("description") or "")[:120]}'
        for s in skills
    ]


def format_resource_sections(
    project_id: int, toolkits: list, mcp: list, agents: list, pipelines: list, skills: list,
) -> dict:
    """The resource placeholders every draft service prompt is formatted with."""
    return {
        "toolkits": "\n".join(_format_toolkit_lines(toolkits)),
        "mcp": "\n".join(_format_mcp_lines(mcp)),
        "agents": "\n".join(_format_app_lines(agents, project_id)),
        "pipelines": "\n".join(_format_app_lines(pipelines, project_id)),
        "skills": "\n".join(_format_skill_lines(skills, project_id)),
    }


def build_system_prompt(
    template: str,
    project_id: int,
    toolkits: list,
    mcp: list,
    agents: list,
    pipelines: list,
    skills: list,
) -> str:
    return template.format(
        **format_resource_sections(project_id, toolkits, mcp, agents, pipelines, skills)
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
    project_id: int,
    current_config: dict,
    toolkits: list,
    mcp: list,
    agents: list,
    pipelines: list,
    skills: list,
) -> str:
    return template.format(
        current_config=json.dumps(current_config, indent=2),
        **format_resource_sections(project_id, toolkits, mcp, agents, pipelines, skills),
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
