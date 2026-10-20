"""Canonical digest of an agent version's definition (#6756, C-1009).

A binding is approved for one definition. The digest ("adv1:" + sha256) is what a later
check compares to know the agent still is what was approved. It covers what the agent does
(type, instructions, model settings, the toolkits and the tool subset it may call, and which
agent version an agent-as-tool points at). It leaves out everything that changes without
changing behaviour: ids, names, timestamps, status, descriptions, tags, variables, layout.
Toolkit credential and settings content is not covered here; that is the credential binding's job.
"""
import hashlib
import json
import math
import unicodedata
import uuid

DIGEST_PREFIX = "adv1:"
DIGEST_VERSION = 1


class DigestError(ValueError):
    """The definition cannot be digested deterministically. ``reason_code`` says why."""

    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = reason_code


def norm_text(value: str) -> str:
    """NFC and ``\\n`` line endings. No trimming: whitespace can be meaningful in YAML."""
    return unicodedata.normalize("NFC", value).replace("\r\n", "\n").replace("\r", "\n")


def norm_json(value):
    """Drop null-valued keys, make keys strings, turn integral floats into ints, refuse NaN/inf
    and any value JSON cannot carry. List order is kept: it is meaningful in settings."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DigestError("DIGEST_UNSTABLE", "non-finite number in the definition")
        return int(value) if value.is_integer() else value
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if item is None:
                continue
            if str(key) in out:
                raise DigestError("DIGEST_UNSTABLE", "two keys collide once converted to strings")
            out[str(key)] = norm_json(item)
        return out
    if isinstance(value, (list, tuple)):
        return [norm_json(item) for item in value]
    raise DigestError("DIGEST_UNSTABLE", f"unsupported value type in the definition: {type(value).__name__}")


def child_ref(tool):
    """Agent-as-tool toolkits point at another agent version; a change there changes this agent."""
    if tool.type != "application":
        return None
    settings = tool.settings or {}
    return {"application_id": settings.get("application_id"), "version_id": settings.get("application_version_id")}


def canonical_definition(version, mappings_with_tools) -> dict:
    """``mappings_with_tools``: iterable of ``(EntityToolMapping, EliteATool)`` for the version."""
    tools = [
        {
            "toolkit_id": tool.id,
            "type": tool.type,
            "selected_tools": sorted({str(name) for name in (mapping.selected_tools or [])}),
            "ref": child_ref(tool),
        }
        for mapping, tool in mappings_with_tools
    ]
    tools.sort(key=lambda item: (item["type"], item["toolkit_id"]))
    return {
        "v": DIGEST_VERSION,
        "agent_type": version.agent_type,
        "instructions": norm_text(version.instructions or ""),
        "llm_settings": norm_json(version.llm_settings or {}),
        "tools": tools,
    }


def definition_digest(canonical: dict) -> str:
    try:
        payload = json.dumps(
            canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
        ).encode("utf-8")
    except ValueError as exc:  # NaN/inf or text that is not valid unicode
        raise DigestError("DIGEST_UNSTABLE", "definition cannot be serialised canonically") from exc
    return DIGEST_PREFIX + hashlib.sha256(payload).hexdigest()


def toolkit_rows(session, version_id: int):
    """``(EntityToolMapping, EliteATool)`` of the version's toolkits: one joined query."""
    from ..models.elitea_tools import EliteATool, EntityToolMapping  # pylint: disable=C0415
    from ..models.enums.all import ToolEntityTypes  # pylint: disable=C0415

    return (
        session.query(EntityToolMapping, EliteATool)
        .join(EliteATool, EliteATool.id == EntityToolMapping.tool_id)
        .filter(
            EntityToolMapping.entity_version_id == version_id,
            EntityToolMapping.entity_type == ToolEntityTypes.agent.value,
        )
        .all()
    )


def digest_for_version(session, version_id: int) -> str:
    """Two queries, whatever the number of toolkits: the version, then its mappings joined to tools."""
    from ..models.all import ApplicationVersion  # pylint: disable=C0415

    version = session.query(ApplicationVersion).filter(ApplicationVersion.id == version_id).first()
    if version is None:
        raise DigestError("VERSION_NOT_FOUND", f"application version {version_id} does not exist")
    return definition_digest(canonical_definition(version, toolkit_rows(session, version_id)))


def _configuration_uuid(settings):
    """The first stored configuration a toolkit's settings point to (``{"configuration_uuid": ...}``), or None."""
    found = []
    for value in (settings or {}).values():
        if isinstance(value, dict) and value.get("configuration_uuid"):
            try:
                found.append(str(uuid.UUID(str(value["configuration_uuid"]))))
            except ValueError:
                continue
    return min(found) if found else None


def digest_inputs(version, mappings_with_tools) -> dict:
    """What the digest of a version is made of, as the operator is shown it: names, ids and hashes only.

    The instructions are never returned, only their length and the hash of the same normalised text the digest
    covers. Toolkit settings are not returned at all; a toolkit shows the stored configuration it points to.
    """
    llm_settings = version.llm_settings if isinstance(version.llm_settings, dict) else {}
    model = llm_settings.get("model_name")
    instructions = norm_text(version.instructions or "")
    tools = [
        {
            "id": tool.id, "type": tool.type, "name": tool.name,
            "credential_configuration_uuid": _configuration_uuid(tool.settings),
        }
        for _mapping, tool in mappings_with_tools
    ]
    tools.sort(key=lambda item: (item["type"], item["id"]))
    return {
        "agent_type": version.agent_type,
        "model": model if isinstance(model, str) else None,
        "instructions": {"length": len(instructions), "sha256": hashlib.sha256(instructions.encode("utf-8")).hexdigest()},
        "tools": tools,
    }


def digest_inputs_for_version(session, version_id: int) -> dict:
    """``digest_inputs`` of one version: the version, then its mappings joined to tools (two queries)."""
    from ..models.all import ApplicationVersion  # pylint: disable=C0415

    version = session.query(ApplicationVersion).filter(ApplicationVersion.id == version_id).first()
    if version is None:
        raise DigestError("VERSION_NOT_FOUND", f"application version {version_id} does not exist")
    return digest_inputs(version, toolkit_rows(session, version_id))
