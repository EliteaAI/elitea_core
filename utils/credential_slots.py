"""Reference-only payloads of governed runs (#6946, WP-1D slice 1, C-1009).

A governed payload leaves Main with a credential *slot* where the secret fields of a credential-section
configuration were: the credential reference, its generation, the credential-use decision it was admitted under, and
the names of the fields the worker has to ask for. The material itself is released later, to the worker that builds the
toolkit, by the trusted resolver (``credential_release``). ``seal_payload`` is the one place that turns a payload into
that shape; it runs at the dispatch chokepoint (``admission.admit_dispatch``) after the run was admitted or its
inherited context accepted, and before the payload is handed to the task node.

Nothing here reads a secret store or logs a value. A credential that cannot be matched to a decision, or whose type has
no known secret fields, is a refusal, never a pass: several toolkits treat a missing secret as anonymous access. A
credential configuration that holds no secret value at all is refused too (a slot asking for nothing would let a toolkit
run without its credential).

Secret fields come from ``configurations.configuration_secret_fields``: dotted paths, and ``<path>.*`` for a dict whose
every value is secret (``additionalProperties`` of format password, e.g. the headers of an OpenAPI configuration); the
seal removes every key under it and asks for the concrete keys. Until the walker lists those, a dict-valued field named
like a header map (``DICT_SECRET_FIELDS``) is treated the same way, so no resolved header value stays in a payload.

What is and is not covered, stated so nobody has to guess:
  - the free-form ``{{secret.*}}`` check reads toolkit settings (the run's and the prefetched sub-agents'), including the
    non-secret fields of credential configurations; the instructions, variables and user input of the run are out of its
    scope;
  - ``mcp_tokens`` (the browser's OAuth tokens for MCP servers) are not store material: they stay in the payload and in the
    parallel stash, as before;
  - a release is bound to the context (and the decision), not to a task id: any process of the run that holds both can ask
    (design-accepted, WP-1D design section 5).
"""
import re
from typing import Callable, Mapping, Optional

from tools import this

from .credential_decision import (
    CREDENTIAL_SECTION, CredentialDecisionError, _int_or_none, _payload_toolkits, _uuid_text, configurations_in,
)

SLOT_KEY = "credential_slot"
SLOT_VERSION = 1
# Set by the predict entry points (governed only) when a toolkit holds a ``{{secret.*}}`` outside a configuration, read
# and removed by ``admit_dispatch``: the payload-wide unsecret that follows would otherwise put that value into the
# payload where no slot governs it
FREE_FORM_SECRET_MARK = "_governed_free_form_secret"
SECRET_TEMPLATE = "{{secret."
MAX_SCAN_NODES = 100_000
MAX_SCAN_DEPTH = 32
REQUIRED = "CREDENTIAL_REFERENCE_REQUIRED"
CONFIGURATION_TYPE = "configuration_type"
WILDCARD = ".*"
MAX_SLOT_FIELDS = 32   # what one slot may ask for; the release refuses more
# Dict-valued fields that carry secrets whether or not the schema walker says so (HTTP header maps)
DICT_SECRET_FIELDS = ("headers", "extra_headers", "auth_headers")
_KEY = re.compile(r"^[A-Za-z0-9_-]{1,64}$")   # a concrete key under a wildcard path: what a release request can name


def prefetched_tool_lists(payload) -> dict:
    """``{"<application id>:<version id>": [toolkit dicts]}`` of the sub-agents the predict payload carries ready-expanded
    (``application.subagent_version_details``, expanded with the same unsecret as the run's own): material rides there
    too. Main builds each entry as ``{name, description, version_details: {tools}}`` (``subagent_prefetch``); a ``tools``
    list on the entry itself is read as well, so no shape of an entry leaves a toolkit out."""
    application = payload.get("application") if isinstance(payload, dict) else None
    prefetched = application.get("subagent_version_details") if isinstance(application, dict) else None
    if not isinstance(prefetched, dict):
        return {}
    found = {}
    for key, entry in prefetched.items():
        if not isinstance(entry, dict):
            continue
        details = entry.get("version_details") if isinstance(entry.get("version_details"), dict) else {}
        found[str(key)] = [tool for tools in (entry.get("tools"), details.get("tools")) if isinstance(tools, list)
                           for tool in tools if isinstance(tool, dict)]
    return found


def _prefetched_toolkits(payload) -> list:
    """The toolkits of every prefetched sub-agent of a payload (``prefetched_tool_lists``), in one list."""
    return [tool for tools in prefetched_tool_lists(payload).values() for tool in tools]


def dispatched_toolkits(payload) -> list:
    """Every toolkit that carries settings in a dispatched payload: the run's own and the prefetched sub-agents'."""
    return [*_payload_toolkits(payload), *_prefetched_toolkits(payload)]


# --- free-form secret templates (checked before the payload-wide unsecret) --------------------

def _walk_templates(node, budget, depth, *, configs=None, path="", skip=None) -> bool:
    """A ``{{secret.*}}`` string in ``node``. Without ``skip`` a configuration dict is collected into ``configs`` and not
    entered; with ``skip`` (``(exact paths, wildcard bases)``) the walk enters it and passes over its secret fields."""
    budget[0] -= 1
    if budget[0] < 0 or depth > MAX_SCAN_DEPTH:
        return True   # a payload too large or deep to look through counts as holding one
    if isinstance(node, str):
        return SECRET_TEMPLATE in node
    if isinstance(node, dict):
        if configs is not None and CONFIGURATION_TYPE in node:
            configs.append(node)
            return False
        if skip is not None and path in skip[1]:
            return False
        for key, value in node.items():
            child = f"{path}.{key}" if path else str(key)
            if skip is not None and child in skip[0]:
                continue
            if _walk_templates(value, budget, depth + 1, configs=configs, path=child, skip=skip):
                return True
        return False
    if isinstance(node, (list, tuple)):
        return any(_walk_templates(value, budget, depth + 1, configs=configs, path=path, skip=skip) for value in node)
    return False


def _skip_of(paths) -> tuple:
    """``(exact paths, wildcard bases)`` of the secret fields of a type, with the header-map fields always included."""
    exact = {path for path in paths if not path.endswith(WILDCARD)}
    wild = {path[:-len(WILDCARD)] for path in paths if path.endswith(WILDCARD)}
    return exact | set(DICT_SECRET_FIELDS), wild | set(DICT_SECRET_FIELDS)


def has_secret_template(node) -> bool:
    """Is there a ``{{secret.*}}`` string anywhere in ``node`` (bounded; too large or deep counts as yes)?"""
    budget = [MAX_SCAN_NODES]

    def walk(item, depth):
        budget[0] -= 1
        if budget[0] < 0 or depth > MAX_SCAN_DEPTH:
            return True
        if isinstance(item, str):
            return SECRET_TEMPLATE in item
        if isinstance(item, dict):
            return any(walk(value, depth + 1) for value in item.values())
        if isinstance(item, (list, tuple)):
            return any(walk(value, depth + 1) for value in item)
        return False

    return walk(node, 0)


def has_free_form_secret(module, payload) -> bool:
    """Does a toolkit of this payload hold a ``{{secret.*}}`` template that is not in a secret field of a configuration?
    Always ``False`` (and nothing is read) when governance is off; audit only looks as enforce does, to rehearse the
    refusal. Call it before the payload is unsecreted. The non-secret
    fields of the configurations are read too, with one call for the secret fields of all their types; a type that call
    does not know is read whole, and a call that fails counts as found."""
    from .governance import is_auditing, is_governed  # pylint: disable=C0415

    if not (is_governed(module) or is_auditing(module)):
        return False
    budget, configs = [MAX_SCAN_NODES], []
    for tool in dispatched_toolkits(payload):
        if _walk_templates(tool.get("settings"), budget, 0, configs=configs):
            return True
    if not configs:
        return False
    names = sorted({config.get(CONFIGURATION_TYPE) for config in configs if isinstance(config.get(CONFIGURATION_TYPE), str)})
    try:
        known = configuration_secret_fields(names) if names else {}
    except Exception:  # pylint: disable=W0703
        return True
    for config in configs:
        listed = known.get(config.get(CONFIGURATION_TYPE)) if isinstance(known, Mapping) else None
        skip = _skip_of(listed) if isinstance(listed, (list, tuple)) else None
        if _walk_templates(config, budget, 0, skip=skip):
            return True
    return False


def mark_free_form_secret(payload, found: bool) -> None:
    """Record the answer of ``has_free_form_secret`` on the payload (``admit_dispatch`` consumes it)."""
    if found:
        payload[FREE_FORM_SECRET_MARK] = True


# --- the secret fields of a type -------------------------------------------------------------

def configuration_secret_fields(types) -> Mapping[str, list]:
    """``{type: [secret field paths]}`` from the configurations plugin (schema only, no database, no values)."""
    return this.for_module("configurations").module.configuration_secret_fields(sorted(types))


def secret_paths_for(types, secret_fields_of: Callable = configuration_secret_fields) -> dict:
    """The secret paths of each of ``types``; a type the plugin does not know is a refusal."""
    try:
        found = secret_fields_of(sorted(types)) if types else {}
    except Exception:  # pylint: disable=W0703
        raise CredentialDecisionError("DEPENDENCY_UNAVAILABLE") from None
    paths = {}
    for name in types:
        listed = found.get(name) if isinstance(found, Mapping) else None
        if not isinstance(listed, (list, tuple)) or not all(isinstance(path, str) and path for path in listed):
            raise CredentialDecisionError(REQUIRED)
        paths[name] = list(listed)
    return paths


# --- taking the secret fields out of one configuration dict ----------------------------------

def _remove_path(node: dict, path: str) -> bool:
    """Delete one ``.``-joined path; ``True`` when a non-empty value was there."""
    *parents, leaf = path.split(".")
    for part in parents:
        node = node.get(part) if isinstance(node, dict) else None
        if not isinstance(node, dict):
            return False
    if not isinstance(node, dict) or leaf not in node:
        return False
    value = node.pop(leaf)
    return value not in (None, "")


def _path_present(node: dict, path: str) -> bool:
    *parents, leaf = path.split(".")
    for part in parents:
        node = node.get(part) if isinstance(node, dict) else None
        if not isinstance(node, dict):
            return False
    return isinstance(node, dict) and leaf in node


def _value_at(config, path):
    node = config
    for part in path.split("."):
        node = node.get(part) if isinstance(node, dict) else None
    return node


def _strip_dict(config: dict, base: str, held: list) -> None:
    """Remove every key under the dict at ``base`` (a wildcard path); the keys that held a value are asked for."""
    node = _value_at(config, base)
    if isinstance(node, dict):
        for key in list(node):
            value = node.pop(key)
            if value not in (None, ""):
                if not isinstance(key, str) or not _KEY.match(key):
                    raise CredentialDecisionError(REQUIRED)   # a key a release request cannot name
                held.append(f"{base}.{key}")
    elif node not in (None, "") and _remove_path(config, base):
        held.append(base)


def strip_secret_fields(config: dict, paths) -> list:
    """Remove every secret path from a configuration dict (empty ones too) and return the paths that held a value, in
    schema order. A path ``<base>.*`` removes every key under the dict at ``<base>`` and returns the concrete
    ``<base>.<key>``; a header-map field (``DICT_SECRET_FIELDS``) the paths do not cover is treated the same way. Asserts
    the result: a path or a dict still holding something is a refusal, and so is a slot asking for too much."""
    paths = list(paths)
    held = []
    for path in paths:
        if path.endswith(WILDCARD):
            _strip_dict(config, path[:-len(WILDCARD)], held)
        elif _remove_path(config, path):
            held.append(path)
    covered = {path[:-len(WILDCARD)] for path in paths if path.endswith(WILDCARD)} | set(paths)
    for name in DICT_SECRET_FIELDS:
        if name in config and name not in covered and not any(path.startswith(f"{name}.") for path in paths):
            _strip_dict(config, name, held)
    for path in paths:
        base = path[:-len(WILDCARD)] if path.endswith(WILDCARD) else None
        if base is not None:
            left = _value_at(config, base)
            if left not in (None, "", {}):
                raise CredentialDecisionError(REQUIRED)
        elif _path_present(config, path):
            raise CredentialDecisionError(REQUIRED)
    if any(config.get(name) not in (None, "", {}) for name in DICT_SECRET_FIELDS if name not in covered):
        raise CredentialDecisionError(REQUIRED)
    if len(held) > MAX_SLOT_FIELDS:
        raise CredentialDecisionError(REQUIRED)
    return held


def slot_of(entry: Mapping, fields: list, *, ticket: Optional[str] = None) -> dict:
    """The slot of a run's decision entry, or (``ticket`` set) of a probe: same shape, ``probe_ref`` for
    ``decision_id``."""
    slot = {
        "v": SLOT_VERSION, "credential_ref": entry["credential_ref"],
        "credential_generation": entry["credential_generation"], "fields": list(fields),
    }
    if ticket is None:
        slot["decision_id"] = entry["decision_id"]
    else:
        slot["probe_ref"] = ticket
    return slot


def _slot_matches(slot, entry: Mapping) -> bool:
    return (
        isinstance(slot, dict) and slot.get("v") == SLOT_VERSION
        and slot.get("credential_ref") == entry.get("credential_ref")
        and slot.get("credential_generation") == entry.get("credential_generation")
        and slot.get("decision_id") == entry.get("decision_id")
        and isinstance(slot.get("fields"), list) and 0 < len(slot["fields"]) <= MAX_SLOT_FIELDS
        and all(isinstance(path, str) and path for path in slot["fields"])
    )


# --- sealing a payload -----------------------------------------------------------------------

def _own_view(payload) -> dict:
    """The run's own toolkits of a payload (request level and version), without its prefetched sub-agents: the same
    dicts, so sealing the view seals the payload."""
    application = payload.get("application") if isinstance(payload.get("application"), dict) else {}
    return {"project_id": payload.get("project_id"), "tools": payload.get("tools"),
            "application": {"version_details": application.get("version_details")}}


def seal_payload(  # pylint: disable=R0912,R0913,R0914
    payload, entries_of: Callable[[], list], *, secret_fields_of: Callable = configuration_secret_fields,
    classify: Optional[Callable] = None, decided: bool = False, children: Optional[Mapping[str, list]] = None,
) -> int:
    """Replace the secret fields of every credential configuration in the dispatched toolkits with a slot, in place.
    The number of configurations sealed; ``CredentialDecisionError`` (nothing is returned) for a refusal.

    ``entries_of`` gives the decisions the run holds (``allowed_bindings`` entries), asked once and only when the
    payload carries a configuration at all. A configuration is sealed when a decision exists for exactly its toolkit,
    field and uuid; one that already carries a slot must carry the slot of that decision (a re-dispatch). Every other
    configuration is left alone only when ``classify`` (``[(uuid, project id)] -> {uuid: facts}``) shows it is not a
    credential. ``decided`` says admission decided every credential of the run's own toolkits, so those need no
    ``classify``; the toolkits of prefetched sub-agents are never decided (a sub-agent's credential is not admitted,
    WP-1B) and are always classified. A configuration that two toolkits share by identity is refused: a slot names
    one decision.

    ``children`` (``{"<application id>:<version id>": entries}``, #7227): the sub-agents admitted with decisions of their
    own. The run's own toolkits are then sealed against ``entries_of`` and the toolkits of each admitted sub-agent
    against its own entries (decided); a prefetched sub-agent that was not admitted is sealed against the run's
    entries, as before (undecided).
    """
    if children is not None:
        sealed = seal_payload(_own_view(payload), entries_of, secret_fields_of=secret_fields_of, classify=classify,
                              decided=decided)
        for key, tools in prefetched_tool_lists(payload).items():
            entries = children.get(key)
            sealed += seal_payload(
                {"project_id": payload.get("project_id"), "tools": tools},
                entries_of if entries is None else (lambda found=entries: found),
                secret_fields_of=secret_fields_of, classify=classify, decided=entries is not None)
        return sealed
    index = None
    toolkits_seen, handled = set(), {}
    to_seal, stray = [], []
    decided_ids = {id(tool) for tool in _payload_toolkits(payload)} if decided else set()
    for tool in dispatched_toolkits(payload):
        if id(tool) in toolkits_seen:
            continue   # the request and the version list share their tool dicts
        toolkits_seen.add(id(tool))
        settings = tool.get("settings")
        if not isinstance(settings, dict):
            continue
        found = configurations_in(settings)   # the walk the decision made, within one dispatch
        if not found:
            continue
        if index is None:
            entries = entries_of()
            if not isinstance(entries, list):
                raise CredentialDecisionError("STALE_CONTEXT")
            index = {(entry.get("toolkit_instance_id"), entry.get("field"), entry.get("configuration_uuid")): entry
                     for entry in entries if isinstance(entry, dict)}
        toolkit_id = str(tool.get("id"))
        top = {id(value): field for field, value in settings.items()
               if isinstance(value, dict) and CONFIGURATION_TYPE in value}
        for item in found:
            data = item["data"]
            field = top.get(id(data))
            owner = (toolkit_id, field)
            entry = index.get((toolkit_id, field, _uuid_text(data.get("configuration_uuid")))) if field else None
            if id(data) in handled:
                if handled[id(data)][0] != owner and (entry is not None or handled[id(data)][1] is not None):
                    raise CredentialDecisionError(REQUIRED)
                continue
            handled[id(data)] = (owner, entry)
            if entry is None:
                if id(tool) not in decided_ids:
                    stray.append(data)
            else:
                to_seal.append((data, entry))
    if stray:
        if classify is None:
            raise CredentialDecisionError(REQUIRED)   # nothing shows these are not credentials
        _refuse_credentials(stray, classify, payload)
    if not to_seal:
        return 0
    types = {data.get(CONFIGURATION_TYPE) for data, _ in to_seal}
    if not all(isinstance(name, str) and name for name in types):
        raise CredentialDecisionError(REQUIRED)
    paths = secret_paths_for(types, secret_fields_of)
    for data, entry in to_seal:
        existing = data.get(SLOT_KEY)
        if existing is not None and not _slot_matches(existing, entry):
            raise CredentialDecisionError(REQUIRED)
        held = strip_secret_fields(data, paths[data[CONFIGURATION_TYPE]])
        if existing is None:
            if not held:
                raise CredentialDecisionError(REQUIRED)   # nothing to ask for: a slot would let the toolkit run without it
            data[SLOT_KEY] = slot_of(entry, held)
    return len(to_seal)


def _refuse_credentials(stray, classify, payload) -> None:
    """A configuration with no decision of its own must be shown to be platform-internal (not a ``credentials``
    section one); one whose section cannot be established counts as a credential."""
    project_id = payload.get("project_id") if isinstance(payload, dict) else None
    pairs = []
    for data in stray:
        cfg_uuid = _uuid_text(data.get("configuration_uuid"))
        if cfg_uuid is None:
            raise CredentialDecisionError(REQUIRED)
        pairs.append((cfg_uuid, _int_or_none(data.get("configuration_project_id")) or project_id))
    facts = classify(pairs)
    for cfg_uuid, _ in pairs:
        fact = facts.get(cfg_uuid) if isinstance(facts, Mapping) else None
        if not isinstance(fact, Mapping) or fact.get("section") == CREDENTIAL_SECTION:
            raise CredentialDecisionError(REQUIRED)
