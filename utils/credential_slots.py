"""Reference-only payloads of governed runs (#6946, WP-1D slice 1, C-1009).

A governed payload leaves Main with a credential *slot* where the secret fields of a credential-section
configuration were: the credential reference, its generation, the credential-use decision it was admitted under, and
the names of the fields the worker has to ask for. The material itself is released later, to the worker that builds the
toolkit, by the trusted resolver (``credential_release``). ``seal_payload`` is the one place that turns a payload into
that shape; it runs at the dispatch chokepoint (``admission.admit_dispatch``) after the run was admitted or its
inherited context accepted, and before the payload is handed to the task node.

Nothing here reads a secret store or logs a value. A credential that cannot be matched to a decision, or whose type has
no known secret fields, is a refusal, never a pass: several toolkits treat a missing secret as anonymous access.
"""
from typing import Callable, Mapping, Optional

from tools import this

from .credential_decision import (
    CREDENTIAL_SECTION, CredentialDecisionError, _int_or_none, _payload_toolkits, _uuid_text,
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


def _prefetched_toolkits(payload) -> list:
    """The toolkits of the sub-agents the predict payload carries ready-expanded (``application.subagent_version_details``,
    expanded with the same unsecret as the run's own): material rides there too."""
    application = payload.get("application") if isinstance(payload, dict) else None
    prefetched = application.get("subagent_version_details") if isinstance(application, dict) else None
    if not isinstance(prefetched, dict):
        return []
    return [tool for details in prefetched.values() if isinstance(details, dict) and isinstance(details.get("tools"), list)
            for tool in details["tools"] if isinstance(tool, dict)]


def dispatched_toolkits(payload) -> list:
    """Every toolkit that carries settings in a dispatched payload: the run's own and the prefetched sub-agents'."""
    return [*_payload_toolkits(payload), *_prefetched_toolkits(payload)]


# --- free-form secret templates (checked before the payload-wide unsecret) --------------------

def _template_outside_configurations(node, budget, depth) -> bool:
    budget[0] -= 1
    if budget[0] < 0 or depth > MAX_SCAN_DEPTH:
        return True   # a payload too large or deep to look through counts as holding one
    if isinstance(node, str):
        return SECRET_TEMPLATE in node
    if isinstance(node, dict):
        if CONFIGURATION_TYPE in node:
            return False   # a configuration: its secret fields are sealed (credentials) or platform-internal
        return any(_template_outside_configurations(value, budget, depth + 1) for value in node.values())
    if isinstance(node, (list, tuple)):
        return any(_template_outside_configurations(value, budget, depth + 1) for value in node)
    return False


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
    """Does a toolkit of this payload hold a ``{{secret.*}}`` template outside a configuration dict? Always ``False``
    (and nothing is read) when governance is off. Call it before the payload is unsecreted."""
    from .governance import is_governed  # pylint: disable=C0415

    if not is_governed(module):
        return False
    budget = [MAX_SCAN_NODES]
    return any(
        _template_outside_configurations(tool.get("settings"), budget, 0) for tool in dispatched_toolkits(payload))


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


def strip_secret_fields(config: dict, paths) -> list:
    """Remove every secret path from a configuration dict (empty ones too) and return the paths that held a value,
    in schema order. Asserts the result: a path still present is a refusal."""
    held = [path for path in paths if _remove_path(config, path)]
    if any(_path_present(config, path) for path in paths):
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
        and isinstance(slot.get("fields"), list) and all(isinstance(path, str) and path for path in slot["fields"])
    )


# --- sealing a payload -----------------------------------------------------------------------

def seal_payload(  # pylint: disable=R0912,R0913,R0914
    payload, entries_of: Callable[[], list], *, secret_fields_of: Callable = configuration_secret_fields,
    classify: Optional[Callable] = None, decided: bool = False,
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
    """
    from .application_utils import _find_configurations_in_settings  # pylint: disable=C0415

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
        found = _find_configurations_in_settings(settings)
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
