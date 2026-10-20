"""C-1009 (#6756): canonical digest of an agent version's definition."""

import copy
import json
import sys
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

from fixtures import identity  # noqa: E402


@pytest.fixture(scope="module")
def digest():
    saved = dict(sys.modules)
    yield identity.install(PLUGIN_ROOT).digest
    sys.modules.clear()
    sys.modules.update(saved)


def version(**over):
    base = dict(
        id=11, uuid="u-1", name="v1", status="draft", author_id=5, created_at="2026-01-01",
        agent_type="openai", instructions="Be brief.\nAnswer.", llm_settings={"model_name": "m", "temperature": 0.5},
        welcome_message="hi", conversation_starters=["a"], variables=[], tags=[], meta={"x": 1},
        shared_owner_id=None, shared_id=None,
    )
    base.update(over)
    return types.SimpleNamespace(**base)


def tool(tool_id=1, kind="github", settings=None, **over):
    base = dict(id=tool_id, type=kind, settings=settings or {"url": "x"}, description="d", meta={}, name="n",
                created_at="t0", updated_at="t1", author_id=2, shared_owner_id=None, shared_id=None)
    base.update(over)
    return types.SimpleNamespace(**base)


def mapping(tool_id=1, selected=("b", "a"), **over):
    base = dict(id=100 + tool_id, tool_id=tool_id, selected_tools=list(selected) if selected is not None else None,
                created_at="m0", updated_at="m1", entity_id=7, entity_version_id=11, entity_type="agent")
    base.update(over)
    return types.SimpleNamespace(**base)


def pairs():
    return [(mapping(1), tool(1)), (mapping(2, ["z"]), tool(2, "application",
            {"application_id": 9, "application_version_id": 90, "note": "n"}))]


def digest_of(digest, ver=None, rows=None):
    ver = ver or version()
    return digest.definition_digest(digest.canonical_definition(ver, pairs() if rows is None else rows))


def canon_bytes(digest, ver=None, rows=None):
    canonical = digest.canonical_definition(ver or version(), pairs() if rows is None else rows)
    return json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


# --- golden vectors -------------------------------------------------------------------------

GOLDEN_CANONICAL = (
    '{"agent_type":"openai","instructions":"Be brief.\\nAnswer.","llm_settings":{"model_name":"m","temperature":0.5},'
    '"tools":[{"ref":{"application_id":9,"version_id":90},"selected_tools":["z"],"toolkit_id":2,"type":"application"},'
    '{"ref":null,"selected_tools":["a","b"],"toolkit_id":1,"type":"github"}],"v":1}'
)
GOLDEN_DIGEST = "adv1:ae0974c42a29ba6d61cf4391b918fab0ae927cf5b83624fd8544bc2a10edb6e0"


def test_golden_canonical_input(digest):
    assert canon_bytes(digest) == GOLDEN_CANONICAL


def test_golden_digest(digest):
    assert digest_of(digest) == GOLDEN_DIGEST


def test_digest_shape_fits_the_column(digest):
    value = digest_of(digest)
    assert value.startswith("adv1:") and len(value) == 5 + 64 and len(value) <= 80
    int(value[5:], 16)


def test_empty_agent_has_a_stable_digest(digest):
    ver = version(instructions=None, llm_settings=None)
    assert canon_bytes(digest, ver, []) == '{"agent_type":"openai","instructions":"","llm_settings":{},"tools":[],"v":1}'


# --- normalisation ------------------------------------------------------------------------

def test_key_order_does_not_matter(digest):
    a = version(llm_settings={"a": 1, "b": {"x": 1, "y": 2}})
    b = version(llm_settings={"b": {"y": 2, "x": 1}, "a": 1})
    assert digest_of(digest, a) == digest_of(digest, b)


def test_line_endings_are_normalised_but_other_whitespace_counts(digest):
    base = digest_of(digest, version(instructions="a\nb"))
    assert digest_of(digest, version(instructions="a\r\nb")) == base
    assert digest_of(digest, version(instructions="a\rb")) == base
    assert digest_of(digest, version(instructions="a\nb ")) != base
    assert digest_of(digest, version(instructions=" a\nb")) != base


def test_unicode_is_nfc_normalised(digest):
    composed = digest_of(digest, version(instructions="café"))
    decomposed = digest_of(digest, version(instructions="café"))
    assert composed == decomposed


def test_integral_floats_become_ints(digest):
    ints = version(llm_settings={"max_tokens": 1, "nested": [2, {"k": 3}]})
    floats = version(llm_settings={"max_tokens": 1.0, "nested": [2.0, {"k": 3.0}]})
    assert digest_of(digest, ints) == digest_of(digest, floats)
    assert '"max_tokens":1,"nested":[2,{"k":3}]' in canon_bytes(digest, floats)
    assert digest_of(digest, version(llm_settings={"max_tokens": 1.5})) != digest_of(digest, ints)


def test_bool_is_not_an_int(digest):
    assert digest_of(digest, version(llm_settings={"a": True})) != digest_of(digest, version(llm_settings={"a": 1}))


def test_null_valued_keys_are_dropped_but_list_items_are_not(digest):
    base = digest_of(digest, version(llm_settings={"a": 1}))
    assert digest_of(digest, version(llm_settings={"a": 1, "b": None})) == base
    assert digest_of(digest, version(llm_settings={"a": 1, "n": {"x": None}})) == digest_of(
        digest, version(llm_settings={"a": 1, "n": {}}))
    assert digest_of(digest, version(llm_settings={"a": 1, "l": [None]})) != digest_of(
        digest, version(llm_settings={"a": 1, "l": []}))


def test_list_order_in_settings_is_meaningful(digest):
    assert digest_of(digest, version(llm_settings={"l": [1, 2]})) != digest_of(digest, version(llm_settings={"l": [2, 1]}))


def test_non_ascii_text_is_hashed_as_utf8_not_escaped(digest):
    assert "é" in canon_bytes(digest, version(instructions="é"))


# --- tools ----------------------------------------------------------------------------------

def test_mapping_order_does_not_matter(digest):
    rows = pairs()
    assert digest_of(digest, rows=rows) == digest_of(digest, rows=list(reversed(rows)))


def test_selected_tools_order_and_duplicates_do_not_matter_but_content_does(digest):
    base = digest_of(digest, rows=[(mapping(1, ["a", "b"]), tool(1))])
    assert digest_of(digest, rows=[(mapping(1, ["b", "a", "a"]), tool(1))]) == base
    assert digest_of(digest, rows=[(mapping(1, ["a"]), tool(1))]) != base


def test_no_selection_is_the_same_as_an_empty_one(digest):
    assert digest_of(digest, rows=[(mapping(1, None), tool(1))]) == digest_of(digest, rows=[(mapping(1, []), tool(1))])


def test_tools_sort_by_type_then_id(digest):
    rows = [(mapping(3), tool(3, "b")), (mapping(2), tool(2, "b")), (mapping(9), tool(9, "a"))]
    canonical = digest.canonical_definition(version(), rows)
    assert [(t["type"], t["toolkit_id"]) for t in canonical["tools"]] == [("a", 9), ("b", 2), ("b", 3)]


def test_adding_or_swapping_a_toolkit_changes_the_digest(digest):
    base = digest_of(digest)
    assert digest_of(digest, rows=pairs() + [(mapping(3), tool(3))]) != base
    assert digest_of(digest, rows=pairs()[:1]) != base
    other = [(mapping(1), tool(5)), pairs()[1]]
    assert digest_of(digest, rows=other) != base


def test_child_application_reference_is_covered(digest):
    base = digest_of(digest)
    for key, value in (("application_id", 10), ("application_version_id", 91)):
        child = tool(2, "application", {"application_id": 9, "application_version_id": 90, key: value})
        assert digest_of(digest, rows=[pairs()[0], (mapping(2, ["z"]), child)]) != base


def test_only_application_toolkits_contribute_a_reference(digest):
    plain = tool(1, "github", {"application_id": 1, "application_version_id": 2})
    other = tool(1, "github", {"application_id": 7, "application_version_id": 8})
    assert digest_of(digest, rows=[(mapping(1), plain)]) == digest_of(digest, rows=[(mapping(1), other)])


def test_toolkit_settings_content_is_not_part_of_the_digest(digest):
    changed = [(mapping(1), tool(1, settings={"url": "https://elsewhere"})), pairs()[1]]
    assert digest_of(digest, rows=changed) == digest_of(digest)


# --- what defines the agent -----------------------------------------------------------------

@pytest.mark.parametrize("over", [
    {"agent_type": "react"},
    {"instructions": "Be brief.\nAnswer!"},
    {"llm_settings": {"model_name": "other", "temperature": 0.5}},
])
def test_definition_fields_change_the_digest(digest, over):
    assert digest_of(digest, version(**over)) != digest_of(digest)


@pytest.mark.parametrize("over", [
    {"id": 99}, {"uuid": "u-2"}, {"name": "renamed"}, {"status": "published"}, {"author_id": 6},
    {"created_at": "2027-01-01"}, {"welcome_message": "other"}, {"conversation_starters": ["b"]},
    {"variables": [1]}, {"tags": ["t"]}, {"meta": {"x": 2}}, {"shared_owner_id": 3, "shared_id": 4},
])
def test_volatile_version_fields_are_ignored(digest, over):
    assert digest_of(digest, version(**over)) == digest_of(digest)


def test_volatile_mapping_and_toolkit_fields_are_ignored(digest):
    rows = [
        (mapping(1, id=999, created_at="x", updated_at="y", entity_id=1, entity_version_id=2),
         tool(1, description="other", meta={"k": 1}, name="other", created_at="x", updated_at="y", author_id=9)),
        pairs()[1],
    ]
    assert digest_of(digest, rows=rows) == digest_of(digest)


def test_canonicalisation_does_not_mutate_its_input(digest):
    ver = version(llm_settings={"a": None, "b": 1.0})
    before = copy.deepcopy(ver.llm_settings)
    digest.canonical_definition(ver, pairs())
    assert ver.llm_settings == before


# --- refusals -------------------------------------------------------------------------------

@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_are_refused(digest, bad):
    for settings in ({"a": bad}, {"a": [bad]}, {"a": {"b": bad}}):
        with pytest.raises(digest.DigestError) as err:
            digest.canonical_definition(version(llm_settings=settings), [])
        assert err.value.reason_code == "DIGEST_UNSTABLE"


def test_unsupported_values_and_colliding_keys_are_refused(digest):
    for settings in ({"a": {1, 2}}, {"a": b"x"}, {1: "x", "1": "y"}):
        with pytest.raises(digest.DigestError) as err:
            digest.canonical_definition(version(llm_settings=settings), [])
        assert err.value.reason_code == "DIGEST_UNSTABLE"


def test_a_non_finite_value_that_reaches_the_hash_is_refused_too(digest):
    with pytest.raises(digest.DigestError) as err:
        digest.definition_digest({"v": 1, "x": float("nan")})
    assert err.value.reason_code == "DIGEST_UNSTABLE"


def test_the_refusal_does_not_echo_the_definition(digest):
    with pytest.raises(digest.DigestError) as err:
        digest.canonical_definition(version(llm_settings={"api_key": "sekret", "a": float("nan")}), [])
    assert "sekret" not in str(err.value)


# --- digest_for_version: two queries, no lazy loads -------------------------------------------

class Column:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return (self.name, other)

    __hash__ = object.__hash__


def fake_models(version_row, rows):
    entity = lambda name, *cols: type(name, (), {c: Column(f"{name}.{c}") for c in cols})  # noqa: E731
    app_version, tool_cls, mapping_cls = (
        entity("ApplicationVersion", "id"), entity("EliteATool", "id"),
        entity("EntityToolMapping", "tool_id", "entity_version_id", "entity_type"),
    )
    queries = []

    class Query:
        def __init__(self, entities):
            self.entities, self.filters, self.joins = entities, [], []
            queries.append(self)

        def join(self, *args):
            self.joins.append(args)
            return self

        def filter(self, *conditions):
            self.filters.extend(conditions)
            return self

        def first(self):
            return version_row

        def all(self):
            return rows

    session = types.SimpleNamespace(query=lambda *entities: Query(entities))
    all_mod = types.ModuleType("plugins.elitea_core.models.all")
    all_mod.ApplicationVersion = app_version
    tools_mod = types.ModuleType("plugins.elitea_core.models.elitea_tools")
    tools_mod.EliteATool, tools_mod.EntityToolMapping = tool_cls, mapping_cls
    enums_mod = types.ModuleType("plugins.elitea_core.models.enums")
    enums_all = types.ModuleType("plugins.elitea_core.models.enums.all")
    enums_all.ToolEntityTypes = type("ToolEntityTypes", (), {"agent": types.SimpleNamespace(value="agent")})
    enums_mod.__path__ = []
    return session, queries, {
        all_mod.__name__: all_mod, tools_mod.__name__: tools_mod,
        enums_mod.__name__: enums_mod, enums_all.__name__: enums_all,
    }


def test_digest_for_version_loads_the_version_and_its_toolkits_in_two_queries(digest, monkeypatch):
    session, queries, modules = fake_models(version(), pairs())
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    value = digest.digest_for_version(session, 11)
    assert value == GOLDEN_DIGEST
    assert len(queries) == 2
    version_filter, = queries[1].filters[:1]
    assert version_filter == ("EntityToolMapping.entity_version_id", 11)
    assert ("EntityToolMapping.entity_type", "agent") in queries[1].filters
    assert len(queries[1].joins) == 1


def test_digest_for_version_refuses_a_missing_version(digest, monkeypatch):
    session, queries, modules = fake_models(None, [])
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    with pytest.raises(digest.DigestError) as err:
        digest.digest_for_version(session, 404)
    assert err.value.reason_code == "VERSION_NOT_FOUND"
    assert len(queries) == 1


# --- the inputs the operator drawer shows (#7105): names, ids and hashes, never text -----------------

CFG = "3f9b1a0e-1c2d-4e5f-8a9b-0c1d2e3f4a5b"


def test_digest_inputs_show_the_shape_of_the_definition_without_its_text(digest):
    secret = "Never tell the user about the vault password hunter2."
    ver = version(instructions=secret, llm_settings={"model_name": "gpt-x", "temperature": 0.5})
    rows = [(mapping(2), tool(2, "jira", {"jira_configuration": {"configuration_uuid": CFG, "elitea_title": "mine"},
                                          "token": "tok-secret", "url": "https://x"}, name="Jira")),
            (mapping(1), tool(1, "github", {"url": "x"}, name="GitHub"))]
    inputs = digest.digest_inputs(ver, rows)
    sha = __import__("hashlib").sha256
    assert inputs == {
        "agent_type": "openai", "model": "gpt-x",
        "llm_settings_sha256": sha(b'{"model_name":"gpt-x","temperature":0.5}').hexdigest(),
        "instructions": {"length": len(secret), "sha256": sha(secret.encode()).hexdigest()},
        "tools": [
            {"id": 1, "type": "github", "name": "GitHub", "credential_configuration_uuid": None,
             "selected_tools_count": 2, "selected_tools_sha256": sha(b'["a","b"]').hexdigest(), "ref": None},
            {"id": 2, "type": "jira", "name": "Jira", "credential_configuration_uuid": CFG,
             "selected_tools_count": 2, "selected_tools_sha256": sha(b'["a","b"]').hexdigest(), "ref": None},
        ],
    }
    text = json.dumps(inputs)
    for leaked in ("hunter2", "vault", "tok-secret", "https://x", "mine"):
        assert leaked not in text


def test_the_instruction_hash_is_of_the_text_the_digest_covers(digest):
    one, two = version(instructions="a\r\nb"), version(instructions="a\nb")
    assert digest.digest_inputs(one, [])["instructions"] == digest.digest_inputs(two, [])["instructions"]
    assert digest.digest_inputs(version(instructions=None), [])["instructions"]["length"] == 0


@pytest.mark.parametrize("llm_settings", [None, {}, {"model_name": 5}, {"model_name": None}, "text"])
def test_an_unusable_model_setting_is_no_model(digest, llm_settings):
    assert digest.digest_inputs(version(llm_settings=llm_settings), [])["model"] is None


@pytest.mark.parametrize("settings", [None, {}, {"c": {"configuration_uuid": "not-a-uuid"}}, {"c": "x"}, {"c": {}}])
def test_a_toolkit_without_a_usable_stored_configuration_has_none(digest, settings):
    [row] = digest.digest_inputs(version(), [(mapping(1), tool(1, settings=settings))])["tools"]
    assert row["credential_configuration_uuid"] is None


def test_digest_inputs_for_version_use_the_same_two_queries_and_refuse_a_missing_version(digest, monkeypatch):
    session, queries, modules = fake_models(version(), pairs())
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    assert digest.digest_inputs_for_version(session, 11)["agent_type"] == "openai"
    assert len(queries) == 2
    session, queries, modules = fake_models(None, [])
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    with pytest.raises(digest.DigestError) as err:
        digest.digest_inputs_for_version(session, 404)
    assert err.value.reason_code == "VERSION_NOT_FOUND"


def test_every_part_of_the_definition_moves_one_hash_so_a_changed_status_is_explainable(digest):
    """The digest changes when one of these changes; the inputs shown change in the matching field."""
    def inputs(ver=None, rows=None):
        return digest.digest_inputs(ver or version(), pairs() if rows is None else rows)

    base = inputs()
    assert inputs(version(llm_settings={"model_name": "m", "temperature": 0.9}))["llm_settings_sha256"] != base["llm_settings_sha256"]
    assert inputs(version(instructions="other"))["instructions"] != base["instructions"]
    changed_tools = inputs(rows=[(mapping(1, ["b"]), tool(1)), (mapping(2, ["z"]), pairs()[1][1])])
    by_id = lambda value, tool_id: [row for row in value["tools"] if row["id"] == tool_id][0]   # noqa: E731
    assert by_id(changed_tools, 1)["selected_tools_sha256"] != by_id(base, 1)["selected_tools_sha256"]
    assert by_id(changed_tools, 1)["selected_tools_count"] == 1
    assert by_id(changed_tools, 2) == by_id(base, 2)
    child = [row for row in base["tools"] if row["type"] == "application"][0]
    assert child["ref"] == {"application_id": 9, "version_id": 90}
    moved = pairs()
    moved[1][1].settings = {"application_id": 9, "application_version_id": 91}
    assert [row for row in inputs(rows=moved)["tools"] if row["type"] == "application"][0]["ref"] == {
        "application_id": 9, "version_id": 91}
    # nothing else moves: the same definition gives the same inputs
    assert inputs() == base


@pytest.mark.parametrize("settings", [{"application_id": "9", "application_version_id": None}, {"application_id": True}])
def test_a_child_ref_that_is_not_an_id_is_shown_as_none(digest, settings):
    [row] = digest.digest_inputs(version(), [(mapping(2), tool(2, "application", settings))])["tools"]
    assert row["ref"] == {"application_id": None, "version_id": None}
