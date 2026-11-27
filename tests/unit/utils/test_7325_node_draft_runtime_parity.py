"""EL-7325: the node draft models match the real SDK runtime that builds the pipeline.

The SDK is read as source (``ast``), never imported, so the test needs nothing from langchain. For each
node type the AI can draft, a small abstract interpreter walks the node loop of ``create_graph``
(``elitea_sdk/runtime/langchain/langraph_agent.py``): it decides every ``node_type`` check for that type,
stops a branch at ``continue``/``raise``, and collects the ``node`` keys the runtime reads. Those keys must
be exactly what ``node_draft.schema`` lets the model write, apart from the keys it hides on purpose.

It runs against every SDK copy it finds: the workspace checkout and the copies installed for the indexer.
It skips when there is none (CI without the SDK), like test_6388_pipeline_authoring_twin.

Run via:
    python tests/run_tests.py unit/utils/test_7325_node_draft_runtime_parity.py -v
"""
import ast
import os
import pathlib
import sys

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PLUGIN_ROOT / 'tests'))

from fixtures.node_draft import load  # noqa: E402  pylint: disable=C0413

SCHEMA = load('utils.node_draft.schema')
VALIDATE = load('utils.node_draft.validate')
REGISTRY = load('utils.pipeline_authoring.registry')
VALIDATOR = load('utils.pipeline_authoring.validator')

RUNTIME_FILE = 'elitea_sdk/runtime/langchain/langraph_agent.py'
MAPPING_FILE = 'elitea_sdk/runtime/langchain/utils.py'
# Read at the top of the loop for every node, whatever its type (node id and type)
STRUCTURAL_KEYS = frozenset({'id', 'type'})
# Read at the top of the loop for every node, but only used by the tool-backed types
TOOL_LOOKUP_KEYS = frozenset({'tool', 'toolkit_name'})
TOOL_TYPES = frozenset({'toolkit', 'mcp', 'agent'})
# Known differences between the runtime and what the draft may write, each with the reason.
# A difference that disappears fails the test below, so this table can't go stale.
RUNTIME_GAPS = {
    # The router/decision branch shares one edge that reads `routes`; a decision node has none, so the
    # edge gets []. The draft must not start writing routes on a decision node.
    ('decision', 'runtime_only'): {'routes'},
    # The editor shows a Structured output switch and the registry lists it, but create_graph never
    # passes structured_output to FunctionTool for toolkit/mcp nodes (langraph_agent.py, create_graph).
    ('toolkit', 'draft_only'): {'structured_output'},
    ('mcp', 'draft_only'): {'structured_output'},
}


def _sdk_roots():
    """Every SDK source tree near this plugin: $ELITEA_SDK_PATH, a checkout next to a parent, the indexer copies."""
    candidates = [pathlib.Path(os.environ['ELITEA_SDK_PATH'])] if os.environ.get('ELITEA_SDK_PATH') else []
    for parent in PLUGIN_ROOT.parents:
        candidates += [parent / name for name in ('elitea-sdk', 'elitea_sdk', 'Alita_SDK')]
        candidates += sorted(parent.glob('pylon_indexer/requirements/*/lib/python*/site-packages'))
    seen, roots = set(), []
    for root in candidates:
        if (root / RUNTIME_FILE).is_file() and root.resolve() not in seen:
            seen.add(root.resolve())
            roots.append(root)
    return roots


SDK_ROOTS = _sdk_roots()
pytestmark = pytest.mark.skipif(not SDK_ROOTS, reason='elitea-sdk source not found next to this plugin')


# ---- abstract interpreter over create_graph's node loop -----------------------------------------

class _NodeKeyReader:
    """Keys read from `node` when `node_type == node_type_value`."""

    def __init__(self, module: ast.Module, node_type_value: str):
        self.type = node_type_value
        self.functions = {f.name: f for f in module.body if isinstance(f, ast.FunctionDef)}

    def run(self, statements) -> set:
        keys, _ = self._block(statements)
        return keys

    def _block(self, statements):
        keys = set()
        for statement in statements:
            found, terminated = self._statement(statement)
            keys |= found
            if terminated:
                return keys, True
        return keys, False

    def _statement(self, statement):
        if isinstance(statement, (ast.Continue, ast.Raise, ast.Return)):
            return self._reads(statement), True
        if isinstance(statement, ast.If):
            keys = self._reads(statement.test)
            verdict = self._evaluate(statement.test)
            if verdict is True:
                found, terminated = self._block(statement.body)
                return keys | found, terminated
            if verdict is False:
                found, terminated = self._block(statement.orelse)
                return keys | found, terminated
            body, body_done = self._block(statement.body)
            other, other_done = self._block(statement.orelse)
            return keys | body | other, body_done and other_done
        if isinstance(statement, (ast.For, ast.While, ast.With, ast.Try)):
            keys = set()
            for field in ('iter', 'test', 'items'):
                part = getattr(statement, field, None)
                for item in part if isinstance(part, list) else [part] if part is not None else []:
                    keys |= self._reads(item)
            for field in ('body', 'orelse', 'finalbody'):
                keys |= self._block(getattr(statement, field, []) or [])[0]
            for handler in getattr(statement, 'handlers', []) or []:
                keys |= self._block(handler.body)[0]
            return keys, False
        return self._reads(statement), False

    def _evaluate(self, test):
        """True/False when the test is decided by node_type alone, None when it depends on more."""
        if isinstance(test, ast.Compare) and isinstance(test.left, ast.Name) and test.left.id == 'node_type' \
                and len(test.ops) == 1:
            op, right = test.ops[0], test.comparators[0]
            values = ({right.value} if isinstance(right, ast.Constant)
                      else {e.value for e in right.elts if isinstance(e, ast.Constant)}
                      if isinstance(right, (ast.List, ast.Tuple, ast.Set)) else None)
            if values is None:
                return None
            matched = self.type in values
            return {ast.Eq: matched, ast.In: matched, ast.NotEq: not matched, ast.NotIn: not matched}.get(type(op))
        if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
            inner = self._evaluate(test.operand)
            return None if inner is None else not inner
        if isinstance(test, ast.BoolOp):
            verdicts = [self._evaluate(v) for v in test.values]
            if isinstance(test.op, ast.And):
                return False if False in verdicts else True if all(v is True for v in verdicts) else None
            return True if True in verdicts else False if all(v is False for v in verdicts) else None
        return None

    def _reads(self, tree) -> set:
        keys = set()
        for item in ast.walk(tree):
            if isinstance(item, ast.Call) and isinstance(item.func, ast.Attribute) and item.func.attr == 'get' \
                    and _is_node(item.func.value) and item.args and isinstance(item.args[0], ast.Constant):
                keys.add(item.args[0].value)
            elif isinstance(item, ast.Subscript) and _is_node(item.value) and isinstance(item.slice, ast.Constant):
                keys.add(item.slice.value)
            elif isinstance(item, ast.Call) and isinstance(item.func, ast.Name) \
                    and item.func.id in self.functions and _passes_node(item):
                keys |= _node_keys_in(self.functions[item.func.id])
        return keys


def _is_node(expr) -> bool:
    return isinstance(expr, ast.Name) and expr.id == 'node'


def _passes_node(call: ast.Call) -> bool:
    return any(_is_node(a) for a in call.args) or any(_is_node(k.value) for k in call.keywords)


def _node_keys_in(function: ast.FunctionDef) -> set:
    reader = _NodeKeyReader(ast.Module(body=[], type_ignores=[]), '')
    return reader._reads(function)  # pylint: disable=W0212


def _parse(root: pathlib.Path, relative: str) -> ast.Module:
    return ast.parse((root / relative).read_text(encoding='utf-8'))


def _function(module: ast.Module, name: str) -> ast.FunctionDef:
    return next(n for n in ast.walk(module) if isinstance(n, ast.FunctionDef) and n.name == name)


def _node_loop(module: ast.Module) -> ast.For:
    create_graph = _function(module, 'create_graph')
    return next(n for n in ast.walk(create_graph)
                if isinstance(n, ast.For) and isinstance(n.target, ast.Name) and n.target.id == 'node')


def _dispatched_types(loop: ast.For) -> set:
    types_ = set()
    for item in ast.walk(loop):
        if isinstance(item, ast.Compare) and isinstance(item.left, ast.Name) and item.left.id == 'node_type':
            for right in item.comparators:
                if isinstance(right, ast.Constant):
                    types_.add(right.value)
                elif isinstance(right, (ast.List, ast.Tuple, ast.Set)):
                    types_ |= {e.value for e in right.elts if isinstance(e, ast.Constant)}
    return types_


def _runtime_keys(root: pathlib.Path, node_type: str) -> set:
    """Node keys the runtime uses for this type: what its branch reads, minus the per-node bookkeeping
    every type goes through (structural keys, the tool lookup for non-tool types, legacy inline blocks)."""
    module = _parse(root, RUNTIME_FILE)
    keys = _NodeKeyReader(module, node_type).run(_node_loop(module).body) - STRUCTURAL_KEYS
    if node_type not in TOOL_TYPES:
        keys -= TOOL_LOOKUP_KEYS
    return keys - (_inline_construct_keys() - _branch_keys(module, node_type))


def _branch_keys(module: ast.Module, node_type: str) -> set:
    """Keys read inside the branches that test for this node type only (e.g. the router's own condition)."""
    reader, found = _NodeKeyReader(module, node_type), set()
    for item in ast.walk(_node_loop(module)):
        if isinstance(item, ast.If) and reader._evaluate(item.test) is True:  # pylint: disable=W0212
            found |= reader._reads(ast.Module(body=item.body, type_ignores=[]))  # pylint: disable=W0212
    return found


def _inline_construct_keys() -> set:
    """Legacy inline `condition:`/`decision:` blocks the runtime still honours on any node."""
    return set(REGISTRY.get_node_registry()['inline_constructs'])


@pytest.fixture(params=SDK_ROOTS, ids=lambda root: str(root))
def sdk(request):
    return request.param


# ---- tests ------------------------------------------------------------------------------------

def test_runtime_dispatches_every_supported_type(sdk):
    module = _parse(sdk, RUNTIME_FILE)
    missing = set(SCHEMA.SUPPORTED_TYPES) - _dispatched_types(_node_loop(module))
    assert not missing, f'create_graph no longer builds: {sorted(missing)}'


@pytest.mark.parametrize('node_type', SCHEMA.SUPPORTED_TYPES)
def test_model_keys_match_runtime_keys(sdk, node_type):
    runtime = _runtime_keys(sdk, node_type)
    drafted = set(SCHEMA.allowed_keys(node_type))
    runtime_only = runtime - drafted - set(SCHEMA.HIDDEN_KEYS)
    draft_only = drafted - runtime

    assert runtime_only == RUNTIME_GAPS.get((node_type, 'runtime_only'), set()), (
        f'{node_type}: the runtime reads {sorted(runtime_only)}, which the AI draft can never set. '
        'Add them to the registry (node_registry.yaml), to schema.HIDDEN_KEYS, or to RUNTIME_GAPS with a reason.')
    assert draft_only == RUNTIME_GAPS.get((node_type, 'draft_only'), set()), (
        f'{node_type}: the AI draft may set {sorted(draft_only)}, which the runtime never reads. '
        'Remove them from the registry, or list them in RUNTIME_GAPS with a reason.')


def test_hidden_keys_are_still_read_by_the_runtime(sdk):
    """A hidden key the runtime stopped reading is dead weight in schema.HIDDEN_KEYS."""
    read_anywhere = set().union(*(_runtime_keys(sdk, t) for t in _dispatched_types(
        _node_loop(_parse(sdk, RUNTIME_FILE)))))
    assert set(SCHEMA.HIDDEN_KEYS) <= read_anywhere


def test_value_object_types_match_the_runtime(sdk):
    """propagate_the_input_mapping handles `fstring` and `fixed` and treats anything else as a variable."""
    function = _function(_parse(sdk, MAPPING_FILE), 'propagate_the_input_mapping')
    compared = {c.value for item in ast.walk(function) if isinstance(item, ast.Compare)
                for c in item.comparators if isinstance(c, ast.Constant) and isinstance(c.value, str)}
    named = compared & set(VALIDATOR.MAPPING_TYPES)
    assert named == {'fstring', 'fixed'}, f'the runtime now handles {sorted(compared)}'
    assert set(VALIDATOR.MAPPING_TYPES) - named == {'variable'}


def test_agent_required_mapping_matches_the_runtime(sdk):
    """validate_agent_mappings rejects an agent node without these input_mapping keys."""
    function = _function(_parse(sdk, RUNTIME_FILE), 'validate_agent_mappings')
    required = next({e.value for e in item.value.elts}
                    for item in ast.walk(function)
                    if isinstance(item, ast.Assign) and isinstance(item.value, ast.Set)
                    and any(isinstance(t, ast.Name) and t.id == 'required_agent_params' for t in item.targets))
    checked = {path.split('.', 1)[1] for group in VALIDATE.required_text('agent') for path in group
               if path.startswith('input_mapping.')}
    assert required <= checked, f'the runtime requires input_mapping {sorted(required)}'


def test_routing_matches_the_runtime(sdk):
    """Types that route by `transition` read it; types with their own routes never do."""
    for node_type in SCHEMA.SUPPORTED_TYPES:
        reads_transition = 'transition' in _runtime_keys(sdk, node_type)
        routing = (REGISTRY.node_spec(node_type) or {}).get('routing')
        assert reads_transition == (routing == 'transition'), (node_type, routing)


def test_reader_notices_runtime_drift(sdk):
    """The check is real: a runtime that starts reading a new key, or stops reading one, is caught."""
    source = (sdk / RUNTIME_FILE).read_text(encoding='utf-8')
    read_by_state_modifier = "variables_to_clean=node.get('variables_to_clean', []),"
    read_by_printer = "final_message=node.get('final_message'),"
    assert read_by_state_modifier in source and read_by_printer in source
    drifted = ast.parse(source
                        .replace(read_by_state_modifier, read_by_state_modifier + " keep=node.get('keep_history'),")
                        .replace(read_by_printer, 'final_message=None,'))
    loop = _node_loop(drifted)
    assert 'keep_history' in _NodeKeyReader(drifted, 'state_modifier').run(loop.body)
    assert 'final_message' not in _NodeKeyReader(drifted, 'printer').run(loop.body)
    # and the per-type branches don't leak into each other
    assert 'variables_to_clean' not in _NodeKeyReader(drifted, 'llm').run(loop.body)


def test_state_types_are_accepted_by_the_runtime(sdk):
    """create_state turns every type the draft may add to `state` into a real channel."""
    function = _function(_parse(sdk, MAPPING_FILE), 'create_state')
    accepted = set()
    for item in ast.walk(function):
        if isinstance(item, ast.Compare) and any(isinstance(op, ast.In) for op in item.ops):
            for right in item.comparators:
                if isinstance(right, (ast.List, ast.Tuple, ast.Set)):
                    accepted |= {e.value for e in right.elts if isinstance(e, ast.Constant)}
    models = load('models.pd.node_draft')
    assert set(models.STATE_TYPES) <= accepted, f'the runtime accepts {sorted(accepted)}'
