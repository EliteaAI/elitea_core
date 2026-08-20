"""Author-time AST pre-screen (Layer 1) for eval code-validation bodies (EVAL-P1-B1).

This is the *static* screen run when a code-validation definition is authored — it
rejects unsafe code (imports outside a small allow-list, escape builtins, dunder traversal)
before it is ever stored, so the editor can surface errors inline (§19, Layer 1).

Imports are an **allow-list** (:data:`ALLOWED_IMPORTS`) while builtins and dunders remain
deny-lists. That asymmetry is deliberate: the importable surface is the stdlib, which is too large
and too fluid to enumerate the dangerous half of — every deny-list revision so far has been
answered with another module that executes strings or reads paths — whereas the builtin and dunder
vocabularies are small and fixed.

It is **not** the sandbox. Actual isolated execution (network-denied Pyodide, resource
limits, the ``result`` bool/number contract) is Layer 2 and lives on the indexer RPC
(EVAL-H2). This pre-screen deliberately duplicates none of that — it only fails *closed*
on statically-detectable escapes. Keep this a **pure function** (no I/O, no DB) so it is
unit-testable and can be reused verbatim by the H2 path.
"""

import ast
from typing import List

# Modules a scoring snippet may import — an **allow-list**, deliberately.
#
# A deny-list cannot end this class of finding: the review round that produced this change
# demonstrated seven fresh escapes (``pdb.run``, ``cProfile.run``, ``timeit.timeit``,
# ``code.InteractiveInterpreter().runsource``, ``linecache.getlines``, ``zipfile``, ``tarfile``)
# simply by naming stdlib modules nobody had thought to enumerate, and the stdlib will keep
# supplying more. Everything here is pure computation over values already in scope: no
# filesystem, no network, no process, no code execution, no introspection.
ALLOWED_IMPORTS = frozenset({
    'math', 'cmath', 'statistics', 'decimal', 'fractions', 'numbers',
    're', 'json', 'string', 'textwrap', 'unicodedata', 'difflib',
    'collections', 'itertools', 'functools', 'operator',
    'datetime', 'calendar', 'time',
    'hashlib', 'base64', 'binascii', 'typing',
})

# Builtins that execute arbitrary code, do I/O, or break out of the sandbox namespace.
# The reflection trio (getattr/setattr/delattr) is here because it reaches every dunder in
# BLOCKED_ATTRS through a *string*, which no attribute-access rule can see.
BLOCKED_BUILTINS = frozenset({
    'exec', 'eval', 'compile', 'execfile', '__import__', 'open',
    'input', 'breakpoint', 'globals', 'locals', 'vars', 'memoryview',
    'help', 'exit', 'quit',
    'getattr', 'setattr', 'delattr',
})

# Dunder attribute traversal used to reach the object graph / builtins from a literal.
BLOCKED_ATTRS = frozenset({
    '__globals__', '__builtins__', '__subclasses__', '__bases__', '__mro__',
    '__class__', '__code__', '__closure__', '__dict__', '__getattribute__',
    '__import__', '__loader__', '__spec__', '__reduce__', '__reduce_ex__',
})


def _allowed_module_aliases(tree: ast.AST) -> frozenset:
    """Local names bound to an allow-listed module by an ``import`` in this body.

    ``import re`` binds ``re``, ``import re as r`` binds ``r``; a dotted ``import a.b`` binds only
    ``a``. Blocked imports are reported separately, so their names are deliberately not collected —
    an alias for a rejected module stays unrecognised here and its calls keep being screened.
    """
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split('.')[0] in ALLOWED_IMPORTS:
                    names.add(alias.asname or alias.name.split('.')[0])
    return frozenset(names)


def _is_allowed_module_ref(value: ast.AST, module_aliases: frozenset) -> bool:
    """Whether ``value`` is a reference to an allow-listed module (``re``, ``json.decoder``).

    Used to exempt ``re.compile(...)`` from the builtin-call rule: the rule matches on the
    attribute name alone, which cannot otherwise tell a module's method from the builtin of the
    same name.
    """
    while isinstance(value, ast.Attribute):
        value = value.value
    return isinstance(value, ast.Name) and value.id in module_aliases


def screen_validation_code(code: str) -> List[str]:
    """Statically screen a code-validation body. Returns a list of human-readable
    violation strings; an empty list means the body passed Layer 1.

    Fails *closed*: a syntax error is itself a violation (the body cannot be stored).
    """
    violations: List[str] = []

    if not code or not code.strip():
        return ['code body is empty']

    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return [f'syntax error: {exc.msg} (line {exc.lineno})']

    # Names bound to an allow-listed module, so `re.compile(...)` is not mistaken for the
    # builtin `compile`. Collected up front because ast.walk does not visit in source order.
    module_aliases = _allowed_module_aliases(tree)

    for node in ast.walk(tree):
        # import os / import socket, ...
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split('.')[0]
                if root not in ALLOWED_IMPORTS:
                    violations.append(f"import of '{alias.name}' is not allowed (line {node.lineno})")
        # from os import ... / from urllib.request import ...
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or '').split('.')[0]
            if root not in ALLOWED_IMPORTS:
                violations.append(f"import from '{node.module}' is not allowed (line {node.lineno})")
        # exec(...) / eval(...) / open(...) / __import__(...), and the same names reached as
        # an attribute (`m.eval(...)`). A call dispatched through a subscript is rejected
        # outright: `tbl['eval']()` cannot be resolved statically and a scoring snippet has
        # no legitimate reason to dispatch that way.
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in BLOCKED_BUILTINS:
                violations.append(f"call to '{node.func.id}()' is not allowed (line {node.lineno})")
            elif (isinstance(node.func, ast.Attribute) and node.func.attr in BLOCKED_BUILTINS
                    and not _is_allowed_module_ref(node.func.value, module_aliases)):
                violations.append(f"call to '{node.func.attr}()' is not allowed (line {node.lineno})")
            elif isinstance(node.func, ast.Subscript):
                violations.append(f'calling a subscripted value is not allowed (line {node.lineno})')
        # x.__globals__ / ().__class__ ...
        elif isinstance(node, ast.Attribute):
            if node.attr in BLOCKED_ATTRS:
                violations.append(f"access to '{node.attr}' is not allowed (line {node.lineno})")
        # A blocked dunder named as a *string* — `d['__builtins__']`, `getattr(o, '__globals__')`,
        # `__import__` looked up in a dict. Attribute rules are blind to these, which is what
        # made the object-graph walk reachable at all. Matched against BLOCKED_ATTRS rather than
        # by dunder *shape*: shape-matching also rejected snippets that merely mention a harmless
        # dunder as data (`result = '__init__' not in input`), which is a legitimate thing for a
        # scoring snippet to assert about an agent's output.
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value in BLOCKED_ATTRS:
                violations.append(f"reference to '{node.value}' is not allowed (line {node.lineno})")
        # getattr(o, '__globals__') style dynamic escapes referenced as bare names
        elif isinstance(node, ast.Name) and node.id in BLOCKED_BUILTINS:
            # 'input' is also the harness-injected evidence variable (§19.4) — a bare
            # reference to it is legitimate case-input access, not an escape. Calls to
            # input(...) are still blocked above (the Call branch checks BLOCKED_BUILTINS
            # independently of this bare-name check).
            if node.id == 'input':
                continue
            # a bare reference (e.g. passing `exec` as a callback) is also an escape
            violations.append(f"reference to '{node.id}' is not allowed (line {node.lineno})")

    return violations
