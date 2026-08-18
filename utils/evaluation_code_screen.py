"""Author-time AST pre-screen (Layer 1) for eval code-validation bodies (EVAL-P1-B1).

This is the *static* screen run when a code-validation definition is authored — it
rejects obviously-unsafe code (dangerous imports, escape builtins, dunder traversal)
before it is ever stored, so the editor can surface errors inline (§19, Layer 1).

It is **not** the sandbox. Actual isolated execution (network-denied Pyodide, resource
limits, the ``result`` bool/number contract) is Layer 2 and lives on the indexer RPC
(EVAL-H2). This pre-screen deliberately duplicates none of that — it only fails *closed*
on statically-detectable escapes. Keep this a **pure function** (no I/O, no DB) so it is
unit-testable and can be reused verbatim by the H2 path.
"""

import ast
from typing import List

# Modules that give filesystem / network / process / introspection reach. The list
# extends the design doc with the extras flagged during H2 verification
# (fnmatch, webbrowser, execfile-style escapes).
BLOCKED_IMPORTS = frozenset({
    'os', 'sys', 'subprocess', 'shutil', 'socket', 'ssl', 'select', 'selectors',
    'threading', 'multiprocessing', 'asyncio', 'concurrent',
    'ctypes', 'cffi', 'mmap', 'signal', 'resource', 'gc', 'inspect',
    'importlib', 'imp', 'builtins', '__builtin__', 'pkgutil', 'runpy',
    'pathlib', 'glob', 'fnmatch', 'tempfile', 'fileinput', 'io',
    'pickle', 'shelve', 'marshal', 'dbm', 'sqlite3',
    'requests', 'httpx', 'aiohttp', 'urllib', 'urllib2', 'urllib3', 'http',
    'ftplib', 'smtplib', 'poplib', 'imaplib', 'telnetlib', 'webbrowser',
    'platform', 'pty', 'tty', 'ptyprocess',
})

# Builtins that execute arbitrary code, do I/O, or break out of the sandbox namespace.
BLOCKED_BUILTINS = frozenset({
    'exec', 'eval', 'compile', 'execfile', '__import__', 'open',
    'input', 'breakpoint', 'globals', 'locals', 'vars', 'memoryview',
    'help', 'exit', 'quit',
})

# Dunder attribute traversal used to reach the object graph / builtins from a literal.
BLOCKED_ATTRS = frozenset({
    '__globals__', '__builtins__', '__subclasses__', '__bases__', '__mro__',
    '__class__', '__code__', '__closure__', '__dict__', '__getattribute__',
    '__import__', '__loader__', '__spec__', '__reduce__', '__reduce_ex__',
})


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

    for node in ast.walk(tree):
        # import os / import socket, ...
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split('.')[0]
                if root in BLOCKED_IMPORTS:
                    violations.append(f"import of '{alias.name}' is not allowed (line {node.lineno})")
        # from os import ... / from urllib.request import ...
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or '').split('.')[0]
            if root in BLOCKED_IMPORTS:
                violations.append(f"import from '{node.module}' is not allowed (line {node.lineno})")
        # exec(...) / eval(...) / open(...) / __import__(...)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in BLOCKED_BUILTINS:
                violations.append(f"call to '{node.func.id}()' is not allowed (line {node.lineno})")
        # x.__globals__ / ().__class__ ...
        elif isinstance(node, ast.Attribute):
            if node.attr in BLOCKED_ATTRS:
                violations.append(f"access to '{node.attr}' is not allowed (line {node.lineno})")
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
