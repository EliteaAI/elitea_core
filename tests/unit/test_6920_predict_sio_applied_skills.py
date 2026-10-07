import ast
import pathlib

APPLICATION_RPC = pathlib.Path(__file__).resolve().parents[2] / 'rpc' / 'application.py'


def _predict_sio():
    tree = ast.parse(APPLICATION_RPC.read_text())
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == 'predict_sio')


def _is_applied_skills_payload_target(node):
    return (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name) and node.value.id == 'payload'
        and isinstance(node.slice, ast.Constant) and node.slice.value == 'applied_skills'
    )


def test_predict_sio_accepts_server_declared_applied_skills():
    assert 'applied_skills' in [arg.arg for arg in _predict_sio().args.args]


def test_declared_applied_skills_reach_the_payload_ahead_of_invoked_ones():
    guarded = [
        node for node in ast.walk(_predict_sio())
        if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == 'applied_skills'
    ]
    assert len(guarded) == 1
    assignment = guarded[0].body[0]
    assert isinstance(assignment, ast.Assign) and _is_applied_skills_payload_target(assignment.targets[0])
    declared, invoked = assignment.value.elts
    assert ast.unparse(declared) == '*applied_skills'
    assert ast.unparse(invoked) == "*payload.get('applied_skills', [])"
