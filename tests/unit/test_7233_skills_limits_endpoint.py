"""#7233 - the import dialog reads the skills-per-agent limit before any agent version exists."""
import ast
import pathlib

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _get_handler():
    tree = ast.parse((PLUGIN_ROOT / 'api/v2/skills_limits.py').read_text())
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == 'get')


def test_the_endpoint_returns_the_same_limit_the_attach_guard_enforces():
    assert ast.unparse(_get_handler().body[-1]) == "return ({'max_skills_per_agent': get_max_skills_per_agent()}, 200)"


def test_anyone_who_may_list_skills_can_read_the_limit():
    assert "'models.applications.skills.list'" in ast.unparse(_get_handler().decorator_list[0])
