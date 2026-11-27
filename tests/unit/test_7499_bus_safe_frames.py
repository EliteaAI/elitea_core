"""#7499: every pylon on the event bus unpickles every frame, so frames carry no elitea_core classes.

pylon_auth loads no elitea_core plugin. An enum member in an event name or payload, or a python-mode
model dump, made it log a ModuleNotFoundError traceback for each such event.
"""
import ast
import datetime
import pathlib
import types

import pytest

from fixtures.bus_frame import assert_bus_safe, foreign_values
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
INDEX_SCHEDULING = PLUGIN_ROOT / 'utils' / 'index_scheduling.py'

PLANTED = {
    'enum member as the name': (
        "from ..models.enums.events import ApplicationEvents\n"
        "ctx.fire_event(ApplicationEvents.toolkit_updated, {})"),
    'enum member in the payload': (
        "from ..models.enums.all import NotificationEventTypes\n"
        "ctx.fire_event('notifications_stream', {'event_type': NotificationEventTypes.chat_user_added})"),
    'enum members in a conditional': (
        "from ..models.enums.all import NotificationEventTypes as X\n"
        "ctx.fire_event('notifications_stream', {'event_type': X.a if expired else X.b})"),
    'enum member through an alias': (
        "from ..models.enums.all import NotificationEventTypes as Types\n"
        "ctx.fire_event('notifications_stream', {'project_id': 1, 'event_type': Types.schedule_expired})"),
    'enum member by lookup': (
        "from ..models.enums.all import NotificationEventTypes as Types\n"
        "ctx.fire_event('notifications_stream', {'event_type': Types(event_type)})"),
    'python-mode model_dump': "ctx.fire_event('application_deleted', Detail.from_orm(app).model_dump())",
    'python-mode dict': "ctx.fire_event('toolkit_updated', {'data': details.dict()})",
}

CONVERTED = (
    "from ..models.enums.all import NotificationEventTypes as Types\n"
    "from ..models.enums.events import ApplicationEvents\n"
    "ctx.fire_event(ApplicationEvents.toolkit_updated.value, {\n"
    "    'event_type': Types(event_type).value,\n"
    "    'kind': Types.a.value if expired else Types.b.value,\n"
    "    'data': Detail.from_orm(app).model_dump(mode='json'),\n"
    "})"
)


def _enum_names(tree):
    """Local names the file binds to classes imported from an enums module, aliases included."""
    return {alias.asname or alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and 'enums' in (node.module or '')
            for alias in node.names}


def _is_fire_event(node):
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'fire_event'


def _is_enum_member(node, enum_names):
    """`Enum.member` or `Enum(value)`: both evaluate to a member, not to its string."""
    if isinstance(node, ast.Attribute):
        return isinstance(node.value, ast.Name) and node.value.id in enum_names
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in enum_names


def _is_python_mode_dump(node):
    if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
        return False
    json_mode = any(keyword.arg == 'mode' and getattr(keyword.value, 'value', None) == 'json'
                    for keyword in node.keywords)
    return node.func.attr == 'dict' or (node.func.attr == 'model_dump' and not json_mode)


def bus_frame_violations(source, filename='<snippet>'):
    """Each place a fire_event call in source puts an enum member or a python-mode model dump on the bus."""
    tree = ast.parse(source, filename)
    enum_names = _enum_names(tree)
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    found = []
    for call in filter(_is_fire_event, ast.walk(tree)):
        for argument in [*call.args[:2], *(keyword.value for keyword in call.keywords)]:
            for node in ast.walk(argument):
                parent = parents.get(node)
                converted = isinstance(parent, ast.Attribute) and parent.attr == 'value'
                if (_is_enum_member(node, enum_names) and not converted) or _is_python_mode_dump(node):
                    found.append(f'{filename}:{node.lineno} {ast.unparse(node)}')
    return found


def _sources_that_fire_events():
    for path in sorted(PLUGIN_ROOT.rglob('*.py')):
        relative = path.relative_to(PLUGIN_ROOT)
        if relative.parts[0] in ('tests', 'static', 'node_modules') or '__pycache__' in relative.parts:
            continue
        text = path.read_text(encoding='utf-8')
        if 'fire_event' in text:
            yield relative, text


@pytest.fixture
def enums(isolated_sys_modules):
    return load_module_with_stubs(PLUGIN_ROOT / 'models' / 'enums' / 'all.py', 'plugins.elitea_core.models.enums.all')


def test_no_fire_event_call_in_the_plugin_puts_a_plugin_class_on_the_bus():
    sources = list(_sources_that_fire_events())

    assert len(sources) > 10
    assert [found for relative, text in sources for found in bus_frame_violations(text, str(relative))] == []


@pytest.mark.parametrize('source', PLANTED.values(), ids=PLANTED.keys())
def test_the_scan_catches_each_form_that_broke_pylon_auth(source):
    assert bus_frame_violations(source)


def test_the_scan_accepts_the_converted_forms():
    assert bus_frame_violations(CONVERTED) == []


def test_the_frame_check_rejects_plugin_classes_and_accepts_stdlib_values(enums):
    assert foreign_values({'event_type': enums.NotificationEventTypes.chat_user_added}) == [
        "frame['event_type']: plugins.elitea_core.models.enums.all.NotificationEventTypes",
    ]
    assert_bus_safe('notifications_stream', {
        'event_type': enums.NotificationEventTypes.chat_user_added.value,
        'meta': {'created_at': datetime.datetime(2026, 11, 15), 'ids': [1, 2], 'pair': ('a', None), 'ok': True},
    })


def test_a_failed_scheduled_index_reports_its_initiator_as_a_plain_string(enums):
    tree = ast.parse(INDEX_SCHEDULING.read_text(encoding='utf-8'))
    handler = next(node for node in tree.body
                   if isinstance(node, ast.FunctionDef) and node.name == 'handle_failed_index_schedule')
    sent = []
    namespace = {
        'InitiatorType': enums.InitiatorType,
        'CONCLUDED': True,
        'NOT_CONCLUDED': False,
        'IndexMetaLockTimeoutError': type('IndexMetaLockTimeoutError', (Exception,), {}),
        'index_log_context': lambda *args: 'ctx',
        'log': types.SimpleNamespace(**{level: lambda *args, **kwargs: None
                                        for level in ('debug', 'info', 'warning', 'exception')}),
        'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=types.SimpleNamespace(
            timeout=lambda seconds: types.SimpleNamespace(
                configurations_expand=lambda **kwargs: {'connection_string': 'postgresql://'}),
        ))),
        'update_toolkit_index_meta_history_with_failed_state': lambda *args, **kwargs: {'flipped': True},
        'this': types.SimpleNamespace(module=types.SimpleNamespace(notify_index_data_status=sent.append)),
    }
    exec(compile(ast.Module([handler], []), str(INDEX_SCHEDULING), 'exec'), namespace)  # noqa: S102

    namespace['handle_failed_index_schedule'](1, {}, 7, types.SimpleNamespace(id=42, type='confluence'), 'docs', 'boom')

    assert [status['initiator'] for status in sent] == ['schedule']
    assert_bus_safe('notifications_stream', {'meta': sent[0]})
