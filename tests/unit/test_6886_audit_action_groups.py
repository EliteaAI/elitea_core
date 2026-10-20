"""#6886: audit trail ``action_prefix`` filter and ``group_by=action`` counts.

The admin "Input detections" view lists ``input:<category>.<rule>:<action>`` counts. Run
against SQLite with a table shaped like ``audit_events``; the API module needs tools and
flask only to be importable, so those are stubbed.
"""
import importlib.util
import pathlib
import sys
import types

import pytest
from sqlalchemy import Boolean, Column, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
Base = declarative_base()


class AuditEvent(Base):
    __tablename__ = 'audit_events'
    id = Column(Integer, primary_key=True)
    action = Column(String)
    event_type = Column(String)
    is_error = Column(Boolean, default=False)


@pytest.fixture()
def audit_api(monkeypatch):
    passthrough = lambda *a, **k: (lambda f: f)
    tools = types.ModuleType('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, with_modes=lambda urls: urls,
    )
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=passthrough))
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration')
    monkeypatch.setitem(sys.modules, 'tools', tools)
    # Other suites leave bare flask/pylon stubs in sys.modules; bring our own
    flask = types.ModuleType('flask')
    flask.request = types.SimpleNamespace(args={})
    monkeypatch.setitem(sys.modules, 'flask', flask)
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(error=lambda *a, **k: None)
    for name, module in (('pylon', types.ModuleType('pylon')), ('pylon.core', types.ModuleType('pylon.core')), ('pylon.core.tools', pylon_tools)):
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location('audit_api_6886_under_test', PLUGIN_ROOT / 'api' / 'v2' / 'audit.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.AdminAPI


@pytest.fixture()
def session():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add_all([AuditEvent(action=a, event_type='guardrail') for a in (
            'input:credentials.github_token:block',
            'input:credentials.github_token:block',
            'input:credentials.github_token:block',
            'input:personal_data.email:redact',
            'input:credentials.jwt:block',
            'output:credentials.jwt:block',
            'input_x:other',
        )])
        s.commit()
        yield s


def test_group_by_action_counts_most_frequent_first(audit_api, session):
    result = audit_api._group_by_action(session.query(AuditEvent), AuditEvent, limit=50, offset=0)

    assert result['total'] == 5
    assert result['groups'][0] == {'action': 'input:credentials.github_token:block', 'count': 3}
    # Ties order by action so pages are stable
    assert [g['action'] for g in result['groups'][1:]] == [
        'input:credentials.jwt:block', 'input:personal_data.email:redact', 'input_x:other',
        'output:credentials.jwt:block',
    ]


def test_group_by_action_pages(audit_api, session):
    result = audit_api._group_by_action(session.query(AuditEvent), AuditEvent, limit=2, offset=1)

    assert result['total'] == 5
    assert [g['action'] for g in result['groups']] == ['input:credentials.jwt:block', 'input:personal_data.email:redact']


def test_action_prefix_is_a_literal_prefix(audit_api, session):
    query = session.query(AuditEvent).filter(AuditEvent.action.startswith('input:', autoescape=True))
    result = audit_api._group_by_action(query, AuditEvent, limit=50, offset=0)

    assert {g['action'] for g in result['groups']} == {
        'input:credentials.github_token:block', 'input:credentials.jwt:block', 'input:personal_data.email:redact',
    }


def test_action_prefix_escapes_like_wildcards(session):
    query = session.query(AuditEvent).filter(AuditEvent.action.startswith('input_', autoescape=True))

    assert [r.action for r in query] == ['input_x:other']


def test_endpoint_wires_prefix_and_group_by():
    source = (PLUGIN_ROOT / 'api' / 'v2' / 'audit.py').read_text()

    assert 'AuditEvent.action.startswith(action_prefix, autoescape=True)' in source
    assert 'return self._group_by_action(query, AuditEvent, limit, offset), 200' in source
