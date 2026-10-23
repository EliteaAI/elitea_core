"""EL-7011 US-18 R1.1, R3.1, R3.2: the run starter auto-approves only with the rule and the project secret.

The real starter runs on faked neighbours; the project secret check is faked.
"""
import sys
import types

import pytest
from unit.utils.test_7011_run_starter_pipeline import OWNER, PKG, _module, load_starter

PROJECT_ID, SCHEDULE_ID, RUN_ID = 34, 1, 3


@pytest.fixture
def run(monkeypatch):
    predicts, secret_reads = [], []

    def allowed(project_id):
        secret_reads.append(project_id)
        return allowed.value

    monkeypatch.setitem(
        sys.modules, f"{PKG}.utils.schedule_risk", _module(f"{PKG}.utils.schedule_risk",
                                                           is_run_without_asking_allowed=allowed))
    starter = load_starter(
        monkeypatch,
        create=lambda **kw: ("uuid-1", 7, "resp-1"),
        predict=lambda **kw: predicts.append(kw),
        prerun=lambda *a: types.SimpleNamespace(ok=True, owner_id=OWNER),
    )

    def start(rule, secret, entity_type="pipeline"):
        allowed.value = secret
        loaded = types.SimpleNamespace(
            schedule=types.SimpleNamespace(
                id=SCHEDULE_ID, entity_type=entity_type, run_mode="prompt", approval_rule=rule),
            version=types.SimpleNamespace(id=9), entity_name="Flow", prompt="Go", variables=[])
        monkeypatch.setattr(starter, "_load", lambda *a: loaded)
        starter.run_owner_schedule(PROJECT_ID, SCHEDULE_ID, RUN_ID)
        return predicts[-1], secret_reads

    return start


# R3.1 / R3.2
@pytest.mark.parametrize("entity_type", ["agent", "pipeline"])
def test_el_7011_r3_1_run_without_asking_with_the_secret_auto_approves(run, entity_type):
    kwargs, reads = run("run_without_asking", True, entity_type)

    assert kwargs["auto_approve_sensitive_actions"] is True
    assert reads == [PROJECT_ID]


# R3.2
def test_el_7011_r3_2_run_without_asking_without_the_secret_pauses(run):
    kwargs, _ = run("run_without_asking", False)

    assert kwargs["auto_approve_sensitive_actions"] is False


# R1.1
def test_el_7011_r1_1_pause_and_ask_pauses_and_never_reads_the_secret(run):
    kwargs, reads = run("pause_and_ask", True)

    assert kwargs["auto_approve_sensitive_actions"] is False
    assert reads == []
