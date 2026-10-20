"""EL-7011 US-10 R3.1: an owner change marks the request span through the shared schedule span marker."""
import sys
import types
from types import SimpleNamespace

from unit.utils.test_7011_new_owner_eligibility import load_owner

SERVICE = "plugins.elitea_core.utils.schedule_service"


def test_7011_r3_1_owner_change_marks_the_span_with_the_schedule(monkeypatch):
    """TC-US10-05, TC-US10-06: the audit marker is the schedule_service one, called with the schedule."""
    marked = []
    service = types.ModuleType(SERVICE)
    service.mark_audit_span = marked.append
    monkeypatch.setitem(sys.modules, SERVICE, service)
    _, owner = load_owner(monkeypatch)
    schedule = SimpleNamespace(id=7, name="Nightly")
    owner.mark_owner_change_span(schedule)
    assert marked == [schedule]
