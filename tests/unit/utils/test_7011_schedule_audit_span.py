"""EL-7011 US-10 R3.1: an owner change marks the request span through the shared schedule span marker."""
import sys
import types
from types import SimpleNamespace

from unit._tool_spend_loader import load

SERVICE = "plugins.elitea_core.utils.schedule_service"


def test_7011_r3_1_owner_change_marks_the_span_with_the_schedule(monkeypatch):
    """TC-US10-05, TC-US10-06: the audit marker is the schedule_service one, called with the schedule."""
    marked = []
    service = types.ModuleType(SERVICE)
    service.mark_audit_span = marked.append
    monkeypatch.setitem(sys.modules, SERVICE, service)
    load("utils/schedule_access.py")
    load("utils/schedule_prerun.py")
    owner = load("utils/schedule_owner.py", fresh=True)
    schedule = SimpleNamespace(id=7, name="Nightly")
    owner.mark_owner_change_span(schedule)
    assert marked == [schedule]
