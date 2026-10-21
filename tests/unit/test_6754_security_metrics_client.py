"""C-1009 (#6754): the governed-dispatch counter uses the shared tool, or a local fallback."""

import importlib.util
import sys
import time
import types
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[2]


@pytest.fixture
def client(monkeypatch):
    lines = []
    log = types.SimpleNamespace(info=lambda msg, *a: lines.append(msg), debug=lambda *a: None)
    monkeypatch.setitem(sys.modules, "pylon", types.ModuleType("pylon"))
    monkeypatch.setitem(sys.modules, "pylon.core", types.ModuleType("pylon.core"))
    monkeypatch.setitem(sys.modules, "pylon.core.tools", types.SimpleNamespace(log=log))
    monkeypatch.setitem(sys.modules, "tools", types.ModuleType("tools"))
    spec = importlib.util.spec_from_file_location(
        "security_metrics_client_under_test", PLUGIN_ROOT / "utils" / "security_metrics_client.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return types.SimpleNamespace(mod=module, lines=lines)


class SharedStub:
    def __init__(self):
        self.calls, self.counters = [], {}

    def counter(self, name, description, labels=()):
        self.calls.append((name, description, tuple(labels)))
        return self.counters.setdefault(name, SharedCounter())


class SharedCounter:
    def __init__(self):
        self.incs, self.n = [], 0

    def inc(self, value=1, **labels):
        self.incs.append((value, labels))
        self.n += value

    def total(self, **labels):
        return self.n


def test_without_the_tool_it_counts_locally_and_logs_the_structured_line(client):
    counter = client.mod.SecurityCounter("governed_dispatch_unavailable_total", "d")

    counter.inc()
    client.mod._clock = lambda: time.monotonic() + 3600  # past the log rate-cap window
    counter.inc()

    assert counter.total() == 2
    assert client.lines == ["security_metric name=governed_dispatch_unavailable_total value=1"] * 2


def test_with_the_tool_it_delegates_and_does_not_log_itself(client):
    shared = SharedStub()
    sys.modules["tools"].security_metrics = shared
    counter = client.mod.SecurityCounter("governed_dispatch_unavailable_total", "d")

    counter.inc()

    assert shared.counters["governed_dispatch_unavailable_total"].incs == [(1, {})]
    assert shared.calls == [("governed_dispatch_unavailable_total", "d", ())]
    assert client.lines == []  # the shared helper owns the log line
    assert counter.total() == 1


def test_tool_registered_after_import_is_picked_up_lazily(client):
    counter = client.mod.SecurityCounter("c_total", "d")
    counter.inc()  # before tracing is loaded: local
    sys.modules["tools"].security_metrics = SharedStub()
    counter.inc()  # after: shared

    assert counter.total() == 2


def test_labels_are_validated_and_tokenised_in_the_fallback(client):
    counter = client.mod.SecurityCounter("c_total", "d", ("capability",))

    counter.inc(capability="a b")
    with pytest.raises(ValueError):
        counter.inc()

    assert client.lines == ["security_metric name=c_total value=1 capability=a_b"]
    assert counter.total(capability="a b") == 1 and counter.total(capability="x") == 0


def test_the_dispatch_gate_counts_through_the_shared_counter():
    source = (PLUGIN_ROOT / "utils" / "worker_capabilities.py").read_text()

    assert "GOVERNED_DISPATCH_UNAVAILABLE = SecurityCounter(" in source
    assert '"governed_dispatch_unavailable_total"' in source
    assert "prometheus_client" not in source
