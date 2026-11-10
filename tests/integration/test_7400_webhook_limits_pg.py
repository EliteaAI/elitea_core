"""#7400 on a real PostgreSQL schema: the project advisory lock makes the running caps hold under a burst.

SQLite has no advisory lock, so only this suite proves the race. Each thread opens its own session and runs
`admit_webhook_run` exactly as the door does (count and insert in one transaction, then commit). Reuses the
`world` fixture of test_7398_webhook_rows_pg.py (own `p_<big id>` schema, dropped after).

    ELITEA_7011_PG_DSN=postgresql://centry:changeme@localhost:5432/db python3 run_tests.py integration/test_7400_webhook_limits_pg.py
"""
import sys
import threading
from datetime import UTC, datetime

from integration.test_7398_webhook_rows_pg import (  # noqa: F401
    PROJECT_ID, SCHEMA, _add, _migrate, pytestmark, world,
)
from integration.test_7453_webhook_door_pg import _door

BURST = 20


def _limits(per_webhook=3, per_project=10):
    sys.modules["tools"].this.module.descriptor.config["scheduler"] = {"schedules": {
        "webhook_max_concurrent_runs": per_webhook, "webhook_max_concurrent_runs_limit": 20,
        "webhook_project_max_concurrent_runs": per_project}}


def _burst(world, door, hooks, now=None):
    """BURST threads released together; each admits one run of `hooks[i % len(hooks)]` and records a refusal."""
    gate, answers, errors = threading.Barrier(BURST), [], []

    def deliver(index):
        hook = hooks[index % len(hooks)]
        try:
            gate.wait()
            with world.session() as session:
                answer = door.admit_webhook_run(session, PROJECT_ID, hook)
                if isinstance(answer, door.Capped):
                    door.record_capped(session, hook, now or datetime.now(UTC))
                session.commit()
            answers.append((hook, answer))
        except Exception as error:  # noqa: BLE001 -- reported by the test, not swallowed
            errors.append(error)

    threads = [threading.Thread(target=deliver, args=(index,)) for index in range(BURST)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert errors == []
    return answers


def _running(world, hook=None):
    where = "status = 'running'" + (" AND schedule_id = :i" if hook else "")
    (count,) = world.sql(f"SELECT count(*) FROM {SCHEMA}.entity_schedule_runs WHERE {where}", i=hook)[0]
    return count


def test_a_burst_of_twenty_deliveries_never_starts_more_than_the_webhook_cap(world, monkeypatch):
    _migrate(world)
    _limits(per_webhook=3)
    door = _door(world, monkeypatch)
    hook = _add(world, trigger_kind="webhook", webhook_type="custom")

    answers = _burst(world, door, [hook], now=datetime(2026, 11, 10, 9, 0, 5, tzinfo=UTC))

    assert sum(isinstance(answer, int) for _, answer in answers) == 3
    assert _running(world, hook) == 3
    skipped = world.sql(f"SELECT events FROM {SCHEMA}.entity_schedule_runs "
                        f"WHERE schedule_id = :i AND status = 'skipped'", i=hook)
    assert len(skipped) == 1 and skipped[0][0][-1]["count"] == BURST - 3  # AC6: one row, the counter


def test_a_burst_over_two_webhooks_never_passes_the_project_cap(world, monkeypatch):
    _migrate(world)
    _limits(per_webhook=3, per_project=4)
    door = _door(world, monkeypatch)
    hooks = [_add(world, trigger_kind="webhook", webhook_type="custom") for _ in range(2)]

    _burst(world, door, hooks)

    assert _running(world) == 4
    assert all(_running(world, hook) <= 3 for hook in hooks)
