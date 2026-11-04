"""#7404: the webhook secret writers of one project are serialised by a real PG advisory lock.

`utils/webhook_secrets.py` runs against PostgreSQL with a fake VaultClient whose write is slow, from two
threads with their own sessions, as two parallel creates would. Without the lock one key is lost (the hidden
store is one document written whole); with it both survive. A writer in another project is not blocked.
Set ELITEA_7011_PG_DSN as for test_7011_tick_claim.py:

    ELITEA_7011_PG_DSN=postgresql://centry:changeme@localhost:5432/db python3 run_tests.py integration/test_7404_secret_lock_pg.py

No table is touched: the lock is the only SQL.
"""
import os
import sys
import threading
import time
import types
import uuid

import pytest
from fixtures.helpers import load_module_with_stubs
from integration.test_7011_entity_schedule_tables import REAL_SQLALCHEMY

DSN = os.environ.get("ELITEA_7011_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="ELITEA_7011_PG_DSN is not set")
PACKAGE = "plugins.elitea_core"
PLUGIN_ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
PROJECT_A, PROJECT_B = 990001, 990002  # lock keys only; no schema is needed


class SlowVault:
    stores: dict = {}

    def __init__(self, project_id):
        self.project_id = project_id

    def get_hidden_secrets(self):
        return dict(SlowVault.stores.get(self.project_id, {}))

    def set_hidden_secrets(self, secrets):
        time.sleep(0.3)  # wide enough that an unlocked second reader would read the old document
        SlowVault.stores[self.project_id] = dict(secrets)


@pytest.fixture
def world(monkeypatch):
    for name, module in REAL_SQLALCHEMY.items():
        monkeypatch.setitem(sys.modules, name, module)
    for name in ("plugins", PACKAGE, f"{PACKAGE}.utils"):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    monkeypatch.setitem(sys.modules, "tools", types.SimpleNamespace(VaultClient=SlowVault))
    SlowVault.stores = {}
    secrets = load_module_with_stubs(PLUGIN_ROOT / "utils/webhook_secrets.py", f"{PACKAGE}.utils.webhook_secrets")
    sa, orm = REAL_SQLALCHEMY["sqlalchemy"], REAL_SQLALCHEMY["sqlalchemy.orm"]
    engine = sa.create_engine(DSN, pool_size=4)
    yield secrets, (lambda: orm.Session(engine))
    engine.dispose()


def _hook():
    return types.SimpleNamespace(uuid=uuid.uuid4(), webhook_type="github", gitlab_auth_method=None,
                                 secret_ref=None, signing_secret_ref=None)


def _create(secrets, session_factory, project_id, row, timings):
    with session_factory() as session:
        started = time.monotonic()
        secrets.store_row_secret(session, project_id, row)
        session.commit()  # releases the advisory lock
        timings.append(time.monotonic() - started)


def _parallel(secrets, session_factory, jobs):
    timings = []
    threads = [threading.Thread(target=_create, args=(secrets, session_factory, project, row, timings))
               for project, row in jobs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return sorted(timings)


def test_7404_two_parallel_creates_in_one_project_keep_both_keys(world):
    secrets, session_factory = world
    rows = [_hook(), _hook()]
    timings = _parallel(secrets, session_factory, [(PROJECT_A, row) for row in rows])
    assert set(SlowVault.stores[PROJECT_A]) == {f"webhook_secret_{row.uuid}" for row in rows}
    assert timings[1] >= 0.55  # the second writer waited for the first commit


def test_7404_creates_in_two_projects_do_not_wait_for_each_other(world):
    secrets, session_factory = world
    timings = _parallel(secrets, session_factory, [(PROJECT_A, _hook()), (PROJECT_B, _hook())])
    assert timings[1] < 0.55
