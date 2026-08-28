"""Issue #6232 - live-SQL physics for elitea_index_runs coordination (pg 18, real jsonb).

Runs against the platform's live Postgres through `docker exec ... psql` (the DB is not
published to the host), entirely inside a scratch schema that is dropped afterwards.

Proves, on the real engine:
- the guard-TOCTOU interleaving: a failed-state guard reading the runs table under the meta
  row's FOR UPDATE stays authoritative because registration takes the same meta lock before
  its INSERT (a refactor deleting that "unnecessary" lock reopens the hole silently);
- registration arbitration by the partial unique index (stale heartbeats have no carve-out,
  cancelled tombstones never block);
- the run-scoped cancel DELETE semantics compiled from the real ORM predicates: multi-index
  and untyped stamped rows are caught, while the meta row, legacy rows and typed active
  rows (the Jira-attachment data-loss regression) all survive a Stop.

Run via:
    python tests/run_tests.py integration/test_6232_live_sql_run_coordination.py -v
"""

import importlib.util
import json
import pathlib
import subprocess
import threading
import time
import uuid

import pytest
from sqlalchemy import delete, or_
from sqlalchemy.dialects import postgresql


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PSQL = ["docker", "exec", "-i", "centry-postgres-1",
        "psql", "-U", "centry", "-d", "db", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1"]


def _load_indexer_model():
    spec = importlib.util.spec_from_file_location(
        "indexer_model_6232", PLUGIN_ROOT / "models" / "indexer.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


indexer = _load_indexer_model()


def psql(sql: str) -> str:
    result = subprocess.run(PSQL, input=sql, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise AssertionError(f"psql failed:\n{sql}\n--- stderr ---\n{result.stderr}")
    return result.stdout.strip()


class PsqlSession:
    """One persistent psql connection whose statements can interleave with another's."""

    def __init__(self, schema):
        self.proc = subprocess.Popen(
            PSQL, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1,
        )
        self.lines = []
        self.lock = threading.Lock()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.send(f"SET search_path TO {schema};")

    def _read(self):
        for line in self.proc.stdout:
            with self.lock:
                self.lines.append(line.rstrip("\n"))

    def send(self, sql):
        self.proc.stdin.write(sql + "\n")
        self.proc.stdin.flush()

    def mark(self, name):
        self.send(f"\\echo {name}")

    def saw(self, name):
        with self.lock:
            return any(line == name for line in self.lines)

    def wait_for(self, name, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.saw(name):
                return
            time.sleep(0.05)
        with self.lock:
            output = "\n".join(self.lines)
        raise AssertionError(f"marker {name} never arrived; output so far:\n{output}")

    def close(self):
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        self.proc.wait(timeout=10)


def _docker_pg_available():
    try:
        return subprocess.run(
            PSQL, input="SELECT 1;", capture_output=True, text=True, timeout=20
        ).returncode == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_pg_available(),
    reason="live Postgres (docker exec centry-postgres-1 psql) is not reachable",
)


@pytest.fixture()
def schema():
    name = f"r14_scratch_core_{uuid.uuid4().hex[:8]}"
    statuses = ", ".join(f"'{s}'" for s in indexer.INDEX_RUN_STATUSES)
    psql(f"""
        CREATE SCHEMA {name};
        CREATE TABLE {name}.langchain_pg_embedding (
            id TEXT PRIMARY KEY,
            cmetadata JSONB,
            document TEXT
        );
        CREATE TABLE {name}.elitea_index_runs (
            run_id TEXT PRIMARY KEY,
            collection TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '{indexer.INDEX_RUN_PENDING}'
                CHECK (status IN ({statuses})),
            task_id TEXT,
            started_on DOUBLE PRECISION NOT NULL,
            heartbeat DOUBLE PRECISION NOT NULL,
            promoted_on DOUBLE PRECISION
        );
        CREATE UNIQUE INDEX {indexer.INDEX_RUN_LIVE_INDEX_NAME}
            ON {name}.elitea_index_runs (collection)
            WHERE {indexer.INDEX_RUN_LIVE_INDEX_PREDICATE};
        CREATE INDEX ix_elitea_index_runs_collection
            ON {name}.elitea_index_runs (collection);
        INSERT INTO {name}.langchain_pg_embedding (id, cmetadata, document) VALUES
            ('meta-1', '{{"type": "index_meta", "collection": "docs", "state": "in_progress"}}', 'index_meta_docs');
    """)
    yield name
    psql(f"DROP SCHEMA {name} CASCADE;")


def _register_sql(run_id, collection="docs", heartbeat_expr="extract(epoch from now())"):
    return (
        f"INSERT INTO elitea_index_runs (run_id, collection, status, started_on, heartbeat) "
        f"VALUES ('{run_id}', '{collection}', '{indexer.INDEX_RUN_PENDING}', "
        f"extract(epoch from now()), {heartbeat_expr}) "
        f"ON CONFLICT (collection) WHERE {indexer.INDEX_RUN_LIVE_INDEX_PREDICATE} DO NOTHING "
        f"RETURNING run_id;"
    )


class TestRegistrationArbitration:

    def test_the_partial_unique_index_refuses_a_second_pending_registration(self, schema):
        assert psql(f"SET search_path TO {schema}; {_register_sql('r1')}") == "r1"
        assert psql(f"SET search_path TO {schema}; {_register_sql('r2')}") == ""

    def test_a_stale_heartbeat_has_no_carve_out_so_the_sweep_must_run_first(self, schema):
        psql(f"SET search_path TO {schema}; "
             f"{_register_sql('r1', heartbeat_expr='extract(epoch from now()) - 40000')}")
        assert psql(f"SET search_path TO {schema}; {_register_sql('r2')}") == ""

    def test_a_cancelled_tombstone_never_blocks_a_new_registration(self, schema):
        psql(f"SET search_path TO {schema}; {_register_sql('r1')}")
        psql(f"SET search_path TO {schema}; "
             f"UPDATE elitea_index_runs SET status = '{indexer.INDEX_RUN_CANCELLED}' "
             f"WHERE run_id = 'r1';")
        assert psql(f"SET search_path TO {schema}; {_register_sql('r2')}") == "r2"


class TestGuardToctouInterleaving:
    """Registration takes the meta row's FOR UPDATE as a pure lock before its INSERT,
    solely so a guard's in-lock runs-table read stays authoritative. These interleavings
    pin that coupling against being refactored away as unnecessary."""

    META_LOCK = "SELECT id FROM langchain_pg_embedding WHERE cmetadata->>'type' = 'index_meta' AND cmetadata->>'collection' = 'docs' FOR UPDATE;"
    PROBE = ("SELECT count(*) FROM elitea_index_runs "
             f"WHERE collection = 'docs' AND status = '{indexer.INDEX_RUN_PENDING}';")

    def test_a_queued_registration_cannot_commit_while_the_guard_holds_the_meta_lock(self, schema):
        guard, registration = PsqlSession(schema), PsqlSession(schema)
        try:
            guard.send("BEGIN;")
            guard.send(self.META_LOCK)
            guard.mark("GUARD_LOCKED")
            guard.wait_for("GUARD_LOCKED")

            registration.send("BEGIN;")
            registration.send(self.META_LOCK)
            registration.send(_register_sql("r1"))
            registration.send("COMMIT;")
            registration.mark("REG_DONE")

            time.sleep(1.0)
            assert not registration.saw("REG_DONE"), (
                "registration committed while the guard held the meta lock - "
                "the R2 coupling is broken"
            )

            guard.send(self.PROBE)
            guard.send("UPDATE langchain_pg_embedding SET cmetadata = "
                       "jsonb_set(cmetadata, '{state}', '\"failed\"') WHERE id = 'meta-1';")
            guard.send("COMMIT;")
            guard.mark("GUARD_DONE")
            guard.wait_for("GUARD_DONE")

            registration.wait_for("REG_DONE")
            assert psql(f"SET search_path TO {schema}; {self.PROBE}") == "1"
        finally:
            guard.close()
            registration.close()

    def test_a_registration_that_committed_first_is_seen_by_the_guard_probe(self, schema):
        psql(f"SET search_path TO {schema}; {_register_sql('r1')}")
        guard = PsqlSession(schema)
        try:
            guard.send("BEGIN;")
            guard.send(self.META_LOCK)
            guard.send(self.PROBE)
            guard.send("ROLLBACK;")
            guard.mark("GUARD_DONE")
            guard.wait_for("GUARD_DONE")
            assert guard.saw("1"), "the in-lock probe must see the committed pending row"
        finally:
            guard.close()


def _saw_containing(session, needle, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with session.lock:
            if any(needle in line for line in session.lines):
                return True
        time.sleep(0.05)
    return False


class TestWholeIndexDeleteLockOrder:
    """The whole-index DELETE endpoint follows the universal lock order (meta row →
    run rows → chunk rows). Chunk-first ordering AB-BA deadlocks against a concurrent
    Stop, which locks meta then run rows before its run-scoped chunk delete."""

    CANCEL_META_LOCK = ("SELECT id FROM langchain_pg_embedding "
                       "WHERE cmetadata->>'type' = 'index_meta' "
                       "AND cmetadata->>'collection' = 'docs' FOR UPDATE;")
    CANCEL_RUN_LOCK = "SELECT run_id FROM elitea_index_runs WHERE collection = 'docs' FOR UPDATE;"
    CANCEL_CHUNK_DELETE = ("DELETE FROM langchain_pg_embedding "
                           """WHERE cmetadata @> '{"_elitea_run_id": "rP"}' """
                           "AND (cmetadata->>'type' IS NULL OR cmetadata->>'type' <> 'index_meta');")
    ENDPOINT_CHUNK_DELETE = ("DELETE FROM langchain_pg_embedding "
                             "WHERE cmetadata->>'collection' = 'docs';")
    ENDPOINT_RUN_DELETE = "DELETE FROM elitea_index_runs WHERE collection = 'docs';"

    def _seed(self, schema, meta_last):
        psql(f"SET search_path TO {schema}; {_register_sql('rP')}")
        if meta_last:
            psql(f"SET search_path TO {schema}; "
                 f"DELETE FROM langchain_pg_embedding WHERE id = 'meta-1';")
        psql(f"SET search_path TO {schema}; "
             f"INSERT INTO langchain_pg_embedding (id, cmetadata, document) VALUES "
             f"""('staged-1', '{{"collection": "docs", "_elitea_run_id": "rP"}}', 'doc'),"""
             f"""('staged-2', '{{"collection": "docs", "_elitea_run_id": "rP"}}', 'doc');""")
        if meta_last:
            psql(f"SET search_path TO {schema}; "
                 f"INSERT INTO langchain_pg_embedding (id, cmetadata, document) VALUES "
                 f"""('meta-1', '{{"type": "index_meta", "collection": "docs", "state": "in_progress"}}', 'index_meta_docs');""")

    def test_chunk_first_endpoint_order_deadlocks_against_a_concurrent_stop(self, schema):
        self._seed(schema, meta_last=True)
        cancel, endpoint = PsqlSession(schema), PsqlSession(schema)
        try:
            cancel.send("BEGIN;")
            cancel.send(self.CANCEL_META_LOCK)
            cancel.send(self.CANCEL_RUN_LOCK)
            cancel.mark("CANCEL_LOCKED")
            cancel.wait_for("CANCEL_LOCKED")

            endpoint.send("BEGIN;")
            endpoint.send(self.ENDPOINT_CHUNK_DELETE)
            endpoint.send(self.ENDPOINT_RUN_DELETE)
            endpoint.send("COMMIT;")
            time.sleep(0.5)

            cancel.send(self.CANCEL_CHUNK_DELETE)
            cancel.send("COMMIT;")
            assert (_saw_containing(cancel, "deadlock detected")
                    or _saw_containing(endpoint, "deadlock detected", timeout=5)), (
                "the inverted chunk-first order no longer deadlocks - "
                "re-check whether this pin is still needed"
            )
        finally:
            cancel.close()
            endpoint.close()

    def test_meta_first_endpoint_order_serializes_cleanly_behind_a_stop(self, schema):
        self._seed(schema, meta_last=False)
        cancel, endpoint = PsqlSession(schema), PsqlSession(schema)
        try:
            cancel.send("BEGIN;")
            cancel.send(self.CANCEL_META_LOCK)
            cancel.send(self.CANCEL_RUN_LOCK)
            cancel.mark("CANCEL_LOCKED")
            cancel.wait_for("CANCEL_LOCKED")

            endpoint.send("BEGIN;")
            endpoint.send("SELECT id FROM langchain_pg_embedding WHERE id = 'meta-1' FOR UPDATE;")
            endpoint.send(self.ENDPOINT_RUN_DELETE)
            endpoint.send(self.ENDPOINT_CHUNK_DELETE)
            endpoint.send("COMMIT;")
            endpoint.mark("ENDPOINT_DONE")
            time.sleep(1.0)
            assert not endpoint.saw("ENDPOINT_DONE"), (
                "the endpoint must queue on the meta lock, not proceed past a live Stop"
            )

            cancel.send(self.CANCEL_CHUNK_DELETE)
            cancel.send("UPDATE elitea_index_runs SET status = "
                        f"'{indexer.INDEX_RUN_CANCELLED}' WHERE run_id = 'rP';")
            cancel.send("COMMIT;")
            cancel.mark("CANCEL_DONE")
            cancel.wait_for("CANCEL_DONE")

            endpoint.wait_for("ENDPOINT_DONE")
            assert not _saw_containing(cancel, "deadlock detected", timeout=1)
            assert not _saw_containing(endpoint, "deadlock detected", timeout=1)
            assert psql(f"SET search_path TO {schema}; "
                        f"SELECT count(*) FROM langchain_pg_embedding "
                        f"WHERE cmetadata->>'collection' = 'docs';") == "0"
            assert psql(f"SET search_path TO {schema}; "
                        f"SELECT count(*) FROM elitea_index_runs;") == "0"
        finally:
            cancel.close()
            endpoint.close()


def _render(statement):
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    rendered = {}
    for name, value in compiled.params.items():
        if isinstance(value, dict):
            rendered[name] = "'" + json.dumps(value) + "'"
        else:
            rendered[name] = "'" + str(value).replace("'", "''") + "'"
    return sql % rendered


class TestCancelDeleteSemantics:
    """The exact ORM predicates cancel compiles, executed against real jsonb rows."""

    def _run_scoped_delete(self, run_id):
        E = indexer.EmbeddingStore
        return _render(delete(E).where(
            E.cmetadata.contains({"_elitea_run_id": run_id}),
            or_(E.cmetadata['type'].astext.is_(None),
                E.cmetadata['type'].astext != "index_meta"),
        ))

    def _seed(self, schema):
        rows = [
            ("legacy-1", {"collection": "docs", "id": "doc-legacy"}),
            ("typed-active", {"collection": "docs", "type": "attachment", "id": "doc-att"}),
            ("staged-typed", {"collection": "docs", "type": "attachment",
                              "_elitea_run_id": "rP"}),
            ("staged-untyped", {"collection": "docs", "_elitea_run_id": "rP"}),
            ("staged-multi", {"collection": "docs;other", "_elitea_run_id": "rP"}),
            ("other-run", {"collection": "docs", "_elitea_run_id": "rOld"}),
        ]
        values = ", ".join(
            f"('{row_id}', '{json.dumps(meta)}', 'doc')" for row_id, meta in rows
        )
        psql(f"SET search_path TO {schema}; "
             f"INSERT INTO langchain_pg_embedding (id, cmetadata, document) VALUES {values};")

    def test_the_run_scoped_delete_catches_multi_index_and_untyped_rows_only_for_that_run(self, schema):
        self._seed(schema)
        psql(f"SET search_path TO {schema}; {self._run_scoped_delete('rP')}")
        survivors = psql(
            f"SET search_path TO {schema}; "
            f"SELECT id FROM langchain_pg_embedding ORDER BY id;"
        ).splitlines()
        assert survivors == ["legacy-1", "meta-1", "other-run", "typed-active"]

    def test_stop_with_no_pending_stamped_rows_deletes_zero_active_rows(self, schema):
        """Test z2: typed (Jira-attachment-style) and untyped active rows all survive."""
        psql(f"SET search_path TO {schema}; INSERT INTO langchain_pg_embedding VALUES "
             f"""('att-1', '{{"collection": "docs", "type": "attachment"}}', 'doc'),"""
             f"""('plain-1', '{{"collection": "docs"}}', 'doc');""")
        before = psql(f"SET search_path TO {schema}; SELECT count(*) FROM langchain_pg_embedding;")
        psql(f"SET search_path TO {schema}; {self._run_scoped_delete('rNothingPending')}")
        after = psql(f"SET search_path TO {schema}; SELECT count(*) FROM langchain_pg_embedding;")
        assert before == after == "3"

    def test_the_meta_row_is_protected_even_if_it_were_ever_stamped(self, schema):
        psql(f"SET search_path TO {schema}; "
             f"UPDATE langchain_pg_embedding SET cmetadata = cmetadata || "
             f"""'{{"_elitea_run_id": "rP"}}' WHERE id = 'meta-1';""")
        psql(f"SET search_path TO {schema}; {self._run_scoped_delete('rP')}")
        assert psql(f"SET search_path TO {schema}; "
                    f"SELECT count(*) FROM langchain_pg_embedding WHERE id = 'meta-1';") == "1"
