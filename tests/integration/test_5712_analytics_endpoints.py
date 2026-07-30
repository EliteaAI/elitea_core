"""Issue #5712 — analytics-endpoint SQL invariants.

Seeds a synthetic ``audit_events`` table in an in-memory SQLite database and
runs the load-bearing SQLAlchemy query fragments from the four analytics
endpoints against it. This does not spin up Flask/auth/RPC — those layers
are re-shipping the same query dicts as before; the risk is in the SQL.

Covers:
  * analytics_agents cost_map + func.max() anti-inflation invariant
    (1 LLM row + N application rows on the same trace must not multiply the
    LLM cost by N)
  * analytics_user_detail llm_kpi filters event_type == 'llm'
  * analytics_costs per-agent calls / avg_cost round-trip
  * analytics_agent_detail avg_cost_per_call = llm_cost / llm_calls

Run via:
    python tests/run_tests.py integration/test_5712_analytics_endpoints.py -v
"""

import importlib
import pathlib
import sys
import types

import pytest
# Capture the REAL sqlalchemy submodules before any sibling test can install
# stubs into sys.modules (tests/stubs/orm.py does this). We re-inject these
# in the fixture so this file works whether it runs first or last.
import sqlalchemy as _real_sqlalchemy
import sqlalchemy.orm as _real_sqlalchemy_orm
import sqlalchemy.orm.attributes as _real_sqlalchemy_orm_attributes
import sqlalchemy.dialects.sqlite as _real_sqlite_dialect  # noqa: F401
_REAL_SQLA_MODULES = {
    "sqlalchemy": _real_sqlalchemy,
    "sqlalchemy.orm": _real_sqlalchemy_orm,
    "sqlalchemy.orm.attributes": _real_sqlalchemy_orm_attributes,
    "sqlalchemy.dialects": sys.modules["sqlalchemy.dialects"],
    "sqlalchemy.dialects.sqlite": _real_sqlite_dialect,
}

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Index, Integer, MetaData, Numeric,
    SmallInteger, String, Table, and_, case, create_engine, func, or_,
)
from sqlalchemy.orm import Session, aliased, declarative_base, mapped_column, Mapped
from datetime import datetime, timedelta, timezone


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


# --- Minimal replica of models/audit_event.py, sans pylon deps ---


@pytest.fixture(scope="module")
def audit_event_orm():
    """Standalone AuditEvent ORM against a fresh in-memory SQLite DB.

    We can't import the real model directly because it imports ``pylon``
    and ``tools`` at module scope. The columns and behavior tested here
    only exercise the shape and query logic — kept in sync with the real
    model manually. If the real model gains a column, add it here too.
    """
    # Re-inject the real sqlalchemy modules in case a sibling test replaced
    # them with stubs (tests/stubs/orm.py, test_5694_toolkit_selected_tools_migration.py,
    # test_list_collections_to_list_indexes_rename.py). Without this, when
    # this test runs after them in the same process, create_engine can't
    # locate the sqlite dialect.
    for name, mod in _REAL_SQLA_MODULES.items():
        sys.modules[name] = mod
    from decimal import Decimal
    from typing import Optional
    Base = declarative_base()

    class AuditEvent(Base):
        __tablename__ = "audit_events"

        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
        user_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
        user_email: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
        project_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
        event_type: Mapped[str] = mapped_column(String(32), nullable=False)
        action: Mapped[str] = mapped_column(String(512), nullable=False, default="")
        duration_ms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
        is_error: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
        entity_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
        entity_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
        entity_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
        tool_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
        model_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
        input_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
        output_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
        llm_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
        token_source: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
        cost_source: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
        trace_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
        span_id: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
        parent_span_id: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return AuditEvent, engine


def _mkevent(AE, **kwargs):
    """Build an AuditEvent with sensible defaults."""
    defaults = dict(
        timestamp=datetime(2026, 7, 1, 12, 0, 0),
        project_id=1,
        event_type="llm",
        action="test",
        is_error=False,
    )
    defaults.update(kwargs)
    return AE(**defaults)


class TestAnalyticsAgentsCostMap:
    """analytics_agents.py — verify the func.max() anti-inflation invariant."""

    def test_cost_map_max_prevents_join_fanout(self, audit_event_orm):
        """1 LLM row + N application rows sharing trace_id: cost must NOT multiply."""
        AuditEvent, engine = audit_event_orm
        with Session(engine) as session:
            # Clean the table for isolation across tests
            session.query(AuditEvent).delete()
            session.commit()

            # Agent 42 has 3 application events on trace T1, and ONE LLM event
            # on trace T1 with cost $0.05. Naive SUM in a joined query would
            # count the $0.05 three times → $0.15. The func.max() idiom in the
            # real code returns $0.05.
            session.add_all([
                _mkevent(AuditEvent, event_type="application", entity_type="application",
                         entity_id=42, entity_name="Agent42", trace_id="T1", action="run"),
                _mkevent(AuditEvent, event_type="application", entity_type="application",
                         entity_id=42, entity_name="Agent42", trace_id="T1", action="run"),
                _mkevent(AuditEvent, event_type="application", entity_type="application",
                         entity_id=42, entity_name="Agent42", trace_id="T1", action="run"),
                _mkevent(AuditEvent, event_type="llm", trace_id="T1", action="llm-call",
                         model_name="gpt-4o", input_tokens=100, output_tokens=50,
                         llm_cost=0.05),
            ])
            session.commit()

            # Reproduce the cost_map + func.max() pattern from analytics_agents.py.
            app_traces = session.query(
                AuditEvent.trace_id.label("trace_id"),
                func.min(AuditEvent.entity_id).label("entity_id"),
            ).filter(
                AuditEvent.entity_type == "application",
                AuditEvent.entity_id.isnot(None),
                AuditEvent.trace_id.isnot(None),
                AuditEvent.trace_id != "",
            ).group_by(AuditEvent.trace_id).subquery()

            llm_ev = aliased(AuditEvent)
            cost_map = session.query(
                app_traces.c.entity_id.label("entity_id"),
                func.sum(llm_ev.llm_cost).label("llm_cost"),
                func.sum(func.coalesce(llm_ev.input_tokens, 0)).label("input_tokens"),
                func.sum(func.coalesce(llm_ev.output_tokens, 0)).label("output_tokens"),
                func.count().label("llm_calls"),
            ).select_from(llm_ev).join(
                app_traces, llm_ev.trace_id == app_traces.c.trace_id,
            ).filter(
                llm_ev.event_type == "llm",
            ).group_by(app_traces.c.entity_id).subquery()

            # LEFT JOIN + GROUP BY entity_id with func.max() over cost_map columns.
            base = session.query(AuditEvent).filter(
                AuditEvent.entity_type == "application",
                AuditEvent.entity_id.isnot(None),
            )
            rows = base.outerjoin(
                cost_map, AuditEvent.entity_id == cost_map.c.entity_id,
            ).with_entities(
                AuditEvent.entity_id,
                func.count().label("events"),
                func.coalesce(func.max(cost_map.c.llm_cost), 0).label("llm_cost"),
                func.coalesce(func.max(cost_map.c.input_tokens), 0).label("input_tokens"),
                func.coalesce(func.max(cost_map.c.llm_calls), 0).label("llm_calls"),
            ).group_by(AuditEvent.entity_id).all()

            assert len(rows) == 1
            r = rows[0]
            assert r.entity_id == 42
            assert r.events == 3  # three application events
            # THE INVARIANT: not 0.15 (naïve SUM would fanout 3x)
            assert float(r.llm_cost) == pytest.approx(0.05, abs=1e-8)
            assert r.input_tokens == 100
            assert r.llm_calls == 1


class TestAnalyticsUserDetailLlmKpi:
    """analytics_user_detail.py — verify event_type='llm' filter."""

    def test_llm_kpi_ignores_non_llm_rows(self, audit_event_orm):
        AuditEvent, engine = audit_event_orm
        with Session(engine) as session:
            session.query(AuditEvent).delete()
            session.commit()

            # User 7: 2 tool events + 1 chat event + 1 LLM event with known cost.
            # llm_kpi must return values from only the LLM row.
            session.add_all([
                _mkevent(AuditEvent, event_type="tool", user_id=7, tool_name="t1",
                         input_tokens=99999, output_tokens=99999),  # must be excluded
                _mkevent(AuditEvent, event_type="tool", user_id=7, tool_name="t2",
                         input_tokens=99999, output_tokens=99999),
                _mkevent(AuditEvent, event_type="socketio", user_id=7,
                         action="SIO chat_predict", input_tokens=99999),
                _mkevent(AuditEvent, event_type="llm", user_id=7,
                         input_tokens=100, output_tokens=50, llm_cost=0.001),
            ])
            session.commit()

            base = session.query(AuditEvent).filter(
                AuditEvent.project_id == 1,
                AuditEvent.user_id == 7,
            )

            llm_kpi = base.with_entities(
                func.sum(func.coalesce(AuditEvent.input_tokens, 0)).label("input_tokens"),
                func.sum(func.coalesce(AuditEvent.output_tokens, 0)).label("output_tokens"),
                func.sum(AuditEvent.llm_cost).label("llm_cost"),
                func.count().label("llm_calls"),
            ).filter(AuditEvent.event_type == "llm").first()

            assert llm_kpi.input_tokens == 100, "must ignore tool/socketio rows"
            assert llm_kpi.output_tokens == 50
            assert float(llm_kpi.llm_cost) == pytest.approx(0.001, abs=1e-8)
            assert llm_kpi.llm_calls == 1


class TestAnalyticsCostsByAgent:
    """analytics_costs.py — verify calls / avg_cost round-trip on the by_agent aggregation."""

    def test_by_agent_calls_and_avg_cost(self, audit_event_orm):
        AuditEvent, engine = audit_event_orm
        with Session(engine) as session:
            session.query(AuditEvent).delete()
            session.commit()

            # Agent 99: 3 traces, each with 1 application row + 1 LLM row.
            # LLM costs 0.01, 0.02, 0.03 → total 0.06 over 3 calls → avg 0.02.
            for i, cost in enumerate([0.01, 0.02, 0.03], start=1):
                trace = f"trace-{i}"
                session.add_all([
                    _mkevent(AuditEvent, event_type="application", entity_type="application",
                             entity_id=99, entity_name="Agent99", trace_id=trace,
                             action="run"),
                    _mkevent(AuditEvent, event_type="llm", trace_id=trace,
                             action="llm-call", input_tokens=100, output_tokens=50,
                             llm_cost=cost),
                ])
            session.commit()

            base = session.query(AuditEvent).filter(
                AuditEvent.project_id == 1,
                AuditEvent.event_type == "llm",
            )

            app_trace_map = session.query(
                AuditEvent.trace_id.label("trace_id"),
                func.min(AuditEvent.entity_id).label("entity_id"),
                func.min(AuditEvent.entity_name).label("entity_name"),
            ).filter(
                AuditEvent.project_id == 1,
                AuditEvent.entity_type == "application",
                AuditEvent.entity_id.isnot(None),
                AuditEvent.trace_id.isnot(None),
                AuditEvent.trace_id != "",
            ).group_by(AuditEvent.trace_id).subquery()

            agent_rows = base.join(
                app_trace_map,
                AuditEvent.trace_id == app_trace_map.c.trace_id,
            ).with_entities(
                app_trace_map.c.entity_id.label("entity_id"),
                func.min(app_trace_map.c.entity_name).label("entity_name"),
                func.sum(AuditEvent.llm_cost).label("total_cost"),
                func.count().label("calls"),
            ).group_by(app_trace_map.c.entity_id).all()

            assert len(agent_rows) == 1
            r = agent_rows[0]
            assert r.entity_id == 99
            assert r.calls == 3
            assert float(r.total_cost) == pytest.approx(0.06, abs=1e-8)
            # avg_cost computed Python-side in the real endpoint
            avg = float(r.total_cost) / r.calls
            assert avg == pytest.approx(0.02, abs=1e-8)


class TestAnalyticsAgentDetailAvgCost:
    """analytics_agent_detail.py — verify avg_cost_per_call = llm_cost / llm_calls."""

    def test_avg_cost_per_call(self, audit_event_orm):
        AuditEvent, engine = audit_event_orm
        with Session(engine) as session:
            session.query(AuditEvent).delete()
            session.commit()

            # 4 LLM events sharing one trace with the agent's application row.
            session.add_all([
                _mkevent(AuditEvent, event_type="application", entity_type="application",
                         entity_id=555, entity_name="Agent555", trace_id="TR",
                         action="run"),
                *[
                    _mkevent(AuditEvent, event_type="llm", trace_id="TR",
                             action="llm-call", input_tokens=100, output_tokens=50,
                             llm_cost=0.01)
                    for _ in range(4)
                ],
            ])
            session.commit()

            base = session.query(AuditEvent).filter(
                AuditEvent.entity_type == "application",
                AuditEvent.entity_id == 555,
            )

            trace_subq = base.with_entities(
                AuditEvent.trace_id,
            ).filter(
                AuditEvent.trace_id.isnot(None),
                AuditEvent.trace_id != "",
            ).distinct().subquery()

            cost_row = session.query(
                func.sum(AuditEvent.llm_cost).label("llm_cost"),
                func.sum(func.coalesce(AuditEvent.input_tokens, 0)).label("input_tokens"),
                func.sum(func.coalesce(AuditEvent.output_tokens, 0)).label("output_tokens"),
                func.count().label("llm_calls"),
            ).filter(
                AuditEvent.trace_id.in_(session.query(trace_subq.c.trace_id)),
                AuditEvent.event_type == "llm",
            ).first()

            assert cost_row.llm_calls == 4
            assert float(cost_row.llm_cost) == pytest.approx(0.04, abs=1e-8)
            avg = float(cost_row.llm_cost) / cost_row.llm_calls
            assert avg == pytest.approx(0.01, abs=1e-8)
