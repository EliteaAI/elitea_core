from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import MutableDict

Base = declarative_base()

class EmbeddingStore(Base):
    __tablename__ = "langchain_pg_embedding"
    # __table_args__ = {"schema": schema_name, "extend_existing": True}
    id = Column(String, primary_key=True)
    cmetadata = Column(MutableDict.as_mutable(JSONB), nullable=True)
    document = Column(String, nullable=True)


# Twin contract: the canonical definition of this table lives in elitea-sdk
# (elitea_sdk/runtime/tools/index_runs_model.py); this mirror and the DDL twin in
# utils/application_tools.ensure_pgvector_schema_and_tables must stay in lock-step
# with it. v1 columns are FROZEN — create_all never ALTERs, so future columns must be
# additive, nullable-or-defaulted, and feature-detected; bump the version together
# with the SDK on any change.
ELITEA_INDEX_RUNS_SCHEMA_VERSION = 1

INDEX_RUN_PENDING = "pending"
INDEX_RUN_CANCELLED = "cancelled"
INDEX_RUN_PROMOTED = "promoted"
INDEX_RUN_DISCARDED = "discarded"
INDEX_RUN_STATUSES = (
    INDEX_RUN_PENDING, INDEX_RUN_CANCELLED, INDEX_RUN_PROMOTED, INDEX_RUN_DISCARDED,
)
# The SDK's registration INSERT names this index in its ON CONFLICT clause — a drifted
# name or predicate makes every registration hard-error, so both live here as the one
# source the DDL twin builds from.
INDEX_RUN_LIVE_INDEX_NAME = "uq_elitea_index_runs_live"
INDEX_RUN_LIVE_INDEX_PREDICATE = f"status = '{INDEX_RUN_PENDING}'"


class IndexRun(Base):
    __tablename__ = "elitea_index_runs"
    run_id = Column(String, primary_key=True)
    collection = Column(String, nullable=False)
    status = Column(String, nullable=False, server_default=INDEX_RUN_PENDING)
    task_id = Column(String, nullable=True)
    started_on = Column(DOUBLE_PRECISION, nullable=False)
    heartbeat = Column(DOUBLE_PRECISION, nullable=False)
    promoted_on = Column(DOUBLE_PRECISION, nullable=True)
