from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import MutableDict

Base = declarative_base()

class EmbeddingStore(Base):
    __tablename__ = "langchain_pg_embedding"
    # __table_args__ = {"schema": schema_name, "extend_existing": True}
    id = Column(String, primary_key=True)
    cmetadata = Column(MutableDict.as_mutable(JSONB), nullable=True)
    document = Column(String, nullable=True)