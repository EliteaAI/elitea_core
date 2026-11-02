"""The consent of a scoped API token's owner to its runs acting with their Microsoft sign-in (#7337, C-1009).

One row per token that has one (``token_id`` = auth_core's token id): where the owner's offline material is kept at the
secrets plugin (``material_project_id``: the owner's personal project; ``material_ref``; holder ``token:<id>``) and
when it was given. The owner's
subject is not copied: it is the linked IdP subject the token's admission already resolves, and the material is bound
to it (its custodian). Withdrawn, replaced or retired: the row goes (a conditional delete), the material is retired.

A table of the shared schema with its own ``MetaData`` (not ``db.Base``): it exists only where the deployment lists
the source ``token_owner_delegated`` in ``mandate_identity_sources`` (created at start then, ``ensure_table``), so a
platform without the source has no new table and nothing reads one.
"""
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, MetaData
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from tools import config as c

TABLE = 'api_token_identity_consent'
METADATA = MetaData(schema=c.POSTGRES_SCHEMA)
Base = declarative_base(metadata=METADATA)


class ApiTokenIdentityConsent(Base):
    __tablename__ = TABLE
    # the owner's status and the hourly sweep read by owner (#7337 review N-2)
    __table_args__ = (Index('ix_api_token_identity_consent_owner', 'owner_user_id'),)

    token_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    owner_user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    material_project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    material_ref: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    consented_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
