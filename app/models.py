# SQLAlchemy ORM models: Python classes that map directly to database tables.
#
# Each class below becomes one Postgres table. The type hints (Mapped[...])
# tell SQLAlchemy what SQL column type to use, whether it's nullable, etc.
# `Base.metadata` (collected automatically from every class that inherits
# Base) is what Alembic reads later to autogenerate migrations.

import enum
import uuid
from datetime import datetime

from sqlalchemy import Enum as SqlEnum
from sqlalchemy import ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


# --- Enums -----------------------------------------------------------------
# Real Python enums (instead of plain strings) mean an invalid status like
# "pnding" (typo) is rejected by Python/Postgres instead of silently stored.

class LeadStatus(str, enum.Enum):
    PENDING = "pending"
    ENRICHING = "enriching"
    ENRICHED = "enriched"
    FAILED = "failed"


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# --- leads -------------------------------------------------------------

class Lead(Base):
    """A single real estate lead and its current enrichment status."""

    __tablename__ = "leads"

    # UUID primary key, generated in Python (uuid.uuid4) when a new Lead()
    # is created, rather than relying on a Postgres extension to generate it.
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # server_default=func.now() means "let Postgres itself stamp the current
    # time," so it's correct even for rows inserted outside of our app code.
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    # onupdate=func.now() re-stamps this column whenever a row is UPDATEd
    # through SQLAlchemy. Note: this only fires for updates that go through
    # SQLAlchemy - it is not a database-level trigger.
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )

    name: Mapped[str] = mapped_column(String(255))
    phone: Mapped[str | None] = mapped_column(String(50), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    property_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Raw responses from the mock enrichment services, stored as-is (JSONB)
    # rather than broken out into columns - the worker (app/worker.py) is
    # the only writer, and keeping the whole payload means new fields those
    # services add later show up here for free, with no migration needed.
    property_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    phone_validation: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # values_callable tells SQLAlchemy to store the lowercase .value
    # ("pending") in Postgres, rather than the Python member name ("PENDING").
    status: Mapped[LeadStatus] = mapped_column(
        SqlEnum(
            LeadStatus,
            name="lead_status",
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        default=LeadStatus.PENDING,
        server_default=LeadStatus.PENDING.value,
    )

    # Claude's hot/warm/cold triage call (see app/worker.py) - a category,
    # not a numeric score, so this is a short string rather than a Float.
    enrichment_score: Mapped[str | None] = mapped_column(String(10), nullable=True)
    personalized_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Lets an API caller safely retry the same request (e.g. after a network
    # blip) without creating a duplicate lead - the second attempt with the
    # same key hits the unique constraint instead of inserting twice.
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)

    # Generated once, the first time we attempt CRM delivery (see
    # post_to_crm in app/worker.py), then persisted here and reused on
    # every subsequent attempt - retries and reprocessing alike - so the
    # CRM always sees the same Idempotency-Key for this lead's delivery,
    # no matter how many times we actually send the request.
    crm_idempotency_key: Mapped[str | None] = mapped_column(String(36), unique=True, nullable=True)

    # relationship() gives us Python-level access, e.g. `some_lead.events`,
    # without writing a JOIN by hand. It doesn't add a database column -
    # the actual link is the lead_id foreign key defined on the other tables.
    dead_letter_entries: Mapped[list["DeadLetterQueueEntry"]] = relationship(
        back_populates="lead"
    )
    events: Mapped[list["EnrichmentEvent"]] = relationship(back_populates="lead")


# --- dead_letter_queue ----------------------------------------------------

class DeadLetterQueueEntry(Base):
    """
    Enrichment jobs that failed and need manual review or a scheduled retry,
    e.g. when a downstream enrichment service keeps erroring for a lead.
    """

    __tablename__ = "dead_letter_queue"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # ForeignKey("leads.id") tells Postgres this column must reference an
    # existing row in leads.id - it stops orphaned rows from being created.
    lead_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("leads.id"), nullable=False
    )

    service_name: Mapped[str] = mapped_column(String(100))
    error_message: Mapped[str] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    next_retry_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    lead: Mapped["Lead"] = relationship(back_populates="dead_letter_entries")


# --- circuit_breaker_state -------------------------------------------------

class CircuitBreakerState(Base):
    """
    One row per external service, implementing the "circuit breaker"
    resilience pattern: CLOSED means calls flow normally, OPEN means we're
    refusing to call a service that's been failing repeatedly, and
    HALF_OPEN means we're cautiously letting a test call through to see if
    it has recovered.
    """

    __tablename__ = "circuit_breaker_state"

    # The service name itself is the primary key - there's naturally only
    # one "current state" row per service, so no separate id column is needed.
    service_name: Mapped[str] = mapped_column(String(100), primary_key=True)

    state: Mapped[CircuitState] = mapped_column(
        SqlEnum(
            CircuitState,
            name="circuit_state",
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        default=CircuitState.CLOSED,
        server_default=CircuitState.CLOSED.value,
    )
    failure_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    opened_at: Mapped[datetime | None] = mapped_column(nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(nullable=True)


# --- enrichment_events -------------------------------------------------------

class EnrichmentEvent(Base):
    """
    Append-only audit log of everything that happens to a lead during
    enrichment. Useful for debugging a specific lead's history and for
    showing a timeline in a future UI.
    """

    __tablename__ = "enrichment_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    lead_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("leads.id"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(100))
    service_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    lead: Mapped["Lead"] = relationship(back_populates="events")


# --- system_config -----------------------------------------------------

class SystemConfig(Base):
    """
    A generic key/value store for runtime-toggleable settings that need to
    survive a restart and be shared between the API and worker processes -
    right now just the automation kill switch (see app/kill_switch.py), but
    key/value rather than a dedicated column means future settings like
    this don't each need their own migration.
    """

    __tablename__ = "system_config"

    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    value: Mapped[str] = mapped_column(String(255))
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )
