"""Validated source configuration; connection secrets are never serialized."""

import json
import os
import re
from dataclasses import dataclass, field


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value):
        raise ValueError("identifiers must use lowercase letters, digits and underscores (max 63)")
    return value


def quoted(value):
    return '"' + value.replace('"', '""') + '"'


def lsn_text(lsn):
    """An LSN as PostgreSQL prints it: X/Y in uppercase hex."""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def optional_epoch():
    """CDC_EPOCH, if set: an expectation of the registered epoch, not a setting."""
    value = os.getenv("CDC_EPOCH", "")
    return int(value) if value else None


def table_name(value):
    parts = value.split(".")
    if len(parts) != 2:
        raise ValueError("tables must be explicitly schema-qualified")
    return tuple(identifier(part) for part in parts)


def configured_tables():
    """CDC_TABLES: optional (the registry is the table set; see preflight.classify).
    Unset or "" means "every registered table"; a set value restricts apply and
    backfill and is checked by ingest. An explicit [] is refused, so a list emptied
    by mistake never widens to every table (cli.required refuses it)."""
    return tuple(json.loads(os.getenv("CDC_TABLES") or "[]"))


def check_tables(tables):
    """Empty means unset (the registry decides); set, the names are unique and qualified."""
    if len(set(tables)) != len(tables):
        raise ValueError("provide a unique table list")
    for table in tables:
        table_name(table)


class LeaseBusy(RuntimeError):
    """A lease (the ingest lease, a table's apply lease or a generation lease) is held by
    another run. ONE class, re-exported by ingest_log and apply: two same-named classes
    let membership catch the ingest one around apply.acquire, so a race crashed instead
    of returning busy (found under load)."""


HEARTBEAT_WRITERS = ("self", "external")


def heartbeat_writer(value):
    if value not in HEARTBEAT_WRITERS:
        raise ValueError(f"CDC_HEARTBEAT_WRITER must be one of {HEARTBEAT_WRITERS}")
    return value


@dataclass(frozen=True)
class IngestConfig:
    source_id: str
    slot: str
    publication: str
    tables: tuple[str, ...]
    heartbeat: str = "public.md_cdc_heartbeat"
    database: str = "cdc_internal"
    # An optional expectation: when set, it must equal the registered source epoch,
    # or the run fails closed. Unset follows the registration (see begin_epoch).
    epoch: int | None = None
    chunk_bytes: int = 10 * 1024 * 1024
    batch_bytes: int = 50 * 1024 * 1024
    # Flush once pending items carry this many units: each chunk's events, and each
    # commit counts one (so empty transactions are bounded too). Small transactions
    # then share one append's fixed round trips; batch_bytes still caps the size.
    batch_events: int = 5_000
    max_transaction_bytes: int = 1024 * 1024 * 1024
    max_event_bytes: int = 16 * 1024 * 1024
    flush_seconds: float = 5
    runtime_seconds: float = 540
    lease_seconds: int = 900
    status_seconds: float = 1
    allow_local: bool = False
    # Read replica capture (docs/read-replica.md): the mode (recorded, not in the
    # binding), the bound on a slot's creation, and the DBA job's interval (display only).
    heartbeat_writer: str = "self"
    slot_wait_seconds: float = 90
    heartbeat_interval_seconds: float = 60
    dsn: str = field(default="", repr=False, compare=False)

    def __post_init__(self):
        for value in (self.source_id, self.slot, self.publication, self.database):
            identifier(value)
        heartbeat_writer(self.heartbeat_writer)
        if not 1 <= self.slot_wait_seconds <= 3600:
            raise ValueError("CDC_SLOT_WAIT_SECONDS must be 1..3600")
        if not 1 <= self.heartbeat_interval_seconds <= 86400:
            raise ValueError("CDC_HEARTBEAT_INTERVAL_SECONDS must be 1..86400")
        table_name(self.heartbeat)
        check_tables(self.tables)
        if self.heartbeat in self.tables:
            raise ValueError("heartbeat is managed separately from captured tables")
        if self.epoch is not None and not 1 <= self.epoch < 2**31:
            raise ValueError("epoch must be a positive integer")
        if not 1024 <= self.chunk_bytes <= 100 * 1024 * 1024:
            raise ValueError("chunk_bytes must be 1 KiB..100 MiB")
        if not self.chunk_bytes <= self.batch_bytes <= 256 * 1024 * 1024:
            raise ValueError("batch_bytes must be chunk_bytes..256 MiB")
        if not 1 <= self.batch_events <= 1_000_000:
            raise ValueError("batch_events must be 1..1,000,000")
        if not 1 <= self.max_event_bytes <= 64 * 1024 * 1024:
            raise ValueError("max_event_bytes must be 1 byte..64 MiB")
        if self.max_transaction_bytes < self.max_event_bytes:
            raise ValueError("transaction budget must cover the event budget")
        if not 0 < self.status_seconds <= 10 or not 0 < self.flush_seconds <= 60:
            raise ValueError("invalid status/flush interval")
        if not 0 < self.runtime_seconds <= 27000 or self.lease_seconds < 30:
            raise ValueError("invalid runtime/lease interval")

    def binding(self):
        return json.dumps(
            dict(
                slot=self.slot,
                publication=self.publication,
                heartbeat=self.heartbeat,
                chunk_bytes=self.chunk_bytes,
                format_version=1,
            ),
            sort_keys=True,
        )

    @classmethod
    def from_env(cls):
        source = identifier(os.environ["CDC_SOURCE_ID"])
        secret_key = os.getenv("CDC_DSN_ENV", "CDC_PG_DSN")
        dsn = os.environ.get(secret_key, "")
        if not dsn:
            raise ValueError("Postgres secret environment variable is missing")
        return cls(
            source,
            os.getenv("CDC_SLOT", f"md_cdc_{source}"),
            os.getenv("CDC_PUBLICATION", f"md_cdc_{source}_pub"),
            configured_tables(),
            heartbeat=os.getenv("CDC_HEARTBEAT", "public.md_cdc_heartbeat"),
            database=os.getenv("CDC_DATABASE", "cdc_internal"),
            epoch=optional_epoch(),
            runtime_seconds=float(os.getenv("CDC_RUNTIME_SECONDS", "540")),
            heartbeat_writer=os.getenv("CDC_HEARTBEAT_WRITER") or "self",
            slot_wait_seconds=float(os.getenv("CDC_SLOT_WAIT_SECONDS") or "90"),
            heartbeat_interval_seconds=float(os.getenv("CDC_HEARTBEAT_INTERVAL_SECONDS") or "60"),
            dsn=dsn,
        )


@dataclass(frozen=True)
class ApplyConfig:
    """Apply reads the change log and writes the source's mirror database, plus its
    retention pins in the change-log database (separate transactions).

    It never connects to PostgreSQL, so it needs no source secret.
    """

    source_id: str
    tables: tuple[str, ...]
    database: str = "cdc_internal"
    mirror_database: str = ""
    # As in IngestConfig: an optional expectation of the registered source epoch.
    epoch: int | None = None
    max_batch_events: int = 100_000
    max_batch_bytes: int = 256 * 1024 * 1024
    runtime_seconds: float = 540
    lease_seconds: int = 900

    def __post_init__(self):
        identifier(self.source_id)
        identifier(self.database)
        if not self.mirror_database:
            object.__setattr__(self, "mirror_database", f"{self.source_id}_mirror")
        identifier(self.mirror_database)
        if self.mirror_database == self.database:
            raise ValueError("mirror and change-log databases must differ")
        check_tables(self.tables)
        if self.epoch is not None and not 1 <= self.epoch < 2**31:
            raise ValueError("epoch must be a positive integer")
        if not 1 <= self.max_batch_events <= 10_000_000:
            raise ValueError("max_batch_events must be 1..10,000,000")
        if not 1024 <= self.max_batch_bytes <= 4 * 1024 * 1024 * 1024:
            raise ValueError("max_batch_bytes must be 1 KiB..4 GiB")
        if not 0 < self.runtime_seconds <= 27000 or self.lease_seconds < 30:
            raise ValueError("invalid runtime/lease interval")
        if self.runtime_seconds >= self.lease_seconds:
            raise ValueError("apply runtime must stay below the lease TTL")

    @classmethod
    def from_env(cls):
        source = identifier(os.environ["CDC_SOURCE_ID"])
        return cls(
            source,
            configured_tables(),
            database=os.getenv("CDC_DATABASE", "cdc_internal"),
            mirror_database=os.getenv("CDC_MIRROR_DATABASE", ""),
            epoch=optional_epoch(),
            runtime_seconds=float(os.getenv("CDC_RUNTIME_SECONDS", "540")),
        )


@dataclass(frozen=True)
class BackfillConfig(ApplyConfig):
    """Backfill reads the source table (it needs the Postgres secret), writes only
    the mirror database, and reads the change log for catch-up."""

    heartbeat: str = "public.md_cdc_heartbeat"
    chunk_rows: int = 50_000
    max_chunk_bytes: int = 64 * 1024 * 1024
    chunk_timeout_seconds: float = 600
    marker_wait_seconds: float = 60
    allow_local: bool = False
    dsn: str = field(default="", repr=False, compare=False)
    publication: str = ""
    append_snapshot_max_bytes: int = 1024**3
    # Replica capture: the mode (checked against the source's recorded one) and, for an
    # append snapshot from a standby, the bound on its slot's creation.
    heartbeat_writer: str = "self"
    slot_wait_seconds: float = 90

    def __post_init__(self):
        super().__post_init__()
        table_name(self.heartbeat)
        heartbeat_writer(self.heartbeat_writer)
        if not 1 <= self.slot_wait_seconds <= 3600:
            raise ValueError("CDC_SLOT_WAIT_SECONDS must be 1..3600")
        if not self.publication:
            object.__setattr__(self, "publication", f"md_cdc_{self.source_id}_pub")
        if self.append_snapshot_max_bytes < 0:
            raise ValueError("bad append snapshot limit")
        if not 1 <= self.chunk_rows <= 1_000_000:
            raise ValueError("chunk_rows must be 1..1,000,000")
        if not 1024 <= self.max_chunk_bytes <= 1024 * 1024 * 1024:
            raise ValueError("max_chunk_bytes must be 1 KiB..1 GiB")
        if not 1 <= self.chunk_timeout_seconds < self.lease_seconds:
            raise ValueError("chunk_timeout_seconds must be at least 1 and below the lease TTL")
        # The marker wait does not renew the generation lease.
        if not 0 <= self.marker_wait_seconds < self.lease_seconds:
            raise ValueError("marker_wait_seconds must be below the lease TTL")

    @classmethod
    def from_env(cls):
        source = identifier(os.environ["CDC_SOURCE_ID"])
        secret_key = os.getenv("CDC_DSN_ENV", "CDC_PG_DSN")
        dsn = os.environ.get(secret_key, "")
        if not dsn:
            raise ValueError("Postgres secret environment variable is missing")
        return cls(
            source,
            configured_tables(),
            database=os.getenv("CDC_DATABASE", "cdc_internal"),
            mirror_database=os.getenv("CDC_MIRROR_DATABASE", ""),
            epoch=optional_epoch(),
            runtime_seconds=float(os.getenv("CDC_RUNTIME_SECONDS", "540")),
            heartbeat=os.getenv("CDC_HEARTBEAT", "public.md_cdc_heartbeat"),
            chunk_rows=int(os.getenv("CDC_BACKFILL_CHUNK_ROWS", "50000")),
            dsn=dsn,
            publication=os.getenv("CDC_PUBLICATION", ""),
            append_snapshot_max_bytes=int(os.getenv("CDC_APPEND_SNAPSHOT_MAX_BYTES", str(1024**3))),
            heartbeat_writer=os.getenv("CDC_HEARTBEAT_WRITER") or "self",
            slot_wait_seconds=float(os.getenv("CDC_SLOT_WAIT_SECONDS") or "90"),
        )
