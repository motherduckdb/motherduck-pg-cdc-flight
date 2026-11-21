"""Shared source registry and lease contract: one source schema, one database per write transaction."""

import json
import time
import uuid
from contextlib import contextmanager

import duckdb

from cdc_flight import lifecycle_commands as lc
from cdc_flight.config import LeaseBusy, quoted  # noqa: F401 (re-exported)
from cdc_flight.retention_common import is_conflict, refuse_lifecycle

# Every table the ingest log owns, in creation order. Each statement is idempotent.
TABLES = {
    "source": "CREATE TABLE IF NOT EXISTS {s}source (id INTEGER PRIMARY KEY, epoch INTEGER, binding VARCHAR, system_id VARCHAR, database_oid UBIGINT, confirmed_lsn UBIGINT, token BIGINT, holder VARCHAR, expires_at TIMESTAMPTZ, guard VARCHAR, open_commit_lsn UBIGINT, last_capped_commit UBIGINT, last_capped_count UBIGINT, state VARCHAR DEFAULT 'ready')",
    "tables": "CREATE TABLE IF NOT EXISTS {s}tables (relation_id UINTEGER PRIMARY KEY, schema_name VARCHAR, table_name VARCHAR, key_columns VARCHAR, state VARCHAR)",
    "runs": "CREATE TABLE IF NOT EXISTS {s}runs (run_id VARCHAR PRIMARY KEY, status VARCHAR, boundary UBIGINT, detail VARCHAR, updated_at TIMESTAMPTZ)",
    "key_changes": "CREATE TABLE IF NOT EXISTS {s}key_changes (relation_id UINTEGER, version INTEGER, key_columns VARCHAR, cut_lsn UBIGINT, new_from_lsn UBIGINT, PRIMARY KEY(relation_id, version))",
    "source_epochs": "CREATE TABLE IF NOT EXISTS {s}source_epochs (epoch INTEGER PRIMARY KEY, previous_epoch INTEGER, start_lsn UBIGINT, previous_confirmed_lsn UBIGINT, reason VARCHAR, slot_name VARCHAR, previous_slot_name VARCHAR, timeline_id BIGINT, abandoned_from UBIGINT, abandoned_to UBIGINT, created_at TIMESTAMPTZ)",
}

# Additive columns, added to stores created before them.
ADDED_COLUMNS = {
    "tables": (
        ("empty_after_slot", "BOOLEAN"),
        ("catalog_key", "VARCHAR"),
        ("key_version", "INTEGER"),
        ("key_checked_lsn", "UBIGINT"),
        ("key_epoch", "INTEGER DEFAULT 1"),
        ("held_reason", "VARCHAR"),
        ("held_detail", "VARCHAR"),
        ("held_since", "TIMESTAMPTZ"),
        ("held_checked_at", "TIMESTAMPTZ"),
        ("former_name", "VARCHAR"),
        ("retired_by", "UINTEGER"),
        ("pin_state", "VARCHAR"),
        # NULL: keyed. Written by the release that adds keyless (append) capture; this
        # one only refuses such a table (append_refusal).
        ("capture_mode", "VARCHAR"),
        # Append capture only: skip DELETEs (counted) instead of holding. NULL/false: hold.
        ("ignore_deletes", "BOOLEAN"),
        # The last clean catalog read's facts (catalog_checks): written on change only.
        ("catalog_state", "VARCHAR"),
    ),
    # A catalog hold's reason (catalog_checks.REASONS); NULL for a key change.
    "key_changes": (("epoch", "INTEGER DEFAULT 1"), ("reason", "VARCHAR")),
    "source": (
        ("slot_name", "VARCHAR"),
        ("lifecycle", "VARCHAR"),
        ("deleted_slot_name", "VARCHAR"),
        ("deleted_at", "TIMESTAMPTZ"),
        ("deleted_run_id", "VARCHAR"),
        ("deleted_started_by", "VARCHAR"),
        ("created_slot", "BOOLEAN"),
        ("created_publication", "BOOLEAN"),
        ("created_heartbeat", "BOOLEAN"),
        # Replica capture: `self` or `external`; NULL (stores before it) = self.
        ("heartbeat_writer", "VARCHAR"),
        # External mode only: the standby's timeline at registration; NULL otherwise.
        ("timeline_id", "BIGINT"),
    ),
    "source_epochs": (("started_by", "VARCHAR"),),
}


def check_mode(config, recorded_writer, recorded_timeline, timeline):
    """Replica capture: the recorded heartbeat writer (NULL = self) must equal the
    configured one; in external mode the standby's timeline must equal the recorded one.
    Either change holds the source: promotion/failover isn't supported."""
    recorded = recorded_writer or "self"
    if recorded != config.heartbeat_writer:
        raise SourceMismatch(
            f"heartbeat writer changed: {recorded} -> {config.heartbeat_writer}; "
            "recreate the source"
        )
    if (
        config.heartbeat_writer == "external"
        and recorded_timeline is not None
        and timeline is not None
        and timeline != recorded_timeline
    ):
        raise SourceMismatch(
            f"the standby's timeline changed ({recorded_timeline} -> {timeline}): promotion or "
            "failover isn't supported; revalidate the standby and resync the source"
        )


# The registered epoch, slot and checkpoint, read together under the lease.
POSITION = (
    "SELECT epoch,coalesce(slot_name,json_extract_string(binding,'$.slot')),confirmed_lsn "
    "FROM {s}source WHERE id=1"
)

# Registration and the current slot in one read: register's check, registered_slot's
# answer, and the registered key columns.
REGISTRATION = (
    "SELECT (SELECT {'binding':binding,'system_id':system_id,'database_oid':database_oid,"
    "'epoch':epoch,'slot':coalesce(slot_name,json_extract_string(binding,'$.slot')),"
    # Replica capture's columns through to_json: a store from before them reads NULL
    # (= self) with no migration (ingest never alters an existing format-2 store).
    "'lifecycle':lifecycle,'heartbeat_writer':to_json(src)->>'heartbeat_writer',"
    "'timeline_id':(to_json(src)->>'timeline_id')::BIGINT} "
    "FROM {s}source src WHERE id=1),"
    "(SELECT list({'relation_id':relation_id,'key_columns':key_columns,"
    "'schema_name':schema_name,'table_name':table_name,'held_reason':held_reason,"
    "'former_name':former_name,'capture_mode':capture_mode}) FROM {s}tables)"
)


def bound(binding):
    """The registration's binding without its table list: the registry, not the
    binding, says which tables a source captures (add-table, remove-table). Stores
    registered with a table list compare equal on everything else."""
    value = json.loads(binding)
    value.pop("tables", None)
    return json.dumps(value, sort_keys=True)


def append_refusal(
    table, capture_mode, key_columns, catalog_key=None, held_reason=None, *, append_ok=False
):
    """None for a keyed table; otherwise why this release's keyed paths refuse it.

    A table registered for keyless (append) capture (`tables.capture_mode`, the
    authoritative copy; apply_state's is a copy), or a table meant to be applied whose
    effective key (catalog_key, else key_columns) is empty, must never reach a keyed
    path: an empty key would build `PRIMARY KEY()` or fold every row into one key.
    An unmanaged or removed row may have no key (a keyless publication member): it is
    never applied, and add-table adopts it once it has a key. Every keyed entry point
    checks this before it acts (the guard release, ahead of append capture).

    `append_ok`: the caller is append-aware (apply, start-from-log): an append table
    passes. Every other keyed path (backfill, resync, the table actions' keyed steps)
    still refuses it."""
    if capture_mode is None:
        if held_reason in ("unmanaged", "removed"):
            return None
        # catalog_key 'null': ingest found no key for a moment (the table needs a resync,
        # which apply reports); the registered key_columns stays the effective key.
        catalog = json.loads(catalog_key) if catalog_key else None
        if catalog or (catalog is None and json.loads(key_columns or "[]")):
            return None
        return (
            f"{table} has no registered key (key_columns and catalog_key are empty); the "
            "registry is inconsistent: resync or remove the table"
        )
    if capture_mode == "append" and append_ok:
        return None
    if capture_mode == "append":
        return (
            f"{table} is captured insert-only (append mode): a keyed backfill or resync "
            "can't rebuild it; use start-from-log, or remove it and add it again"
        )
    return (
        f"{table} is registered for {capture_mode} capture, which this release does not "
        "support; deploy the release that adds it"
    )


class TableRefused(RuntimeError):
    """A configured table the registry doesn't know: run add-table."""


RELEASE = (
    "UPDATE {s}source SET guard=uuid()::VARCHAR,holder=NULL,expires_at=NULL WHERE id=1 AND token=?"
)

REASONS = ("slot_lost", "skip")


def managed(t, s):
    """SQL: the registry row `t` (an alias of `{s}tables`) is a managed table, one apply
    covers: never an unmanaged member (logged, not applied), a removed row, or the
    registered heartbeat (the registration's binding names it). Shared by apply's table
    set (table_set.registered) and its not_started status rows (ops_apply)."""
    return (
        f"coalesce({t}.held_reason,'') NOT IN ('unmanaged','removed') "
        f"AND {t}.schema_name||'.'||{t}.table_name IS DISTINCT FROM (SELECT "
        f"json_extract_string(binding,'$.heartbeat') FROM {s}source WHERE id=1)"
    )


# Held reasons whose hold clears by itself when the table is found valid again.
MEMBERSHIP_HOLDS = ("dropped", "renamed", "replaced", "unpublished")
# A per-table check failed (preflight.TableInvalid). Changes logged meanwhile may carry
# incomplete images, so clearing one of these forces a resync (see record_tables).
VALIDATION_HOLDS = (
    "invalid_kind",
    "no_select",
    "no_key",
    "identity_not_full",
    "leaf_invalid",
    "publication_filter",
    # An append (keyless, insert-only) table that is no longer valid for it
    # (preflight.validate_append). Clearing one never forces a resync: see record_tables.
    "append_partitioned",
    "append_identity",
)

# Every guarded write also re-checks the epoch and slot acquire read, at no extra
# round trip: a run never writes or acknowledges across an epoch transition.
GUARD = (
    "UPDATE {s}source SET guard=uuid()::VARCHAR, "
    "expires_at=current_timestamp+?*INTERVAL '1 second' "
    "WHERE id=1 AND token=? AND expires_at>current_timestamp AND epoch=? "
    "AND coalesce(slot_name,json_extract_string(binding,'$.slot'))=?"
)


class FenceLost(RuntimeError):
    pass


class GuardedIngestConflict(duckdb.TransactionException):
    """A format-2 guard/append conflict, propagated after transaction rollback."""


class SourceMismatch(RuntimeError):
    pass


class LogBase:
    format_version = 2

    def __init__(self, con, config):
        self.con, self.config = con, config
        self.prefix = f"{quoted(config.database)}.{quoted(config.source_id)}."
        self.token = None
        # Read under the lease by acquire: the only epoch and slot ingest uses.
        self.epoch = self.slot_name = None
        # Only the delete-source Flight acquires a deleting source (to finish it).
        self.allow_lifecycle = False
        self.forget()

    def forget(self):
        """Drop what one run read: release() calls it, so every run validates its
        registration at least once with a fresh read."""
        self.acquired_lsn = None  # the checkpoint acquire read with the lease
        self.acquired_at_us = None  # server time of that read (epoch microseconds)
        self._registration = None  # registered_slot's read, kept for register
        self._registered = None  # register's verified arguments
        self._keys = None  # registered key columns (write-once; see key_columns)
        self._registry = None  # register's read of every tables row (names, holds)

    def execute(self, sql, parameters=None):
        return self.con.execute(sql.replace("{s}", self.prefix), parameters or [])

    @contextmanager
    def transaction(self):
        self.con.execute("BEGIN")
        try:
            yield
            self.con.execute("COMMIT")
        except BaseException:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def catalog(self):
        """{table: {column, ...}} for this source's schema, from the client's catalog.

        Reading the catalog costs no MotherDuck round trip. A table missing from a
        stale catalog is created with IF NOT EXISTS anyway; nothing drops these
        tables, so a stale "exists" cannot hide a missing one.
        """
        tables = {}
        for table, column in self.con.execute(
            "SELECT table_name,column_name FROM duckdb_columns() "
            "WHERE database_name=? AND schema_name=?",
            [self.config.database, self.config.source_id],
        ).fetchall():
            tables.setdefault(table, set()).add(column)
        return tables

    def register(self, identity, initial_lsn, tables, *, created=None, timeline=None):
        """Register the source once; afterwards, check it is the same one.

        It compares the binding and the cluster identity. The epoch is compared only
        when config.epoch is set, and is never adopted from config: after
        registration only begin_epoch writes it. The heartbeat writer must equal the
        recorded one, and in external mode the standby's `timeline` the recorded one.
        """
        # The same arguments were verified in this run (cli.main, then run_once):
        # nothing a re-read could catch is checked here; acquire re-reads the epoch
        # and slot under the lease, and the write path below re-reads in its transaction.
        created = created or {}
        arguments = json.dumps(
            [identity, initial_lsn, tables, timeline], sort_keys=True, default=str
        )
        if arguments == self._registered and not any(created.values()):
            return
        expected = (bound(self.config.binding()), identity[0], identity[1])
        rows = {}
        for table in tables:
            rows.setdefault(table["oid"], table)  # the first wins, as with DO NOTHING
        keys = {}

        def state(cached=None):
            existing, registered = cached or self.execute(REGISTRATION).fetchone()
            if existing:
                existing = dict(existing)
                epoch = existing.pop("epoch")
                existing.pop("slot")
                # D3: a deleted source id is final; a deleting one is refused too.
                refuse_lifecycle(self.config.source_id, existing.pop("lifecycle", None))
                check_mode(
                    self.config,
                    existing.pop("heartbeat_writer", None),
                    existing.pop("timeline_id", None),
                    timeline,
                )
                existing["binding"] = bound(existing["binding"])
                existing = tuple(existing.values())
                if existing != expected:
                    raise SourceMismatch(
                        "source identity/config changed; explicit resync registration required"
                    )
                self.check_epoch(epoch)
            keys.clear()
            keys.update(
                (row["relation_id"], tuple(json.loads(row["key_columns"])))
                for row in registered or ()
            )
            self._registry = [dict(row) for row in registered or ()]
            missing = [oid for oid in rows if oid not in keys]
            if existing and missing:
                # After the first registration only add-table registers a table.
                names = ", ".join(f"{rows[o]['schema']}.{rows[o]['name']}" for o in missing)
                raise TableRefused(f"{names} is not registered for this source; run add-table")
            return existing, missing

        # Steady state: one autocommit read finds everything registered (or
        # registered_slot's read, made just before in this run). A read-only
        # transaction would cost a commit round trip too.
        cached, self._registration = self._registration, None  # used once, even on error
        existing, missing = state(cached)
        if existing and not missing and not any(created.values()):
            self._registered, self._keys = arguments, keys
            return
        with self.transaction():
            existing, missing = state()
            if not existing:
                # A new source starts at epoch 1; later epochs come only from begin_epoch.
                if self.config.epoch not in (None, 1):
                    raise SourceMismatch("a new source starts at epoch 1; unset CDC_EPOCH")
                lc.statement(
                    self,
                    "register",
                    "source_insert",
                    "INSERT INTO {s}source (id,epoch,binding,system_id,database_oid,"
                    "confirmed_lsn,token,slot_name,created_slot,created_publication,created_heartbeat,"
                    "heartbeat_writer,timeline_id) "
                    "VALUES (1,?,?,?,?,?,0,?,?,?,?,?,?)",
                    [
                        1,
                        *expected,
                        initial_lsn,
                        self.config.slot,
                        *(
                            bool((created or {}).get(k))
                            for k in ("slot", "publication", "heartbeat")
                        ),
                        self.config.heartbeat_writer,
                        timeline if self.config.heartbeat_writer == "external" else None,
                    ],
                )
                self._record_initial_epoch(initial_lsn)
            elif any(created.values()):
                self.record_created(created)
            if missing:
                # Key tracking starts at version 0, confirmed at the registered
                # start: preflight read these keys after that position.
                inserted = [json.dumps(rows[oid]["keys"]) for oid in missing]
                # The spec: exactly the missing tables, re-read in this transaction (no
                # ON CONFLICT: it only runs with the id=1 source INSERT, the oids deduped).
                lc.statement(
                    self,
                    "register",
                    "tables_insert",
                    "INSERT INTO {s}tables (relation_id,schema_name,table_name,key_columns,"
                    "state,empty_after_slot,catalog_key,key_version,key_checked_lsn) "
                    "SELECT unnest(?::UINTEGER[]),unnest(?::VARCHAR[]),unnest(?::VARCHAR[]),"
                    "unnest(?::VARCHAR[]),'snapshotting',unnest(?::BOOLEAN[]),"
                    "unnest(?::VARCHAR[]),0,?",
                    [
                        missing,
                        [rows[oid]["schema"] for oid in missing],
                        [rows[oid]["name"] for oid in missing],
                        inserted,
                        [rows[oid].get("empty") for oid in missing],
                        inserted,
                        initial_lsn,
                    ],
                    expected=len(missing),
                )
                keys.update((oid, tuple(rows[oid]["keys"])) for oid in missing)
        self._registered, self._keys = arguments, keys
        # The registry read predates the insert: add what this transaction registered.
        self._registry = (self._registry or []) + [
            dict(
                relation_id=oid,
                key_columns=json.dumps(rows[oid]["keys"]),
                schema_name=rows[oid]["schema"],
                table_name=rows[oid]["name"],
                held_reason=None,
                former_name=None,
            )
            for oid in missing
            if oid not in {r["relation_id"] for r in self._registry or ()}
        ]

    def check_epoch(self, epoch):
        if self.config.epoch is not None and epoch != self.config.epoch:
            raise SourceMismatch("configured epoch differs from the registered source epoch")

    def record_created(self, created):
        """Record which provisioning objects this run created (preflight with
        create_objects). Monotonic: a flag is only ever set, never cleared by a later
        run that found the object present. A crash between create and
        this write leaves it unset: the safe direction (delete prints the DDL)."""
        flags = [bool(created.get(k)) for k in ("slot", "publication", "heartbeat")]
        if not any(flags):
            return
        lc.statement(
            self,
            "record_created",
            "created_update",
            "UPDATE {s}source SET created_slot=CASE WHEN ? THEN true ELSE created_slot END,"
            "created_publication=CASE WHEN ? THEN true ELSE created_publication END,"
            "created_heartbeat=CASE WHEN ? THEN true ELSE created_heartbeat END WHERE id=1",
            flags,
        )

    def registered_slot(self):
        """The current epoch's slot name, for validation before the lease (preflight).
        Ingest uses the one acquire reads. The read is the registration's, which the
        next register() in this run uses instead of reading again."""
        self._registration = self.execute(REGISTRATION).fetchone()
        self._registry = None  # this read is newer than any earlier one
        existing = self._registration[0]
        if existing:
            refuse_lifecycle(self.config.source_id, existing.get("lifecycle"))
        return existing["slot"] if existing else self.config.slot

    def registry(self):
        """Every tables row (relation_id, names, held_reason, former_name), from the
        registration read this run already made (registered_slot), or one read."""
        if self._registry is not None:
            return [dict(r) for r in self._registry]
        if self._registration is None:
            self._registration = self.execute(REGISTRATION).fetchone()
        return [dict(r) for r in self._registration[1] or ()]

    def record_tables(self, held=None, present=(), unmanaged=(), names=None):
        """Record table membership under the lease, in one guarded transaction, and only
        when something changes (steady state writes nothing).

        held: {relation_id: (reason, detail)} for configured tables whose name no
        longer resolves to their OID (dropped, renamed, replaced). `held_since` keeps
        the first sighting; `held_checked_at` the latest.
        present: relation ids found again under their name: their dropped, renamed or
        replaced hold clears (a rename reverted).
        unmanaged: publication members nobody registered, as dicts with oid, schema,
        name and keys: registered held 'unmanaged', so the spool decodes them; they are
        logged and never applied until add-table adopts them.
        """
        held = held or {}
        names = names or {}
        registry = self.registry()
        current = {r["relation_id"]: r["held_reason"] for r in registry}
        append = {r["relation_id"] for r in registry if r.get("capture_mode") == "append"}
        clear = [
            oid
            for oid in present
            if current.get(oid) in MEMBERSHIP_HOLDS + VALIDATION_HOLDS and oid not in held
        ]
        new = [t for t in unmanaged if t["oid"] not in current]
        if not held and not clear and not new:
            return
        with self.transaction():
            self.guard()
            if held:
                oids = list(held)
                # The name preflight classified from (read before the lease): a row an
                # operator relabelled since doesn't match and isn't re-held.
                self.execute(
                    "UPDATE {s}tables SET held_reason=u.reason,held_detail=u.detail,"
                    "held_since=coalesce(tables.held_since,current_timestamp),"
                    "held_checked_at=current_timestamp FROM (SELECT unnest(?::UINTEGER[]) "
                    "AS relation_id,unnest(?::VARCHAR[]) AS reason,unnest(?::VARCHAR[]) "
                    "AS detail,unnest(?::VARCHAR[]) AS schema_name,unnest(?::VARCHAR[]) "
                    "AS table_name) u WHERE tables.relation_id=u.relation_id "
                    "AND (u.table_name IS NULL OR (tables.schema_name=u.schema_name "
                    "AND tables.table_name=u.table_name)) "
                    "AND coalesce(tables.held_reason,'') NOT IN ('removed','unmanaged')",
                    [
                        oids,
                        [held[o][0] for o in oids],
                        [held[o][1] for o in oids],
                        [names.get(o, (None, None))[0] for o in oids],
                        [names.get(o, (None, None))[1] for o in oids],
                    ],
                )
            if clear:
                # Not for an append table: its inserts carry every column whatever the
                # identity (UPDATE/DELETE hold apply anyway), and it has no key version.
                resync = [
                    oid for oid in clear if current[oid] in VALIDATION_HOLDS and oid not in append
                ]
                if resync:
                    # A new key version with cut 0: apply holds needs_resync at once, even
                    # for changes it applied before the hold was seen (a DBA change made
                    # mid-run is confirmed through that run's end), and only a backfill
                    # of the new version rebuilds the mirror.
                    self.execute(
                        "INSERT INTO {s}key_changes (relation_id,version,key_columns,cut_lsn,"
                        "new_from_lsn,epoch) SELECT relation_id,coalesce(key_version,0)+1,"
                        "coalesce(catalog_key,key_columns),0,?,? FROM {s}tables "
                        "WHERE relation_id IN (SELECT unnest(?::UINTEGER[]))",
                        [self.acquired_lsn or 0, self.epoch, resync],
                    )
                    self.execute(
                        "UPDATE {s}tables SET key_version=coalesce(key_version,0)+1,key_epoch=?,"
                        "key_checked_lsn=NULL WHERE relation_id IN (SELECT unnest(?::UINTEGER[]))",
                        [self.epoch, resync],
                    )
                self.execute(
                    "UPDATE {s}tables SET held_reason=NULL,held_detail=NULL,held_since=NULL,"
                    "held_checked_at=NULL WHERE relation_id IN (SELECT unnest(?::UINTEGER[])) "
                    "AND held_reason IN (SELECT unnest(?::VARCHAR[]))",
                    [clear, list(MEMBERSHIP_HOLDS + VALIDATION_HOLDS)],
                )
            if new:
                keys = [json.dumps(list(t["keys"] or [])) for t in new]
                self.execute(
                    "INSERT INTO {s}tables (relation_id,schema_name,table_name,key_columns,"
                    "state,catalog_key,key_version,held_reason,held_detail,held_since,"
                    "held_checked_at) SELECT unnest(?::UINTEGER[]),unnest(?::VARCHAR[]),"
                    "unnest(?::VARCHAR[]),unnest(?::VARCHAR[]),'unmanaged',"
                    "unnest(?::VARCHAR[]),0,'unmanaged','run add-table, or remove it from "
                    "the publication',current_timestamp,current_timestamp "
                    "ON CONFLICT DO NOTHING",
                    [
                        [t["oid"] for t in new],
                        [t["schema"] for t in new],
                        [t["name"] for t in new],
                        keys,
                        keys,
                    ],
                )
        reasons = {**{o: h[0] for o, h in held.items()}, **dict.fromkeys(clear)}
        registry = [dict(r) for r in self.registry()]
        for row in registry:
            if row["relation_id"] in reasons and row["held_reason"] not in ("removed", "unmanaged"):
                row["held_reason"] = reasons[row["relation_id"]]
        for t in new:
            registry.append(
                dict(
                    relation_id=t["oid"],
                    key_columns=json.dumps(list(t["keys"] or [])),
                    schema_name=t["schema"],
                    table_name=t["name"],
                    held_reason="unmanaged",
                    former_name=None,
                )
            )
            if self._keys is not None:
                self._keys[t["oid"]] = tuple(t["keys"] or ())
        self._registry = registry

    def checkpoint(self):
        row = self.execute("SELECT confirmed_lsn FROM {s}source WHERE id=1").fetchone()
        if not row:
            raise SourceMismatch("source is not registered; run preflight")
        return row[0]

    def key_columns(self):
        # Registration fixes key projection for replay. Current PG catalog keys may
        # describe a later DDL state than the WAL being replayed.
        # tables.key_columns is written once, by register's INSERT (key changes go
        # to key_changes and catalog_key), so register's read of it is current. A
        # future re-key that rewrites it must re-read here. A table another process
        # registers after that read fails closed in the spool ("unregistered
        # relation") until the next run's register, as one registered after a read
        # under the lease always has.
        if self._keys is not None:
            return dict(self._keys)
        return {
            oid: tuple(json.loads(keys))
            for oid, keys in self.execute(
                "SELECT relation_id,key_columns FROM {s}tables"
            ).fetchall()
        }

    def acquire(self, run_id):
        """Take the lease: one read, then one UPDATE that succeeds only if the row is
        unchanged since that read. Every write to the source row changes `guard`
        (acquire, guard, release, and the writes inside guarded transactions), so
        a matching token and guard prove the epoch, slot and checkpoint read are the
        ones this lease covers. A run that acquired and released in between makes
        this one busy (retried by the next run)."""
        # Never UPDATE ... RETURNING on a fenced row: DuckDB 1.5.6 skips write-write
        # conflict detection for it (both writers commit). Plain read and UPDATE.
        self.acquired_lsn = self.acquired_at_us = None
        row = self.execute(
            "SELECT token,epoch,coalesce(slot_name,json_extract_string(binding,'$.slot')),"
            "guard,confirmed_lsn,epoch_us(current_timestamp),lifecycle,"
            # The slot this epoch's recover replaced (a leftover to report), read here at
            # no extra round trip.
            "(SELECT e.previous_slot_name FROM {s}source_epochs e WHERE e.epoch=source.epoch "
            "AND e.reason='slot_lost'),binding FROM {s}source WHERE id=1 "
            "AND (expires_at IS NULL OR expires_at<=current_timestamp)"
        ).fetchone()
        if row is None:
            raise LeaseBusy("another ingest run holds the lease")
        token, epoch, slot_name, guard, confirmed, read_at, lifecycle, previous, binding = row
        if json.loads(binding).get("format_version", 1) != self.format_version:
            raise SourceMismatch("source format does not match this ingest backend")
        if not self.allow_lifecycle:
            refuse_lifecycle(self.config.source_id, lifecycle)
        self._previous_slot = previous
        # Server time of this read (epoch microseconds): the status report's as_of.
        self.acquired_at_us = read_at
        count = self.execute(
            "UPDATE {s}source SET token=token+1,holder=?,guard=uuid()::VARCHAR, "
            "expires_at=current_timestamp+?*INTERVAL '1 second' "
            "WHERE id=1 AND (expires_at IS NULL OR expires_at<=current_timestamp) "
            "AND token=? AND guard IS NOT DISTINCT FROM ?",
            [run_id, self.config.lease_seconds, token, guard],
        ).fetchone()[0]
        if count != 1:
            raise LeaseBusy("another ingest run holds the lease")
        token += 1
        self.token = token
        self.epoch, self.slot_name = epoch, slot_name
        self.acquired_lsn = confirmed
        try:
            self.check_epoch(epoch)
        except SourceMismatch:
            self.release()
            raise
        return token

    def renew(self):
        self.guard()  # one statement: autocommit is its own transaction

    def record_progress(self, status, partial_commit):
        """Surface repeated capped replay with no new durable chunks."""
        with self.transaction():
            self.guard()
            return self._record_progress(status, partial_commit)

    def finish(
        self, run_id, observed, boundary, checked_lsn, status, partial_commit, detail, facts=None
    ):
        """End a run in one guarded transaction: record_keys, record_progress, the
        run's final status (`detail(status)`) and the release. Returns the status,
        "needs_action" when replay stalled. A lost fence writes none of them; the
        caller's cleanup then records the failure. `detail` must be pure: no I/O and
        no use of the lease. `facts` (the catalog checks) are judged by format 2's log
        only."""
        with self.transaction():
            self.guard()
            self._record_keys(observed, boundary, checked_lsn)
            if self._record_progress(status, partial_commit):
                status = "needs_action"
            self.run_status(run_id, status, detail(status))
            self.execute(RELEASE, [self.token])
        self.token = None
        self.forget()
        return status

    def position(self):
        """(epoch, slot name, checkpoint), read now."""
        row = self.execute(POSITION).fetchone()
        if not row:
            raise SourceMismatch("source is not registered; run preflight")
        return row

    def refuse_lifecycle(self):
        """One statement: refuse a deleting or deleted source. For a click's dry run,
        which takes no lease; the real run is refused by acquire too."""
        row = self.execute("SELECT lifecycle FROM {s}source WHERE id=1").fetchone()
        refuse_lifecycle(self.config.source_id, row[0] if row else None)

    def previous_slots(self):
        """The slot the current epoch's recover replaced, if any, as acquire read it (to
        report a leftover the post-commit drop missed)."""
        previous = getattr(self, "_previous_slot", None)
        return (previous,) if previous else ()

    def run_status(self, run_id, status, detail="", *, boundary=None):
        if boundary is not None:
            self.execute(
                "INSERT INTO {s}runs (run_id,status,boundary,detail,updated_at) "
                "VALUES (?,?,?,?,current_timestamp) ON CONFLICT(run_id) DO UPDATE SET "
                "status=excluded.status,boundary=excluded.boundary,detail=excluded.detail,"
                "updated_at=excluded.updated_at",
                [run_id, status, boundary, detail],
            )
            return
        # The boundary is read in the same statement; no source row inserts nothing.
        count = self.execute(
            "INSERT INTO {s}runs (run_id,status,boundary,detail,updated_at) "
            "SELECT ?,?,confirmed_lsn,?,current_timestamp "
            "FROM {s}source WHERE id=1 "
            "ON CONFLICT(run_id) DO UPDATE SET status=excluded.status,boundary=excluded.boundary,"
            "detail=excluded.detail,updated_at=excluded.updated_at",
            [run_id, status, detail],
        ).fetchone()[0]
        if count != 1:
            raise SourceMismatch("source is not registered; run preflight")


def leased(log, action, wait=0, poll=3, sleep=time.sleep, clock=time.monotonic):
    """Run `action(run_id)` under the ingest lease, or None when it stays busy. CLIs try
    once; action Flights (membership, epoch) wait up to `wait` seconds, because ingest
    holds the lease for most of every scheduled run. A write-write
    conflict with another run's acquire counts as busy."""
    run_id = uuid.uuid4().hex
    deadline = clock() + wait
    while True:
        try:
            log.acquire(run_id)
            break
        except (LeaseBusy, duckdb.TransactionException) as exc:
            # A write-write conflict with another run's acquire is busy too (as in
            # ingest.run_once); any other transaction error is a failure.
            if isinstance(exc, duckdb.TransactionException) and not is_conflict(exc):
                raise
            log.forget()
            if clock() >= deadline:
                return None
            sleep(poll)
    try:
        return action(run_id)
    finally:
        log.release()
