"""Small executable protocol model. Every transaction writes one database.

The single guard row deliberately serializes pin registration and compaction.
A SELECT of a lease is insufficient: the guard is UPDATED in every transaction
so a worker holding an old MVCC snapshot conflicts with lease takeover.
"""

import hashlib
import json
import re
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from cdc_flight.pgoutput import event_from_dict, normalize


class StaleFence(RuntimeError):
    pass


class LeaseHeld(RuntimeError):
    pass


class LostCommitResponse(ConnectionError):
    pass


def ident(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError("unsafe probe identifier")
    return '"' + value + '"'


class Store:
    def __init__(self, con, database, mirror_database):
        self.con, self.database = con, ident(database)
        self.mirror_database = ident(mirror_database)
        self.holder = uuid.uuid4().hex
        self.held_leases = {}

    def sql(self, statement, args=None):
        # Explicit placeholders avoid rewriting literal @ characters in SQL.
        return self.con.execute(
            statement.replace("{i}", self.database + ".main.").replace(
                "{m}", self.mirror_database + ".main."
            ),
            args or [],
        )

    def initialize(self):
        for db, scopes in (("{i}", ("ingest", "retention")), ("{m}", ("apply",))):
            self.sql(
                f"CREATE TABLE {db}fence (scope VARCHAR PRIMARY KEY, token BIGINT, "
                "touch VARCHAR, holder VARCHAR, expires_at TIMESTAMPTZ, ttl_seconds INTEGER)"
            )
            for scope in scopes:
                self.sql(f"INSERT INTO {db}fence VALUES (?,0,'',NULL,NULL,900)", [scope])
            self.sql(
                f"CREATE TABLE {db}checkpoint (scope VARCHAR PRIMARY KEY, epoch INTEGER, boundary UBIGINT)"
            )
            self.sql(
                f"INSERT INTO {db}checkpoint VALUES (?,1,0)", ["ingest" if db == "{i}" else "apply"]
            )
        self.sql(
            "CREATE TABLE {i}events (epoch INTEGER, commit_lsn UBIGINT, ordinal INTEGER, "
            "end_lsn UBIGINT, payload VARCHAR, PRIMARY KEY(epoch,commit_lsn,ordinal))"
        )
        self.sql(
            "CREATE TABLE {i}schemas (epoch INTEGER, oid UINTEGER, version VARCHAR, "
            "descriptor VARCHAR, PRIMARY KEY(epoch,oid,version))"
        )
        self.sql("CREATE TABLE {i}batches (id VARCHAR PRIMARY KEY, digest VARCHAR)")
        self.sql(
            "CREATE TABLE {i}manifests (epoch INTEGER, commit_lsn UBIGINT, end_lsn UBIGINT, "
            "n INTEGER, PRIMARY KEY(epoch,commit_lsn))"
        )
        self.sql("CREATE TABLE {m}mirror (key VARCHAR PRIMARY KEY, value VARCHAR)")
        self.sql(
            "CREATE TABLE {m}generation (id VARCHAR PRIMARY KEY, state VARCHAR, "
            "low UBIGINT, high UBIGINT, last_chunk INTEGER, source VARCHAR, epoch INTEGER, "
            "oid UINTEGER, keys VARCHAR, marker VARCHAR, marker_oid UINTEGER)"
        )
        self.sql(
            "CREATE TABLE {m}chunks (generation VARCHAR, id INTEGER, digest VARCHAR, "
            "PRIMARY KEY(generation,id))"
        )
        self.sql(
            "CREATE TABLE {i}pins (owner VARCHAR PRIMARY KEY, epoch INTEGER, boundary UBIGINT)"
        )
        self.sql("CREATE TABLE {m}current_generation (id VARCHAR)")
        self.sql("CREATE TABLE {i}consumers (owner VARCHAR PRIMARY KEY)")
        self.sql(
            "CREATE TABLE {i}partitions (name VARCHAR PRIMARY KEY, high UBIGINT, sealed BOOLEAN, epoch INTEGER DEFAULT 1)"
        )
        self.sql("CREATE TABLE {i}retention (id INTEGER PRIMARY KEY, epoch INTEGER, floor UBIGINT)")
        self.sql("INSERT INTO {i}retention VALUES (1,1,0)")

    @contextmanager
    def transaction(self, *, lose_response=False):
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
        if lose_response:
            raise LostCommitResponse("injected: server committed; client did not observe response")

    @staticmethod
    def fence_db(scope):
        if scope not in ("apply", "ingest", "retention"):
            raise ValueError("unknown scope")
        return "{m}" if scope == "apply" else "{i}"

    def acquire(self, scope, *, test_clock_offset_seconds=0, ttl_seconds=900):
        db = self.fence_db(scope)
        if ttl_seconds <= 0:
            raise ValueError("lease TTL must be positive")
        with self.transaction():
            changed = self.sql(
                f"UPDATE {db}fence SET token=token+1,touch=uuid()::VARCHAR,holder=?, "
                "expires_at=current_timestamp+(?+?)*INTERVAL '1 second',ttl_seconds=? "
                "WHERE scope=? AND (expires_at IS NULL OR "
                "expires_at<=current_timestamp+?*INTERVAL '1 second')",
                [
                    self.holder,
                    ttl_seconds,
                    test_clock_offset_seconds,
                    ttl_seconds,
                    scope,
                    test_clock_offset_seconds,
                ],
            ).fetchone()[0]
            if changed != 1:
                raise LeaseHeld(scope)
            token = self.sql(f"SELECT token FROM {db}fence WHERE scope=?", [scope]).fetchone()[0]
        self.held_leases[scope] = token
        return token

    def release(self, scope, token):
        db = self.fence_db(scope)
        with self.transaction():
            changed = self.sql(
                f"UPDATE {db}fence SET touch=uuid()::VARCHAR,expires_at=NULL,holder=NULL "
                "WHERE scope=? AND token=?",
                [scope, token],
            ).fetchone()[0]
        if self.held_leases.get(scope) == token:
            del self.held_leases[scope]
        return changed == 1

    @contextmanager
    def lease(self, scope, **options):
        """A run owns a lease only for this context, including exceptional exits."""
        token = self.acquire(scope, **options)
        try:
            yield token
        finally:
            self.release(scope, token)

    def release_all(self):
        """Test-run cleanup; abrupt process death still relies on persisted expiry."""
        for scope, token in list(self.held_leases.items()):
            self.release(scope, token)

    def guard(self, scope, token):
        # A database clock and shared write column protect lease ownership. Renew
        # with a commit margin; long-running uploads still need Phase 1 keepalives.
        db = self.fence_db(scope)
        changed = self.sql(
            f"UPDATE {db}fence SET touch=uuid()::VARCHAR, "
            "expires_at=greatest(expires_at,current_timestamp+ttl_seconds*INTERVAL '1 second') "
            "WHERE scope=? AND token=? AND expires_at>current_timestamp",
            [scope, token],
        ).fetchone()[0]
        if changed != 1:
            raise StaleFence(scope)

    def position(self, scope):
        db = self.fence_db(scope)
        return self.sql(
            f"SELECT epoch,boundary FROM {db}checkpoint WHERE scope=?", [scope]
        ).fetchone()

    def checkpoint(self, scope):
        return self.position(scope)[1]

    def start_epoch(self, epoch, token):
        """Explicit slot-recreation boundary; old mirror remains until resync publishes."""
        with self.transaction():
            self.guard("ingest", token)
            current = self.position("ingest")[0]
            if epoch == current:
                return
            if epoch != current + 1:
                raise ValueError("epoch must advance by one")
            self.sql("UPDATE {i}checkpoint SET epoch=?,boundary=0 WHERE scope='ingest'", [epoch])

    def read_events(self, low, high, *, epoch=None):
        epoch = self.position("ingest")[0] if epoch is None else epoch
        records = self.sql(
            "SELECT e.payload FROM {i}events e JOIN {i}manifests m USING(epoch,commit_lsn) "
            "WHERE e.epoch=? AND e.end_lsn>? AND e.end_lsn<=? ORDER BY e.commit_lsn,e.ordinal",
            [epoch, low, high],
        ).fetchall()
        descriptors = {
            (epoch, oid, version): json.loads(desc)
            for epoch, oid, version, desc in self.sql("SELECT * FROM {i}schemas").fetchall()
        }
        result = []
        for (payload,) in records:
            value = json.loads(payload)
            value["relation"] = descriptors[
                (value["epoch"], value.pop("relation_oid"), value.pop("schema_version"))
            ]
            result.append(event_from_dict(value))
        return result

    def append(self, epoch, commit, end, rows, token, *, fail=False, lose_response=False):
        """One complete transaction per batch for the probe; payloads are canonical JSON.

        Event-based identity does not depend on runtime flush timing. Phase 1
        chunk IDs must include ordinal ranges and content digests, not only LSNs.
        """
        if end <= commit:
            raise ValueError("invalid transaction")
        descriptors, compact = {}, []
        for row in rows:
            row = dict(row)
            if "relation" in row:
                from cdc_flight.pgoutput import Relation

                relation = row.pop("relation")
                version = Relation(**relation).version
                descriptors[(epoch, relation["oid"], version)] = json.dumps(
                    relation, sort_keys=True
                )
                row.update(relation_oid=relation["oid"], schema_version=version)
            compact.append(row)
        payloads = [json.dumps(row, sort_keys=True, separators=(",", ":")) for row in compact]
        digest = hashlib.sha256(json.dumps([end, payloads]).encode()).hexdigest()
        batch = f"{epoch}:{commit}:0:{len(rows)}"
        with self.transaction(lose_response=lose_response):
            self.guard("ingest", token)
            if epoch != self.position("ingest")[0]:
                raise ValueError("append requires active source epoch")
            existing = self.sql("SELECT digest FROM {i}batches WHERE id=?", [batch]).fetchone()
            if existing:
                if existing[0] != digest:
                    raise ValueError("replayed identity has different contents")
                return
            if end <= self.checkpoint("ingest"):
                raise ValueError("unknown transaction below checkpoint")
            for (schema_epoch, oid, version), descriptor in descriptors.items():
                self.sql(
                    "INSERT INTO {i}schemas VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
                    [schema_epoch, oid, version, descriptor],
                )
            # Parquet is the same bulk load path used in the throughput probe.
            with tempfile.TemporaryDirectory(prefix="cdc_p0_") as directory:
                path = str(Path(directory) / "events.parquet")
                pq.write_table(
                    pa.table(
                        {
                            "ordinal": pa.array(range(len(payloads)), type=pa.int32()),
                            "payload": pa.array(payloads, type=pa.string()),
                        }
                    ),
                    path,
                )
                self.sql(
                    "INSERT INTO {i}events SELECT ?, ?, ordinal, ?, payload FROM read_parquet(?)",
                    [epoch, commit, end, path],
                )
            self.sql("INSERT INTO {i}batches VALUES (?,?)", [batch, digest])
            if fail:
                raise RuntimeError("injected before manifest/checkpoint")
            self.sql("INSERT INTO {i}manifests VALUES (?,?,?,?)", [epoch, commit, end, len(rows)])
            self.sql("UPDATE {i}checkpoint SET boundary=? WHERE scope='ingest'", [end])

    def merge(self, rows, boundary, token, *, epoch=None, fail=False, lose_response=False):
        previous = self.position("apply")
        epoch = previous[0] if epoch is None else epoch
        self.pin("table:mirror", previous[1], epoch=previous[0])
        with self.transaction(lose_response=lose_response):
            self.guard("apply", token)
            if (
                epoch != self.position("apply")[0]
                or epoch != self.position("ingest")[0]
                or (epoch, boundary) < self.position("apply")
            ):
                raise ValueError("checkpoint regression")
            # None is a tombstone; JSON string values may contain JSON nulls.
            keys, vals = list(rows), list(rows.values())
            self.sql(
                "MERGE INTO {m}mirror m USING "
                "(SELECT unnest(?::VARCHAR[]) AS key, unnest(?::VARCHAR[]) AS value) s "
                "ON m.key=s.key WHEN MATCHED AND s.value IS NULL THEN DELETE "
                "WHEN MATCHED THEN UPDATE SET value=s.value "
                "WHEN NOT MATCHED AND s.value IS NOT NULL THEN INSERT VALUES(s.key,s.value)",
                [keys, vals],
            )
            if fail:
                raise RuntimeError("injected after merge")
            self.sql("UPDATE {m}checkpoint SET boundary=? WHERE scope='apply'", [boundary])
        self.pin("table:mirror", boundary, epoch=epoch)

    def begin_generation(
        self, name, low, token, *, source="probe", epoch=1, oid=1, keys=("id",), lose_response=False
    ):
        table = ident("stage_" + name)
        if epoch != self.position("ingest")[0] or low > self.checkpoint("ingest"):
            raise ValueError("generation requires a committed ingest boundary")
        known = self.sql("SELECT id FROM {m}generation WHERE id=?", [name]).fetchone()
        if not known:
            self.pin("generation:" + name, low, epoch=epoch)
        with self.transaction(lose_response=lose_response):
            self.guard("apply", token)
            # Repeat the check inside the mirror transaction's cross-database snapshot.
            if epoch != self.position("ingest")[0] or low > self.checkpoint("ingest"):
                raise ValueError("generation requires a committed ingest boundary")
            existing = self.sql(
                "SELECT low,source,epoch,oid,keys FROM {m}generation WHERE id=?", [name]
            ).fetchone()
            binding = (low, source, epoch, oid, json.dumps(keys))
            if existing:
                if existing != binding:
                    raise ValueError("generation identity reused with different inputs")
            else:
                if (epoch, low) < self.position("apply"):
                    raise ValueError("generation boundary regresses mirror")
                self.sql("UPDATE {m}generation SET state='abandoned' WHERE state!='published'")
                self.sql(f"CREATE TABLE {{m}}{table} (key VARCHAR PRIMARY KEY, value VARCHAR)")
                self.sql(
                    "INSERT INTO {m}generation VALUES (?, 'snapshotting', ?, NULL, -1, "
                    "?,?,?,?,NULL,NULL)",
                    [name, *binding],
                )
                self.sql("DELETE FROM {m}current_generation")
                self.sql("INSERT INTO {m}current_generation VALUES (?)", [name])
        # Failure between commits leaks only conservative pins. A retry cleans them.
        abandoned = [
            "generation:" + row[0]
            for row in self.sql("SELECT id FROM {m}generation WHERE state='abandoned'").fetchall()
        ]
        self.release_pins(abandoned)

    def chunk(self, name, chunk_id, rows, token, *, fail=False, lose_response=False):
        table = ident("stage_" + name)
        digest = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
        with self.transaction(lose_response=lose_response):
            self.guard("apply", token)
            state = self.sql(
                "SELECT state,last_chunk FROM {m}generation WHERE id=?", [name]
            ).fetchone()
            if not state or state[0] != "snapshotting":
                raise ValueError("inactive generation")
            if chunk_id <= state[1]:
                previous = self.sql(
                    "SELECT digest FROM {m}chunks WHERE generation=? AND id=?", [name, chunk_id]
                ).fetchone()
                if previous != (digest,):
                    raise ValueError("snapshot chunk identity reused")
                return
            if chunk_id != state[1] + 1:
                raise ValueError("non-contiguous snapshot chunk")
            self.sql(
                f"INSERT INTO {{m}}{table} SELECT unnest(?::VARCHAR[]), unnest(?::VARCHAR[])",
                [list(rows), list(rows.values())],
            )
            if fail:
                raise RuntimeError("injected after snapshot rows")
            self.sql("INSERT INTO {m}chunks VALUES (?,?,?)", [name, chunk_id, digest])
            self.sql("UPDATE {m}generation SET last_chunk=? WHERE id=?", [chunk_id, name])

    def finish_scan(self, name, marker, marker_oid, token):
        with self.transaction():
            self.guard("apply", token)
            row = self.sql(
                "SELECT state,marker,marker_oid FROM {m}generation WHERE id=?", [name]
            ).fetchone()
            if row == ("catching_up", marker, marker_oid):
                return
            if not row or row[0] != "snapshotting":
                raise ValueError("inactive generation")
            self.sql(
                "UPDATE {m}generation SET state='catching_up',marker=?,marker_oid=? WHERE id=?",
                [marker, marker_oid, name],
            )

    def publish(self, name, high, token, *, fail=False, lose_response=False):
        """Read durable chunks + internal log, write only mirror DB, then advance pin."""
        table = ident("stage_" + name)
        with self.transaction(lose_response=lose_response):
            self.guard("apply", token)
            state = self.sql(
                "SELECT state,low,high,source,epoch,oid,keys,marker,marker_oid "
                "FROM {m}generation WHERE id=?",
                [name],
            ).fetchone()
            if self.sql("SELECT id FROM {m}current_generation").fetchone() != (name,):
                return  # delayed retry of an older published or abandoned generation
            if state and state[0] == "published" and state[2] == high:
                pass
            else:
                if (
                    not state
                    or state[0] != "catching_up"
                    or high < state[1]
                    or (state[4], high) < self.position("apply")
                    or state[4] != self.position("ingest")[0]
                ):
                    raise ValueError("inactive generation or invalid marker")
                events = [
                    e
                    for e in self.read_events(state[1], high, epoch=state[4])
                    if (e.source, e.epoch) == (state[3], state[4])
                ]
                if not any(
                    e.relation.oid == state[8]
                    and e.end == high
                    and e.new
                    and e.new[0].value == state[7]
                    for e in events
                ):
                    raise ValueError("marker is not durable at requested boundary")
                baseline = {
                    tuple(json.loads(key)): json.loads(value)
                    for key, value in self.sql(f"SELECT * FROM {{m}}{table}").fetchall()
                }
                folded = normalize(
                    baseline,
                    [e for e in events if e.relation.oid == state[5]],
                    tuple(json.loads(state[6])),
                )
                rows = {
                    json.dumps(key): json.dumps(value, sort_keys=True)
                    for key, value in folded.items()
                }
                self.sql(f"DELETE FROM {{m}}{table}")
                self.sql(
                    f"INSERT INTO {{m}}{table} SELECT unnest(?::VARCHAR[]), unnest(?::VARCHAR[])",
                    [list(rows), list(rows.values())],
                )
                self.sql(f"CREATE OR REPLACE TABLE {{m}}mirror AS SELECT * FROM {{m}}{table}")
                if fail:
                    raise RuntimeError("injected after table replacement")
                self.sql(
                    "UPDATE {m}generation SET state='published',high=? WHERE id=?", [high, name]
                )
                self.sql(
                    "UPDATE {m}checkpoint SET epoch=?,boundary=? WHERE scope='apply'",
                    [state[4], high],
                )
        # A failure before this second commit leaves a conservative old pin.
        with self.transaction():
            self.retention_guard()
            # Monotonic table position also protects a racing newer publication.
            self.put_pin("table:mirror", state[4], high)
            self.remove_pin("generation:" + name)

    def retention_guard(self):
        self.sql("UPDATE {i}fence SET touch=uuid()::VARCHAR WHERE scope='retention'")

    def put_pin(self, owner, epoch, boundary):
        floor = self.sql("SELECT epoch,floor FROM {i}retention").fetchone()
        if (epoch, boundary) < floor:
            raise ValueError("requested history already compacted")
        self.sql("INSERT INTO {i}consumers VALUES (?) ON CONFLICT DO NOTHING", [owner])
        current = self.sql("SELECT epoch,boundary FROM {i}pins WHERE owner=?", [owner]).fetchone()
        if current is None:
            self.sql("INSERT INTO {i}pins VALUES (?,?,?)", [owner, epoch, boundary])
        elif (epoch, boundary) > current:
            self.sql(
                "UPDATE {i}pins SET epoch=?,boundary=? WHERE owner=?", [epoch, boundary, owner]
            )

    def pin(self, owner, boundary, *, epoch=None):
        epoch = self.position("ingest")[0] if epoch is None else epoch
        with self.transaction():
            self.retention_guard()
            self.put_pin(owner, epoch, boundary)

    def remove_pin(self, owner):
        self.sql("DELETE FROM {i}pins WHERE owner=?", [owner])
        self.sql("DELETE FROM {i}consumers WHERE owner=?", [owner])

    def release_pins(self, owners):
        if not owners:
            return
        with self.transaction():
            self.retention_guard()
            for owner in owners:
                self.remove_pin(owner)

    def compact(self, name, token, *, fail=False):
        table = ident(name)
        with self.transaction():
            self.guard("retention", token)
            part = self.sql(
                "SELECT epoch,high,sealed FROM {i}partitions WHERE name=?", [name]
            ).fetchone()
            if not part or not part[2]:
                return False
            minimum = self.sql(
                "SELECT epoch,boundary FROM {i}pins ORDER BY epoch,boundary LIMIT 1"
            ).fetchone()
            missing = self.sql(
                "SELECT count(*) FROM {i}consumers c ANTI JOIN {i}pins p USING(owner)"
            ).fetchone()[0]
            if minimum is None or missing or part[:2] >= minimum:
                return False
            self.sql(f"DROP TABLE {{i}}{table}")
            if fail:
                raise RuntimeError("injected after drop")
            floor = self.sql("SELECT epoch,floor FROM {i}retention").fetchone()
            if part[:2] > floor:
                self.sql("UPDATE {i}retention SET epoch=?,floor=?", list(part[:2]))
            self.sql("DELETE FROM {i}partitions WHERE name=?", [name])
            return True
