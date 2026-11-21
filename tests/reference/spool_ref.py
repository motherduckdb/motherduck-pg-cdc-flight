# FROZEN reference copy of src/cdc_flight/spool.py at 2462253. Never edit.
# Test-only oracle for tests/test_spool_equivalence.py; its sha256 is pinned there.
"""Deterministic bounded pgoutput transaction chunks, held in memory as Arrow tables."""

import hashlib
import json
from dataclasses import asdict, dataclass, field

import pyarrow as pa

from cdc_flight.pgoutput import decode

EVENT_SCHEMA = pa.schema(
    [
        ("source_epoch", pa.int32()),
        ("commit_lsn", pa.uint64()),
        ("event_ordinal", pa.uint32()),
        ("commit_ts_us", pa.int64()),
        ("txid", pa.uint64()),
        ("relation_id", pa.uint32()),
        ("schema_name", pa.string()),
        ("table_name", pa.string()),
        ("schema_version", pa.string()),
        ("op", pa.string()),
        ("old_key", pa.string()),
        ("new_key", pa.string()),
        ("payload", pa.string()),
    ]
)


@dataclass(frozen=True)
class Chunk:
    # A label ("<commit>_<first>"), not a file: the rows travel in `table`.
    path: str
    epoch: int
    commit: int
    first: int
    count: int
    digest: str
    chain_digest: str
    descriptors: tuple
    bytes: int
    # The chunk's rows (EVENT_SCHEMA), held in memory until the append inserts them.
    table: pa.Table = field(default=None, compare=False, repr=False)

    @property
    def id(self):
        return f"{self.epoch}:{self.commit}:{self.first}:{self.count}"


@dataclass(frozen=True)
class Commit:
    epoch: int
    commit: int
    end: int
    count: int
    digest: str
    marker: bool
    truncates: tuple = ()


class Spool:
    def __init__(self, config, key_columns, heartbeat_oid, marker):
        self.config = config
        self.key_columns, self.heartbeat_oid, self.marker = key_columns, heartbeat_oid, marker
        self.relations = {}
        self.active = False
        self.rows, self.descriptors = [], {}
        self.buffer_bytes = self.total_bytes = self.ordinal = 0
        self.marker_seen = False

    def feed(self, wire):
        if len(wire) > self.config.max_event_bytes:
            raise ValueError("pgoutput message exceeds max_event_bytes; resize explicitly")
        message = decode(wire)
        kind = message["kind"]
        output = []
        if kind == "begin":
            if self.active:
                raise ValueError("nested transaction")
            self.active = True
            self.commit, self.timestamp, self.txid = (
                message["final_lsn"],
                message["time"],
                message["xid"],
            )
            self.ordinal = self.total_bytes = 0
            self.chain = hashlib.sha256()
            self.marker_seen = False
            self.truncated = set()
        elif kind == "relation":
            relation = message["relation"]
            self.relations[relation.oid] = relation
        elif kind in ("insert", "update", "delete", "truncate"):
            if not self.active:
                raise ValueError("change outside transaction")
            relations = message["relations"] if kind == "truncate" else (message["oid"],)
            for oid in relations:
                if kind == "truncate":
                    self.truncated.add(oid)
                relation = self.relations.get(oid)
                if relation is None and kind != "truncate":
                    raise ValueError("missing Relation descriptor")
                old = new = None
                old_key = new_key = None
                if kind != "truncate":
                    names = [column[1] for column in relation.columns]

                    def cells(row):
                        if row is None:
                            return None
                        if len(row) != len(names):
                            raise ValueError("tuple/descriptor width mismatch")
                        return {name: asdict(cell) for name, cell in zip(names, row)}

                    old, new = cells(message["old"]), cells(message["new"])
                    keys = self.key_columns.get(oid)
                    if keys is None:
                        raise ValueError("unregistered relation; run preflight before ingest")

                    def key(row):
                        if row is None:
                            return None
                        # Unsupported DDL must not stop raw capture. Preserve absent,
                        # NULL and unchanged identities for the apply schema gate.
                        return {
                            name: row.get(name, {"kind": "absent", "value": None}) for name in keys
                        }

                    old_key, new_key = key(old), key(new)
                    if (
                        oid == self.heartbeat_oid
                        and new
                        and new.get("marker", {}).get("value") == self.marker
                    ):
                        self.marker_seen = True
                row = dict(
                    source_epoch=self.config.epoch,
                    commit_lsn=self.commit,
                    event_ordinal=self.ordinal,
                    commit_ts_us=self.timestamp,
                    txid=self.txid,
                    relation_id=oid,
                    schema_name=relation.namespace if relation else None,
                    table_name=relation.name if relation else None,
                    schema_version=relation.version if relation else None,
                    op=kind,
                    old_key=json.dumps(old_key, sort_keys=True),
                    new_key=json.dumps(new_key, sort_keys=True),
                    payload=json.dumps(
                        dict(old=old, new=new, truncate_options=message.get("options")),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
                canonical = json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
                self.total_bytes += len(canonical)
                if self.total_bytes > self.config.max_transaction_bytes:
                    raise ValueError("transaction exceeds configured disk/spool budget")
                # Flush before adding a row: restart boundaries depend only on event bytes.
                if self.rows and self.buffer_bytes + len(canonical) > self.config.chunk_bytes:
                    output.append(self.flush())
                self.rows.append(row)
                self.buffer_bytes += len(canonical)
                self.chain.update(canonical + b"\n")
                if relation:
                    self.descriptors[(oid, relation.version)] = json.dumps(
                        asdict(relation), sort_keys=True
                    )
                self.ordinal += 1
        elif kind == "commit":
            if not self.active or message["lsn"] != self.commit or message["end"] <= self.commit:
                raise ValueError("invalid commit boundary")
            if self.rows:
                output.append(self.flush())
            output.append(
                Commit(
                    self.config.epoch,
                    self.commit,
                    message["end"],
                    self.ordinal,
                    self.chain.hexdigest(),
                    self.marker_seen,
                    tuple(sorted(self.truncated)),
                )
            )
            self.active = False
        return output

    def flush(self):
        first = self.rows[0]["event_ordinal"]
        table = pa.Table.from_pylist(self.rows, schema=EVENT_SCHEMA)
        # Hash logical values, not Parquet encoder bytes, which may change by library version.
        digest = hashlib.sha256(
            b"".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                for row in self.rows
            )
        ).hexdigest()
        chunk = Chunk(
            f"{self.commit}_{first}",
            self.config.epoch,
            self.commit,
            first,
            len(self.rows),
            digest,
            self.chain.hexdigest(),
            tuple((oid, version, desc) for (oid, version), desc in self.descriptors.items()),
            self.buffer_bytes,
            table,
        )
        self.rows, self.descriptors, self.buffer_bytes = [], {}, 0
        return chunk
