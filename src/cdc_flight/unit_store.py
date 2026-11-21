"""Format-2 storage and fail-closed, whole-unit reads (active since the format-2 cutover).

A reader never identifies a unit
by a member COMMIT LSN, and every data read is bounded by a snapshotted high.
"""

import json
from dataclasses import dataclass

from cdc_flight.config import quoted
from cdc_flight.format2 import Binding, Descriptor, uint
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.retention_common import HistoryDropped, PinMissing


def pending_key_observations(payload):
    """Pure validation shared by guarded key activation and endpoint reclamation."""
    try:
        queue = json.loads(payload) if payload is not None else []
    except (TypeError, ValueError) as exc:
        raise SourceMismatch("malformed pending key observations") from exc
    if type(queue) is not list:
        raise SourceMismatch("malformed pending key observations")
    for entry in queue:
        if type(entry) is not dict or set(entry) != {"epoch", "keys", "cut_lsn", "observed_lsn"}:
            raise SourceMismatch("malformed pending key observation")
        for name, bits in (("epoch", 32), ("cut_lsn", 64), ("observed_lsn", 64)):
            uint(entry[name], bits, "pending " + name)
        keys = entry["keys"]
        if keys is not None and (
            type(keys) is not list
            or not keys
            or any(type(key) is not str or not key for key in keys)
            or len(set(keys)) != len(keys)
        ):
            raise SourceMismatch("malformed pending key columns")
    if payload is not None and not queue:
        raise SourceMismatch("empty pending key observations")
    return queue


PROTOCOL = {
    "protocol_v2": "id INTEGER PRIMARY KEY,format_version INTEGER,min_reader INTEGER,binding_hash VARCHAR,binding_json VARCHAR,active_revision UBIGINT,server_major INTEGER,last_capped_epoch UINTEGER,last_capped_anchor UBIGINT,last_capped_commit UBIGINT,last_capped_frame UBIGINT",
    "unit_configs_v2": "revision UBIGINT PRIMARY KEY,binding_hash VARCHAR,config_json VARCHAR",
    "units_v2": "epoch UINTEGER,unit_start_lsn UBIGINT,end_lsn UBIGINT,complete BOOLEAN,config_revision UBIGINT,member_count UBIGINT,event_count UBIGINT,frame_count UBIGINT,bytes UBIGINT,chain_digest VARCHAR,digest VARCHAR,first_commit UBIGINT,last_commit UBIGINT,tail_commit UBIGINT,tail_ordinal UBIGINT,last_end UBIGINT,tail_txid UINTEGER,tail_timestamp BIGINT,abandoned_into_epoch UINTEGER,abandoned_reason VARCHAR,abandoned_frame_end UBIGINT,PRIMARY KEY(epoch,unit_start_lsn)",
    "chunks_v2": "id VARCHAR PRIMARY KEY,epoch UINTEGER,unit_start_lsn UBIGINT,first_frame UBIGINT,n UBIGINT,event_count UBIGINT,member_count UBIGINT,bytes UBIGINT,digest VARCHAR,chain_digest VARCHAR,partition_day VARCHAR,config_revision UBIGINT,mapping_revision UBIGINT,descriptor_keys VARCHAR",
    "descriptors_v2": "epoch UINTEGER,relation_id UINTEGER,version VARCHAR,key_hash VARCHAR,names VARCHAR,keys VARCHAR,descriptor VARCHAR,namespace VARCHAR,table_name VARCHAR,key_indices VARCHAR,PRIMARY KEY(epoch,relation_id,version,key_hash)",
    "frame_partitions_v2": "partition_day VARCHAR,epoch UINTEGER,min_unit UBIGINT,max_unit UBIGINT,n UBIGINT,bytes UBIGINT,status VARCHAR,PRIMARY KEY(partition_day,epoch)",
}

UNIT_TYPES = dict(
    epoch="UINTEGER",
    unit_start_lsn="UBIGINT",
    end_lsn="UBIGINT",
    complete="BOOLEAN",
    config_revision="UBIGINT",
    member_count="UBIGINT",
    event_count="UBIGINT",
    frame_count="UBIGINT",
    bytes="UBIGINT",
    chain_digest="VARCHAR",
    digest="VARCHAR",
    first_commit="UBIGINT",
    last_commit="UBIGINT",
    tail_commit="UBIGINT",
    tail_ordinal="UBIGINT",
    last_end="UBIGINT",
    tail_txid="UINTEGER",
    tail_timestamp="BIGINT",
    abandoned_into_epoch="UINTEGER",
    abandoned_reason="VARCHAR",
    abandoned_frame_end="UBIGINT",
)
CHUNK_TYPES = dict(
    id="VARCHAR",
    epoch="UINTEGER",
    unit_start_lsn="UBIGINT",
    first_frame="UBIGINT",
    n="UBIGINT",
    event_count="UBIGINT",
    member_count="UBIGINT",
    bytes="UBIGINT",
    digest="VARCHAR",
    chain_digest="VARCHAR",
    partition_day="VARCHAR",
    config_revision="UBIGINT",
    mapping_revision="UBIGINT",
    descriptor_keys="VARCHAR",
)
DESCRIPTOR_TYPES = dict(
    epoch="UINTEGER",
    relation_id="UINTEGER",
    version="VARCHAR",
    key_hash="VARCHAR",
    names="VARCHAR",
    keys="VARCHAR",
    descriptor="VARCHAR",
    namespace="VARCHAR",
    table_name="VARCHAR",
    key_indices="VARCHAR",
)


def encoded(value):
    return json.dumps(value, separators=(",", ":"), allow_nan=False)


def metadata_parameter(rows, types, nullable=()):
    """SQL JSON casts coerce: enforce Python types and integer ranges first."""
    for row in rows:
        if row.keys() != types.keys():
            raise ValueError("invalid metadata columns")
        for name, dtype in types.items():
            value = row[name]
            if value is None and name in nullable:
                continue
            kind = {
                "VARCHAR": str,
                "BOOLEAN": bool,
                "UINTEGER": int,
                "UBIGINT": int,
                "BIGINT": int,
            }[dtype]
            if type(value) is not kind:
                raise ValueError(f"invalid {name} metadata type")
            if kind is int and not (
                -(1 << 63) <= value < 1 << 63
                if dtype == "BIGINT"
                else 0 <= value < 1 << (32 if dtype == "UINTEGER" else 64)
            ):
                raise ValueError(f"invalid {name} metadata range")
    return encoded(rows)


def insert_statement(table, types, conflict=""):
    # One JSON parameter, fixed source names: folded input never becomes a label.
    structure = encoded([{name: dtype for name, dtype in types.items()}]).replace("'", "''")
    columns = ",".join(quoted(name) for name in types)
    projection = ",".join(f"r.{quoted(name)} AS {quoted(name)}" for name in types)
    return (
        f"INSERT INTO {{s}}{table} ({columns}) SELECT {projection} FROM "
        f"(SELECT unnest(from_json_strict(?, '{structure}')) AS r) AS input {conflict}"
    )


def descriptor_row(epoch, desc):
    return dict(
        epoch=epoch,
        relation_id=desc.oid,
        version=desc.version,
        key_hash=desc.key_hash.hex(),
        names=encoded(desc.names),
        keys=encoded(desc.keys),
        descriptor=desc.json,
        namespace=desc.namespace,
        table_name=desc.table_name,
        key_indices=encoded(desc.key_indices),
    )


def descriptor_from_row(row):
    return Descriptor(
        row["relation_id"],
        row["version"],
        tuple(json.loads(row["names"])),
        tuple(json.loads(row["keys"])),
        row["descriptor"],
        bytes.fromhex(row["key_hash"]),
        row["namespace"],
        row["table_name"],
        tuple(json.loads(row["key_indices"])),
    )


def stored_binding(payload, digest, source_id):
    """Persist the output contract itself, not just an unrecoverable fingerprint."""
    try:
        value = json.loads(payload)
        if type(value) is not dict or set(value) != {
            "source_id",
            "settings",
            "extensions",
            "mapping_revision",
        }:
            raise ValueError("unknown stored binding fields")
        binding = Binding(
            value["source_id"],
            tuple(tuple(p) for p in value["settings"]),
            tuple(tuple(p) for p in value["extensions"]),
            value["mapping_revision"],
        )
    except (ValueError, TypeError, KeyError) as exc:
        raise SourceMismatch("unsupported or malformed stored text binding") from exc
    if binding.source_id != source_id or binding.digest.hex() != digest:
        raise SourceMismatch("stored text binding identity is inconsistent")
    return binding


@dataclass(frozen=True)
class Head:
    epoch: int
    high: int
    open_anchor: int | None
    config_revision: int


@dataclass(frozen=True)
class Unit:
    epoch: int
    unit_start_lsn: int
    end_lsn: int
    member_count: int
    event_count: int
    frame_count: int
    bytes: int
    config_revision: int
    first_commit: int
    last_commit: int
    chain_digest: str
    digest: str
    days: tuple

    @property
    def start(self):
        return self.unit_start_lsn

    @property
    def end(self):
        return self.end_lsn


FETCH_ROWS = 8192  # frame rows per fetchmany


class UnitReader:
    def __init__(self, con, config):
        self.con, self.config = con, config
        self.prefix = f"{quoted(config.database)}.{quoted(config.source_id)}."
        self._head = self._binding = None
        self.commit_times = {}  # (epoch, unit end) -> last commit_ts_us; see check_frames

    def execute(self, statement, parameters=None):
        return self.con.execute(statement.replace("{s}", self.prefix), parameters or [])

    def head(self):
        row = self.execute(
            "SELECT s.epoch,s.confirmed_lsn,s.open_unit_anchor,p.active_revision,"
            "p.format_version,p.min_reader,p.binding_hash,s.binding,p.binding_json "
            "FROM {s}source s,{s}protocol_v2 p WHERE s.id=1 AND p.id=1"
        ).fetchone()
        if (
            row is None
            or row[4] != 2
            or row[5] > 2
            or json.loads(row[7]).get("format_version") != 2
        ):
            raise SourceMismatch("unsupported or unregistered source format")
        self._binding = stored_binding(row[8], row[6], self.config.source_id)
        binding = getattr(self.config, "text_binding", None)
        if binding is not None and row[6] != binding.digest.hex():
            raise SourceMismatch("format-2 source binding mismatch")
        if row[2] is not None and row[2] != row[1]:
            raise SourceMismatch("open unit anchor differs from confirmed end")
        self._head = Head(*row[:4])
        return self._head

    def read_binding(self):
        self.head()
        return self._binding

    def units(
        self, epoch, after, high, *, max_events=100_000, max_bytes=50 * 1024 * 1024, max_units=512
    ):
        head = self.head()
        if (
            any(type(n) is not int or n < 0 for n in (epoch, after, high))
            or epoch != head.epoch
            or not after <= high <= head.high
            or any(type(n) is not int or n <= 0 for n in (max_events, max_bytes, max_units))
        ):
            raise ValueError("invalid unit read bounds")
        # A bounded page plus one lookahead. Empty-event units must not turn
        # repeated backlog planning into a scan of all remaining history.
        page = min(max_units, 512)
        records, straddles_high, floor = self.execute(
            "SELECT (SELECT list(u ORDER BY unit_start_lsn) FROM (SELECT u.*,"
            "(SELECT list(DISTINCT partition_day ORDER BY partition_day) "
            "FROM {s}chunks_v2 c WHERE c.epoch=u.epoch AND c.unit_start_lsn=u.unit_start_lsn) AS days "
            "FROM {s}units_v2 u WHERE epoch=? AND ((unit_start_lsn>=? AND unit_start_lsn<?) "
            "OR (unit_start_lsn<? AND (end_lsn>? OR NOT complete))) "
            "ORDER BY unit_start_lsn LIMIT ?) u),"
            "EXISTS(SELECT 1 FROM {s}units_v2 WHERE epoch=? AND unit_start_lsn<? "
            "AND (end_lsn>? OR NOT complete)), "
            "(SELECT [floor_epoch,floor_lsn] FROM {s}retention WHERE id=1)",
            [epoch, after, high, after, after, page + 1, epoch, high, high],
        ).fetchone()
        self._check_floor(floor, epoch, after)
        if straddles_high:
            raise ValueError("unit chain has an incomplete unit or high straddle")
        records = records or []
        boundary, selected, events, size = after, [], 0, 0
        for row in records:
            if selected and (
                len(selected) == page
                or events + row["event_count"] > max_events
                or size + row["bytes"] > max_bytes
            ):
                return tuple(selected)
            if (
                row["unit_start_lsn"] != boundary
                or not row["complete"]
                or row["end_lsn"] is None
                or not boundary < row["end_lsn"] <= high
            ):
                raise ValueError("unit chain has a gap, overlap, incomplete unit or straddle")
            selected.append(
                Unit(
                    **{k: row[k] for k in Unit.__dataclass_fields__ if k != "days"},
                    days=tuple(row["days"] or ()),
                )
            )
            boundary = row["end_lsn"]
            events += row["event_count"]
            size += row["bytes"]
        if boundary != high:
            raise ValueError("unit chain does not reach the snapshotted high")
        return tuple(selected)

    def _bounds(self, units, high):
        if self._head is None:
            self.head()  # format/capability dispatch before the first physical scan
        if not units:
            raise ValueError("a nonempty contiguous unit range is required")
        epoch, low, end = units[0].epoch, units[0].start, units[-1].end
        if end > high:
            raise ValueError("unit range exceeds high")
        for previous, current in zip(units, units[1:]):
            if current.epoch != epoch or previous.end != current.start:
                raise ValueError("non-contiguous unit range")
        if any(
            type(value) is not int or not 0 <= value < 1 << 64 for value in (epoch, low, end, high)
        ):
            raise ValueError("invalid snapshotted high")
        return epoch, low, end

    def _check_position(self, position, epoch, high):
        if (
            position is None
            or position["epoch"] != epoch
            or position["high"] < high
            or position["format"] != 2
            or position["reader"] > 2
        ):
            raise SourceMismatch("reader source/epoch/version changed")
        binding = stored_binding(
            position["binding_json"], position["binding_hash"], self.config.source_id
        )
        expected = getattr(self.config, "text_binding", None)
        if expected is not None and binding.digest != expected.digest:
            raise SourceMismatch("reader source binding changed")

    @staticmethod
    def _check_floor(floor, epoch, low):
        if floor is None:
            raise PinMissing("retention metadata is not initialized")
        if floor[0] is not None and (epoch, low) <= tuple(floor):
            raise HistoryDropped("unit history was dropped under this reader")

    def frames(self, units, relation_id=None, *, high):
        epoch, low, end = self._bounds(units, high)
        days = sorted({day for unit in units for day in unit.days})
        if any(
            type(day) is not str or len(day) != 8 or not day.isascii() or not day.isdigit()
            for day in days
        ):
            raise ValueError("invalid physical day")
        branches = [
            f"SELECT * FROM {{s}}frames_v2_{day} WHERE source_epoch={epoch} "
            f"AND unit_start_lsn>={low} AND unit_start_lsn<{end}"
            for day in days
        ]
        if not branches:
            raise ValueError("complete unit has no physical frame days")
        # Filter before transfer, while validating the entire physical range via
        # small server-side per-unit aggregates. Applying one table must not
        # download every other table's rows in the same units.
        predicate = ""
        parameters = []
        if relation_id is not None:
            if type(relation_id) is not int or not 0 <= relation_id < 1 << 32:
                raise ValueError("invalid relation id")
            predicate = " WHERE record_kind=2 OR relation_id=? OR list_contains(truncate_oids,?)"
            parameters = [relation_id, relation_id]
        # One statement: frames and checks share a snapshot. Row 1 is the checks; frames
        # follow as rows (a list(STRUCT) value costs ~3x the time and memory in Python).
        cursor = self.execute(
            "WITH physical AS MATERIALIZED (" + " UNION ALL ".join(branches) + "), "
            "checks AS (SELECT 0 AS _part,"
            "(SELECT [floor_epoch,floor_lsn] FROM {s}retention WHERE id=1) AS _floor,"
            "(SELECT {'epoch':s.epoch,'high':s.confirmed_lsn,'format':p.format_version,"
            "'reader':p.min_reader,'binding_hash':p.binding_hash,'binding_json':p.binding_json} "
            "FROM {s}source s,{s}protocol_v2 p WHERE s.id=1 AND p.id=1) AS _position,"
            "(SELECT list(c) FROM (SELECT unit_start_lsn,count(*) AS n,"
            "count(DISTINCT frame_ordinal) AS distinct_n,min(frame_ordinal) AS first,"
            "max(frame_ordinal) AS last,"
            "arg_max(commit_ts_us,frame_ordinal) FILTER (WHERE record_kind=2) AS last_commit_ts FROM physical GROUP BY unit_start_lsn) c) AS _counts), "
            "rows AS (SELECT 1 AS _part,f.* FROM physical f" + predicate + ") "
            "SELECT _part,_floor,_position,_counts,"
            "COLUMNS(* EXCLUDE (_part,_floor,_position,_counts)) "
            "FROM (FROM checks UNION ALL BY NAME FROM rows) "
            "ORDER BY _part,unit_start_lsn,frame_ordinal",
            parameters,
        )
        names = [column[0] for column in cursor.description][4:]
        first = cursor.fetchone()
        if first is None or first[0] != 0:
            raise ValueError("frame read returned no checks row")
        floor, position, counts = first[1:4]
        rows = []
        while batch := cursor.fetchmany(FETCH_ROWS):
            rows.extend(dict(zip(names, row[4:])) for row in batch)
        self.check_frames(units, high, floor, position, counts)
        return rows or []

    def check_frames(self, units, high, floor, position, counts, unknown_kinds=0):
        """The checks every physical read of `units` makes on its own statement's results,
        in this order: position, retention floor, per-unit counts and gaps (over the
        unfiltered range), then (apply's SQL fold, sql_fold.plan_sql) unknown record kinds,
        which logical_events otherwise refuses."""
        epoch, low, _ = self._bounds(units, high)
        self._check_position(position, epoch, high)
        self._check_floor(floor, epoch, low)
        raw = counts or ()
        counts = {
            r["unit_start_lsn"]: (r["n"], r["distinct_n"], r["first"], r["last"]) for r in raw
        }
        expected = {u.start: (u.frame_count, u.frame_count, 0, u.frame_count - 1) for u in units}
        if set(counts) != set(expected) or sum(r[0] for r in counts.values()) != sum(
            u.frame_count for u in units
        ):
            raise ValueError("physical unit frame count mismatch")
        if counts != expected:
            raise ValueError("unit frames contain a gap or duplicate")
        if unknown_kinds:
            raise ValueError("unknown format-2 frame kind")
        # Checked complete units only: (epoch, unit end) -> the unit's last commit time
        # (PostgreSQL-epoch microseconds), apply's "current as of" (ops_apply).
        times = {r["unit_start_lsn"]: r.get("last_commit_ts") for r in raw}
        for unit in units:
            if times.get(unit.start) is not None:
                self.commit_times[(epoch, unit.end)] = times[unit.start]

    def events(self, units, relation_id=None, *, high):
        # Imported here, not at module level, so table-membership, which never reads
        # unit frames, doesn't load spool2 and pgoutput.
        from cdc_flight.spool2 import logical_events

        return [
            e
            for e in logical_events(self.frames(units, relation_id, high=high))
            if relation_id is None or e["relation_id"] == relation_id
        ]

    def descriptors(self, units, *, high):
        epoch, low, end = self._bounds(units, high)
        rows, floor, position = self.execute(
            "SELECT (SELECT list(d) FROM {s}descriptors_v2 d WHERE epoch=? AND "
            "EXISTS (SELECT 1 FROM {s}chunks_v2 c,"
            "unnest(from_json_strict(c.descriptor_keys,'[[\"VARCHAR\"]]')) refs(k) "
            "WHERE c.epoch=d.epoch AND c.unit_start_lsn>=? AND c.unit_start_lsn<? "
            "AND refs.k=[d.relation_id::VARCHAR,d.version,d.key_hash])),"
            "(SELECT [floor_epoch,floor_lsn] FROM {s}retention WHERE id=1),"
            "(SELECT {'epoch':s.epoch,'high':s.confirmed_lsn,'format':p.format_version,"
            "'reader':p.min_reader,'binding_hash':p.binding_hash,'binding_json':p.binding_json} "
            "FROM {s}source s,{s}protocol_v2 p WHERE s.id=1 AND p.id=1)",
            [epoch, low, end],
        ).fetchone()
        self._check_position(position, epoch, high)
        self._check_floor(floor, epoch, low)
        descriptors = {
            (r["relation_id"], r["version"], r["key_hash"]): descriptor_from_row(r)
            for r in rows or ()
        }
        names = {}
        for desc in descriptors.values():
            key = (desc.oid, desc.version)
            if key in names and names[key] != desc.names:
                raise ValueError("descriptor key variants disagree on column names")
            names[key] = desc.names
        return descriptors
