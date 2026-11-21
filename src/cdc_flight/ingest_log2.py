"""Format-2 grouped writer, with durable complete-unit feedback boundaries."""

import hashlib
import json
import logging
import struct
import time
import uuid
from dataclasses import asdict, dataclass, field, replace

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from cdc_flight import catalog_checks
from cdc_flight import lifecycle_commands as lc
from cdc_flight.config import IngestConfig, quoted
from cdc_flight.format2 import (
    ABSENT,
    COMMIT,
    COMMIT_HEADER,
    EVENT,
    EVENT_HEADER,
    FRAME_SCHEMA,
    NO_OLD,
    OLD_FULL,
    OLD_KEY,
    Binding,
    Chunk,
    UnitComplete,
    UnitConfig,
    domain,
    key_layout,
    uint,
    validate_image,
    validate_projection,
)
from cdc_flight.ingest_common import (
    ADDED_COLUMNS,
    GUARD,
    RELEASE,
    TABLES,
    VALIDATION_HOLDS,
    FenceLost,
    GuardedIngestConflict,
    LogBase,
    SourceMismatch,
)
from cdc_flight.retention_common import RetentionBase, is_conflict
from cdc_flight.unit_progress import Progress
from cdc_flight.unit_store import (
    CHUNK_TYPES,
    DESCRIPTOR_TYPES,
    PROTOCOL,
    UNIT_TYPES,
    UnitReader,
    descriptor_from_row,
    descriptor_row,
    encoded,
    insert_statement,
    metadata_parameter,
    pending_key_observations,
    stored_binding,
)

logger = logging.getLogger(__name__)

NULLABLE_UNIT = {
    "end_lsn",
    "digest",
    "first_commit",
    "last_commit",
    "tail_commit",
    "last_end",
    "tail_txid",
    "tail_timestamp",
    "abandoned_into_epoch",
    "abandoned_reason",
    "abandoned_frame_end",
}


@dataclass(frozen=True)
class IngestConfig2(IngestConfig):
    text_binding: Binding | None = None
    unit_config: UnitConfig = field(default_factory=UnitConfig)
    server_major: int = 16

    def __post_init__(self):
        super().__post_init__()
        if (
            not isinstance(self.text_binding, Binding)
            or self.text_binding.source_id != self.source_id
        ):
            raise ValueError("an explicit matching format-2 text binding is required")
        if type(self.server_major) is not int or self.server_major < 13:
            raise ValueError("a PostgreSQL server major is required")
        if not isinstance(self.unit_config, UnitConfig):
            raise ValueError("an immutable unit config is required")
        if self.unit_config.mapping_revision != self.text_binding.mapping_revision:
            raise ValueError("unit config and text binding mapping revisions differ")

    def binding(self):
        value = json.loads(super().binding())
        value.update(format_version=2)
        # Wire/mapping identity is authoritative in protocol_v2; keeping it out
        # of registry identity lets a mismatched binary acquire solely to report
        # a guarded needs_action before recovery refuses to stream.
        # Chunk/group sizes belong to immutable unit revisions, not source identity.
        value.pop("chunk_bytes")
        return json.dumps(value, sort_keys=True)


@dataclass(frozen=True)
class Recovery:
    epoch: int
    confirmed_end: int
    binding: Binding
    unit_config: UnitConfig
    open_anchor: int | None
    prefix: tuple
    terminal: UnitComplete | None = None


def _hash(value, name):
    if type(value) is not str or len(value) != 64:
        raise ValueError(f"invalid {name}")
    try:
        raw = bytes.fromhex(value)
    except ValueError as exc:
        raise ValueError(f"invalid {name}") from exc
    if len(raw) != 32 or raw.hex() != value:
        raise ValueError(f"invalid {name}")
    return raw


def _chunk_row(chunk, day):
    return dict(
        id=chunk.id,
        epoch=chunk.epoch,
        unit_start_lsn=chunk.unit_start_lsn,
        first_frame=chunk.first_frame,
        n=chunk.count,
        event_count=chunk.event_count,
        member_count=chunk.member_count,
        bytes=chunk.bytes,
        digest=chunk.digest,
        chain_digest=chunk.chain_digest,
        partition_day=day,
        config_revision=chunk.config_revision,
        mapping_revision=chunk.mapping_revision,
        descriptor_keys=encoded(
            [[str(d.oid), d.version, d.key_hash.hex()] for d in chunk.descriptors]
        ),
    )


def _validate_descriptor(desc):
    row = descriptor_row(1, desc)
    metadata_parameter([row], DESCRIPTOR_TYPES)
    uint(desc.oid, 32, "descriptor oid")
    if _hash(desc.version, "Relation version") != hashlib.sha256(
        desc.json.encode()
    ).digest() or desc.key_hash != key_layout(desc.keys):
        raise ValueError("descriptor identity mismatch")
    relation = json.loads(desc.json)
    names = tuple(c[1] for c in relation["columns"])
    if (
        relation["oid"] != desc.oid
        or relation["namespace"] != desc.namespace
        or relation["name"] != desc.table_name
        or names != desc.names
        or desc.key_indices != tuple(names.index(k) if k in names else -1 for k in desc.keys)
    ):
        raise ValueError("descriptor projection mismatch")


def _validate_chunk_rows(chunk):
    """The exact validator: every check, row by row. It decides every chunk the columnar
    fast path doesn't accept, so its errors (classes and messages) are the contract."""
    metadata_parameter([_chunk_row(chunk, "")], CHUNK_TYPES)
    if not chunk.count or chunk.first_frame + chunk.count >= 1 << 64:
        raise ValueError("invalid frame range")
    _hash(chunk.digest, "chunk digest")
    _hash(chunk.chain_digest, "chunk chain")
    if not isinstance(chunk.table, pa.Table) or not chunk.table.schema.equals(
        FRAME_SCHEMA, check_metadata=True
    ):
        raise ValueError("frame Arrow schema mismatch")
    if chunk.table.num_rows != chunk.count:
        raise ValueError("frame count mismatch")
    descriptors = {}
    for desc in chunk.descriptors:
        _validate_descriptor(desc)
        identity = (desc.oid, desc.version, desc.key_hash.hex())
        if identity in descriptors:
            raise ValueError("duplicate chunk descriptor")
        descriptors[identity] = desc
    events = members = size = 0
    rows = chunk.table.to_pylist()
    for ordinal, row in enumerate(rows, chunk.first_frame):
        for name, bits in (
            ("frame_ordinal", 64),
            ("unit_start_lsn", 64),
            ("source_epoch", 32),
            ("commit_lsn", 64),
            ("event_ordinal", 64),
            ("txid", 32),
            ("wire_length", 64),
        ):
            uint(row[name], bits, name)
        if (
            row["frame_ordinal"] != ordinal
            or row["unit_start_lsn"] != chunk.unit_start_lsn
            or row["source_epoch"] != chunk.epoch
        ):
            raise ValueError("frame identity mismatch")
        if type(row["commit_ts_us"]) is not int or not -(1 << 63) <= row["commit_ts_us"] < 1 << 63:
            raise ValueError("invalid transaction timestamp")
        if row["record_kind"] == EVENT:
            size += 8 + EVENT_HEADER.size + row["wire_length"]
            if row["op"] == "truncate":
                oids = row["truncate_oids"]
                if type(oids) is not list or not oids or row["relation_id"] != 0:
                    raise ValueError("invalid TRUNCATE frame")
                for oid in oids:
                    uint(oid, 32, "TRUNCATE oid")
                    if not any(key[0] == oid for key in descriptors):
                        raise ValueError("TRUNCATE descriptor missing")
                if (
                    type(row["truncate_options"]) is not int
                    or not 0 <= row["truncate_options"] <= 3
                ):
                    raise ValueError("invalid TRUNCATE options")
                if any(row[k] is not None for k in ("old", "new", "old_key", "new_key")):
                    raise ValueError("TRUNCATE has tuple images")
                events += len(oids)
            else:
                identity = (row["relation_id"], row["schema_version"], row["key_layout_hash"])
                desc = descriptors.get(identity)
                if desc is None or (row["schema_name"], row["table_name"]) != (
                    desc.namespace,
                    desc.table_name,
                ):
                    raise ValueError("frame descriptor mismatch")
                for name in ("old", "new"):
                    validate_image(row[name], len(desc.names))
                    if row[name] is not None and ABSENT in row[name]["kinds"]:
                        raise ValueError("ABSENT in source image")
                for name in ("old_key", "new_key"):
                    validate_image(row[name], len(desc.keys))
                validate_projection(row["old"], row["old_key"], desc.key_indices)
                validate_projection(row["new"], row["new_key"], desc.key_indices)
                old_kind = row["old_tuple_kind"]
                if (
                    type(old_kind) is not int
                    or old_kind not in (NO_OLD, OLD_KEY, OLD_FULL)
                    or ((row["old"] is None) != (old_kind == NO_OLD))
                ):
                    raise ValueError("invalid old tuple kind")
                if (
                    row["op"] not in ("insert", "update", "delete")
                    or (row["op"] == "insert" and (row["old"] is not None or row["new"] is None))
                    or (row["op"] == "update" and row["new"] is None)
                    or (row["op"] == "delete" and (row["old"] is None or row["new"] is not None))
                ):
                    raise ValueError("invalid tuple presence")
                events += 1
        elif row["record_kind"] == COMMIT:
            size += 8 + COMMIT_HEADER.size + row["wire_length"]
            if row["op"] != "commit" or row["wire_length"] != 26:
                raise ValueError("invalid COMMIT frame")
            uint(row["end_lsn"], 64, "end_lsn")
            if row["end_lsn"] <= row["commit_lsn"]:
                raise ValueError("invalid COMMIT end")
            _hash(row["transaction_digest"], "transaction digest")
            if any(row[k] is not None for k in ("old", "new", "old_key", "new_key")):
                raise ValueError("COMMIT has tuple images")
            members += 1
        else:
            raise ValueError("invalid physical frame kind")
    if (events, members, size) != (chunk.event_count, chunk.member_count, chunk.bytes):
        raise ValueError("chunk accounting mismatch")
    return rows


# The columnar fast path ---------------------------------------------------------------
#
# `_validate_chunk_rows` converts the whole frame table, payload images included, to
# Python and re-checks it row by row: most of format 2's append CPU on batched pages. The
# fast path checks the same invariants over Arrow columns and either accepts the chunk or
# returns None, never raising: any failed check, a TRUNCATE frame (rare) or any exception
# hands the chunk to the exact validator, which raises today's error or passes. So the
# outcome is the exact validator's by construction; the fast path only skips its cost for
# a chunk it has proven valid.
#
# Every value passed to a compute function is a typed Arrow scalar or array. A plain
# Python scalar makes pyarrow convert it with type inference, which imports `decimal` and
# tries `dateutil.relativedelta` on every call; dateutil isn't installed (here or in
# Flights), and a failed import isn't cached, so each call costs about 40 us, more than
# the check itself.

# In-memory, per process: how chunks were validated (fast path / exact validator). It runs
# on the pipelined append thread, so it is approximate under threads: diagnostics only.
VALIDATION_STATS = {"fast": 0, "exact": 0}
# Arrow's sum over uint64 wraps silently; the byte accounting sums wire_length only below
# this bound per frame (no pgoutput message is near 4 GiB), and sends anything larger to
# the exact validator, whose Python integers don't wrap.
_WIRE_LENGTH_LIMIT = 1 << 32

# The scalar fields `append` reads from each validated row (`_extend` and TRUNCATE oids).
_ROW_FIELDS = (
    "record_kind",
    "commit_lsn",
    "event_ordinal",
    "txid",
    "commit_ts_us",
    "end_lsn",
    "op",
    "truncate_oids",
)
# Their unsigned widths in FRAME_SCHEMA (checked equal above) imply each uint range check
# of the row loop; only non-null is checked here. A schema change is a protocol change, and
# would have to revisit this.
_UINT_COLUMNS = (
    "frame_ordinal",
    "unit_start_lsn",
    "source_epoch",
    "commit_lsn",
    "event_ordinal",
    "txid",
    "wire_length",
)
_HEX64 = "^[0-9a-f]{64}$"
_FALSE = pa.scalar(False, pa.bool_())
_U8 = {k: pa.scalar(k, pa.uint8()) for k in (ABSENT, EVENT, COMMIT, NO_OLD)}
_STR = {s: pa.scalar(s, pa.string()) for s in ("insert", "update", "delete", "truncate", "commit")}
_COMMIT_WIRE = pa.scalar(26, pa.uint64())
_EVENT_OPS = pa.array(["insert", "update", "delete"], pa.string())
_FRAME_KINDS = pa.array([EVENT, COMMIT], pa.uint8())
_OLD_KINDS = pa.array([NO_OLD, OLD_KEY, OLD_FULL], pa.uint8())
_CELL_KINDS = pa.array([0, 1, 2, 3], pa.uint8())
_SOURCE_CELL_KINDS = pa.array([1, 2, 3], pa.uint8())  # no ABSENT in a source tuple
_TEXT = pa.scalar(3, pa.uint8())
_GROUP = ("relation_id", "schema_version", "key_layout_hash", "schema_name", "table_name")
_GROUP_TYPES = (pa.uint32(), pa.string(), pa.string(), pa.string(), pa.string())


def _all(mask):
    """True when a boolean array is all true and has no nulls."""
    return mask.null_count == 0 and pc.all(mask).as_py() is True


def _images_ok(image, width, kinds_allowed):
    """validate_image (plus the old/new ABSENT rule through kinds_allowed) over a column."""
    present = image.filter(image.is_valid())
    if len(present) == 0:
        return True
    values, kinds = pc.struct_field(present, "values"), pc.struct_field(present, "kinds")
    if values.null_count or kinds.null_count:
        return False
    w = pa.scalar(width, pa.int32())
    if not _all(pc.equal(pc.list_value_length(values), w)):
        return False
    if not _all(pc.equal(pc.list_value_length(kinds), w)):
        return False
    flat_kinds, flat_values = pc.list_flatten(kinds), pc.list_flatten(values)
    if flat_kinds.null_count or not _all(pc.is_in(flat_kinds, value_set=kinds_allowed)):
        return False
    # TEXT carries a value; every other kind carries none.
    return _all(pc.equal(pc.equal(flat_kinds, _TEXT), flat_values.is_valid()))


def _projection_ok(image, key, indices):
    """validate_projection over a column: a key image is its tuple image at key_indices."""
    if not _all(pc.equal(image.is_null(), key.is_null())):
        return False
    present = image.is_valid()
    image, key = image.filter(present), key.filter(present)
    if len(image) == 0:
        return True
    ivals, ikinds = pc.struct_field(image, "values"), pc.struct_field(image, "kinds")
    kvals, kkinds = pc.struct_field(key, "values"), pc.struct_field(key, "kinds")
    for i, source in enumerate(indices):
        at = pa.scalar(i, pa.int32())
        kk, kv = pc.list_element(kkinds, at), pc.list_element(kvals, at)
        if source < 0:
            if not _all(pc.equal(kk, _U8[ABSENT])) or kv.null_count != len(kv):
                return False
            continue
        src = pa.scalar(source, pa.int32())
        ik, iv = pc.list_element(ikinds, src), pc.list_element(ivals, src)
        if not _all(pc.equal(kk, ik)):
            return False
        same = pc.or_kleene(pc.and_(kv.is_null(), iv.is_null()), pc.equal(kv, iv))
        if not _all(pc.fill_null(same, _FALSE)):
            return False
    return True


def _fast_rows(chunk):
    """The rows `append` reads if the whole chunk is valid, else None (never raises)."""
    # Chunk-level checks: the exact validator's own helpers; a ValueError means "not valid".
    try:
        metadata_parameter([_chunk_row(chunk, "")], CHUNK_TYPES)
        if not chunk.count or chunk.first_frame + chunk.count >= 1 << 64:
            return None
        _hash(chunk.digest, "chunk digest")
        _hash(chunk.chain_digest, "chunk chain")
        descriptors = {}
        for desc in chunk.descriptors:
            _validate_descriptor(desc)
            identity = (desc.oid, desc.version, desc.key_hash.hex())
            if identity in descriptors:
                return None
            descriptors[identity] = desc
    except ValueError:
        return None
    t = chunk.table
    if not isinstance(t, pa.Table) or not t.schema.equals(FRAME_SCHEMA, check_metadata=True):
        return None
    if t.num_rows != chunk.count:
        return None
    col = t.column
    for name in _UINT_COLUMNS + ("commit_ts_us", "record_kind"):
        if col(name).null_count:
            return None
    # Frame identity: ordinals first_frame, first_frame + 1, ...; one unit and epoch.
    ordinals = col("frame_ordinal").combine_chunks()
    if ordinals[0].as_py() != chunk.first_frame:
        return None
    if len(ordinals) > 1:
        # pairwise_diff wraps on uint64 too, but first_frame + count < 2**64 is checked
        # above, so a run of steps of 1 from first_frame can't wrap.
        steps = pc.pairwise_diff(ordinals).slice(1)
        if not _all(pc.equal(steps, pa.scalar(1, steps.type))):
            return None
    for name, expected in (("unit_start_lsn", chunk.unit_start_lsn), ("source_epoch", chunk.epoch)):
        bounds = pc.min_max(col(name))
        if bounds["min"].as_py() != expected or bounds["max"].as_py() != expected:
            return None
    kind = col("record_kind")
    if not _all(pc.is_in(kind, value_set=_FRAME_KINDS)):
        return None
    if pc.any(pc.fill_null(pc.equal(col("op"), _STR["truncate"]), _FALSE)).as_py():
        return None  # TRUNCATE frames: the exact validator (the op check below would also)
    is_event = pc.equal(kind, _U8[EVENT])
    events, commits = t.filter(is_event), t.filter(pc.invert(is_event))
    if len(commits):
        c = commits.column
        if not _all(pc.equal(c("op"), _STR["commit"])) or not _all(
            pc.equal(c("wire_length"), _COMMIT_WIRE)
        ):
            return None
        if c("end_lsn").null_count or not _all(pc.greater(c("end_lsn"), c("commit_lsn"))):
            return None
        digest = c("transaction_digest")
        if digest.null_count or not _all(pc.match_substring_regex(digest, _HEX64)):
            return None
        if any(c(name).null_count != len(commits) for name in ("old", "new", "old_key", "new_key")):
            return None
    if len(events):
        e = events.column
        op, old_kind = e("op"), e("old_tuple_kind")
        if op.null_count or not _all(pc.is_in(op, value_set=_EVENT_OPS)):
            return None
        if old_kind.null_count or not _all(pc.is_in(old_kind, value_set=_OLD_KINDS)):
            return None
        old_null, new_null = e("old").is_null(), e("new").is_null()
        if not _all(pc.equal(old_null, pc.equal(old_kind, _U8[NO_OLD]))):
            return None
        ins, upd = pc.equal(op, _STR["insert"]), pc.equal(op, _STR["update"])
        dele = pc.equal(op, _STR["delete"])
        bad = pc.or_(
            pc.or_(pc.and_(ins, pc.or_(pc.invert(old_null), new_null)), pc.and_(upd, new_null)),
            pc.and_(dele, pc.or_(old_null, pc.invert(new_null))),
        )
        if pc.any(bad).as_py():
            return None
        # Per descriptor identity (a few per chunk): names, widths, kinds, projections.
        for g in events.select(_GROUP).group_by(list(_GROUP)).aggregate([]).to_pylist():
            desc = descriptors.get((g["relation_id"], g["schema_version"], g["key_layout_hash"]))
            if desc is None or (g["schema_name"], g["table_name"]) != (
                desc.namespace,
                desc.table_name,
            ):
                return None
            mask = None
            for name, typ in zip(_GROUP, _GROUP_TYPES):
                m = pc.equal(e(name), pa.scalar(g[name], typ))
                mask = m if mask is None else pc.and_(mask, m)
            s = events.filter(pc.fill_null(mask, _FALSE)).column
            width, keys = len(desc.names), len(desc.keys)
            if not (
                _images_ok(s("old"), width, _SOURCE_CELL_KINDS)
                and _images_ok(s("new"), width, _SOURCE_CELL_KINDS)
                and _images_ok(s("old_key"), keys, _CELL_KINDS)
                and _images_ok(s("new_key"), keys, _CELL_KINDS)
                and _projection_ok(s("old"), s("old_key"), desc.key_indices)
                and _projection_ok(s("new"), s("new_key"), desc.key_indices)
            ):
                return None
    size = (8 + COMMIT_HEADER.size + 26) * len(commits)
    if len(events):
        if pc.max(events.column("wire_length")).as_py() >= _WIRE_LENGTH_LIMIT:
            return None  # the sum below could wrap
        size += (8 + EVENT_HEADER.size) * len(events) + pc.sum(events.column("wire_length")).as_py()
    if (len(events), len(commits), size) != (chunk.event_count, chunk.member_count, chunk.bytes):
        return None
    return t.select(list(_ROW_FIELDS)).to_pylist()


def validate_chunk(chunk):
    """Validate one chunk before upload; return the rows `append` reads. The columnar fast
    path decides only chunks it proves valid; everything else (any failed check, TRUNCATE,
    any exception) goes to the exact validator, unchanged. Stops and interrupts
    (BaseException) propagate."""
    try:
        rows = _fast_rows(chunk)
    except Exception:
        rows = None
    if rows is not None:
        VALIDATION_STATS["fast"] += 1
        return rows
    VALIDATION_STATS["exact"] += 1
    return _validate_chunk_rows(chunk)


class IngestLog2(LogBase):
    format_version = 2

    def __init__(self, con, config):
        if not isinstance(config, IngestConfig2):
            raise ValueError("the v2 backend requires explicit IngestConfig2")
        super().__init__(con, config)
        self._days = None
        self._spool = None
        self._valid_again = set()

    def release(self, *, sleep=time.sleep):
        """Token-matched autocommit release, with bounded v2-only conflict retries.

        A compaction drop may still own source.guard while the loop cleans up.
        Each retry is a NEW statement/transaction; nothing retries an append or
        commits a conflicted transaction. Exhaustion leaves the token held.
        """
        self.forget()
        self.release_attempts = 0
        if self.token is None:
            return
        last = None
        for attempt, pause in enumerate((0, 0.1, 0.3, 0.9), 1):
            if pause:
                sleep(pause)
            self.release_attempts = attempt
            try:
                self.execute(RELEASE, [self.token])
            except duckdb.TransactionException as exc:
                if not is_conflict(exc):
                    raise
                last = exc
            else:
                self.token = None
                if last is not None:
                    logger.warning(
                        "ingest lease release retried: attempts=%d error=%s",
                        attempt,
                        type(last).__name__,
                    )
                return
        logger.warning(
            "ingest lease release exhausted: attempts=%d error=%s",
            self.release_attempts,
            type(last).__name__,
        )
        raise GuardedIngestConflict(
            f"lease release still conflicting after {self.release_attempts} attempts"
        ) from last

    def forget(self):
        super().forget()
        self._days = self._spool = None
        self._valid_again = set()

    def _record_initial_epoch(self, initial_lsn):
        uint(initial_lsn, 64, "initial epoch start")
        lc.statement(
            self,
            "register",
            "initial_epoch_insert",
            "INSERT INTO {s}source_epochs (epoch,start_lsn,reason,slot_name,created_at) "
            "VALUES (1,?,'initial',?,current_timestamp)",
            [initial_lsn, self.config.slot],
        )

    def unit_position(self):
        """One read-only snapshot for unit-aware lifecycle previews and guarded actions.

        Frame progress is an exclusive ordinal, never a WAL LSN. A durable open
        unit must have its ledger row and at least one persisted chunk.
        """
        row = self.execute(
            "SELECT s.epoch,s.confirmed_lsn,s.open_unit_anchor,p.active_revision,"
            "p.binding_hash,u.tail_commit,u.last_commit,u.last_end,u.frame_count,"
            "(SELECT count(*) FROM {s}chunks_v2 c WHERE c.epoch=s.epoch "
            "AND c.unit_start_lsn=s.open_unit_anchor),p.format_version,p.min_reader "
            "FROM {s}source s CROSS JOIN {s}protocol_v2 p LEFT JOIN {s}units_v2 u "
            "ON u.epoch=s.epoch AND u.unit_start_lsn=s.open_unit_anchor "
            "WHERE s.id=1 AND p.id=1"
        ).fetchone()
        if row is None or row[10] != 2 or row[11] > 2:
            raise SourceMismatch("unsupported or unregistered source format")
        epoch, confirmed, anchor, revision, binding = row[:5]
        if binding != self.config.text_binding.digest.hex():
            raise SourceMismatch("format-2 source binding mismatch")
        if anchor is not None and (anchor != confirmed or row[8] is None or not row[9]):
            raise SourceMismatch("inconsistent durable open unit position")
        return dict(
            format_version=2,
            source_epoch=epoch,
            checkpoint=confirmed,
            confirmed_end=confirmed,
            open_unit_anchor=anchor,
            partial=anchor is not None,
            open_chunks=row[9],
            active_commit_lsn=row[5],
            last_member_commit_lsn=row[6],
            last_member_end_lsn=row[7],
            durable_frame_end=row[8] if anchor is not None else 0,
            config_revision=revision,
            binding_hash=binding,
        )

    def initialize(self):
        catalog = self.catalog()
        if catalog and "protocol_v2" not in catalog:
            raise SourceMismatch("format 2 requires a fresh schema; no implicit migration")
        if "protocol_v2" in catalog:
            self._protocol()
            return False
        common = {
            name: ddl
            for name, ddl in TABLES.items()
            if name not in ("batches", "txn_manifests", "schema_versions", "log_partitions")
        }
        with self.transaction():
            self.execute(
                f"CREATE SCHEMA IF NOT EXISTS {quoted(self.config.database)}."
                f"{quoted(self.config.source_id)}"
            )
            for ddl in common.values():
                self.execute(ddl)
            for table, columns in ADDED_COLUMNS.items():
                if table in common:
                    for column, dtype in columns:
                        self.execute(
                            f"ALTER TABLE {{s}}{table} ADD COLUMN IF NOT EXISTS {column} {dtype}"
                        )
            self.execute("ALTER TABLE {s}source_epochs ADD COLUMN unit_abandonment VARCHAR")
            self.execute("ALTER TABLE {s}source_epochs ADD COLUMN requested_to_lsn UBIGINT")
            self.execute("ALTER TABLE {s}source_epochs ADD COLUMN clamped BOOLEAN")
            self.execute("ALTER TABLE {s}source ADD COLUMN open_unit_anchor UBIGINT")
            self.execute("ALTER TABLE {s}tables ADD COLUMN pending_keys_v2 VARCHAR")
            for column, dtype in (
                ("protocol_reason", "VARCHAR"),
                ("protocol_anchor", "UBIGINT"),
                ("protocol_recorded", "VARCHAR"),
                ("protocol_observed", "VARCHAR"),
            ):
                self.execute(f"ALTER TABLE {{s}}source ADD COLUMN {column} {dtype}")
            for table, columns in PROTOCOL.items():
                self.execute(f"CREATE TABLE {{s}}{table} ({columns})")
            for ddl in RetentionBase(
                self.con, self.config.database, self.config.source_id
            ).statements({}, altering_tables=True):
                self.execute(ddl)
            self.execute(
                "INSERT INTO {s}unit_configs_v2 (revision,binding_hash,config_json) VALUES (?,?,?)",
                [
                    self.config.unit_config.revision,
                    self.config.text_binding.digest.hex(),
                    encoded(asdict(self.config.unit_config)),
                ],
            )
            self.execute(
                "INSERT INTO {s}protocol_v2 (id,format_version,min_reader,binding_hash,"
                "active_revision,server_major,binding_json) VALUES (1,2,2,?,?,?,?)",
                [
                    self.config.text_binding.digest.hex(),
                    self.config.unit_config.revision,
                    self.config.server_major,
                    encoded(asdict(self.config.text_binding)),
                ],
            )

        return True

    def _protocol(self):
        row = self.execute(
            "SELECT format_version,min_reader,binding_hash,active_revision,"
            "(SELECT open_unit_anchor FROM {s}source WHERE id=1),server_major,binding_json FROM {s}protocol_v2 WHERE id=1"
        ).fetchone()
        if row is None or row[0] != 2 or row[1] > 2:
            raise SourceMismatch("format-2 protocol binding or reader capability mismatch")
        return row

    def protocol_changed(self, reason, recorded, observed, anchor, protocol=None):
        """Report only safe hashes/revisions under the recorded lease, then refuse."""
        protocol = protocol or self._protocol()
        with self.transaction():
            count = self.execute(
                GUARD + " AND EXISTS (SELECT 1 FROM {s}protocol_v2 WHERE id=1 "
                "AND format_version=2 AND min_reader<=2 AND binding_hash=? AND active_revision=?)",
                [
                    self.config.lease_seconds,
                    self.token,
                    self.epoch,
                    self.slot_name,
                    protocol[2],
                    protocol[3],
                ],
            ).fetchone()[0]
            if count != 1:
                raise FenceLost("lease changed before protocol mismatch could be recorded")
            self.execute(
                "UPDATE {s}source SET state=?,protocol_reason=?,protocol_anchor=?,"
                "protocol_recorded=?,protocol_observed=? WHERE id=1",
                ["needs_action:" + reason, reason, anchor, str(recorded), str(observed)],
            )
        raise SourceMismatch(
            f"{reason} at unit anchor {anchor}; restore compatible settings/binary and drain, or explicitly resync"
        )

    def guard(self):
        try:
            count = self.execute(
                GUARD + " AND EXISTS (SELECT 1 FROM {s}protocol_v2 WHERE id=1 "
                "AND format_version=2 AND min_reader<=2 AND binding_hash=? AND active_revision=?)",
                [
                    self.config.lease_seconds,
                    self.token,
                    self.epoch,
                    self.slot_name,
                    self.config.text_binding.digest.hex(),
                    self.config.unit_config.revision,
                ],
            ).fetchone()[0]
        except duckdb.TransactionException as exc:
            if is_conflict(exc):
                raise GuardedIngestConflict("format-2 guard conflict") from exc
            raise

        if count != 1:
            raise FenceLost("ingest lease, epoch, slot or unit configuration changed")

    def acquire(self, run_id):
        self._protocol()
        result = super().acquire(run_id)
        self._days = None
        return result

    def activate_config(self, unit_config):
        """A lease-guarded revision change at a drained complete boundary only.

        A control caller must own the connection: never call this while an append
        future is outstanding. During a run, only the ingest loop owns the spool.
        """
        if not isinstance(unit_config, UnitConfig):
            raise ValueError("an immutable unit config is required")
        if self._spool is not None and (self._spool.active or self._spool.frame_count):
            raise SourceMismatch("drain the in-memory unit before changing configuration")
        if unit_config.mapping_revision != self.config.text_binding.mapping_revision:
            raise SourceMismatch("unit config and source binding mapping revisions differ")
        payload = encoded(asdict(unit_config))
        with self.transaction():
            self.guard()
            if (
                self.execute("SELECT open_unit_anchor FROM {s}source WHERE id=1").fetchone()[0]
                is not None
            ):
                raise SourceMismatch("drain the durable open unit before changing configuration")
            old = self.execute(
                "SELECT binding_hash,config_json FROM {s}unit_configs_v2 WHERE revision=?",
                [unit_config.revision],
            ).fetchone()
            expected = (self.config.text_binding.digest.hex(), payload)
            if old is not None and old != expected:
                raise SourceMismatch("an immutable configuration revision cannot be changed")
            if old is None:
                self.execute(
                    "INSERT INTO {s}unit_configs_v2 (revision,binding_hash,config_json) VALUES (?,?,?)",
                    [unit_config.revision, *expected],
                )
            self.execute(
                "UPDATE {s}protocol_v2 SET active_revision=? WHERE id=1", [unit_config.revision]
            )
        self.config = replace(self.config, unit_config=unit_config)
        self._days = None

    def _clear_registry(self):
        self._registry = self._registration = self._keys = self._registered = None

    def require_unit_boundary(self):
        """Control actions own this connection, with no append future outstanding."""
        position = self.unit_position()
        if position["partial"] or (
            self._spool is not None and (self._spool.active or self._spool.frame_count)
        ):
            raise SourceMismatch("drain the open unit before changing registered tables")
        return position

    def activate_member(
        self, oid, schema, name, keys, current, *, append=False, ignore_deletes=False
    ):
        """Membership's source-guarded transaction owns this connection.

        `append`: keyless insert-only capture. No key: key_columns '[]', no catalog key,
        no key version or cut, and nothing pending; ingest never key-tracks it."""
        position = self.require_unit_boundary()
        if append:
            self._activate_append(oid, schema, name, ignore_deletes)
            return
        self._validate_keys({oid: json.loads(keys)})
        self._activate_keys()
        row = self.execute(
            "SELECT key_columns,key_version,key_checked_lsn,key_epoch FROM {s}tables "
            "WHERE relation_id=?",
            [oid],
        ).fetchone()
        if row is None:
            self.execute(
                "INSERT INTO {s}tables (relation_id,schema_name,table_name,key_columns,"
                "catalog_key,key_version,key_epoch,state) VALUES (?,?,?,?,?,0,?,'snapshotting')",
                [oid, schema, name, keys, keys, self.epoch],
            )
            return
        registered, version, checked, key_epoch = row
        if current != "unmanaged" or json.loads(registered) != json.loads(keys):
            cut = (
                checked
                if checked is not None and key_epoch in (None, self.epoch)
                else self.execute(
                    "SELECT start_lsn FROM {s}source_epochs WHERE epoch=?", [self.epoch]
                ).fetchone()[0]
            )
            self._key_cut(self.epoch, cut)
            version = (
                max(
                    version or 0,
                    self.execute(
                        "SELECT coalesce(max(version),0) FROM {s}key_changes WHERE relation_id=?",
                        [oid],
                    ).fetchone()[0],
                )
                + 1
            )
            self.execute(
                "INSERT INTO {s}key_changes (relation_id,version,key_columns,cut_lsn,new_from_lsn,epoch) "
                "VALUES (?,?,?,?,?,?)",
                [oid, version, keys, cut, position["confirmed_end"], self.epoch],
            )
        self.execute(
            "UPDATE {s}tables SET schema_name=?,table_name=?,key_columns=?,catalog_key=?,"
            "key_version=?,key_epoch=?,key_checked_lsn=NULL,pending_keys_v2=NULL,"
            "held_reason=NULL,held_detail=NULL,held_since=NULL,held_checked_at=NULL,retired_by=NULL,"
            "empty_after_slot=NULL,state='snapshotting' WHERE relation_id=?",
            [schema, name, keys, keys, version or 0, self.epoch, oid],
        )

    def _activate_append(self, oid, schema, name, ignore_deletes):
        # A new row, or an unmanaged or removed one (add-table checked which): every hold
        # and key field is reset, key_version to 0, and no key_changes row is written. A
        # removed keyed row's key_changes history stays; a later keyed add numbers past it.
        self.execute(
            "INSERT INTO {s}tables (relation_id,schema_name,table_name,key_columns,catalog_key,"
            "key_version,key_epoch,state,capture_mode,ignore_deletes) "
            "VALUES (?,?,?,'[]',NULL,0,?,'snapshotting','append',?) "
            "ON CONFLICT (relation_id) DO UPDATE SET schema_name=excluded.schema_name,"
            "table_name=excluded.table_name,key_columns='[]',catalog_key=NULL,key_version=0,"
            "key_epoch=excluded.key_epoch,key_checked_lsn=NULL,pending_keys_v2=NULL,"
            "held_reason=NULL,held_detail=NULL,held_since=NULL,held_checked_at=NULL,"
            "retired_by=NULL,empty_after_slot=NULL,state='snapshotting',capture_mode='append',"
            "ignore_deletes=excluded.ignore_deletes",
            [oid, schema, name, self.epoch, bool(ignore_deletes)],
        )

    def _key_cut(self, epoch, cut):
        self._key_cuts(((epoch, cut),))

    def _key_cuts(self, cuts):
        # Saved key cuts may precede the retention floor. Every distinct pair
        # must still name its own epoch start or retained complete-unit END.
        pairs = sorted({(uint(e, 32, "key epoch"), uint(c, 64, "key cut")) for e, c in cuts})
        if not pairs:
            return
        epochs, positions = map(list, zip(*pairs))
        valid = self.execute(
            "SELECT bool_and(EXISTS (SELECT 1 FROM {s}source_epochs e "
            "WHERE e.epoch=c.epoch AND e.start_lsn=c.cut) OR EXISTS "
            "(SELECT 1 FROM {s}units_v2 u WHERE u.epoch=c.epoch AND u.complete AND u.end_lsn=c.cut)) "
            "FROM (SELECT unnest(?::UINTEGER[]) AS epoch,unnest(?::UBIGINT[]) AS cut) c",
            [epochs, positions],
        ).fetchone()[0]
        if not valid:
            raise SourceMismatch("key cut is not an epoch start or complete unit end")

    @staticmethod
    def _validate_keys(observed):
        for oid, keys in observed.items():
            uint(oid, 32, "key relation")
            if keys is not None and (
                type(keys) not in (list, tuple)
                or any(type(key) is not str or not key for key in keys)
                or not keys
                or len(set(keys)) != len(keys)
            ):
                raise ValueError("catalog keys must be unique non-empty column names or NULL")

    def record_tables(self, held=None, present=(), unmanaged=(), names=None):
        held = held or {}
        registry = self.registry()
        current = {r["relation_id"]: r["held_reason"] for r in registry}
        # An append table has no key to activate: its validation hold clears here, at
        # once and without a key version (the parent's clear skips the resync for it).
        append = {r["relation_id"] for r in registry if r.get("capture_mode") == "append"}
        restored = {
            oid
            for oid in present
            if current.get(oid) in VALIDATION_HOLDS and oid not in held and oid not in append
        }
        # Parent's validation-clear path writes cut=0. Keep the hold until a
        # guarded key activation can preserve the last verified whole-unit cut.
        try:
            super().record_tables(held, [o for o in present if o not in restored], unmanaged, names)
        finally:
            self._clear_registry()
        self._valid_again.update(restored)

    def record_keys(self, observed, boundary, checked_lsn):
        self._validate_keys(observed)  # malformed input fails before any remote write
        uint(boundary, 64, "key boundary")
        uint(checked_lsn, 64, "key observation")
        try:
            with self.transaction():
                self.guard()
                activated = self._record_keys(observed, boundary, checked_lsn)
            self._valid_again.difference_update(activated)
        finally:
            self._clear_registry()

    def finish(
        self, run_id, observed, boundary, checked_lsn, status, partial_commit, detail, facts=None
    ):
        """`facts`: the end read's catalog facts (preflight.catalog_facts), judged with
        the keys in the same guarded transaction (catalog_checks). None: not judged."""
        self._validate_keys(observed)
        uint(boundary, 64, "key boundary")
        uint(checked_lsn, 64, "key observation")
        self._facts = facts
        try:
            return super().finish(
                run_id, observed, boundary, checked_lsn, status, partial_commit, detail
            )
        finally:
            self._facts = None
            self._clear_registry()

    def _pending(self, payload):
        queue = pending_key_observations(payload)
        for entry in queue:
            self._key_cut(entry["epoch"], entry["cut_lsn"])
        return queue

    def _activate_keys(self, *, abandoning=False):
        # Called only inside the source-guarded transaction. No cached closed flag.
        position = self.unit_position()
        if not abandoning and (
            position["partial"]
            or (self._spool is not None and (self._spool.active or self._spool.frame_count))
        ):
            return set()
        rows = self.execute(
            "SELECT relation_id,key_version,pending_keys_v2,held_reason,catalog_state FROM {s}tables "
            # An append table never takes a key version (keyless design).
            "WHERE pending_keys_v2 IS NOT NULL AND capture_mode IS DISTINCT FROM 'append'"
        ).fetchall()
        for oid, version, payload, held, stored in rows:
            reason = catalog_checks.pending_reason(stored)  # a catalog hold's, else None
            queue = self._pending(payload)
            if not queue:
                raise SourceMismatch("empty pending key observations")
            version = max(
                version or 0,
                self.execute(
                    "SELECT coalesce(max(version),0) FROM {s}key_changes WHERE relation_id=?",
                    [oid],
                ).fetchone()[0],
            )
            for i, entry in enumerate(queue):
                if entry["epoch"] != position["source_epoch"]:
                    raise SourceMismatch("pending key observation belongs to another epoch")
                version += 1
                self.execute(
                    "INSERT INTO {s}key_changes (relation_id,version,key_columns,cut_lsn,"
                    "new_from_lsn,epoch,reason) VALUES (?,?,?,?,?,?,?)",
                    [
                        oid,
                        version,
                        encoded(entry["keys"]),
                        entry["cut_lsn"],
                        entry["observed_lsn"],
                        entry["epoch"],
                        reason if i == len(queue) - 1 else None,
                    ],
                )
            if reason is not None:  # recorded with the key change: never carried again
                state = json.loads(stored)
                state.pop("reason")
                self.execute(
                    "UPDATE {s}tables SET catalog_state=? WHERE relation_id=?",
                    [catalog_checks.encode(state), oid],
                )
            final = queue[-1]
            valid = final["keys"] is not None
            clear = valid and oid in self._valid_again and held in VALIDATION_HOLDS
            self.execute(
                "UPDATE {s}tables SET catalog_key=?,key_version=?,key_epoch=?,"
                "key_columns=CASE WHEN ? THEN ? ELSE key_columns END,key_checked_lsn=?,"
                "pending_keys_v2=NULL,state='needs_resync',"
                "held_reason=CASE WHEN ? THEN NULL ELSE held_reason END,"
                "held_detail=CASE WHEN ? THEN NULL ELSE held_detail END,"
                "held_since=CASE WHEN ? THEN NULL ELSE held_since END,"
                "held_checked_at=CASE WHEN ? THEN NULL ELSE held_checked_at END "
                "WHERE relation_id=?",
                [
                    encoded(final["keys"]),
                    version,
                    final["epoch"],
                    valid,
                    encoded(final["keys"]),
                    final["cut_lsn"],
                    clear,
                    clear,
                    clear,
                    clear,
                    oid,
                ],
            )

        return {r[0] for r in rows}

    def _record_keys(self, observed, boundary, checked_lsn):
        self._validate_keys(observed)
        self._key_cut(self.epoch, boundary)
        position = self.unit_position()
        if position["source_epoch"] != self.epoch or boundary > position["confirmed_end"]:
            raise SourceMismatch("key verification exceeds the durable unit boundary")
        initial = self.execute(
            "SELECT start_lsn FROM {s}source_epochs WHERE epoch=?", [self.epoch]
        ).fetchone()[0]
        rows = self.execute(
            "SELECT relation_id,key_columns,catalog_key,key_checked_lsn,key_epoch,"
            "pending_keys_v2,held_reason,catalog_state,coalesce(key_version,0),"
            "capture_mode='append' FROM {s}tables"
        ).fetchall()
        facts = getattr(self, "_facts", None)
        prior, queues, confirmations, states, append_holds = [], [], [], [], []
        self.catalog_warnings = 0
        for row in rows:
            oid, registered, catalog, checked, key_epoch, payload, held, stored, version, append = (
                row
            )
            if append:
                # Keyless append: no key, no key gate. A hard signal (a partition change)
                # holds it needs_action at once (catalog_checks.APPEND_REASONS); a new
                # column's fill is a warning only.
                if facts is not None and oid in facts and held not in VALIDATION_HOLDS:
                    reason, state, warning = catalog_checks.judge(
                        stored, facts[oid], append=True, key_version=version
                    )
                    self.catalog_warnings += warning
                    if reason is not None:  # its state is written with the hold, below
                        append_holds.append((oid, reason, json.loads(state)))
                    elif state is not None:
                        states.append((oid, state))
                continue
            if oid not in observed:
                continue
            keys = list(observed[oid]) if observed[oid] is not None else None
            queue = self._pending(payload)
            known = queue[-1]["keys"] if queue else json.loads(catalog or registered)
            cut = (
                queue[0]["cut_lsn"]
                if queue
                else (
                    checked if checked is not None and key_epoch in (None, self.epoch) else initial
                )
            )
            prior.append((self.epoch, cut))
            if (
                facts is not None
                and oid in facts
                and (queue or held in VALIDATION_HOLDS or keys != known)
            ):
                # Not a clean read: the key change or hold in flight ends in a resync,
                # which covers any DDL until then. Keep the stored facts current (on
                # change only), unjudged, so they never fire a second change later.
                fresh = catalog_checks.state_of(facts[oid], version)
                old = json.loads(stored) if stored is not None else None
                reason = old.pop("reason", None) if old is not None else None
                if old != fresh:
                    if reason is not None:  # a queued catalog hold keeps its reason
                        fresh["reason"] = reason
                    states.append((oid, catalog_checks.encode(fresh)))
            if keys != known or (oid in self._valid_again and not queue):
                queue.append(
                    dict(epoch=self.epoch, keys=keys, cut_lsn=cut, observed_lsn=checked_lsn)
                )
                queues.append((oid, encoded(queue)))
            elif not queue and held not in VALIDATION_HOLDS:
                # A clean read: the catalog checks judge it (only with the end read's
                # facts; a table dropped meanwhile has none, and its keys aren't here).
                reason = None
                if facts is not None and oid in facts:
                    reason, state, warning = catalog_checks.judge(
                        stored, facts[oid], key_version=version
                    )
                    if state is not None:
                        states.append((oid, state))
                    self.catalog_warnings += warning
                if reason is None:
                    confirmations.append((oid, max(cut, boundary)))
                else:
                    # Held at the last verified cut, through the key-change queue: the
                    # same key columns, so it activates as a key change (needs_resync)
                    # and apply never passes the cut. The reason is in catalog_state.
                    queue.append(
                        dict(epoch=self.epoch, keys=known, cut_lsn=cut, observed_lsn=checked_lsn)
                    )
                    queues.append((oid, encoded(queue)))
            # With a queue, do not advance key_checked_lsn: Apply must stop at
            # the PRIOR verified cut even while this run drains with old keys.
        self._key_cuts(prior)  # all validation before any table UPDATE
        for oid, payload in queues:
            self.execute(
                "UPDATE {s}tables SET pending_keys_v2=? WHERE relation_id=?", [payload, oid]
            )
        if states:  # only what changed: no write in the steady state
            ids, values = map(list, zip(*states))
            self.execute(
                "UPDATE {s}tables t SET catalog_state=c.state FROM "
                "(SELECT unnest($1::UINTEGER[]) AS relation_id,unnest($2::VARCHAR[]) AS state) c "
                "WHERE t.relation_id=c.relation_id",
                [ids, values],
            )
        for oid, reason, state in append_holds:
            # Apply holds an append table at a later key_changes row with a reason
            # (apply_run._limit), here at this run's durable boundary: the rows ATTACH or
            # DETACH moved are missing, never misapplied. start-from-log clears it.
            version = self.execute(
                "SELECT greatest(coalesce((SELECT key_version FROM {s}tables WHERE relation_id=$1),0),"
                "coalesce((SELECT max(version) FROM {s}key_changes WHERE relation_id=$1),0))+1",
                [oid],
            ).fetchone()[0]
            self.execute(
                "INSERT INTO {s}key_changes (relation_id,version,key_columns,cut_lsn,"
                "new_from_lsn,epoch,reason) VALUES (?,?,NULL,?,?,?,?)",
                [oid, version, boundary, checked_lsn, self.epoch, reason],
            )
            state.pop("reason")
            state["key_version"] = version  # the hold's own version
            self.execute(
                "UPDATE {s}tables SET key_version=?,catalog_state=? WHERE relation_id=?",
                [version, catalog_checks.encode(state), oid],
            )
        if confirmations:
            ids, cuts = map(list, zip(*confirmations))
            if len(ids) != len(set(ids)):
                raise SourceMismatch("duplicate key confirmation")
            self.execute(
                "UPDATE {s}tables t SET key_checked_lsn=c.cut,key_epoch=$1 FROM "
                "(SELECT unnest($2::UINTEGER[]) AS relation_id,unnest($3::UBIGINT[]) AS cut) c "
                "WHERE t.relation_id=c.relation_id",
                [self.epoch, ids, cuts],
            )
        return self._activate_keys()

    def epoch_record(self, epoch):
        row = self.execute(
            "SELECT start_lsn,reason,slot_name,abandoned_from,abandoned_to,requested_to_lsn,clamped,unit_abandonment "
            "FROM {s}source_epochs WHERE epoch=?",
            [epoch],
        ).fetchone()
        if row is None:
            return None
        result = dict(
            zip(("start", "reason", "slot", "from", "to", "requested_to", "clamped"), row[:7])
        )
        result["unit_abandonment"] = json.loads(row[7]) if row[7] is not None else None
        return result

    def begin_epoch(self, new_epoch, start_lsn, reason, slot_name, *, details=None, fault=None):
        """Abandon the whole old prefix and install a new epoch in one guarded commit.

        Control callers own the source lease and connection, with no append future.
        PostgreSQL work has already finished; this method never connects to PG.
        Abandonment resolves retention, not completion: no COMMIT/end is invented.
        """
        uint(new_epoch, 32, "new epoch")
        uint(start_lsn, 64, "new epoch start")
        if reason not in ("slot_lost", "skip"):
            raise ValueError("unknown epoch transition reason")
        details = details or {}
        requested = details.get("requested_to_lsn", start_lsn) if reason == "skip" else None
        if requested is not None:
            uint(requested, 64, "requested epoch start")
            if start_lsn > requested:
                raise SourceMismatch("actual epoch start exceeds the requested skip")
        clamped = requested is not None and start_lsn < requested
        try:
            with self.transaction():
                self.guard()
                position = self.unit_position()
                epoch, confirmed = position["source_epoch"], position["confirmed_end"]
                if epoch == new_epoch:
                    done = self.epoch_record(new_epoch)
                    if (
                        done
                        and (done["start"], done["reason"], done["slot"])
                        == (
                            start_lsn,
                            reason,
                            slot_name,
                        )
                        and (
                            reason != "skip"
                            or (done["requested_to"] == requested and done["clamped"] == clamped)
                        )
                    ):
                        return False
                    raise SourceMismatch("this epoch already began differently")
                if new_epoch != epoch + 1:
                    raise SourceMismatch("the next epoch must follow the registered one")
                if reason == "skip" and (
                    start_lsn <= confirmed
                    or start_lsn < (position["last_member_end_lsn"] or confirmed)
                    or (
                        position["active_commit_lsn"] is not None
                        and start_lsn <= position["active_commit_lsn"]
                    )
                ):
                    raise SourceMismatch("epoch skip is not beyond the durable member prefix")
                previous_slot = self.execute(
                    "SELECT slot_name FROM {s}source WHERE id=1"
                ).fetchone()[0]
                abandonment = None
                if position["partial"]:
                    anchor = position["open_unit_anchor"]
                    first = self.execute(
                        "SELECT first_commit FROM {s}units_v2 WHERE epoch=? AND unit_start_lsn=?",
                        [epoch, anchor],
                    ).fetchone()[0]
                    abandonment = dict(
                        abandoned_unit_start_lsn=anchor,
                        abandoned_frame_end=position["durable_frame_end"],
                        abandoned_chunk_count=position["open_chunks"],
                        abandoned_first_commit_lsn=first,
                        abandoned_last_commit_lsn=position["last_member_commit_lsn"],
                        abandoned_active_commit_lsn=position["active_commit_lsn"],
                        abandoned_into_epoch=new_epoch,
                        abandoned_reason=reason,
                    )
                    count = self.execute(
                        "UPDATE {s}units_v2 SET abandoned_into_epoch=?,abandoned_reason=?,"
                        "abandoned_frame_end=frame_count WHERE epoch=? AND unit_start_lsn=? "
                        "AND NOT complete AND abandoned_into_epoch IS NULL",
                        [new_epoch, reason, epoch, anchor],
                    ).fetchone()[0]
                    if count != 1:
                        raise SourceMismatch("open unit cannot be abandoned consistently")
                lc.statement(
                    self,
                    "begin_epoch",
                    "epoch_insert",
                    "INSERT INTO {s}source_epochs (epoch,previous_epoch,start_lsn,"
                    "previous_confirmed_lsn,reason,slot_name,previous_slot_name,timeline_id,"
                    "abandoned_from,abandoned_to,started_by,unit_abandonment,requested_to_lsn,clamped,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,current_timestamp)",
                    [
                        new_epoch,
                        epoch,
                        start_lsn,
                        confirmed,
                        reason,
                        slot_name,
                        previous_slot,
                        details.get("timeline_id"),
                        confirmed,
                        start_lsn,
                        details.get("started_by"),
                        encoded(abandonment) if abandonment is not None else None,
                        requested,
                        clamped,
                    ],
                )
                self._activate_keys(abandoning=True)
                lc.statement(
                    self,
                    "begin_epoch",
                    "source_update",
                    "UPDATE {s}source SET epoch=?,confirmed_lsn=?,open_unit_anchor=NULL,"
                    "open_commit_lsn=NULL,slot_name=?,last_capped_commit=NULL,last_capped_count=NULL,"
                    "protocol_reason=NULL,protocol_anchor=NULL,protocol_recorded=NULL,"
                    "protocol_observed=NULL,state='ready',created_slot=CASE WHEN ?='slot_lost' "
                    "THEN true ELSE created_slot END WHERE id=1 AND epoch=?",
                    [new_epoch, start_lsn, slot_name, reason, epoch],
                )
                lc.statement(
                    self,
                    "begin_epoch",
                    "protocol_update",
                    "UPDATE {s}protocol_v2 SET last_capped_epoch=NULL,last_capped_anchor=NULL,"
                    "last_capped_commit=NULL,last_capped_frame=NULL WHERE id=1",
                )
                lc.statement(
                    self,
                    "begin_epoch",
                    "partitions_seal",
                    "UPDATE {s}frame_partitions_v2 SET status='sealed' "
                    "WHERE epoch=? AND status='active'",
                    [epoch],
                )
                registry = lc.statement(
                    self,
                    "begin_epoch",
                    "key_changes_insert",
                    "INSERT INTO {s}key_changes (relation_id,version,key_columns,cut_lsn,"
                    "new_from_lsn,epoch) SELECT relation_id,coalesce(key_version,0)+1,"
                    "coalesce(catalog_key,key_columns),?,?,? FROM {s}tables",
                    [start_lsn, start_lsn, new_epoch],
                )
                # Every registry row, or roll back: the same count as the
                # key changes just written over them. Rows committed concurrently can't be
                # missed: every {s}tables INSERT writes the source row this transaction
                # guarded, so they conflict (lifecycle_spec_text, begin_epoch).
                lc.statement(
                    self,
                    "begin_epoch",
                    "tables_update",
                    "UPDATE {s}tables SET key_version=coalesce(key_version,0)+1,key_epoch=?,"
                    "key_checked_lsn=NULL,state='needs_resync'",
                    [new_epoch],
                    expected=registry,
                )
                if fault:
                    fault("before_epoch_commit")
        except BaseException:
            self._days = None
            raise
        self.epoch, self.slot_name, self.acquired_lsn = new_epoch, slot_name, start_lsn
        self._days = self._registry = self._keys = self._spool = None
        self._registered = self._registration = None
        if fault:
            fault("after_epoch_commit")
        return True

    def _load_days(self):
        rows = self.execute(
            "SELECT partition_day,epoch,min_unit,max_unit,n,bytes,status "
            "FROM {s}frame_partitions_v2"
        ).fetchall()
        if any(
            type(r[0]) is not str or len(r[0]) != 8 or not r[0].isascii() or not r[0].isdigit()
            for r in rows
        ):
            raise ValueError("invalid physical partition day")
        days = {
            (r[0], r[1]): dict(zip(("day", "epoch", "min", "max", "n", "bytes", "status"), r))
            for r in rows
        }
        physical = {
            name.removeprefix("frames_v2_")
            for name in self.catalog()
            if name.startswith("frames_v2_")
        }
        if physical != {r["day"] for r in days.values() if r["status"] != "dropped"}:
            raise ValueError("physical frame day is missing partition metadata or table")
        self._days = days

    def _snapshot(self, chunks, anchors):
        candidates = [_chunk_row(c, "") for c in chunks]
        param = metadata_parameter(candidates, CHUNK_TYPES)
        anchors = sorted(set(anchors))
        # The day inventory is authoritative under this lease and worker-owned.
        relevant = [
            r["day"]
            for r in self._days.values()
            if r["epoch"] == self.epoch
            and r["status"] != "dropped"
            and any(r["min"] <= a <= r["max"] for a in anchors)
        ]
        physical = []
        if chunks:
            lo, hi = min(anchors), max(anchors)
            first = min(c.first_frame for c in chunks)
            end = max(c.first_frame + c.count for c in chunks)
            for day in sorted(set(relevant)):
                physical.append(
                    f"SELECT source_epoch,unit_start_lsn,frame_ordinal FROM {{s}}frames_v2_{day} "
                    f"WHERE source_epoch={self.epoch} AND unit_start_lsn BETWEEN {lo} AND {hi} "
                    f"AND frame_ordinal>={first} AND frame_ordinal<{end}"
                )
        physical_sql = " UNION ALL ".join(physical) or (
            "SELECT NULL::UINTEGER source_epoch,NULL::UBIGINT unit_start_lsn,"
            "NULL::UBIGINT frame_ordinal WHERE false"
        )
        return self.execute(
            "WITH candidates AS (SELECT r.id AS id,r.epoch AS epoch,"
            "r.unit_start_lsn AS unit_start_lsn,r.first_frame AS first_frame,r.n AS n,r.descriptor_keys AS descriptor_keys FROM "
            '(SELECT unnest(from_json_strict(?, \'[{"id":"VARCHAR","epoch":"UINTEGER",'
            '"unit_start_lsn":"UBIGINT","first_frame":"UBIGINT","n":"UBIGINT",'
            '"event_count":"UBIGINT","member_count":"UBIGINT","bytes":"UBIGINT",'
            '"digest":"VARCHAR","chain_digest":"VARCHAR","partition_day":"VARCHAR",'
            '"config_revision":"UBIGINT","mapping_revision":"UBIGINT",'
            '"descriptor_keys":"VARCHAR"}]\')) AS r) input), physical AS ('
            + physical_sql
            + ") SELECT s.confirmed_lsn,s.open_unit_anchor,"
            "strftime(current_timestamp AT TIME ZONE 'UTC','%Y%m%d'),"
            "(SELECT list(c) FROM {s}chunks_v2 c WHERE epoch=? AND unit_start_lsn IN "
            "(SELECT unnest(from_json_strict(?,'[\"UBIGINT\"]')) AS lookup_anchor)),"
            "(SELECT list(u) FROM {s}units_v2 u WHERE epoch=? AND unit_start_lsn IN "
            "(SELECT unnest(from_json_strict(?,'[\"UBIGINT\"]')) AS lookup_anchor)),"
            "(SELECT list({'id':c.id,'n':(SELECT count(*) FROM physical f WHERE "
            "f.source_epoch=c.epoch AND f.unit_start_lsn=c.unit_start_lsn AND "
            "f.frame_ordinal>=c.first_frame AND f.frame_ordinal<c.first_frame+c.n)}) FROM candidates c),"
            "(SELECT list({'day':partition_day,'epoch':epoch,'min':min_unit,'max':max_unit,'n':n,'status':status}) FROM {s}frame_partitions_v2),"
            "(SELECT list(d) FROM {s}descriptors_v2 d WHERE epoch=? AND EXISTS "
            "(SELECT 1 FROM candidates c,unnest(from_json_strict(c.descriptor_keys,'[[\"VARCHAR\"]]')) refs(k) "
            "WHERE c.epoch=d.epoch AND refs.k=[d.relation_id::VARCHAR,d.version,d.key_hash])) "
            "FROM {s}source s,{s}protocol_v2 p WHERE s.id=1 AND p.id=1",
            [param, self.epoch, encoded(anchors), self.epoch, encoded(anchors), self.epoch],
        ).fetchone()

    def recovery(self):
        protocol = self._protocol()
        epoch, _, confirmed = self.position()
        uint(epoch, 32, "source epoch")
        uint(confirmed, 64, "confirmed end")
        revision, anchor = protocol[3:5]
        config_row = self.execute(
            "SELECT binding_hash,config_json FROM {s}unit_configs_v2 WHERE revision=?", [revision]
        ).fetchone()
        if config_row is None:
            raise SourceMismatch("missing immutable unit configuration")
        recorded_config = json.loads(config_row[1])
        if recorded_config["mapping_revision"] != self.config.text_binding.mapping_revision:
            self.protocol_changed(
                "mapping_revision_changed",
                recorded_config["mapping_revision"],
                self.config.text_binding.mapping_revision,
                anchor,
                protocol,
            )
        stored_binding(protocol[6], protocol[2], self.config.source_id)
        if config_row[0] != protocol[2]:
            raise SourceMismatch("unit config and protocol binding disagree")
        if protocol[2] != self.config.text_binding.digest.hex():
            self.protocol_changed(
                "binding_changed",
                protocol[2],
                self.config.text_binding.digest.hex(),
                anchor,
                protocol,
            )
        if protocol[5] != self.config.server_major:
            self.protocol_changed(
                "binding_changed", protocol[5], self.config.server_major, anchor, protocol
            )
        if recorded_config != asdict(self.config.unit_config):
            self.protocol_changed(
                "unit_config_changed",
                hashlib.sha256(config_row[1].encode()).hexdigest(),
                hashlib.sha256(encoded(asdict(self.config.unit_config)).encode()).hexdigest(),
                anchor,
                protocol,
            )
        unit_config = UnitConfig(**recorded_config)
        if anchor is not None and anchor != confirmed:
            raise SourceMismatch("open unit anchor differs from confirmed end")
        open_units = self.execute(
            "SELECT unit_start_lsn FROM {s}units_v2 WHERE epoch=? AND NOT complete", [epoch]
        ).fetchall()
        if open_units != ([] if anchor is None else [(anchor,)]):
            raise SourceMismatch("source and incomplete unit inventory disagree")
        self._load_days()
        prefix = []
        if anchor is not None:
            ledger = self.execute(
                "SELECT * FROM {s}chunks_v2 WHERE epoch=? AND unit_start_lsn=? "
                "ORDER BY first_frame",
                [epoch, anchor],
            ).fetchall()
            descriptors = {
                (r["relation_id"], r["version"], r["key_hash"]): descriptor_from_row(r)
                for r in self.execute(
                    "SELECT list(d) FROM {s}descriptors_v2 d WHERE epoch=? AND EXISTS "
                    "(SELECT 1 FROM {s}chunks_v2 c,unnest(from_json_strict(c.descriptor_keys,'[[\"VARCHAR\"]]')) refs(k) "
                    "WHERE c.epoch=d.epoch AND c.unit_start_lsn=? AND refs.k=[d.relation_id::VARCHAR,d.version,d.key_hash])",
                    [epoch, anchor],
                ).fetchone()[0]
                or ()
            }
            first = 0
            for values in ledger:
                row = dict(zip(CHUNK_TYPES, values, strict=True))
                refs = [(int(k[0]), k[1], k[2]) for k in json.loads(row["descriptor_keys"])]
                if row["first_frame"] != first or not row["n"]:
                    raise ValueError("durable prefix contains a gap or overlap")
                prefix.append(
                    Chunk(
                        epoch,
                        anchor,
                        first,
                        row["n"],
                        row["event_count"],
                        row["member_count"],
                        row["digest"],
                        row["chain_digest"],
                        row["bytes"],
                        tuple(descriptors[k] for k in refs),
                        row["config_revision"],
                        row["mapping_revision"],
                        FRAME_SCHEMA.empty_table(),
                    )
                )
                first += row["n"]
            snap = self._snapshot(prefix, [anchor])
            counts = {r["id"]: r["n"] for r in snap[5] or ()}
            if any(counts.get(c.id) != c.count for c in prefix):
                raise ValueError("durable prefix physical count mismatch")
            state = snap[4] or []
            if len(state) != 1 or state[0]["complete"] or state[0]["frame_count"] != first:
                raise ValueError("open unit manifest is missing or inconsistent")
        return Recovery(
            epoch, confirmed, self.config.text_binding, unit_config, anchor, tuple(prefix)
        )

    def _new_unit(self, anchor):
        chain = hashlib.sha256(
            domain("prefix")
            + self.config.unit_config.header(self.config.text_binding.digest, self.epoch, anchor)
        ).hexdigest()
        return dict(
            epoch=self.epoch,
            unit_start_lsn=anchor,
            end_lsn=None,
            complete=False,
            config_revision=self.config.unit_config.revision,
            member_count=0,
            event_count=0,
            frame_count=0,
            bytes=0,
            chain_digest=chain,
            digest=None,
            first_commit=None,
            last_commit=None,
            tail_commit=None,
            tail_ordinal=0,
            last_end=None,
            tail_txid=None,
            tail_timestamp=None,
            abandoned_into_epoch=None,
            abandoned_reason=None,
            abandoned_frame_end=None,
        )

    def _extend(self, state, chunk, rows):
        if (
            state["complete"]
            or state.get("abandoned_into_epoch") is not None
            or state["frame_count"] != chunk.first_frame
        ):
            raise ValueError("non-contiguous or completed unit chunk")
        header = self.config.unit_config.header(
            self.config.text_binding.digest, self.epoch, chunk.unit_start_lsn
        )
        expected_chain = hashlib.sha256(
            domain("chain")
            + bytes.fromhex(state["chain_digest"])
            + header
            + struct.pack(
                "!5Q",
                chunk.first_frame,
                chunk.count,
                chunk.event_count,
                chunk.member_count,
                chunk.bytes,
            )
            + bytes.fromhex(chunk.digest)
        ).hexdigest()
        if expected_chain != chunk.chain_digest:
            raise ValueError("unit chunk chain mismatch")
        for row in rows:
            commit = row["commit_lsn"]
            if commit < (state["last_end"] or state["unit_start_lsn"]):
                raise ValueError("source member is behind the complete boundary")
            if state["tail_commit"] is None:
                state["tail_commit"] = commit
                state["tail_txid"] = row["txid"]
                state["tail_timestamp"] = row["commit_ts_us"]
                state["first_commit"] = (
                    state["first_commit"] if state["first_commit"] is not None else commit
                )
            if (
                state["tail_commit"] != commit
                or row["event_ordinal"] != state["tail_ordinal"]
                or row["txid"] != state["tail_txid"]
                or row["commit_ts_us"] != state["tail_timestamp"]
            ):
                raise ValueError("non-contiguous source transaction frames")
            if row["record_kind"] == COMMIT:
                state.update(
                    tail_commit=None,
                    tail_ordinal=0,
                    tail_txid=None,
                    tail_timestamp=None,
                    last_commit=commit,
                    last_end=row["end_lsn"],
                )
            else:
                state["tail_ordinal"] += len(row["truncate_oids"]) if row["op"] == "truncate" else 1
        for name, increment in (
            ("frame_count", chunk.count),
            ("event_count", chunk.event_count),
            ("member_count", chunk.member_count),
            ("bytes", chunk.bytes),
        ):
            state[name] += increment
        state["chain_digest"] = chunk.chain_digest

    def _complete(self, state, terminal):
        fields = (
            "epoch",
            "unit_start_lsn",
            "end_lsn",
            "member_count",
            "event_count",
            "frame_count",
            "bytes",
            "chain_digest",
            "digest",
            "config_revision",
        )
        if state["complete"]:
            if any(state[k] != getattr(terminal, k) for k in fields):
                raise ValueError("recorded terminal identity changed")
            return False
        header = self.config.unit_config.header(
            self.config.text_binding.digest, self.epoch, terminal.unit_start_lsn
        )
        digest = hashlib.sha256(
            domain("terminal")
            + header
            + bytes.fromhex(state["chain_digest"])
            + struct.pack(
                "!5Q",
                terminal.end_lsn,
                terminal.member_count,
                terminal.event_count,
                terminal.frame_count,
                terminal.bytes,
            )
        ).hexdigest()
        if (
            state["tail_commit"] is not None
            or terminal.end_lsn != state["last_end"]
            or terminal.digest != digest
            or any(
                state[k] != getattr(terminal, k)
                for k in ("member_count", "event_count", "frame_count", "bytes", "chain_digest")
            )
        ):
            raise ValueError("unit terminal is incomplete or inconsistent")
        state.update(complete=True, end_lsn=terminal.end_lsn, digest=digest)
        return True

    def _insert(self, table, types, rows, conflict="", nullable=()):
        if rows:
            self.execute(
                insert_statement(table, types, conflict),
                [metadata_parameter(rows, types, nullable)],
            )

    def append(self, items, *, fault=None, partition_day=None):
        if not items:
            return self.checkpoint()
        if self.epoch is None:
            raise FenceLost("acquire the source lease before append")
        chunks, tables, terminals = [], {}, []
        for item in items:
            if not isinstance(item, (Chunk, UnitComplete)) or item.epoch != self.epoch:
                raise SourceMismatch("invalid item or batch epoch")
            if item.config_revision != self.config.unit_config.revision:
                raise SourceMismatch("unit configuration revision mismatch")
            if isinstance(item, Chunk):
                if item.mapping_revision != self.config.unit_config.mapping_revision:
                    raise SourceMismatch("cell mapping revision mismatch")
                if item.id in tables:
                    raise ValueError("duplicate chunk identity in one append")
                tables[item.id] = validate_chunk(item)
                chunks.append(item)
            else:
                for name in (
                    "epoch",
                    "unit_start_lsn",
                    "end_lsn",
                    "member_count",
                    "event_count",
                    "frame_count",
                    "bytes",
                    "config_revision",
                ):
                    uint(getattr(item, name), 32 if name == "epoch" else 64, name)
                _hash(item.digest, "terminal digest")
                _hash(item.chain_digest, "terminal chain")
                terminals.append(item)
        if partition_day is not None and (
            type(partition_day) is not str
            or len(partition_day) != 8
            or not partition_day.isascii()
            or not partition_day.isdigit()
        ):
            raise ValueError("invalid partition day")
        anchors = sorted({i.unit_start_lsn for i in items})
        if self._days is None:
            self.recovery()
        began = False
        try:
            with self.transaction():
                began = True
                self.guard()
                (
                    boundary,
                    open_anchor,
                    utc_day,
                    old_chunks,
                    old_units,
                    physical,
                    actual_days,
                    old_desc,
                ) = self._snapshot(chunks, anchors)
                day = partition_day or utc_day
                inventory = {
                    (r["day"], r["epoch"]): (r["min"], r["max"], r["n"]) for r in actual_days or ()
                }
                if inventory != {
                    key: (r["min"], r["max"], r["n"]) for key, r in self._days.items()
                }:
                    raise ValueError("partition inventory or bounds changed under this writer")
                # Bounds/counts must match; compaction may legitimately change status.
                # Keep the refreshed cache private until this append commits.
                days = {key: dict(value) for key, value in self._days.items()}
                for row in actual_days or ():
                    days[row["day"], row["epoch"]]["status"] = row["status"]
                newest = max((r["day"] for r in days.values()), default=None)
                if newest and day < newest:
                    raise ValueError("ingestion day cannot move backwards")
                existing = {r["id"]: r for r in old_chunks or ()}
                states = {r["unit_start_lsn"]: dict(r) for r in old_units or ()}
                counts = {r["id"]: r["n"] for r in physical or ()}
                descriptors = {
                    (r["relation_id"], r["version"], r["key_hash"]): r for r in old_desc or ()
                }
                fresh, changed, fresh_desc, truncates = [], set(), {}, set()
                for item in items:
                    anchor = item.unit_start_lsn
                    state = states.setdefault(anchor, self._new_unit(anchor))
                    if isinstance(item, Chunk):
                        row = _chunk_row(item, day)
                        old = existing.get(item.id)
                        if old:
                            if any(old[k] != row[k] for k in CHUNK_TYPES if k != "partition_day"):
                                raise ValueError("replayed chunk identity has different contents")
                            if counts.get(item.id) != item.count:
                                raise ValueError("replayed chunk physical count mismatch")
                            continue
                        if counts.get(item.id, 0):
                            raise ValueError("physical frames exist without a chunk ledger row")
                        if anchor != boundary or open_anchor not in (None, anchor):
                            raise ValueError("chunk crosses an open unit or confirmed boundary")
                        self._extend(state, item, tables[item.id])
                        open_anchor = anchor
                        fresh.append(item)
                        existing[item.id] = row
                        changed.add(anchor)
                        for desc in item.descriptors:
                            key = (desc.oid, desc.version, desc.key_hash.hex())
                            data = descriptor_row(self.epoch, desc)
                            if key in descriptors and descriptors[key] != data:
                                raise ValueError("immutable descriptor changed")
                            if key not in descriptors:
                                fresh_desc[key] = data
                                descriptors[key] = data
                        truncates.update(
                            oid
                            for r in tables[item.id]
                            if r["op"] == "truncate"
                            for oid in r["truncate_oids"]
                        )
                    elif self._complete(state, item):
                        if anchor != boundary or open_anchor != anchor:
                            raise ValueError("terminal crosses an incomplete unit")
                        boundary, open_anchor = item.end_lsn, None
                        changed.add(anchor)
                if fault:
                    fault("before_payload")  # a timing mark only (ingest's timing lines)
                # Cold day DDL/sealing are explicit exceptions to the hot statement budget.
                if fresh:
                    if (day, self.epoch) in days and days[day, self.epoch]["status"] != "active":
                        raise ValueError("cannot append to a sealed day")
                    if day not in {r["day"] for r in days.values() if r["status"] != "dropped"}:
                        self.con.register("cdc_v2_empty", FRAME_SCHEMA.empty_table())
                        try:
                            self.execute(
                                f"CREATE TABLE {{s}}frames_v2_{day} AS SELECT * FROM cdc_v2_empty"
                            )
                        finally:
                            self.con.unregister("cdc_v2_empty")
                    identities = [(c.unit_start_lsn, c.first_frame) for c in fresh]
                    if any(a >= b for a, b in zip(identities, identities[1:])):
                        raise ValueError("fresh chunks are not in source order")
                    upload = pa.concat_tables([c.table for c in fresh])
                    name = "cdc_v2_" + uuid.uuid4().hex
                    self.con.register(name, upload)
                    try:
                        self.execute(
                            f"INSERT INTO {{s}}frames_v2_{day} BY NAME SELECT * FROM {name}"
                        )
                    finally:
                        self.con.unregister(name)
                    self._insert("chunks_v2", CHUNK_TYPES, [_chunk_row(c, day) for c in fresh])
                    self._insert("descriptors_v2", DESCRIPTOR_TYPES, list(fresh_desc.values()))
                if fault:
                    fault("after_payload")
                self._insert(
                    "units_v2",
                    UNIT_TYPES,
                    [states[a] for a in sorted(changed)],
                    "ON CONFLICT(epoch,unit_start_lsn) DO UPDATE SET "
                    + ",".join(
                        f"{k}=excluded.{k}"
                        for k in UNIT_TYPES
                        if k not in ("epoch", "unit_start_lsn")
                    )
                    + " WHERE NOT units_v2.complete AND units_v2.abandoned_into_epoch IS NULL",
                    NULLABLE_UNIT,
                )
                if fresh:
                    self.execute(
                        "INSERT INTO {s}frame_partitions_v2 (partition_day,epoch,min_unit,max_unit,n,bytes,status) "
                        "VALUES (?,?,?,?,?,?,'active') ON CONFLICT(partition_day,epoch) DO UPDATE SET "
                        "min_unit=least(frame_partitions_v2.min_unit,excluded.min_unit),"
                        "max_unit=greatest(frame_partitions_v2.max_unit,excluded.max_unit),"
                        "n=frame_partitions_v2.n+excluded.n,bytes=frame_partitions_v2.bytes+excluded.bytes",
                        [
                            day,
                            self.epoch,
                            min(c.unit_start_lsn for c in fresh),
                            max(c.unit_start_lsn for c in fresh),
                            sum(c.count for c in fresh),
                            sum(c.bytes for c in fresh),
                        ],
                    )
                    if any(r["day"] < day and r["status"] == "active" for r in days.values()):
                        self.execute(
                            "UPDATE {s}frame_partitions_v2 SET status='sealed' WHERE partition_day<? AND status='active'",
                            [day],
                        )
                if truncates:
                    self.execute(
                        "UPDATE {s}tables SET state='needs_resync' WHERE relation_id IN "
                        "(SELECT unnest(from_json_strict(?,'[\"UINTEGER\"]')) AS truncate_oid) "
                        # An append table applies a TRUNCATE in place (keyless design D10).
                        "AND capture_mode IS DISTINCT FROM 'append'",
                        [encoded(sorted(truncates))],
                    )
                self.execute(
                    "UPDATE {s}source SET confirmed_lsn=?,open_unit_anchor=?,protocol_reason=NULL,"
                    "protocol_anchor=NULL,protocol_recorded=NULL,protocol_observed=NULL WHERE id=1",
                    [boundary, open_anchor],
                )
                if fault:
                    fault("before_commit")
            if fault:
                fault("after_commit")
            # Worker-owned cache: update only after COMMIT and before future completion.
            if fresh:
                key = (day, self.epoch)
                fresh_min = min(c.unit_start_lsn for c in fresh)
                fresh_max = max(c.unit_start_lsn for c in fresh)
                previous = days.get(key, dict(n=0, bytes=0, min=fresh_min, max=fresh_max))
                days[key] = dict(
                    day=day,
                    epoch=self.epoch,
                    min=min(previous["min"], fresh_min),
                    max=max(previous["max"], fresh_max),
                    n=previous["n"] + sum(c.count for c in fresh),
                    bytes=previous["bytes"] + sum(c.bytes for c in fresh),
                    status="active",
                )
                for data in days.values():
                    if data["day"] < day and data["status"] == "active":
                        data["status"] = "sealed"
            self._days = days
            return boundary
        except BaseException as exc:
            if began:
                self._days = None
                if is_conflict(exc) and not isinstance(exc, GuardedIngestConflict):
                    raise GuardedIngestConflict("format-2 append conflict") from exc
            raise

    def _record_progress(self, status, progress):
        if progress is not None and (
            not isinstance(progress, Progress) or progress.epoch != self.epoch
        ):
            raise SourceMismatch("invalid source unit progress")
        anchor = progress.unit_anchor if progress is not None else None
        active = progress.active_commit if progress is not None else None
        old_epoch, old_anchor, old_commit, old_count, count = self.execute(
            "SELECT last_capped_epoch,last_capped_anchor,last_capped_commit,last_capped_frame,"
            "(SELECT coalesce(max(first_frame+n),0) FROM {s}chunks_v2 WHERE epoch=? "
            "AND unit_start_lsn=?) FROM {s}protocol_v2 WHERE id=1",
            [self.epoch, anchor],
        ).fetchone()
        if progress is not None and progress.durable_frame_end != count:
            raise ValueError("reported progress differs from the durable frame prefix")
        stalled = (
            status == "capped"
            and progress is not None
            and self.epoch == old_epoch
            and anchor == old_anchor
            and active == old_commit
            and count <= (old_count or 0)
        )
        self.execute(
            "UPDATE {s}protocol_v2 SET last_capped_epoch=?,last_capped_anchor=?,"
            "last_capped_commit=?,last_capped_frame=? WHERE id=1",
            [
                self.epoch if status == "capped" else None,
                anchor if status == "capped" else None,
                active,
                count,
            ],
        )
        self.execute(
            "UPDATE {s}source SET state=? WHERE id=1",
            ["needs_action:large_transaction" if stalled else "ready"],
        )
        return stalled

    def reader(self):
        return UnitReader(self.con, self.config)
