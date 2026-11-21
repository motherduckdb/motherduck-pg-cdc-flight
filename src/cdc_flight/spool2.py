"""Bounded format-2 spool, the active ingest path since the format-2 cutover.

Only source I/U/D/T and COMMIT messages produce frames. Session metadata can be
re-sent anywhere without changing charged bytes or retry identities.
"""

import hashlib
import json
import struct
import time
from dataclasses import asdict

import pyarrow as pa

from cdc_flight.format2 import (
    ABSENT,
    COMMIT,
    COMMIT_HEADER,
    EVENT,
    EVENT_HEADER,
    FORMAT_VERSION,
    FRAME_SCHEMA,
    NO_OLD,
    OLD_FULL,
    OLD_KEY,
    ORDINAL,
    TEXT,
    ZERO_HASH,
    Binding,
    Chunk,
    Descriptor,
    UnitComplete,
    UnitConfig,
    domain,
    image,
    key_layout,
    project,
    uint,
    validate_image,
    validate_projection,
)
from cdc_flight.pgoutput import decode

OP_TAG = {"insert": ord("I"), "update": ord("U"), "delete": ord("D"), "truncate": ord("T")}
TERMINAL_HEADER = struct.Struct("!BH32sIQqIQQ")  # COMMIT_HEADER without its digest field
_FRAME_ROW_TYPE = pa.struct(FRAME_SCHEMA)


def _small_frame_table(rows):
    """One typed conversion; preserve the full schema when extracting its columns."""
    array = pa.array(rows, type=_FRAME_ROW_TYPE, from_pandas=False)
    if array.null_count:
        raise ValueError("null frame rows are invalid")
    return pa.Table.from_struct_array(array).replace_schema_metadata(FRAME_SCHEMA.metadata)


def _frame_table(rows):
    # One EVENT + COMMIT, or a COMMIT-only empty transaction. Larger chunks keep
    # the column builder; both paths have exactly the same persisted row contract.
    if len(rows) <= 2:
        return _small_frame_table(rows)
    return pa.Table.from_pylist(rows, schema=FRAME_SCHEMA)


class Spool:
    def __init__(
        self,
        config,
        key_columns,
        heartbeat_oid,
        marker,
        *,
        binding: Binding,
        start_lsn,
        unit_config=None,
        replay=(),
        terminal=None,
        clock=time.monotonic,
        max_descriptor_bytes=64 * 1024 * 1024,
    ):
        self.config = config
        self.epoch = uint(config.epoch, 32, "epoch")
        self.binding = binding
        if binding.source_id != config.source_id:
            raise ValueError("source binding mismatch")
        self.binding_hash = binding.digest
        self.unit_config = unit_config or UnitConfig(chunk_bytes=config.chunk_bytes)
        if self.unit_config.mapping_revision != binding.mapping_revision:
            raise ValueError("unit cell mapping revision mismatch")
        self.key_columns = {oid: tuple(keys) for oid, keys in key_columns.items()}
        self.heartbeat_oid, self.marker, self.clock = heartbeat_oid, marker, clock
        self.max_descriptor_bytes = uint(max_descriptor_bytes, 64, "max_descriptor_bytes")
        if not self.max_descriptor_bytes:
            raise ValueError("descriptor memory budget must be positive")
        self.relations, self.cache_bytes = {}, 0
        self.active = False
        self.replay = tuple(replay)
        self.terminal = terminal
        if terminal and (terminal.epoch, terminal.unit_start_lsn, terminal.config_revision) != (
            self.epoch,
            start_lsn,
            self.unit_config.revision,
        ):
            raise ValueError("recorded terminal belongs to another unit/config")
        expected = 0
        for chunk in self.replay:
            if (
                chunk.epoch != self.epoch
                or chunk.unit_start_lsn != start_lsn
                or chunk.first_frame != expected
                or chunk.config_revision != self.unit_config.revision
                or chunk.mapping_revision != binding.mapping_revision
                or chunk.count <= 0
            ):
                raise ValueError("invalid durable prefix or cell mapping revision")
            expected += chunk.count
        self.replay_end, self.replay_index = expected, 0
        self._start_unit(uint(start_lsn, 64, "start_lsn"))

    def _start_unit(self, anchor):
        self.anchor = anchor
        self.unit_header = self.unit_config.header(self.binding_hash, self.epoch, anchor)
        self.chain = hashlib.sha256(domain("prefix") + self.unit_header).digest()
        self.frame_count = self.event_count = self.members = self.unit_bytes = 0
        self.rows, self.descriptors = [], {}
        self.buffer_bytes = self.descriptor_bytes = 0
        self.force_close = self.marker_seen = False
        self.unit_opened = None
        self.last_end = None
        self._start_chunk()

    def _start_chunk(self):
        self.chunk_first = self.frame_count
        self.chunk_hash = hashlib.sha256(
            domain("chunk") + self.unit_header + ORDINAL.pack(self.chunk_first)
        )
        self.chunk_events = self.chunk_members = 0

    def _descriptor(self, relation):
        oid = relation.oid
        if oid not in self.key_columns:
            raise ValueError("unregistered relation; run preflight before ingest")
        names = tuple(col[1] for col in relation.columns)
        keys = self.key_columns[oid]
        indices = tuple(names.index(k) if k in names else -1 for k in keys)
        desc = Descriptor(
            oid,
            relation.version,
            names,
            keys,
            json.dumps(asdict(relation), sort_keys=True),
            key_layout(keys),
            relation.namespace,
            relation.name,
            indices,
        )
        size = len(desc.json.encode()) + sum(len(k.encode()) for k in keys)
        previous = self.relations.get(oid)
        total = self.cache_bytes + size - (previous[2] if previous else 0)
        if total > self.max_descriptor_bytes:
            raise ValueError("Relation cache exceeds descriptor memory budget")
        self.relations[oid] = (desc, indices, size)
        self.cache_bytes = total

    def _keep_descriptor(self, entry):
        desc, _indices, size = entry
        key = (desc.oid, desc.version, desc.key_hash)
        if key not in self.descriptors:
            if self.descriptor_bytes + size > self.max_descriptor_bytes:
                # Memory pressure cannot invent a replay-dependent chunk boundary.
                raise ValueError("pending descriptors exceed memory budget")
            self.descriptors[key] = desc
            self.descriptor_bytes += size

    def _add(self, row, header, wire, logical_events):
        charge = ORDINAL.size + len(header) + len(wire)
        if self.transaction_bytes + charge > self.config.max_transaction_bytes:
            raise ValueError("transaction exceeds charged-wire budget")
        out = []
        if self.rows and (
            self.buffer_bytes + charge > self.unit_config.chunk_bytes
            or len(self.rows) + 1 > self.unit_config.chunk_frames
        ):
            out.append(self._flush())
        ordinal = self.frame_count
        row.update(
            unit_start_lsn=self.anchor,
            frame_ordinal=ordinal,
            source_epoch=self.epoch,
            commit_lsn=self.commit,
            commit_ts_us=self.timestamp,
            txid=self.txid,
            wire_length=len(wire),
        )
        self.chunk_hash.update(ORDINAL.pack(ordinal))
        self.chunk_hash.update(header)
        self.chunk_hash.update(wire)
        self.rows.append(row)
        self.frame_count += 1
        self.event_count += logical_events
        self.chunk_events += logical_events
        self.chunk_members += row["record_kind"] == COMMIT
        self.buffer_bytes += charge
        self.unit_bytes += charge
        self.transaction_bytes += charge
        return out

    def feed(self, wire):
        if len(wire) > self.config.max_event_bytes:
            raise ValueError("pgoutput message exceeds max_event_bytes")
        msg = decode(wire)
        kind = msg["kind"]
        if kind == "relation":
            self._descriptor(msg["relation"])
            return []
        if kind in ("type", "origin"):
            return []
        if kind == "begin":
            if self.active:
                raise ValueError("nested transaction")
            self.active = True
            self.commit, self.timestamp, self.txid = msg["final_lsn"], msg["time"], msg["xid"]
            if self.commit < self.anchor or (self.last_end and self.commit < self.last_end):
                raise ValueError("source commit is behind complete boundary")
            if self.unit_opened is None:
                self.unit_opened = self.clock()
            self.ordinal = self.transaction_bytes = 0
            self.transaction_hash = hashlib.sha256(
                domain("transaction")
                + struct.pack(
                    "!32sIQqI",
                    self.binding_hash,
                    self.epoch,
                    self.commit,
                    self.timestamp,
                    self.txid,
                )
            )
            return []
        if kind not in OP_TAG and kind != "commit":
            raise ValueError("unsupported source message")
        if not self.active:
            raise ValueError("change outside transaction")
        if kind == "commit":
            return self._commit(msg, wire)
        if kind == "truncate":
            old = new = old_key = new_key = None
            oid, version, khash, entry, old_kind = 0, None, ZERO_HASH, None, NO_OLD
            oids, options = list(msg["relations"]), msg["options"]
            logical_events = len(oids)
            if any(oid not in self.relations for oid in oids):
                raise ValueError("missing Relation descriptor for TRUNCATE")
        else:
            oid = msg["oid"]
            entry = self.relations.get(oid)
            if entry is None:
                raise ValueError("missing Relation descriptor")
            desc, indices, _size = entry
            for cells in (msg["old"], msg["new"]):
                if cells is not None and len(cells) != len(desc.names):
                    raise ValueError("tuple/descriptor width mismatch")
            old, new = image(msg["old"]), image(msg["new"])
            old_key, new_key = project(old, indices), project(new, indices)
            # decode() has validated this marker; keep K versus O without a second parser.
            marker = wire[5]
            if marker not in (ord("K"), ord("O"), ord("N")) or (
                (msg["old"] is None) != (marker == ord("N"))
            ):
                raise ValueError("old tuple marker disagrees with decoded presence")
            old_kind = {ord("K"): OLD_KEY, ord("O"): OLD_FULL}.get(marker, NO_OLD)
            version, khash = desc.version, desc.key_hash
            oids = options = None
            logical_events = 1
            if oid == self.heartbeat_oid and new is not None and "marker" in desc.names:
                i = desc.names.index("marker")
                if new["kinds"][i] == TEXT and new["values"][i] == self.marker:
                    self.marker_seen = self.force_close = True
                if "id" in desc.names:
                    key_i = desc.names.index("id")
                    if new["kinds"][key_i] == TEXT and new["values"][key_i].startswith("backfill:"):
                        # Backfill writes source-owned heartbeat IDs. This close request
                        # is source content, unlike the current run's marker-match flag.
                        self.force_close = True
        header = EVENT_HEADER.pack(
            EVENT,
            FORMAT_VERSION,
            self.binding_hash,
            self.epoch,
            self.commit,
            self.timestamp,
            self.txid,
            self.ordinal,
            OP_TAG[kind],
            oid,
            bytes.fromhex(version) if version else ZERO_HASH,
            khash,
            len(wire),
        )
        self.transaction_hash.update(header)
        self.transaction_hash.update(wire)
        row = dict(
            record_kind=EVENT,
            event_ordinal=self.ordinal,
            relation_id=oid,
            schema_name=entry[0].namespace if entry else None,
            table_name=entry[0].table_name if entry else None,
            schema_version=version,
            key_layout_hash=khash.hex() if entry else None,
            op=kind,
            old=old,
            new=new,
            old_key=old_key,
            new_key=new_key,
            old_tuple_kind=old_kind,
            truncate_oids=oids,
            truncate_options=options,
        )
        out = self._add(row, header, wire, logical_events)
        if entry:
            self._keep_descriptor(entry)
        elif kind == "truncate":
            for truncated_oid in oids:
                self._keep_descriptor(self.relations[truncated_oid])
        self.ordinal += logical_events
        return out

    def _commit(self, msg, wire):
        if msg["lsn"] != self.commit or msg["end"] <= self.commit:
            raise ValueError("invalid commit boundary")
        terminal = TERMINAL_HEADER.pack(
            COMMIT,
            FORMAT_VERSION,
            self.binding_hash,
            self.epoch,
            self.commit,
            self.timestamp,
            self.txid,
            self.ordinal,
            len(wire),
        )
        self.transaction_hash.update(terminal)
        self.transaction_hash.update(wire)
        digest = self.transaction_hash.digest()
        header = COMMIT_HEADER.pack(
            COMMIT,
            FORMAT_VERSION,
            self.binding_hash,
            self.epoch,
            self.commit,
            self.timestamp,
            self.txid,
            self.ordinal,
            digest,
            len(wire),
        )
        out = self._add(
            dict(
                record_kind=COMMIT,
                # On control rows this stored field is the transaction event count.
                event_ordinal=self.ordinal,
                op="commit",
                old_tuple_kind=NO_OLD,
                end_lsn=msg["end"],
                end_ts_us=msg["time"],
                transaction_digest=digest.hex(),
            ),
            header,
            wire,
            0,
        )
        self.active = False
        self.members += 1
        self.last_end = msg["end"]
        due = (
            self.members >= self.unit_config.max_commits
            or self.unit_bytes >= self.unit_config.target_bytes
            or (self.clock() - self.unit_opened) * 1000 >= self.unit_config.max_delay_ms
            or self.force_close
        )
        if self.terminal:
            if self.last_end > self.terminal.end_lsn:
                raise ValueError("replay passed recorded terminal")
            due = self.last_end == self.terminal.end_lsn
        if due and self.frame_count >= self.replay_end:
            out.extend(self.close())
        return out

    def request_close(self):
        self.force_close = True

    def close(self):
        """Close only at a complete source boundary, after the durable prefix matched."""
        if self.active:
            raise ValueError("cannot close inside a source transaction")
        if not self.members:
            return []
        if self.frame_count < self.replay_end:
            self.force_close = True
            return []
        if self.terminal and self.last_end != self.terminal.end_lsn:
            raise ValueError("cannot cut a recorded complete unit")
        out = [self._flush()] if self.rows else []
        digest = hashlib.sha256(
            domain("terminal")
            + self.unit_header
            + self.chain
            + struct.pack(
                "!5Q",
                self.last_end,
                self.members,
                self.event_count,
                self.frame_count,
                self.unit_bytes,
            )
        ).hexdigest()
        complete = UnitComplete(
            self.epoch,
            self.anchor,
            self.last_end,
            self.members,
            self.event_count,
            self.frame_count,
            self.unit_bytes,
            self.chain.hex(),
            digest,
            self.unit_config.revision,
            self.marker_seen,
        )
        if self.terminal and complete != self.terminal:
            raise ValueError("recorded terminal changed on replay")
        out.append(complete)
        self.replay, self.replay_end, self.replay_index, self.terminal = (), 0, 0, None
        self._start_unit(self.last_end)
        return out

    def poll(self):
        if self.unit_opened is not None and (
            (self.clock() - self.unit_opened) * 1000 >= self.unit_config.max_delay_ms
        ):
            self.force_close = True
            if not self.active:
                return self.close()
        return []

    def _flush(self):
        if not self.rows:
            raise ValueError("cannot flush an empty chunk")
        for row in self.rows:
            if row["record_kind"] == EVENT and row["relation_id"]:
                desc = self.descriptors[
                    (
                        row["relation_id"],
                        row["schema_version"],
                        bytes.fromhex(row["key_layout_hash"]),
                    )
                ]
                for name in ("old", "new"):
                    validate_image(row[name], len(desc.names))
                    if row[name] is not None and ABSENT in row[name]["kinds"]:
                        raise ValueError("ABSENT is not a source tuple cell tag")
                for name in ("old_key", "new_key"):
                    validate_image(row[name], len(desc.keys))
                validate_projection(row["old"], row["old_key"], desc.key_indices)
                validate_projection(row["new"], row["new_key"], desc.key_indices)
                if (
                    type(row["old_tuple_kind"]) is not int
                    or row["old_tuple_kind"] not in (NO_OLD, OLD_KEY, OLD_FULL)
                    or ((row["old"] is None) != (row["old_tuple_kind"] == NO_OLD))
                ):
                    raise ValueError("invalid K/O old tuple presence")
        table = _frame_table(self.rows)
        digest = self.chunk_hash.digest()
        self.chain = hashlib.sha256(
            domain("chain")
            + self.chain
            + self.unit_header
            + struct.pack(
                "!5Q",
                self.chunk_first,
                len(self.rows),
                self.chunk_events,
                self.chunk_members,
                self.buffer_bytes,
            )
            + digest
        ).digest()
        chunk = Chunk(
            self.epoch,
            self.anchor,
            self.chunk_first,
            len(self.rows),
            self.chunk_events,
            self.chunk_members,
            digest.hex(),
            self.chain.hex(),
            self.buffer_bytes,
            tuple(self.descriptors.values()),
            self.unit_config.revision,
            self.binding.mapping_revision,
            table,
        )
        if self.replay_index < len(self.replay):
            expected = self.replay[self.replay_index]
            if chunk != expected:
                raise ValueError(f"durable prefix differs at frame {self.chunk_first}")
            self.replay_index += 1
        self.rows, self.descriptors = [], {}
        self.buffer_bytes = self.descriptor_bytes = 0
        self._start_chunk()
        return chunk


def logical_events(rows):
    """Project physical frames to ordered logical table events without COMMIT controls."""
    for row in rows:
        if row["record_kind"] == COMMIT:
            continue
        if row["record_kind"] != EVENT:
            raise ValueError("unknown format-2 frame kind")
        if row["op"] != "truncate":
            yield row
            continue
        for subindex, oid in enumerate(row["truncate_oids"]):
            yield dict(row, relation_id=oid, event_ordinal=row["event_ordinal"] + subindex)
