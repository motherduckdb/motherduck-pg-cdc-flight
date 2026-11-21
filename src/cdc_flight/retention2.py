"""Whole-unit retention for the active format-2 protocol.

The floor is a complete-unit end (an abandoned unit uses its previous complete
anchor). A drop must leave floor STRICTLY BELOW every live pin and the source
checkpoint, so every legitimate reader low is above it: equality remains refused.
"""

import json
import re

from cdc_flight import lifecycle_commands as lc
from cdc_flight.config import quoted
from cdc_flight.format2 import uint
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.retention_common import (
    CompactionBusy,
    DropResult,
    FloorAboveCheckpoint,
    HistoryDropped,
    RetentionBase,
    refuse_lifecycle,
)
from cdc_flight.unit_store import pending_key_observations


class Retention2(RetentionBase):
    format_version = 2

    def statements(self, catalog, *, altering_tables=False):
        statements = super().statements(catalog, altering_tables=altering_tables)
        for name, dtype in (
            ("format_version", "INTEGER"),
            ("chunks_pruned", "BIGINT"),
            ("units_pruned", "BIGINT"),
        ):
            if "compactions" in catalog and name not in catalog["compactions"]:
                statements.append(
                    f"ALTER TABLE {{s}}compactions ADD COLUMN IF NOT EXISTS {name} {dtype}"
                )
        return statements

    def initialize(self):
        if "protocol_v2" not in self.catalog():
            raise SourceMismatch("retention requires a format-2 store; recreate and re-backfill")
        row = self.execute(
            "SELECT format_version,min_reader FROM {s}protocol_v2 WHERE id=1"
        ).fetchone()
        if row is None or row[0] != 2 or row[1] > 2:
            raise SourceMismatch("unsupported unit retention format")
        super().initialize()

    def initial_cut(self, epoch):
        """The recorded epoch start, never inferred from a pruned unit inventory."""
        uint(epoch, 32, "epoch")
        row = self.execute(
            "SELECT start_lsn FROM {s}source_epochs WHERE epoch=?", [epoch]
        ).fetchone()
        if row is None or row[0] is None:
            raise SourceMismatch("missing immutable epoch start")
        uint(row[0], 64, "recorded epoch start")
        return row[0]

    def _cut(self, epoch, lsn):
        uint(epoch, 32, "epoch")
        uint(lsn, 64, "unit cut")
        row = self.execute(
            "SELECT e.start_lsn,r.floor_epoch,r.floor_lsn,EXISTS "
            "(SELECT 1 FROM {s}units_v2 u WHERE u.epoch=? AND u.complete AND u.end_lsn=?) "
            "FROM {s}retention r LEFT JOIN {s}source_epochs e ON e.epoch=? WHERE r.id=1",
            [epoch, lsn, epoch],
        ).fetchone()
        if row is None:
            raise SourceMismatch("unit retention metadata missing; initialize retention")
        if row[0] is None:
            raise SourceMismatch("missing immutable epoch start")
        if (row[1] is None) != (row[2] is None):
            raise SourceMismatch("inconsistent unit retention floor")
        if row[1] is not None and (epoch, lsn) <= tuple(row[1:3]):
            raise HistoryDropped("requested unit history is at or below the retention floor")
        if lsn != row[0] and not row[3]:
            raise SourceMismatch("position is not an epoch start or complete unit end")

    def register_generation(self, relation_id, generation_id, epoch, lsn):
        self._cut(epoch, lsn)
        return super().register_generation(relation_id, generation_id, epoch, lsn)

    def transfer(self, relation_id, generation_id, epoch, lsn, keep=None):
        self._cut(epoch, lsn)
        return super().transfer(relation_id, generation_id, epoch, lsn, keep)

    def advance(self, owner, generation_id, epoch, lsn):
        """Raise an owned pin and validate its complete-unit cut in one snapshot.

        Like the shared advance, this only raises protection and is deliberately
        unguarded. Ordinary UPDATE preserves conflict detection; no RETURNING.
        """
        uint(epoch, 32, "epoch")
        uint(lsn, 64, "unit cut")
        count = self.execute(
            "UPDATE {s}retention_pins SET pinned_lsn=greatest(pinned_lsn,?),"
            "updated_at=current_timestamp WHERE owner=? AND generation_id=? AND source_epoch=? "
            "AND EXISTS (SELECT 1 FROM {s}source_epochs e CROSS JOIN {s}retention r "
            "WHERE e.epoch=? AND e.start_lsn IS NOT NULL AND r.id=1 "
            "AND ((r.floor_epoch IS NULL AND r.floor_lsn IS NULL) OR "
            "(r.floor_epoch IS NOT NULL AND r.floor_lsn IS NOT NULL AND "
            "(?>r.floor_epoch OR (?=r.floor_epoch AND ?>r.floor_lsn)))) "
            "AND (?=e.start_lsn OR EXISTS (SELECT 1 FROM {s}units_v2 u "
            "WHERE u.epoch=? AND u.complete AND u.end_lsn=?)))",
            [lsn, owner, generation_id, epoch, epoch, epoch, epoch, lsn, lsn, epoch, lsn],
        ).fetchone()[0]
        lc.checked("pin_advance", "pin_update", count)  # 0 or 1
        if count == 1:
            return True
        # Diagnostics use a later snapshot. A unit completing meanwhile can make
        # this report stale ownership instead of an invalid cut; neither wrote.
        self._cut(epoch, lsn)
        return False

    def require(self, owner, generation_id, epoch, low):
        self._cut(epoch, low)
        return super().require(owner, generation_id, epoch, low)

    def check_read(self, epoch, low):
        self._cut(epoch, low)

    def drop_candidates(self):
        return [
            day
            for (day,) in self.execute(
                "SELECT partition_day FROM {s}frame_partitions_v2 GROUP BY partition_day "
                "HAVING bool_or(status<>'dropped') ORDER BY partition_day"
            ).fetchall()
        ]

    def retained_days(self, lag_hours):
        """Expiry's existing day summary, in unit cuts; unresolved days stay active.

        The per-epoch rows still combine by physical day in the existing policy.
        Bytes are charged wire bytes, not measured MotherDuck storage.
        """
        rows = self.execute(
            "WITH spans AS (SELECT c.partition_day,c.epoch,min(c.unit_start_lsn) AS low,"
            "max(CASE WHEN u.complete THEN u.end_lsn ELSE u.unit_start_lsn END) AS high,"
            "bool_and(u.epoch IS NOT NULL AND (u.complete OR u.abandoned_into_epoch IS NOT NULL)) AS resolved,"
            "sum(c.n) AS n FROM {s}chunks_v2 c LEFT JOIN {s}units_v2 u USING(epoch,unit_start_lsn) "
            "GROUP BY c.partition_day,c.epoch) "
            "SELECT p.partition_day,p.epoch,x.high,CASE WHEN p.status='sealed' AND x.resolved "
            "THEN 'sealed' ELSE 'active' END,p.n,p.bytes,"
            "strptime(p.partition_day,'%Y%m%d')<=(current_timestamp AT TIME ZONE 'UTC')-?*INTERVAL '1 hour',"
            "x.low,epoch_us(strptime(p.partition_day,'%Y%m%d')),x.n "
            "FROM {s}frame_partitions_v2 p LEFT JOIN spans x USING(partition_day,epoch) "
            "WHERE p.status<>'dropped' AND p.n>0 ORDER BY p.partition_day,p.epoch",
            [lag_hours],
        ).fetchall()
        if any(row[2] is None or row[9] != row[4] for row in rows):
            raise SourceMismatch("inconsistent retained unit inventory")
        return [row[:9] for row in rows]

    def drop_partition(self, day, token, *, fault=None):
        """Drop one sealed day below every real and virtual unit pin.

        Fixed guard order: retention first, then source. The source update changes
        ONLY guard, with epoch/checkpoint/open-anchor CAS. It conflicts with append
        without taking or renewing the ingest lease. Never swallow a conflict.
        """
        if not re.fullmatch(r"\d{8}", day):
            raise ValueError("invalid partition day")
        with self.transaction():
            count = self.execute(
                "UPDATE {s}retention SET guard=uuid()::VARCHAR,"
                "expires_at=current_timestamp+?*INTERVAL '1 second' "
                "WHERE id=1 AND token=? AND expires_at>current_timestamp",
                [self.lease_seconds, token],
            ).fetchone()[0]
            if count != 1:
                raise CompactionBusy("compaction lease expired or was replaced")
            parts = self.execute(
                "SELECT epoch,n,status FROM {s}frame_partitions_v2 WHERE partition_day=? AND status<>'dropped'",
                [day],
            ).fetchall()
            if not parts or any(status != "sealed" for _, _, status in parts):
                return DropResult(False, "not_sealed")
            unpinned = self.execute(
                "SELECT min(t.relation_id) FROM {s}tables t WHERE t.pin_state='pinned' "
                "AND coalesce(t.held_reason,'') NOT IN ('removed','unmanaged') "
                "AND NOT EXISTS (SELECT 1 FROM {s}retention_pins p "
                "WHERE p.relation_id=t.relation_id)"
            ).fetchone()[0]
            if unpinned is not None:
                return DropResult(False, "held", relation=unpinned)
            pins = sorted(self.pins().values())
            if not pins:
                return DropResult(False, "no_pins")
            source = self.execute(
                "SELECT epoch,confirmed_lsn,open_unit_anchor,lifecycle FROM {s}source WHERE id=1"
            ).fetchone()
            if source is None or source[1] is None:
                raise SourceMismatch("source is not registered")
            refuse_lifecycle(self.names[1], source[3])
            summary = self.execute(
                "WITH touched AS (SELECT c.epoch,c.unit_start_lsn,sum(c.n) AS n FROM {s}chunks_v2 c "
                "JOIN {s}frame_partitions_v2 p USING(partition_day,epoch) "
                "WHERE c.partition_day=? AND p.status<>'dropped' GROUP BY c.epoch,c.unit_start_lsn) "
                "SELECT coalesce(sum(t.n),0),coalesce(bool_and(u.epoch IS NOT NULL),true),"
                "coalesce(bool_or(NOT u.complete AND u.abandoned_into_epoch IS NULL),false),"
                "coalesce(bool_and(CASE WHEN u.complete THEN u.end_lsn IS NOT NULL "
                "AND u.abandoned_into_epoch IS NULL WHEN u.abandoned_into_epoch IS NOT NULL "
                "THEN u.end_lsn IS NULL AND u.abandoned_frame_end=u.frame_count "
                "ELSE u.end_lsn IS NULL END),true),"
                "max(struct_pack(epoch:=t.epoch,cut:=CASE WHEN u.complete THEN u.end_lsn "
                "ELSE u.unit_start_lsn END)) FROM touched t LEFT JOIN {s}units_v2 u "
                "USING (epoch,unit_start_lsn)",
                [day],
            ).fetchone()
            physical = self.execute(
                f"SELECT count(*) FROM {{s}}{quoted('frames_v2_' + day)}"
            ).fetchone()[0]
            if (
                not summary[1]
                or not summary[3]
                or summary[0] != sum(p[1] for p in parts)
                or physical != summary[0]
            ):
                raise SourceMismatch("inconsistent unit partition inventory")
            if summary[2]:
                return DropResult(False, "pinned")
            high = None if summary[4] is None else (summary[4]["epoch"], summary[4]["cut"])
            # Defence in depth: open anchor == checkpoint today, so the later
            # strict checkpoint bound already protects it. Keep the virtual pin
            # explicit if that invariant ever changes.
            protected = pins + ([(source[0], source[2])] if source[2] is not None else [])
            if high is not None and high >= min(protected):
                return DropResult(False, "pinned")
            if high is not None and high >= tuple(source[:2]):
                raise FloorAboveCheckpoint("a drop would raise the unit floor past the checkpoint")
            if fault:
                fault("after_drop_plan")
            count = self.execute(
                "UPDATE {s}source SET guard=uuid()::VARCHAR WHERE id=1 AND epoch=? "
                "AND confirmed_lsn=? AND open_unit_anchor IS NOT DISTINCT FROM ?::UBIGINT",
                list(source[:3]),
            ).fetchone()[0]
            if count != 1:
                raise CompactionBusy("source position changed before unit partition drop")
            if fault:
                fault("after_source_guard")
            self.execute(f"DROP TABLE {{s}}{quoted('frames_v2_' + day)}")
            if fault:
                fault("after_drop")
            self.execute(
                "UPDATE {s}frame_partitions_v2 SET status='dropped' WHERE partition_day=?", [day]
            )
            floor = self.floor()
            if high is not None and (floor is None or high > floor):
                self.execute(
                    "UPDATE {s}retention SET floor_epoch=?,floor_lsn=? WHERE id=1", list(high)
                )
            if fault:
                fault("before_drop_commit")
        # Success is constructed only after COMMIT, never after a caught conflict.
        return DropResult(True)

    def prune(self, token, *, fault=None, out_of_time=None):
        """Prune dropped-day chunks, then resolved units with no remaining chunks.

        A cross-day unit and its marker mapping survive until its LAST physical
        day is gone. Referenced complete endpoints remain immutable metadata:
        current checked cuts, pending cuts and historical key cuts, one row per
        distinct referenced cut (bounded by table checks plus key history).
        Their frames/chunks are gone; readers and pins still refuse at/below the
        floor. Retained endpoints therefore never become readable history.
        Configurations, descriptors and key history remain retained.
        """
        parts = self.execute(
            "SELECT DISTINCT c.partition_day,c.epoch FROM {s}chunks_v2 c "
            "JOIN {s}frame_partitions_v2 p USING(partition_day,epoch) "
            "WHERE p.status='dropped' ORDER BY c.partition_day,c.epoch"
        ).fetchall()
        chunks = units = 0
        for day, epoch in parts:
            if out_of_time and out_of_time():
                return chunks, units
            with self.transaction():
                self._lease(token)
                chunks += self.execute(
                    "DELETE FROM {s}chunks_v2 WHERE partition_day=? AND epoch=?", [day, epoch]
                ).fetchone()[0]
                if fault:
                    fault("prune_chunks")
        floor = self.floor()
        if floor is not None and not (out_of_time and out_of_time()):
            with self.transaction():
                self._lease(token)
                # Only initial epoch-1 registration leaves key_epoch NULL;
                # every later key verification/epoch transition writes it.
                references = set(
                    self.execute(
                        "SELECT coalesce(key_epoch,1),key_checked_lsn FROM {s}tables "
                        "WHERE key_checked_lsn IS NOT NULL UNION SELECT epoch,cut_lsn FROM {s}key_changes"
                    ).fetchall()
                )
                for (payload,) in self.execute(
                    "SELECT pending_keys_v2 FROM {s}tables WHERE pending_keys_v2 IS NOT NULL"
                ).fetchall():
                    for entry in pending_key_observations(payload):
                        epoch, cut = entry["epoch"], entry["cut_lsn"]
                        valid = self.execute(
                            "SELECT EXISTS (SELECT 1 FROM {s}source_epochs WHERE epoch=? AND start_lsn=?) "
                            "OR EXISTS (SELECT 1 FROM {s}units_v2 WHERE epoch=? AND complete AND end_lsn=?)",
                            [epoch, cut, epoch, cut],
                        ).fetchone()[0]
                        if not valid:
                            raise SourceMismatch("pending key cut is not a complete endpoint")
                        references.add((epoch, cut))
                protected = json.dumps([dict(epoch=e, cut=c) for e, c in sorted(references)])
                units = self.execute(
                    "DELETE FROM {s}units_v2 u WHERE (u.complete OR u.abandoned_into_epoch IS NOT NULL) "
                    "AND (u.epoch<? OR (u.epoch=? AND coalesce(u.end_lsn,u.unit_start_lsn)<=?)) "
                    "AND NOT EXISTS (SELECT 1 FROM {s}chunks_v2 c WHERE c.epoch=u.epoch "
                    "AND c.unit_start_lsn=u.unit_start_lsn) "
                    "AND NOT EXISTS (SELECT 1 FROM unnest(from_json_strict(?,"
                    """'[{"epoch":"UINTEGER","cut":"UBIGINT"}]'"""
                    ")) refs(r) WHERE r.epoch=u.epoch AND r.cut=u.end_lsn)",
                    [floor[0], floor[0], floor[1], protected],
                ).fetchone()[0]
                if fault:
                    fault("prune_units")
        return chunks, units
