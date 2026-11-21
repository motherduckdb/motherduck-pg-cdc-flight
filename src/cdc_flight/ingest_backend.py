"""Small representation hooks for the single pipelined ingest loop."""

import pyarrow.compute as pc

from cdc_flight.format2 import COMMIT, OUTPUT_SETTINGS, UnitComplete
from cdc_flight.format2 import Chunk as Chunk2
from cdc_flight.ingest_log import SourceMismatch
from cdc_flight.preflight import query
from cdc_flight.spool2 import Spool as Spool2
from cdc_flight.unit_progress import Progress


class Backend2:
    def __init__(self, log):
        self.log = log
        # "Current as of" (ops): the last COMMIT frame's commit_ts_us per unit, from this
        # run's durable chunks, and (epoch, end, ts) of the last unit made durable (ts None
        # when its last commit was durable before this run). Memory only, no statement.
        self._unit_commit_ts = {}
        self.durable_through = None

    def recover(self):
        recovery = self.log.recovery()
        if recovery.confirmed_end != self.log.acquired_lsn or recovery.epoch != self.log.epoch:
            raise SourceMismatch("spool anchor differs from the leased stream start")
        self.durable_frames = (
            {recovery.open_anchor: sum(c.count for c in recovery.prefix)}
            if recovery.open_anchor is not None
            else {}
        )
        return recovery

    def verify_session(self, show, regular):
        binding = self.log.config.text_binding
        observed = tuple((name, show(name)) for name in OUTPUT_SETTINGS)
        # Extension inventory is database metadata, not session state. All output
        # GUCs above are read on the actual replication connection.
        extensions = tuple(
            query(regular, "SELECT extname,extversion FROM pg_extension ORDER BY extname")
        )
        major = int(show("server_version_num")) // 10000
        try:
            binding.verify(observed, extensions)
            if major != self.log.config.server_major:
                raise ValueError("server major changed")
        except ValueError:
            self.log.protocol_changed(
                "binding_changed",
                binding.digest.hex(),
                "replication_session_mismatch",
                self.log.acquired_lsn,
            )

    def spool(self, config, keys, heartbeat_oid, marker, recovery):
        spool = Spool2(
            config,
            keys,
            heartbeat_oid,
            marker,
            binding=recovery.binding,
            start_lsn=recovery.confirmed_end,
            unit_config=recovery.unit_config,
            replay=recovery.prefix,
            terminal=recovery.terminal,
        )
        self.log._spool = spool
        return spool

    def budget(self, item):
        if isinstance(item, Chunk2):
            descriptors = sum(
                len(d.json.encode()) + sum(len(k.encode()) for k in d.keys)
                for d in item.descriptors
            )
            return max(item.bytes, item.table.nbytes) + descriptors, item.count
        # Bounded terminal metadata; COMMIT controls are already charged in chunks.
        return 256, 1

    def counts(self, item):
        # Called only after collect established and fenced this durable result.
        if isinstance(item, Chunk2):
            self.durable_frames[item.unit_start_lsn] = item.first_frame + item.count
            ts = last_commit_ts(item.table)
            if ts is not None:
                self._unit_commit_ts[item.unit_start_lsn] = ts
            return 1, 0
        self.durable_frames.pop(item.unit_start_lsn, None)
        if isinstance(item, UnitComplete):
            ts = self._unit_commit_ts.pop(item.unit_start_lsn, None)
            self.durable_through = (item.epoch, item.end_lsn, ts)
        return 0, item.member_count

    def progress(self, spool):
        if not self.partial(spool):
            return None
        return Progress(
            spool.epoch,
            spool.anchor,
            spool.commit if spool.active else None,
            self.durable_frames.get(spool.anchor, 0),
        )

    def drained(self, items, past=None):
        """Self mode: this run's heartbeat marker closed a unit. External mode (`past`
        = B, the standby's replay position under the lease; design Rule U): a unit
        that ends strictly after B. Replay at B may already show a commit ending at B
        (redo runs before the pointer moves), so `>=` could stop before it is read."""
        if past is not None:
            return any(isinstance(item, UnitComplete) and item.end_lsn > past for item in items)
        return any(isinstance(item, UnitComplete) and item.marker for item in items)

    def partial(self, spool):
        return spool.active or spool.frame_count > 0

    def poll(self, spool):
        return spool.poll()

    def close_at_boundary(self, spool):
        spool.request_close()
        return [] if spool.active else spool.close()


def last_commit_ts(table):
    """The commit_ts_us of a frames table's last COMMIT frame (frames are in order), or
    None. One vectorized pass over two columns already in memory."""
    commits = table.column("commit_ts_us").filter(pc.equal(table.column("record_kind"), COMMIT))
    return commits[len(commits) - 1].as_py() if len(commits) else None
