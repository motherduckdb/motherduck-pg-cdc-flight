"""Logical replication stream that sends every Standby Status Update itself.

psycopg2's replication cursor reports the server's WAL end as flushed on a keepalive
whenever its last explicit flush is at or past the last data message it received
(`pqpath.c`), and it replies to keepalives on its own. Feedback could then exceed the
durable checkpoint. This stream drives the copy-both protocol through psycopg 3's
libpq wrapper (`psycopg.pq`), and nothing but `_send_status` writes to it, so every
status carries `acked`: the boundary ingest acknowledged after a durable append.

`fence(lsn)` is called before the first status and before every larger value.
Repeats of an already fenced value (keepalive replies, status-interval sends while
an upload runs on another thread's connection) go out without it. A stale worker's
risk is a first status on a slot or a new value, and both are fenced.
"""

import select
import struct
import time
from dataclasses import dataclass

import psycopg
from psycopg import pq
from psycopg2.extensions import make_dsn

PG_EPOCH_US = 946_684_800_000_000  # 2000-01-01 in Unix microseconds
HEADER = 25  # 'w' + data_start + wal_end + send time


class ReplicationError(RuntimeError):
    """A replication connection or protocol failure. Messages never include libpq
    text, which can echo host or user names."""


@dataclass(frozen=True)
class XLogData:
    data_start: int
    wal_end: int
    payload: bytes


def lsn_text(lsn):
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


class ReplicationStream:
    def __init__(self, options, *, fence=None):
        conninfo = make_dsn(**options, replication="database")
        self.conn = pq.PGconn.connect(conninfo.encode())
        if self.conn.status != pq.ConnStatus.OK:
            self.conn.finish()
            raise ReplicationError("replication connection failed")
        self.fence = fence
        self.acked = None
        self.fenced = None
        self.streaming = False

    def fileno(self):
        return self.conn.socket

    def show(self, name):
        """A session setting of this connection (SHOW works on a replication session)."""
        result = self.conn.exec_(f"SHOW {name}".encode())
        if result.status != pq.ExecStatus.TUPLES_OK:
            raise ReplicationError("SHOW failed on the replication session")
        return result.get_value(0, 0).decode()

    def start(self, slot, start_lsn, publication):
        if not slot.replace("_", "").isalnum() or not publication.replace("_", "").isalnum():
            raise ValueError("unsafe slot or publication name")
        self.conn.send_query(
            f"START_REPLICATION SLOT {slot} LOGICAL {lsn_text(start_lsn)} "
            f"(proto_version '1', publication_names '{publication}')".encode()
        )
        result = self.conn.get_result()
        if result is None or result.status != pq.ExecStatus.COPY_BOTH:
            raise ReplicationError("START_REPLICATION was refused")
        self.streaming = True
        self.acked = start_lsn
        self._send_status()

    def acknowledge(self, lsn):
        """Report `lsn` as flushed. Never moves backwards."""
        if lsn > self.acked:
            self.acked = lsn
        self._send_status()

    def status(self):
        self._send_status()

    def _send_status(self, reply=False):
        value = self.acked
        if self.fence is not None and (self.fenced is None or value > self.fenced):
            self.fence(value)
            self.fenced = value
        now = int(time.time() * 1_000_000) - PG_EPOCH_US
        message = b"r" + struct.pack("!QQQqB", value, value, value, now, int(reply))
        try:
            sent = self.conn.put_copy_data(message) == 1 and self.conn.flush() == 0
        except psycopg.Error:
            sent = False
        if not sent:
            raise ReplicationError("sending a replication status failed")

    def read(self, timeout=0.0):
        """The next XLogData, or None if none arrived within `timeout` seconds.
        Keepalives that request a reply are answered with `acked`. It returns at the
        first XLogData, so a keepalive buffered behind it is answered on the next
        call, well inside wal_sender_timeout.

        Complete messages already buffered are returned first; consume_input() runs
        only when none is. libpq compacts its input buffer on every consume_input(),
        so calling it per message made draining a large backlog quadratic in the
        buffered bytes. Messages are still processed strictly in arrival order."""
        deadline = time.monotonic() + timeout
        while True:
            message = self._buffered()
            if message is None:
                try:
                    self.conn.consume_input()
                except psycopg.Error:
                    self.streaming = False
                    raise ReplicationError("the replication connection failed") from None
                message = self._buffered()
            if message is not None:
                return message
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            select.select([self.fileno()], [], [], remaining)

    def _buffered(self):
        """The next XLogData among the complete messages libpq already holds, answering
        reply-requested keepalives on the way; None when none is buffered."""
        while True:
            size, buffer = self.conn.get_copy_data(1)
            if size == 0:
                return None
            if size < 0:
                self.streaming = False
                raise ReplicationError("the server ended the replication stream")
            message = bytes(buffer)
            if message[:1] == b"w":
                data_start, wal_end = struct.unpack("!QQ", message[1:17])
                return XLogData(data_start, wal_end, message[HEADER:])
            if message[:1] == b"k" and message[17]:
                self._send_status()

    def close(self, timeout=5.0):
        """Send a final status and end the copy, then wait (bounded) until the server
        has finished it, so the final status is processed before disconnecting. Never
        raises: it runs in cleanup, where the original error must survive.

        It never calls the fence: the final status is sent only when its value is
        already fenced (a repeat). After a failed fence nothing more is acknowledged."""
        try:
            if self.streaming:
                if self.fence is None or self.fenced == self.acked:
                    self._send_status()
                self.conn.put_copy_end()
                self.conn.flush()
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    self.conn.consume_input()
                    size, _ = self.conn.get_copy_data(1)
                    if size == -1:
                        break
                    if size < -1:
                        return
                    if size == 0:
                        select.select([self.fileno()], [], [], deadline - time.monotonic())
                # Drain the results of the finished copy (command complete).
                while time.monotonic() < deadline:
                    self.conn.consume_input()
                    if self.conn.is_busy():
                        select.select([self.fileno()], [], [], deadline - time.monotonic())
                    elif self.conn.get_result() is None:
                        break
                self.streaming = False
        except Exception:
            self.streaming = False
        finally:
            self.conn.finish()
