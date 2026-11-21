"""ReplicationStream.read drains buffered messages before reading the socket.

libpq compacts its input buffer on every consume_input(), so calling it once per
message made draining a large backlog quadratic in the buffered bytes (about 100x
slower on a 50k-message backlog, all CPU). read() now calls it only when no complete
message is buffered. Order, bytes and keepalive replies are unchanged.
"""

import struct
import time
from dataclasses import replace

import pytest
from psycopg2.extensions import make_dsn
from test_feedback import KEEPALIVE_TIMEOUT, confirmed
from test_phase1 import pg_phase1  # noqa: F401

from cdc_flight.preflight import check, connection_options, query
from cdc_flight.replication import HEADER, ReplicationStream


def xlog(n):
    payload = b"I" + n.to_bytes(4, "big") + bytes(n % 251 for _ in range(n % 97))
    return b"w" + struct.pack("!QQQ", n, n + 1, 0) + payload


def keepalive(reply):
    return b"k" + struct.pack("!QQB", 0, 0, reply)


class FakeConn:
    """libpq stand-in: `buffered` complete messages, and `socket` messages that only
    consume_input() moves into the buffer. Counts consume_input() calls."""

    def __init__(self, buffered=(), socket=()):
        self.buffered, self.socket = list(buffered), list(socket)
        self.consumed = self.sent = 0
        self.socket_fd = None

    def consume_input(self):
        self.consumed += 1
        self.buffered.extend(self.socket)
        self.socket.clear()

    def get_copy_data(self, asynchronous):
        assert asynchronous == 1
        if not self.buffered:
            return 0, None
        message = self.buffered.pop(0)
        return len(message), memoryview(message)

    def put_copy_data(self, message):
        self.sent += 1
        return 1

    def flush(self):
        return 0


def stream_over(conn):
    stream = ReplicationStream.__new__(ReplicationStream)
    stream.conn, stream.fence, stream.acked, stream.fenced, stream.streaming = (
        conn,
        None,
        0,
        None,
        True,
    )
    return stream


@pytest.mark.parametrize("count", [10, 1000, 10_000])
def test_draining_a_buffered_backlog_reads_the_socket_a_constant_number_of_times(count):
    messages = [xlog(n) for n in range(count)]
    conn = FakeConn(buffered=messages)
    stream = stream_over(conn)
    payloads = [stream.read() for _ in range(count)]
    # Every buffered message came back, in order, byte for byte, without reading the
    # socket once. Only the read that finds the buffer empty reads it.
    assert [p.payload for p in payloads] == [m[HEADER:] for m in messages]
    assert [p.data_start for p in payloads] == list(range(count))
    assert conn.consumed == 0
    assert stream.read() is None
    assert conn.consumed == 1


def test_socket_data_is_read_once_the_buffer_is_empty():
    conn = FakeConn(buffered=[xlog(1)], socket=[xlog(2), xlog(3)])
    stream = stream_over(conn)
    assert [stream.read().data_start for _ in range(3)] == [1, 2, 3]
    assert conn.consumed == 1


def test_a_keepalive_behind_a_buffered_backlog_is_answered_in_order():
    """A reply-requested keepalive is answered when the reader reaches it: once, and
    before the message that follows it is returned."""
    conn = FakeConn(
        buffered=[xlog(n) for n in range(500)] + [keepalive(1), keepalive(0), xlog(500)]
    )
    stream = stream_over(conn)
    for _ in range(500):
        stream.read()
    assert conn.sent == 0
    assert stream.read().data_start == 500
    assert conn.sent == 1  # the reply-requested one only
    assert conn.consumed == 0


@pytest.mark.postgres
def test_keepalives_are_answered_while_a_large_backlog_drains(stores, pg_phase1):  # noqa: F811
    """A large published backlog is read at full speed with no status other than
    keepalive replies, across several wal_sender_timeout/2 windows: the server keeps
    the stream, every row arrives in order, and the slot is never moved."""
    source, original = pg_phase1
    config = replace(original, dsn=make_dsn(original.dsn, options=KEEPALIVE_TIMEOUT))
    check(config, create_objects=True)
    start = confirmed(source, config.slot)
    # 1 KB values on the wire (compressed in WAL, so the backlog stays small).
    rows = 20_000
    source.execute(
        "INSERT INTO items SELECT i,repeat(md5(i::text),32),'x' FROM generate_series(1,%s) i",
        [rows],
    )
    stream = ReplicationStream(connection_options(config))
    try:
        stream.start(config.slot, start, config.publication)
        replies, send = [], stream._send_status
        stream._send_status = lambda: (replies.append(1), send())[1]
        inserted, deadline = [], time.monotonic() + 4  # pings at 1.5 s
        while time.monotonic() < deadline:
            message = stream.read(0.1)
            if message and message.payload[:1] == b"I":
                inserted.append(message.data_start)
        stream._send_status = send
        assert len(inserted) == rows and inserted == sorted(inserted)
        assert replies, "no keepalive asked for a reply in the window: the test is vacuous"
        assert stream.streaming
        assert confirmed(source, config.slot) == start
    finally:
        stream.close()
    used = int(query(source.con, "SELECT coalesce(sum(size),0) FROM pg_ls_waldir()")[0][0])
    assert used < 2 * 1024**3


def test_the_end_of_the_stream_is_an_error_not_an_empty_read():
    """get_copy_data returns -1 when the server ends the copy: read() must raise,
    whether the end is already buffered or arrives from the socket."""
    from cdc_flight.replication import ReplicationError

    class Ended(FakeConn):
        def get_copy_data(self, asynchronous):
            if self.buffered and self.buffered[0] is None:
                return -1, None
            return super().get_copy_data(asynchronous)

    for conn in (Ended(buffered=[xlog(1), None]), Ended(socket=[None])):
        stream = stream_over(conn)
        if conn.buffered:
            assert stream.read().data_start == 1
        with pytest.raises(ReplicationError, match="ended the replication stream"):
            stream.read()
        assert not stream.streaming
