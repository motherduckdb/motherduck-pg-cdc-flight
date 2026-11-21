"""Replication feedback never exceeds the durable checkpoint (Phase 1 invariant).

psycopg2's replication cursor reported a keepalive's WAL end as flushed whenever its
last explicit flush was at or past the last data message, and replied to keepalives
on its own. Ingest now sends every status update itself (replication.py).
"""

import time
import uuid
from dataclasses import replace

import psycopg2
import psycopg2.extras
import pytest
from psycopg2.extensions import make_dsn
from test_format2_protocol import SETTINGS
from test_phase1 import pg_phase1  # noqa: F401
from test_phase2 import ITEMS, Wire, relation_message

from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import IngestConfig
from cdc_flight.format2 import Binding, UnitConfig
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log2 import IngestConfig2
from cdc_flight.preflight import check, connect, connection_options, query
from cdc_flight.replication import ReplicationStream
from cdc_flight.spool2 import Spool


def spool(config, keys, heartbeat_oid, start_lsn):
    """The format-2 spool ingest runs, from a recorded IngestConfig2."""
    return Spool(
        config,
        keys,
        heartbeat_oid,
        "m",
        binding=config.text_binding,
        start_lsn=start_lsn,
        unit_config=config.unit_config,
        clock=lambda: 0,
    )


# Pings at 500 ms (timeout/2) and leaves 500 ms to answer: 600 ms left only 300 ms,
# which a loaded -n 4 run sometimes missed, so the server dropped the stream.
FAST_TIMEOUT = "-c wal_sender_timeout=1000"
# For tests that only need some keepalives in a read window: pings at 1.5 s, and 1.5 s
# to answer. 1000 ms dropped the stream once in 20 loaded -n 4 runs.
KEEPALIVE_TIMEOUT = "-c wal_sender_timeout=3000"
# Shared-PG WAL budget (streams README): per-test WAL well under 100 MB, and stop if
# the server's WAL directory is above 2 GB.
WAL_GUARD_BYTES = 2 * 1024**3
BURST_BUDGET_BYTES = 60 * 1024**2


def confirmed(source, slot):
    return int(
        query(
            source.con,
            "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
            [slot],
        )[0][0]
    )


def wal_lsn(source):
    return int(query(source.con, "SELECT pg_current_wal_lsn()-'0/0'::pg_lsn")[0][0])


def unpublished_burst(source, rows=600_000):
    """WAL the walsender must decode and skip before any published change. On the old
    psycopg2 path (replies at most every 1 s) skipping takes longer than
    wal_sender_timeout/2, so it pinged with an advanced WAL end before the first data
    message: the regression this burst exists for. On the current path the run's
    0.25 s statuses pre-empt those pings. Measured on PG16: 600k integer rows are about
    39 MB of WAL and 0.6-1.1 s of decoding; the advance happened 3/3 at a 1 s timeout."""
    used = int(query(source.con, "SELECT coalesce(sum(size),0) FROM pg_ls_waldir()")[0][0])
    if used > WAL_GUARD_BYTES:
        pytest.skip("shared PostgreSQL WAL is above the 2 GB guard")
    before = wal_lsn(source)
    source.execute("CREATE TABLE IF NOT EXISTS junk (id int)")
    source.execute("INSERT INTO junk SELECT generate_series(1, %s)", [rows])
    assert wal_lsn(source) - before < BURST_BUDGET_BYTES


@pytest.mark.postgres
def test_crashed_run_leaves_slot_at_the_durable_checkpoint(stores, pg_phase1):  # noqa: F811
    source, original = pg_phase1
    # A 1 s timeout makes the old psycopg2 path ping (it replies at most every 1 s)
    # while the walsender skips unpublished WAL. Our status interval stays well below
    # half the timeout: during an upload only those statuses keep the stream alive, and
    # 1 s against a 1 s timeout left no margin on a slow CI runner. (psycopg2 rounds
    # its interval up to 1 s, so the old path's run is unchanged.)
    config = replace(
        original, dsn=make_dsn(original.dsn, options=FAST_TIMEOUT), status_seconds=0.25
    )
    log, info = bootstrap_log(stores[0].con, config, create_objects=True)
    config = log.config  # the recorded IngestConfig2, with this run's DSN and interval
    assert run_once(config, log, info=info).status == "drained"
    before = log.checkpoint()
    # Unpublished WAL, then a published row. On the old psycopg2 path the walsender
    # pinged with an advanced WAL end while skipping the burst, and psycopg2 reported
    # that as flushed; on the current path our 0.25 s statuses pre-empt the pings. The
    # run then dies in its first upload.
    unpublished_burst(source)
    source.execute("INSERT INTO items VALUES (1,'a','b')")

    def crash(stage):
        if stage == "after_payload":
            raise RuntimeError("killed during upload")

    with pytest.raises(RuntimeError, match="killed during upload"):
        run_once(config, log, info=info, fault=crash)
    assert log.checkpoint() == before
    assert confirmed(source, config.slot) <= before
    # The next run is not refused as "slot ahead of the durable checkpoint". It uses
    # the normal timeout, so a slow upload can't trip the short one.
    normal = replace(config, dsn=original.dsn, status_seconds=original.status_seconds)
    result = run_once(normal, log, info=info)
    assert result.status == "drained"
    assert confirmed(source, config.slot) == result.boundary


@pytest.mark.postgres
def test_stream_holds_across_keepalives_and_acknowledges_exactly(stores, pg_phase1):  # noqa: F811
    source, original = pg_phase1
    config = replace(original, dsn=make_dsn(original.dsn, options=KEEPALIVE_TIMEOUT))
    check(config, create_objects=True)
    start = confirmed(source, config.slot)
    unpublished_burst(source, rows=50_000)  # about 3 MB
    source.execute("INSERT INTO items VALUES (2,'a','b')")
    fenced = []
    stream = ReplicationStream(connection_options(config), fence=fenced.append)
    try:
        stream.start(config.slot, start, config.publication)
        # The only test that kills a keepalive auto-reply (wal_end) mutant: the
        # crashed-run test's frequent statuses pre-empt pings. Don't loosen this
        # replies guard or the timings above.
        # In this loop only keepalive replies send a status: count them.
        replies, send = [], stream._send_status
        stream._send_status = lambda: (replies.append(1), send())[1]
        # At least two pings at timeout/2 (1.5 s).
        commit_end, deadline = None, time.monotonic() + 4
        while time.monotonic() < deadline:
            message = stream.read(0.1)
            if message and message.payload[:1] == b"C":
                commit_end = int.from_bytes(message.payload[10:18], "big")
        stream._send_status = send
        assert commit_end is not None
        assert replies, "no keepalive asked for a reply in the window: the test is vacuous"
        # Every keepalive was answered with our value only.
        assert confirmed(source, config.slot) == start
        stream.acknowledge(commit_end)
        stream.acknowledge(start)  # never moves backwards
        assert stream.acked == commit_end
    finally:
        stream.close()
    # close() waits for the server to finish the copy, so this is exact, not polled.
    assert confirmed(source, config.slot) == commit_end
    # fence() saw each new value once, never a repeat.
    assert fenced == [start, commit_end]


# pgoutput resends a Relation (and Type) message when its per-walsender cache entry is
# invalidated: PG16 src/backend/replication/pgoutput/pgoutput.c, rel_sync_cache_relation_cb
# (a whole-cache flush on relid InvalidOid, as after a sinval reset) and
# rel_sync_cache_publication_cb (on any pg_namespace or publication map change) set
# replicate_valid = false, and get_rel_sync_entry then sets schema_sent = false. Those are
# live invalidations in each walsender process: another session creating a schema can make
# one stream repeat an identical R that the other stream does not.


def without_repeated_relations(messages):
    """Drop an R/Y message identical to the last R/Y for the same oid on this stream.
    Anything else, including a changed R, still compares exactly."""
    last, kept = {}, []
    for data_start, payload in messages:
        if payload[:1] in (b"R", b"Y"):
            key = payload[:5]
            if last.get(key) == payload:
                continue
            last[key] = payload
        kept.append((data_start, payload))
    return kept


def test_a_repeated_relation_message_changes_nothing_ingest_persists(tmp_path):
    """Replay idempotence: a re-read after a crash may see an R resend that the first
    read did not (see above). Every format-2 item (chunk identity, digests and
    descriptors, unit boundaries) must be identical."""
    w = Wire()
    relation = relation_message(1, "items", ITEMS)
    first = w.txn(relation, *[w.insert(1, (str(i), "v" * 300, "t", "1.00")) for i in range(40)])
    second = w.txn(w.update(1, ("3", "x", "f", "2.00"), ("3", "v" * 300, "t", "1.00")))
    plain = first + second
    # The same stream with a duplicate R mid-transaction and before the second change set.
    resent = first[:20] + [relation] + first[20:] + second[:1] + [relation] + second[1:]
    base = IngestConfig(
        "feedback", "slot", "pub", ("public.items",), chunk_bytes=1024, allow_local=True
    )
    config = IngestConfig2(
        **replace(base, epoch=1).__dict__,
        text_binding=Binding("feedback", SETTINGS, (("plpgsql", "1.0"),)),
        unit_config=UnitConfig(chunk_bytes=1024),
    )

    def chunks(messages, name):
        s = spool(config, {1: ("id",)}, 999, 0)
        # Chunks (identity, digests, descriptors; the Arrow table is compared through
        # the digest) and every other item, as ingest persists them.
        return [item for message in messages for item in s.feed(message)] + list(s.close())

    expected = chunks(plain, "plain")
    assert len(expected) > 3
    assert chunks(resent, "resent") == expected


@pytest.mark.postgres
def test_payloads_are_byte_identical_to_the_psycopg2_path(stores, pg_phase1, tmp_path):  # noqa: F811
    """The spool is unchanged: same (data_start, payload) sequence and chunk identities.
    wal_end is the walsender's send position at send time, so it is not compared."""
    source, original = pg_phase1
    log, info = bootstrap_log(
        stores[0].con, replace(original, chunk_bytes=1024), create_objects=True
    )
    config = log.config
    other = "p1b_" + uuid.uuid4().hex[:8]
    query(source.con, "SELECT pg_create_logical_replication_slot(%s,'pgoutput')", [other])
    try:
        start = max(confirmed(source, config.slot), confirmed(source, other))
        source.execute("ALTER TABLE items REPLICA IDENTITY FULL")
        source.execute("BEGIN")
        source.execute(
            "INSERT INTO items SELECT i, repeat(md5(i::text), 200), NULL "
            "FROM generate_series(10, 40) i"
        )
        source.execute("UPDATE items SET note='n' WHERE id % 3 = 0")
        source.execute("DELETE FROM items WHERE id % 5 = 0")
        source.execute("COMMIT")
        source.execute("UPDATE items SET value=value||'x' WHERE id=11")
        source.execute("TRUNCATE items")
        end = int(query(source.con, "SELECT pg_current_wal_lsn()-'0/0'::pg_lsn")[0][0])

        def old_path(slot):
            con = psycopg2.connect(
                **connection_options(config),
                connection_factory=psycopg2.extras.LogicalReplicationConnection,
            )
            try:
                cur = con.cursor()
                cur.start_replication(
                    slot_name=slot,
                    start_lsn=start,
                    decode=False,
                    options={"proto_version": "1", "publication_names": config.publication},
                )
                got = []
                while not got or got[-1][1][:1] != b"T":
                    message = cur.read_message()
                    if message:
                        got.append((message.data_start, bytes(message.payload)))
                    else:
                        time.sleep(0.01)
                return got
            finally:
                con.close()

        def new_path(slot):
            stream = ReplicationStream(connection_options(config))
            try:
                stream.start(slot, start, config.publication)
                got = []
                while not got or got[-1][1][:1] != b"T":
                    message = stream.read(0.1)
                    if message:
                        got.append((message.data_start, message.payload))
                return got
            finally:
                stream.close()

        old, new = old_path(config.slot), new_path(other)

        def first_difference():
            for i, (o, n) in enumerate(zip(old, new)):
                if o != n:
                    return (i, (o[0], o[1][:60], len(o[1])), (n[0], n[1][:60], len(n[1])))
            return ("lengths", len(old), len(new))

        keys = {t["oid"]: tuple(t["keys"]) for t in info.tables}

        def chunks(messages):
            # A fresh bootstrap registers epoch 1 (run_once supplies it under its lease).
            s = spool(replace(config, epoch=1), keys, info.heartbeat_oid, start)
            items = [i for _, payload in messages for i in s.feed(payload)]
            return [(getattr(i, "id", None), getattr(i, "digest", None)) for i in items]

        # What ingest persists first (chunk ids and digests), then the raw stream. A
        # mismatch reports the first differing message on both sides: one unexplained
        # raw mismatch was seen once under parallel MotherDuck load.
        assert chunks(old) == chunks(new), first_difference()
        assert without_repeated_relations(old) == without_repeated_relations(new), (
            first_difference()
        )
        assert len(old) > 40
        assert max(start for start, _ in new) <= end
    finally:
        query(
            source.con,
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name=%s AND NOT active",
            [other],
        )


@pytest.mark.postgres
def test_the_stream_session_uses_the_recorded_text_formats_over_dsn_options(stores, pg_phase1):  # noqa: F811
    """Format 2 records the text-output settings at bootstrap and appends them after
    the DSN's options on every session, the replication stream included: a DSN asking
    for another DateStyle can't change what is logged (format 1 refused the run)."""
    source, original = pg_phase1
    log, info = bootstrap_log(stores[0].con, original, create_objects=True)
    original = log.config
    source.execute("INSERT INTO items VALUES (3,'a','b')")
    config = replace(original, dsn=make_dsn(original.dsn, options="-c DateStyle=SQL,DMY"))
    con = connect(config, replication=True)
    try:
        shown = query(con, "SHOW DateStyle")[0][0]
    finally:
        con.close()
    assert shown == dict(original.text_binding.settings)["DateStyle"]
    result = run_once(config, log, info=info)
    assert result.status == "drained" and log.checkpoint() > info.initial_lsn
    assert confirmed(source, original.slot) == result.boundary


def test_normalizing_drops_only_identical_relation_repeats():
    r1, r1_changed, r2 = b"R\x00\x00\x00\x01a", b"R\x00\x00\x00\x01b", b"R\x00\x00\x00\x02a"
    data = b"I\x00\x00\x00\x01x"
    stream = [(1, r1), (2, data), (3, r1), (4, r2), (5, r1_changed), (6, data)]
    assert without_repeated_relations(stream) == [
        (1, r1),
        (2, data),
        (4, r2),
        (5, r1_changed),
        (6, data),
    ]


def test_close_never_runs_the_fence_after_it_failed():
    """A failed fence leaves the value unacknowledged: close() sends no final status
    (which would fence again), never raises, and still finishes the connection."""
    from cdc_flight.ingest_log import FenceLost
    from cdc_flight.replication import ReplicationStream

    class Conn:
        finished = False
        sent = 0

        def finish(self):
            self.finished = True

        def put_copy_data(self, message):
            self.sent += 1
            return 1

        def flush(self):
            return 0

        def put_copy_end(self):
            return 1

        def consume_input(self):
            pass

        def get_copy_data(self, wait):
            return -1, None

        def is_busy(self):
            return False

        def get_result(self):
            return None

    calls = []

    def fence(lsn):
        calls.append(lsn)
        raise FenceLost("lease replaced")

    stream = ReplicationStream.__new__(ReplicationStream)
    stream.conn, stream.fence, stream.acked, stream.fenced = Conn(), fence, 100, None
    stream.streaming = True
    with pytest.raises(FenceLost):
        stream._send_status()  # the first status, at start()
    stream.close()
    assert calls == [100] and stream.conn.sent == 0 and stream.conn.finished
    # An already fenced value is repeated on close, without the fence.
    stream = ReplicationStream.__new__(ReplicationStream)
    stream.conn, stream.fence, stream.acked, stream.fenced = Conn(), fence, 100, 100
    stream.streaming = True
    stream.close()
    assert calls == [100] and stream.conn.sent == 1
