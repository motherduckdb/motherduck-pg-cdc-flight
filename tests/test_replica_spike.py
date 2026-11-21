"""The replica capture spike (docs/read-replica.md), against a
primary plus a physical streaming standby per major (compose profile `replica`).

Opt-in: marked `replica`, and skipped unless CDC_TEST_REPLICA_DSNS holds a JSON map of
major -> {"primary": dsn, "standby": dsn}, e.g. for the compose fixture:

  docker compose --profile replica up -d --wait replica16-primary replica16-standby
  CDC_TEST_REPLICA_DSNS='{"16": {"primary": "host=127.0.0.1 port=55616 ...",
                                 "standby": "host=127.0.0.1 port=55626 ..."}}'

No production code is exercised: this proves PostgreSQL behaviour the design relies on.
"""

import json
import os
import threading
import time
import uuid

import psycopg2
import psycopg2.extras
import pytest

pytestmark = pytest.mark.replica

PAIRS = json.loads(os.getenv("CDC_TEST_REPLICA_DSNS") or "{}")
# How long creation may take once the primary has logged a standby snapshot with nothing
# open: generous (measured ~0 s locally).
PROMPT = 5.0


@pytest.fixture(params=sorted(PAIRS) or ["none"])
def pair(request):
    if request.param == "none":
        pytest.skip("set CDC_TEST_REPLICA_DSNS (a primary + standby per major) to run")
    dsns = PAIRS[request.param]
    primary, standby = connect(dsns["primary"]), connect(dsns["standby"])
    created = dict(slots=[], tables=[], publications=[])
    try:
        yield dict(major=int(request.param), dsns=dsns, primary=primary, standby=standby, **created)
    finally:
        for slot in created["slots"]:
            try:
                released(standby, slot)
            except AssertionError:
                pass  # teardown is best effort; the drop below skips an active slot
            query(standby, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                  "WHERE slot_name=%s AND NOT active", [slot])  # fmt: skip
        for publication in created["publications"]:
            query(primary, f"DROP PUBLICATION IF EXISTS {publication}")
        for table in created["tables"]:
            query(primary, f"DROP TABLE IF EXISTS {table}")
        primary.close()
        standby.close()


def connect(dsn, **options):
    con = psycopg2.connect(dsn, **options)
    con.autocommit = True
    return con


def query(con, sql, params=None):
    with con.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else None


def name(pair, kind):
    value = f"spike_{kind}_{uuid.uuid4().hex[:8]}"
    pair[{"slot": "slots", "t": "tables", "pub": "publications"}[kind]].append(value)
    return value


def released(standby, slot, seconds=10.0):
    """Wait (bounded) until no walsender holds the slot: closing a replication connection
    returns before its walsender has released it."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        rows = query(standby, "SELECT active FROM pg_replication_slots WHERE slot_name=%s", [slot])
        if not rows or not rows[0][0]:
            return
        time.sleep(0.05)
    raise AssertionError(f"slot {slot} is still active after {seconds} s")


def unblock(pair, *transactions):
    """End any primary transaction a failed test left open and log a snapshot, so a
    creation still waiting on the standby finishes and its slot can be dropped."""
    for con in transactions:
        try:
            con.rollback()
            con.close()
        except psycopg2.Error:
            pass
    query(pair["primary"], "SELECT pg_log_standby_snapshot()")


def replayed(pair):
    """Wait until the standby has replayed everything the primary wrote so far."""
    (lsn,) = query(pair["primary"], "SELECT pg_current_wal_lsn()")[0]
    for _ in range(200):
        if query(pair["standby"], "SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", [lsn])[0][0]:
            return
        time.sleep(0.05)
    raise AssertionError("the standby didn't replay the primary's WAL")


class Creation(threading.Thread):
    """pg_create_logical_replication_slot on the standby, in a thread (it may wait for the
    primary): `.seconds` once done, `.error` if it failed."""

    def __init__(self, pair, slot, timeout="60s"):
        super().__init__(daemon=True)
        self.dsn, self.slot, self.timeout = pair["dsns"]["standby"], slot, timeout
        self.seconds = self.error = None

    def run(self):
        con = connect(self.dsn)
        try:
            query(con, f"SET statement_timeout='{self.timeout}'")
            started = time.monotonic()
            query(con, "SELECT pg_create_logical_replication_slot(%s,'pgoutput')", [self.slot])
            self.seconds = time.monotonic() - started
        except psycopg2.Error as exc:
            self.error = exc
        finally:
            con.close()


# Item 3: slot bootstrap on a standby --------------------------------------------------------


def test_pg_log_standby_snapshot_completes_a_standby_slot_creation_promptly(pair):
    """With nothing open on the primary, the DBA's `SELECT pg_log_standby_snapshot()`
    (the design's guidance, PG16+) completes a standby slot's creation at once."""
    creation = Creation(pair, name(pair, "slot"))
    creation.start()
    time.sleep(1.0)
    asked = time.monotonic()
    query(pair["primary"], "SELECT pg_log_standby_snapshot()")
    creation.join(PROMPT + 5)
    assert creation.error is None, creation.error
    assert not creation.is_alive(), "creation didn't complete after pg_log_standby_snapshot()"
    assert time.monotonic() - asked < PROMPT


def test_a_long_primary_transaction_holds_standby_slot_creation_until_it_ends(pair):
    """A transaction open on the primary keeps creation waiting even after
    pg_log_standby_snapshot() (its running-xacts record lists it); it completes once the
    transaction ends and another snapshot is logged. The DBA docs need this."""
    table = name(pair, "t")
    query(pair["primary"], f"CREATE TABLE {table} (id int)")
    long = psycopg2.connect(pair["dsns"]["primary"])
    creation = Creation(pair, name(pair, "slot"))
    try:
        query(long, f"INSERT INTO {table} VALUES (1)")  # open, uncommitted
        creation.start()
        time.sleep(1.0)
        query(pair["primary"], "SELECT pg_log_standby_snapshot()")
        time.sleep(3.0)
        assert creation.is_alive(), "creation finished while a primary transaction was open"
        long.commit()
        query(pair["primary"], "SELECT pg_log_standby_snapshot()")
        creation.join(PROMPT + 5)
        assert creation.error is None and not creation.is_alive()
    finally:
        unblock(pair, long)
        creation.join(PROMPT + 5)


# Item 2: EXPORT_SNAPSHOT on a standby, imported in a second session -------------------------


def stream_inserts(pair, slot, publication):
    """The ids inserted in the slot's pgoutput stream (first column of each Insert),
    peeked from a standby session."""
    rows = query(
        pair["standby"],
        "SELECT data FROM pg_logical_slot_peek_binary_changes(%s,NULL,NULL,'proto_version','1',"
        "'publication_names',%s)",
        [slot, publication],
    )
    ids = []
    for (data,) in rows:
        data = bytes(data)
        if data[:1] == b"I":
            # Insert: 'I', relation oid (4), 'N', column count (2), then 't', length (4), value.
            at = 1 + 4 + 1 + 2
            assert data[at : at + 1] == b"t"
            length = int.from_bytes(data[at + 1 : at + 5], "big")
            ids.append(int(data[at + 5 : at + 5 + length]))
    return ids


def test_an_exported_standby_snapshot_and_its_stream_cover_each_row_exactly_once(pair):
    """CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export') on a standby: a second standby
    session imports the snapshot (REPEATABLE READ); every row is in exactly one of the
    snapshot and the slot's stream, including a primary transaction open before the
    creation and committed while it waited (the boundary)."""
    primary = pair["primary"]
    table, publication, slot = name(pair, "t"), name(pair, "pub"), name(pair, "slot")
    query(primary, f"CREATE TABLE {table} (id int PRIMARY KEY, phase text)")
    query(primary, f"CREATE PUBLICATION {publication} FOR TABLE {table}")
    query(primary, f"INSERT INTO {table} VALUES (1,'before'),(2,'before')")
    replayed(pair)
    boundary = psycopg2.connect(pair["dsns"]["primary"])
    query(boundary, f"INSERT INTO {table} VALUES (3,'boundary')")  # open across creation
    try:
        exported = export(pair, slot, boundary)
    finally:
        unblock(pair, boundary)  # a no-op after the commit; ends it if the test failed
    in_snapshot = imported(pair, exported, table, slot)
    query(primary, f"INSERT INTO {table} VALUES (4,'after')")
    replayed(pair)
    in_stream = set(stream_inserts(pair, slot, publication))
    assert not in_snapshot & in_stream, (in_snapshot, in_stream)
    assert in_snapshot | in_stream == {1, 2, 3, 4}, (in_snapshot, in_stream)
    assert 3 in in_snapshot  # the boundary transaction committed before the consistent point


def export(pair, slot, boundary):
    """Create the exporting slot while `boundary` is open (creation must wait for it), then
    commit it; returns (replication connection, snapshot name)."""
    primary = pair["primary"]
    exporter = psycopg2.connect(
        pair["dsns"]["standby"], connection_factory=psycopg2.extras.LogicalReplicationConnection
    )
    created = {}

    def create():
        with exporter.cursor() as cur:
            cur.execute(f"CREATE_REPLICATION_SLOT {slot} LOGICAL pgoutput (SNAPSHOT 'export')")
            created["row"] = cur.fetchone()

    thread = threading.Thread(target=create, daemon=True)
    try:
        thread.start()
        time.sleep(1.0)
        query(primary, "SELECT pg_log_standby_snapshot()")
        time.sleep(1.5)
        assert thread.is_alive(), "creation finished while the boundary transaction was open"
        boundary.commit()
        query(primary, "SELECT pg_log_standby_snapshot()")
        thread.join(PROMPT + 5)
        assert "row" in created, "the exported slot wasn't created"
    except BaseException:
        exporter.close()  # releases the slot (and a waiting creation) for teardown
        raise
    return exporter, created["row"][2]


def imported(pair, exported, table, slot):
    """B: a second standby session imports the snapshot (SERIALIZABLE isn't allowed on a
    standby); then the exporter closes and the slot is released for the peek."""
    exporter, snapshot = exported
    reader = psycopg2.connect(pair["dsns"]["standby"])
    try:
        with reader.cursor() as cur:
            cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SET TRANSACTION SNAPSHOT %s", [snapshot])
            cur.execute(f"SELECT id FROM {table}")
            seen = {r[0] for r in cur.fetchall()}
        reader.rollback()
        with reader.cursor() as cur, pytest.raises(psycopg2.Error, match="hot standby"):
            cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            cur.execute("SET TRANSACTION SNAPSHOT %s", [snapshot])
        reader.rollback()
    finally:
        reader.close()
        exporter.close()  # the walsender owns the slot until it lets go: wait for that
        released(pair["standby"], slot)
    return seen
