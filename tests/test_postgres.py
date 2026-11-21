import json
import os

import conftest
import psycopg2
import pytest
from psycopg2 import sql

from cdc_flight.pgoutput import event_dict, normalize
from cdc_flight.source import Source
from cdc_flight.store import LostCommitResponse


@pytest.fixture
def pg():
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CDC_TEST_PG_DSN for disposable real Postgres probes")
    with Source(dsn) as source:
        try:
            yield source
        finally:
            conftest.wait_slot_released(source.con, source.slot)


@pytest.fixture
def unpublished_commit(pg):
    """Another connection writes outside this source's publication."""
    pg.execute("CREATE TABLE unpublished (id integer)")
    connection = psycopg2.connect(os.environ["CDC_TEST_PG_DSN"])
    connection.autocommit = True

    def commit():
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("INSERT INTO {}.unpublished VALUES (1)").format(sql.Identifier(pg.schema))
            )

    try:
        yield commit
    finally:
        connection.close()


def assert_unpublished_suffix(transactions, boundary, version):
    if version < 150000:
        # The deliberate post-marker write must be visible on PG13/14. Never
        # excuse a remaining prefix transaction or a published payload.
        assert transactions
        assert all(end > boundary and not events for end, events in transactions)
    else:
        # PG15+ suppress unpublished empty transactions entirely.
        assert transactions == []


def item_events(transactions):
    return [e for _, events in transactions for e in events if e.relation.name == "items"]


def encoded(rows):
    return {json.dumps(key): json.dumps(value, sort_keys=True) for key, value in rows.items()}


@pytest.mark.postgres
def test_real_pgoutput_and_generation_catchup(pg, stores, unpublished_commit, pg_version):
    s, observer = stores
    ingest, apply = s.acquire("ingest"), s.acquire("apply")
    pg.execute("INSERT INTO items VALUES (1,'old','before'),(2,'delete me','before')")
    unpublished_commit()
    pg.marker()
    initial = pg.transactions()
    low = initial[-1][0]
    for end, events in initial:
        s.append(1, events.commit_lsn, end, [event_dict(e) for e in events], ingest)
    pg.advance(low)
    s.pin("generation", low)
    oid = pg.execute("SELECT 'items'::regclass::oid")[0][0]
    marker_oid = pg.execute("SELECT 'heartbeat'::regclass::oid")[0][0]
    s.begin_generation("fresh", low, apply, oid=oid)
    # Scan first chunk, then mutate both sides of its key boundary.
    scan = {("1",): dict(zip(("id", "value", "note"), ("1", "old", "before")))}
    s.chunk("fresh", 0, encoded(scan), apply)
    pg.execute("UPDATE items SET id=3,value='new' WHERE id=1")
    pg.execute("DELETE FROM items WHERE id=2")
    pg.execute("INSERT INTO items VALUES (4,'late','after')")
    scanned = pg.execute("SELECT id::text,value,note FROM items WHERE id>1 ORDER BY id")
    chunk = {(row[0],): dict(zip(("id", "value", "note"), row)) for row in scanned}
    scan.update(chunk)
    s.chunk("fresh", 1, encoded(chunk), apply)
    # A post-scan write ensures publication requires the marker's catch-up interval.
    pg.execute("UPDATE items SET note=NULL WHERE id=3")
    unpublished_commit()
    marker = pg.marker()
    s.finish_scan("fresh", marker, marker_oid, apply)
    del scan, chunk
    txns = pg.transactions()
    unpublished_commit()
    high = next(
        end
        for end, events in txns
        if any(e.relation.name == "heartbeat" and e.new[0].value == marker for e in events)
    )
    for end, events in txns:
        if end <= high:
            payload = [event_dict(e) for e in events]
            with pytest.raises(LostCommitResponse):
                s.append(1, events.commit_lsn, end, payload, ingest, lose_response=True)
            s.append(1, events.commit_lsn, end, payload, ingest)
    # Replay before feedback is identical and does not duplicate effects.
    replay = pg.transactions()
    if pg_version < 150000:
        # PG13/14 emit empty commits for the other connection's unpublished
        # writes. Keep every transaction through the marker, including empties.
        assert [txn for txn in replay if txn[0] <= high] == [txn for txn in txns if txn[0] <= high]
        assert_unpublished_suffix([txn for txn in replay if txn[0] > high], high, pg_version)
    else:
        assert replay == txns
    expected = {
        (r[0],): dict(zip(("id", "value", "note"), r))
        for r in pg.execute("SELECT id::text,value,note FROM items ORDER BY id")
    }
    observer.publish("fresh", high, apply)
    assert dict(observer.sql("SELECT * FROM {m}mirror").fetchall()) == encoded(expected)
    assert observer.checkpoint("apply") == high
    pg.advance(high)
    unpublished_commit()
    assert_unpublished_suffix(pg.transactions(), high, pg_version)


@pytest.mark.postgres
def test_real_toast_key_change_null_and_schema_descriptor(pg):
    # Incompressible text is stored out-of-line by Postgres.
    toast = "".join(__import__("hashlib").sha256(str(i).encode()).hexdigest() for i in range(200))
    pg.execute("INSERT INTO items VALUES (1,%s,%s)", [toast, "old"])
    pg.execute("UPDATE items SET note='new' WHERE id=1")
    pg.execute("UPDATE items SET id=2 WHERE id=1")
    pg.execute("UPDATE items SET note=NULL WHERE id=2")
    pg.execute("ALTER TABLE items ADD COLUMN extra text")
    pg.execute("UPDATE items SET extra='added' WHERE id=2")
    pg.marker()
    events = item_events(pg.transactions())
    assert any(c.kind == "unchanged" for e in events for c in (e.new or ()))
    assert len({e.relation.version for e in events}) == 2
    rows = normalize({}, events, ("id",))
    assert rows == {("2",): {"id": "2", "value": toast, "note": None, "extra": "added"}}


@pytest.mark.postgres
def test_replication_socket_ack_follows_commit_and_retries_lost_response(
    pg, stores, unpublished_commit, pg_version
):
    s, _ = stores
    token = s.acquire("ingest")
    pg.execute("INSERT INTO items VALUES (1,'socket','test')")
    unpublished_commit()
    marker = pg.marker()
    before = pg.execute(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s", [pg.slot]
    )[0][0]

    def lost_response(end, events):
        s.append(
            1, events.commit_lsn, end, [event_dict(e) for e in events], token, lose_response=True
        )

    with pytest.raises(LostCommitResponse):
        pg.stream(marker, lost_response)
    after = pg.execute(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s", [pg.slot]
    )[0][0]
    assert after == before

    def commit(end, events):
        s.append(1, events.commit_lsn, end, [event_dict(e) for e in events], token)

    end = pg.stream(marker, commit)
    conftest.wait_slot_released(pg.con, pg.slot)
    unpublished_commit()
    assert s.checkpoint("ingest") == end
    assert s.sql("SELECT count(*) FROM {i}events").fetchone() == (2,)
    assert_unpublished_suffix(pg.transactions(), end, pg_version)
