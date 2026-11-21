"""Opt-in measured smoke envelope, never a production throughput claim."""

import json
import os
import time
from pathlib import Path

import pytest
from psycopg2.extras import execute_values

from cdc_flight.pgoutput import Decoder, event_dict, normalize
from cdc_flight.source import Source


@pytest.mark.postgres
def test_complete_path_benchmark(stores):
    if os.getenv("CDC_BENCHMARK") != "1":
        pytest.skip("set CDC_BENCHMARK=1 after correctness probes pass")
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.fail("benchmark requires CDC_TEST_PG_DSN")
    s, _ = stores
    ingest, apply = s.acquire("ingest", ttl_seconds=3600), s.acquire("apply", ttl_seconds=3600)
    count = int(os.getenv("CDC_BENCHMARK_ROWS", "1000"))
    if not 1 <= count <= 100000:
        pytest.fail("CDC_BENCHMARK_ROWS must be 1..100000")
    results = []
    epoch = 0
    for width in (1024, 10240):
        epoch += 1
        with Source(dsn) as pg:
            if epoch > 1:
                s.start_epoch(epoch, ingest)
                oid = pg.execute("SELECT 'items'::regclass::oid")[0][0]
                marker_oid = pg.execute("SELECT 'heartbeat'::regclass::oid")[0][0]
                s.begin_generation("benchmark_reset", 0, apply, epoch=epoch, oid=oid)
                marker = pg.marker()
                s.finish_scan("benchmark_reset", marker, marker_oid, apply)
                transactions = pg.transactions()
                for end, events in transactions:
                    # The fixture decoder defaults to epoch 1; bind the new history.
                    from dataclasses import replace

                    events = [replace(e, epoch=epoch) for e in events]
                    s.append(epoch, events[0].commit, end, [event_dict(e) for e in events], ingest)
                high = transactions[-1][0]
                s.publish("benchmark_reset", high, apply)
                pg.advance(high)
            for operation in ("insert", "update"):
                timings = dict(
                    fetch_s=0.0,
                    parse_s=0.0,
                    parquet_append_s=0.0,
                    reload_normalize_s=0.0,
                    merge_s=0.0,
                    total_s=0.0,
                )
                wire_bytes = 0
                for first in range(0, count, 3000):
                    size = min(3000, count - first)
                    if operation == "insert":
                        with pg.con.cursor() as cursor:
                            execute_values(
                                cursor,
                                "INSERT INTO items VALUES %s",
                                [(i, "x" * width, "old") for i in range(first, first + size)],
                                page_size=size,
                            )
                    else:
                        pg.execute(
                            "UPDATE items SET note='new' WHERE id>=%s AND id<%s",
                            [first, first + size],
                        )
                    low = s.checkpoint("ingest")
                    start = time.perf_counter()
                    wire = pg.peek()
                    wire_bytes += sum(map(len, wire))
                    fetched = time.perf_counter()
                    decoder, transactions = Decoder(epoch=epoch), []
                    for message in wire:
                        result = decoder.feed(message)
                        if result:
                            transactions.append(result)
                    parsed = time.perf_counter()
                    for end, events in transactions:
                        s.append(
                            epoch, events.commit_lsn, end, [event_dict(e) for e in events], ingest
                        )
                    appended = time.perf_counter()
                    high = transactions[-1][0]
                    durable = s.read_events(low, high)
                    rows = normalize(
                        {}, [e for e in durable if e.relation.name == "items"], ("id",)
                    )
                    assert len(rows) == size
                    assert all(
                        row["note"] == ("old" if operation == "insert" else "new")
                        for row in rows.values()
                    )
                    normalized = time.perf_counter()
                    s.merge(
                        {
                            json.dumps(key): json.dumps(value, sort_keys=True)
                            for key, value in rows.items()
                        },
                        high,
                        apply,
                    )
                    finished = time.perf_counter()
                    pg.advance(high)
                    for field, duration in dict(
                        fetch_s=fetched - start,
                        parse_s=parsed - fetched,
                        parquet_append_s=appended - parsed,
                        reload_normalize_s=normalized - appended,
                        merge_s=finished - normalized,
                        total_s=finished - start,
                    ).items():
                        timings[field] += duration
                assert s.sql("SELECT count(*) FROM {m}mirror").fetchone() == (count,)
                results.append(
                    dict(
                        row_bytes=width,
                        operation=operation,
                        rows=count,
                        max_transaction_rows=3000,
                        wire_bytes=wire_bytes,
                        **timings,
                        rows_per_s=count / timings["total_s"],
                        stage_rows_per_s={k: count / v for k, v in timings.items()},
                    )
                )
    destination = Path("artifacts")
    destination.mkdir(exist_ok=True)
    backend = (
        "motherduck"
        if os.getenv("CDC_TEST_MD") == "1"
        and s.con.execute(
            "SELECT type FROM duckdb_databases() WHERE database_name=?", [s.database.strip('"')]
        ).fetchone()[0]
        == "motherduck"
        else "local"
    )
    (destination / f"benchmark-{backend}.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps({"backend": backend, "results": results}))
