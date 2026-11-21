"""Chunks are in-memory Arrow tables, not Parquet files: the append inserts the fresh
chunks' tables, registered under a name unique to the call. The stored rows, their
types survive the Parquet control path, and a failed append
leaves no registration behind for its retry.
"""

import re
import struct
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_format2_protocol import SETTINGS

from cdc_flight.config import IngestConfig
from cdc_flight.format2 import FRAME_SCHEMA, Binding, UnitConfig
from cdc_flight.ingest_log import IngestLog
from cdc_flight.ingest_log2 import IngestConfig2
from cdc_flight.spool2 import Spool

SRC = Path(__file__).resolve().parent.parent / "src" / "cdc_flight"


def tuple_data(*values):
    """pgoutput TupleData: None is NULL, ... is an unchanged TOAST value."""
    out = struct.pack("!H", len(values))
    for value in values:
        if value is None:
            out += b"n"
        elif value is ...:
            out += b"u"
        else:
            out += b"t" + struct.pack("!I", len(value.encode())) + value.encode()
    return out


RELATION = (
    b"R"
    + struct.pack("!I", 1)
    + b"public\0items\0f"
    + struct.pack("!H", 3)
    + b"\1id\0"
    + struct.pack("!Ii", 23, -1)
    + b"\0value\0"
    + struct.pack("!Ii", 25, -1)
    + b"\0note\0"
    + struct.pack("!Ii", 25, -1)
)


def txn(commit, *messages):
    begin = b"B" + struct.pack("!QqI", commit, 1_700_000_000_000_000 + commit, commit)
    end = b"C\0" + struct.pack("!QQq", commit, commit + 1, 0)
    return [begin, RELATION, *messages, end]


def insert(i, value, note="n"):
    return b"I" + struct.pack("!I", 1) + b"N" + tuple_data(str(i), value, note)


def workload():
    """Inserts (NULLs, non-ASCII, large values), updates with old images and unchanged
    TOAST values, deletes, a truncate, and a transaction spanning several chunks."""
    first = txn(100, *[insert(i, "v" * 300, None if i % 4 else "ünï") for i in range(40)])
    second = txn(
        200,
        b"U"
        + struct.pack("!I", 1)
        + b"O"
        + tuple_data("1", "old", "n")
        + b"N"
        + tuple_data("1", ..., "changed"),
        b"U" + struct.pack("!I", 1) + b"N" + tuple_data("2", "new", None),
        b"D" + struct.pack("!I", 1) + b"O" + tuple_data("3", "gone", "n"),
        b"D" + struct.pack("!I", 1) + b"K" + tuple_data("4", None, None),
    )
    third = txn(300, b"T" + struct.pack("!IBI", 1, 0, 1), insert(99, "after truncate"))
    return [first, second, third]


@pytest.fixture
def config(stores):
    base = IngestConfig(
        "arrow",
        "arrow_slot",
        "arrow_pub",
        ("public.items",),
        database=stores[0].database.strip('"'),
        chunk_bytes=1024,
        batch_bytes=2048,
        max_event_bytes=100_000,
        allow_local=True,
    )
    return IngestConfig2(
        **base.__dict__,
        text_binding=Binding(base.source_id, SETTINGS, (("plpgsql", "1.0"),)),
        unit_config=UnitConfig(max_commits=1, chunk_bytes=1024),
    )


class Recording:
    """The real connection, recording each registered source's name and result types."""

    def __init__(self, con):
        self.con, self.names, self.types = con, [], []

    def register(self, name, table):
        self.con.register(name, table)
        if name == "cdc_v2_empty":
            return
        self.names.append(name)
        self.types.append(self.con.execute(f"DESCRIBE SELECT * FROM {name}").fetchall())

    def __getattr__(self, name):
        return getattr(self.con, name)


class ParquetPath(Recording):
    """The pre-Arrow path: each batch is written to zstd Parquet and read back with
    read_parquet, as Spool.flush and the append's INSERT did."""

    def __init__(self, con, directory):
        super().__init__(con)
        self.directory = directory

    def register(self, name, table):
        if name == "cdc_v2_empty":
            return super().register(name, table)
        path = self.directory / f"{name}.parquet"
        pq.write_table(table, path, compression="zstd")
        self.con.execute(f"CREATE TEMP VIEW {name} AS SELECT * FROM read_parquet('{path}')")
        self.names.append(name)
        self.types.append(self.con.execute(f"DESCRIBE SELECT * FROM {name}").fetchall())

    def unregister(self, name):
        self.con.execute(f"DROP VIEW {name}")


def open_log(con, config):
    log = IngestLog(con, config)
    log.initialize()
    log.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])
    log.acquire("test")
    return log


def batches(config):
    spool = Spool(
        replace(config, epoch=1),
        {1: ("id",)},
        2,
        "marker",
        binding=config.text_binding,
        start_lsn=0,
        unit_config=config.unit_config,
    )
    return [[item for message in t for item in spool.feed(message)] for t in workload()]


def registrations(con):
    return con.execute(
        "SELECT view_name FROM duckdb_views() WHERE view_name LIKE 'cdc_v2_%'"
    ).fetchall()


def test_nothing_in_src_reads_a_chunk_path_as_a_file():
    """chunk.path is a label: no source file reads it, writes Parquet or spools to disk."""
    for file in SRC.glob("*.py"):
        text = file.read_text()
        assert not re.search(r"\b(chunk|item|c)\.path\b", text), file.name
    spool, ingest = (SRC / "spool2.py").read_text(), (SRC / "ingest.py").read_text()
    assert not re.search(r"pyarrow\.parquet|write_table|\.parquet\b", spool)
    assert "TemporaryDirectory" not in ingest
    assert "read_parquet" not in (SRC / "ingest_log2.py").read_text()


def test_the_spool_writes_no_files(config, tmp_path, monkeypatch):
    directory = tmp_path / "cwd"
    directory.mkdir()
    monkeypatch.chdir(directory)  # a relative-path write would land here
    items = [i for b in batches(config) for i in b]
    chunks = [i for i in items if hasattr(i, "table")]
    assert len(chunks) > 3 and not any(directory.iterdir())
    assert all(c.table.schema == FRAME_SCHEMA and c.table.num_rows == c.count for c in chunks)
    assert all(not hasattr(c, "path") for c in chunks)


def test_arrow_rows_and_types_match_the_parquet_path(stores, config, tmp_path):
    """Nested frame cells survive both transports; v2 writer/ledger equality is
    covered separately by test_unit_store and test_ingest_log2."""
    arrow = Recording(stores[0].con)
    parquet = ParquetPath(stores[0].con, tmp_path)
    chunks = [i for batch in batches(config) for i in batch if hasattr(i, "table")]
    assert len(chunks) > 3
    for index, chunk in enumerate(chunks):
        left, right = f"arrow_{index}", f"parquet_{index}"
        arrow.register(left, chunk.table)
        parquet.register(right, chunk.table)
        try:
            assert arrow.types[-1] == parquet.types[-1]
            for name in (left, right):
                back = stores[0].con.execute(f"SELECT * FROM {name}").to_arrow_table()
                assert back.to_pylist() == chunk.table.to_pylist()
        finally:
            arrow.unregister(left)
            parquet.unregister(right)
    assert registrations(stores[0].con) == []


class FailsAfterInsert(Recording):
    """The first changes INSERT runs, then fails: the append's transaction rolls back."""

    def __init__(self, con):
        super().__init__(con)
        self.failed = False

    def execute(self, sql, parameters=None):
        result = self.con.execute(sql, parameters or [])
        if not self.failed and "INSERT INTO" in sql and "cdc_v2_" in sql:
            self.failed = True
            assert self.con.execute(f"SELECT count(*) FROM {self.names[-1]}").fetchone()[0]
            raise RuntimeError("injected after the changes INSERT")
        return result


def test_a_failed_append_leaves_no_registration_for_its_retry(stores, config, tmp_path):
    con = FailsAfterInsert(stores[0].con)
    log = open_log(con, config)
    try:
        first, second, _ = batches(config)
        with pytest.raises(RuntimeError, match="injected"):
            log.append(first + second, partition_day="20261104")
        assert registrations(stores[0].con) == []
        assert log.checkpoint() == 0
        # The retry regroups: the first transaction alone, then both.
        log.append(first, partition_day="20261104")
        log.append(first + second, partition_day="20261104")
    finally:
        log.release()
    assert len(con.names) == len(set(con.names)) == 3  # a fresh name per call
    assert registrations(stores[0].con) == []
    events = sum(i.count for i in first + second if hasattr(i, "table"))
    stored, distinct = (
        stores[0]
        .con.execute(
            f"SELECT count(*), count(DISTINCT (source_epoch, unit_start_lsn, frame_ordinal)) "
            f"FROM {log.prefix}frames_v2_20261104"
        )
        .fetchone()
    )
    assert stored == distinct == events
    assert log.checkpoint() == 201
