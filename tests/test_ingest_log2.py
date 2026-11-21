"""Grouped append is atomic, replay validates physical rows, and reads use unit cuts."""

from dataclasses import replace

import pytest
from test_format2_protocol import binding, config
from test_spool2 import REL, transaction

from cdc_flight.format2 import Chunk, UnitConfig
from cdc_flight.ingest_log import SourceMismatch
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.spool2 import Spool


@pytest.fixture
def log2(stores):
    store, _ = stores
    base = replace(config(), database=store.database.strip('"'))
    cfg = IngestConfig2(
        **base.__dict__, text_binding=binding(), unit_config=UnitConfig(chunk_frames=3)
    )
    log = IngestLog2(store.con, cfg)
    log.initialize()
    log.register(("system", 1), 90, [dict(oid=101, schema="public", name="items", keys=["id"])])
    log.acquire("writer")
    try:
        yield log
    finally:
        log.release()


def build(log, messages=None, *, recovery=None):
    s = Spool(
        log.config,
        {101: ("id",)},
        900,
        "marker",
        binding=log.config.text_binding,
        start_lsn=90 if recovery is None else recovery.confirmed_end,
        unit_config=log.config.unit_config,
        replay=() if recovery is None else recovery.prefix,
        clock=lambda: 0,
    )
    out = [
        item
        for wire in (messages or [REL] + transaction() + transaction(101))
        for item in s.feed(wire)
    ]
    out.extend(s.close())
    return out


def test_grouped_append_and_whole_unit_reader(log2):
    items = build(log2)
    assert log2.append(items, partition_day="20261106") == 102
    reader = log2.reader()
    units = reader.units(1, 90, 102)
    assert len(units) == 1
    assert (units[0].member_count, units[0].event_count, units[0].frame_count) == (2, 2, 4)
    assert reader.frames(units, high=102) == [
        r for c in items if isinstance(c, Chunk) for r in c.table.to_pylist()
    ]
    descriptors = reader.descriptors(units, high=102)
    assert len(descriptors) == 1
    assert next(iter(descriptors.values())).names == ("id", "v")
    assert log2.append(items, partition_day="20261107") == 102
    assert log2.execute("SELECT count(*) FROM {s}frames_v2_20261106").fetchone()[0] == 4
    assert "frames_v2_20261107" not in log2.catalog()


def test_prefix_recovery_recreates_exact_chunk_descriptors(log2):
    items = build(log2)
    first = next(c for c in items if isinstance(c, Chunk))
    assert log2.append([first], partition_day="20261106") == 90
    recovered = log2.recovery()
    assert recovered.open_anchor == recovered.confirmed_end == 90
    assert recovered.prefix == (first,)
    assert build(log2, recovery=recovered) == items
    assert log2.append(build(log2, recovery=recovered), partition_day="20261107") == 102
    assert log2.execute("SELECT sum(n) FROM {s}chunks_v2").fetchone()[0] == 4


def test_missing_ledger_cannot_duplicate_physical_frames(log2):
    items = build(log2)
    log2.append(items, partition_day="20261106")
    log2.execute("DELETE FROM {s}chunks_v2")
    with pytest.raises(ValueError, match="physical frames exist"):
        log2.append(items, partition_day="20261106")
    assert log2.execute("SELECT count(*) FROM {s}frames_v2_20261106").fetchone()[0] == 4


def test_exception_after_commit_invalidates_days_before_retry(log2):
    items = build(log2)

    def fault(stage):
        if stage == "after_commit":
            raise RuntimeError("response lost")

    with pytest.raises(RuntimeError, match="response lost"):
        log2.append(items, fault=fault, partition_day="20261106")
    assert log2._days is None
    assert log2.checkpoint() == 102
    assert log2.append(items, partition_day="20261107") == 102
    assert log2.execute("SELECT count(*) FROM {s}frames_v2_20261106").fetchone()[0] == 4


def test_legacy_catalog_is_refused_before_ddl_or_lease_change(log2):
    from reference.legacy_store import create

    from cdc_flight.runtime import open_log

    cfg = replace(
        log2.config,
        source_id="legacy",
        text_binding=replace(log2.config.text_binding, source_id="legacy"),
    )
    create(log2.con, cfg)
    prefix = f'"{cfg.database}"."legacy".'
    log2.con.execute(f"INSERT INTO {prefix}source (id,token,guard) VALUES (1,77,'original')")
    before = log2.con.execute(f"SELECT * FROM {prefix}source").fetchall()
    old = IngestLog2(log2.con, cfg)
    with pytest.raises(SourceMismatch, match="fresh schema"):
        old.initialize()
    with pytest.raises(SourceMismatch, match="fresh store"):
        open_log(log2.con, cfg)
    assert log2.con.execute(f"SELECT * FROM {prefix}source").fetchall() == before
    assert "protocol_v2" not in old.catalog()


def test_reader_rejects_high_or_after_inside_a_unit(log2):
    log2.append(build(log2), partition_day="20261106")
    with pytest.raises(ValueError, match="straddle"):
        log2.reader().units(1, 90, 101)
    with pytest.raises(ValueError, match="straddle"):
        log2.reader().units(1, 101, 102)


@pytest.mark.parametrize("damage", ["ledger", "joint", "physical"])
def test_retry_checks_every_physical_range_even_when_lookup_is_empty(log2, monkeypatch, damage):
    items = build(log2)
    log2.append(items, partition_day="20261106")
    if damage == "physical":
        log2.execute("DELETE FROM {s}frames_v2_20261106 WHERE frame_ordinal=0")
    else:
        snapshot = log2._snapshot

        def missing(*args):
            row = list(snapshot(*args))
            row[3] = []
            if damage == "joint":
                row[4] = []
            return tuple(row)

        monkeypatch.setattr(log2, "_snapshot", missing)
    with pytest.raises(ValueError, match="physical"):
        log2.append(items, partition_day="20261106")
    assert log2.execute("SELECT count(*) FROM {s}frames_v2_20261106").fetchone()[0] == (
        3 if damage == "physical" else 4
    )


@pytest.mark.parametrize(
    "field,value", [("end_lsn", 102.0), ("member_count", True), ("digest", 123)]
)
def test_malformed_terminal_is_rejected_before_guard(log2, monkeypatch, field, value):
    terminal = build(log2)[-1]

    def forbidden():
        pytest.fail("guard ran before metadata validation")

    monkeypatch.setattr(log2, "guard", forbidden)
    with pytest.raises(ValueError):
        log2.append([replace(terminal, **{field: value})])


@pytest.mark.parametrize(
    "field,value", [("event_count", True), ("count", 2.0), ("first_frame", -1)]
)
def test_malformed_chunk_metadata_is_rejected_before_guard(log2, monkeypatch, field, value):
    chunk = build(log2)[0]

    def forbidden():
        pytest.fail("guard ran before metadata validation")

    monkeypatch.setattr(log2, "guard", forbidden)
    with pytest.raises(ValueError):
        log2.append([replace(chunk, **{field: value})])


def test_terminal_is_immutable_and_short_or_overlapping_prefixes_fail(log2):
    items = build(log2)
    with pytest.raises(ValueError, match="non-contiguous"):
        log2.append(items[1:])
    assert log2.checkpoint() == 90
    log2.append(items, partition_day="20261106")
    with pytest.raises(ValueError, match="terminal identity"):
        log2.append([replace(items[-1], event_count=100)])
    assert log2.checkpoint() == 102


def test_config_changes_only_at_a_complete_boundary_and_revisions_are_immutable(log2):
    items = build(log2)
    log2.append(items[:1], partition_day="20261106")
    new = replace(log2.config.unit_config, revision=2, max_commits=1)
    with pytest.raises(SourceMismatch, match="drain"):
        log2.activate_config(new)
    log2.append(items, partition_day="20261106")
    log2.activate_config(new)
    assert log2.recovery().unit_config == new
    with pytest.raises(SourceMismatch, match="immutable"):
        log2.activate_config(replace(new, max_commits=2))


def test_cross_day_recovery_reuses_every_old_placement(log2):
    items = build(log2)
    log2.append(items[:1], partition_day="20261105")
    log2.append(items[1:2], partition_day="20261106")
    recovered = log2.recovery()
    assert recovered.prefix == tuple(items[:2])
    assert log2.append(items, partition_day="20261107") == 102
    assert log2.execute(
        "SELECT partition_day,sum(n) FROM {s}chunks_v2 GROUP BY 1 ORDER BY 1"
    ).fetchall() == [("20261105", 3), ("20261106", 1)]
    unit = log2.reader().units(1, 90, 102)[0]
    assert unit.days == ("20261105", "20261106")


@pytest.mark.parametrize("partial_commit", [101, None])
def test_no_progress_uses_durable_frames_not_member_or_byte_counts(log2, partial_commit):
    items = build(log2)
    log2.append(items[:1], partition_day="20261106")
    from cdc_flight.ingest_backend import Progress

    progress = Progress(1, 90, partial_commit, items[0].count)
    assert log2.record_progress("capped", progress) is False
    assert log2.record_progress("capped", progress) is True
    log2.append(items[1:2], partition_day="20261106")
    progress = replace(progress, durable_frame_end=items[0].count + items[1].count)
    assert log2.record_progress("capped", progress) is False
    assert log2.record_progress("capped", progress) is True
    log2.append(items[-1:], partition_day="20261106")
    assert log2.record_progress("drained", None) is False


def test_open_anchor_corruption_and_foreign_revision_fail_before_stream(log2):
    log2.append(build(log2)[:1], partition_day="20261106")
    log2.execute("UPDATE {s}source SET open_unit_anchor=89 WHERE id=1")
    with pytest.raises(SourceMismatch, match="anchor"):
        log2.recovery()


def test_partition_cache_bound_drift_fails_before_a_replay_can_skip_a_day(log2):
    items = build(log2)
    log2.append(items, partition_day="20261106")
    log2._days["20261106", 1]["min"] = 91
    with pytest.raises(ValueError, match="bounds"):
        log2.append(items, partition_day="20261106")
    assert log2._days is None


@pytest.mark.parametrize("kind", ["binding", "mapping", "config"])
def test_recovery_mismatch_records_safe_needs_action_under_the_lease(log2, monkeypatch, kind):
    from cdc_flight import format2

    log2.append(build(log2)[:1], partition_day="20261106")
    if kind == "binding":
        old = log2.config.text_binding
        changed = replace(old, extensions=(("plpgsql", "99"),))
        log2.config = replace(log2.config, text_binding=changed)
        reason = "binding_changed"
    elif kind == "mapping":
        monkeypatch.setattr(format2, "CELL_MAPPING_REVISION", 2)
        log2.config = replace(
            log2.config,
            text_binding=replace(log2.config.text_binding, mapping_revision=2),
            unit_config=replace(log2.config.unit_config, mapping_revision=2),
        )
        reason = "mapping_revision_changed"
    else:
        log2.config = replace(
            log2.config, unit_config=replace(log2.config.unit_config, max_commits=1)
        )
        reason = "unit_config_changed"
    with pytest.raises(SourceMismatch, match=reason):
        log2.recovery()
    state = log2.execute(
        "SELECT state,protocol_reason,protocol_anchor,protocol_recorded,protocol_observed FROM {s}source"
    ).fetchone()
    assert state[:3] == ("needs_action:" + reason, reason, 90)
    assert state[3] != state[4]
    assert log2.checkpoint() == 90


def test_spool_anchor_mismatch_is_rejected_before_a_stream_can_start(log2, monkeypatch):
    from cdc_flight.ingest_backend import Backend2

    recovery = log2.recovery()
    monkeypatch.setattr(log2, "recovery", lambda: replace(recovery, confirmed_end=89))
    with pytest.raises(SourceMismatch, match="stream start"):
        Backend2(log2).recover()


def test_two_capped_runs_without_even_one_durable_chunk_are_visible(log2):
    from cdc_flight.ingest_backend import Progress

    progress = Progress(1, 90, 100, 0)
    assert log2.record_progress("capped", progress) is False
    assert log2.record_progress("capped", progress) is True
    assert (
        log2.execute("SELECT state FROM {s}source").fetchone()[0]
        == "needs_action:large_transaction"
    )


def test_recovery_preserves_two_descriptor_versions_in_one_durable_chunk(log2):
    import pgstream

    relation2 = pgstream.relation(
        101, "public", "items", "f", ((1, "id", 25, -1), (0, "v", 25, -1), (0, "added", 25, -1))
    )
    messages = [REL] + transaction() + [relation2] + transaction(101, ("2", "two", "new"))
    items = build(log2, messages)
    assert len(items[0].descriptors) == 2
    log2.append(items[:1], partition_day="20261106")
    recovery = log2.recovery()
    assert recovery.prefix == (items[0],)
    replayed = build(log2, messages, recovery=recovery)
    assert replayed == items
    log2.append(replayed, partition_day="20261106")
    units = log2.reader().units(1, 90, 102)
    assert log2.reader().frames(units, high=102) == [
        r for c in items if isinstance(c, Chunk) for r in c.table.to_pylist()
    ]


def test_adjacent_commit_at_the_previous_unit_end_is_retained(log2):
    from cdc_flight.format2 import UnitConfig

    log2.activate_config(UnitConfig(revision=2, max_commits=1, chunk_frames=3))
    items = build(log2)
    assert log2.append(items, partition_day="20261106") == 102
    reader = log2.reader()
    first = reader.units(1, 90, 102, max_events=1)
    assert len(first) == 1 and first[0].end == 101
    second = reader.units(1, 101, 102)
    events = reader.frames(second, high=102)
    assert events[0]["commit_lsn"] == first[0].end == second[0].start
    assert events[0]["new"]["values"] == ["1", "text"]


@pytest.mark.parametrize("damage", ["text_null", "key_projection", "schema_metadata"])
def test_malformed_arrow_cells_and_projections_fail_before_guard(log2, monkeypatch, damage):
    import pyarrow as pa

    from cdc_flight.format2 import FRAME_SCHEMA

    chunk = build(log2)[0]
    rows = chunk.table.to_pylist()
    if damage == "schema_metadata":
        table = chunk.table.replace_schema_metadata({b"unexpected": b"metadata"})
    else:
        if damage == "text_null":
            rows[0]["new"]["values"][0] = None
        else:
            rows[0]["new_key"]["values"][0] = "wrong"
        table = pa.Table.from_pylist(rows, schema=FRAME_SCHEMA)

    def forbidden():
        pytest.fail("guard ran before structured-cell validation")

    monkeypatch.setattr(log2, "guard", forbidden)
    with pytest.raises(ValueError):
        log2.append([replace(chunk, table=table)])


def test_missing_chunk_and_unit_metadata_cannot_duplicate_a_durable_prefix(log2):
    items = build(log2)
    log2.append(items[:1], partition_day="20261106")
    log2.execute("DELETE FROM {s}chunks_v2")
    log2.execute("DELETE FROM {s}units_v2")
    with pytest.raises(ValueError, match="physical frames exist"):
        log2.append(items, partition_day="20261106")
    assert (
        log2.execute("SELECT count(*) FROM {s}frames_v2_20261106").fetchone()[0] == items[0].count
    )
    assert log2.checkpoint() == 90


def test_stale_token_and_changed_active_revision_never_write(log2):
    from cdc_flight.ingest_log import FenceLost

    items = build(log2)
    log2.recovery()
    log2.execute("UPDATE {s}protocol_v2 SET active_revision=2")
    with pytest.raises(FenceLost):
        log2.append(items, partition_day="20261106")
    log2.execute("UPDATE {s}protocol_v2 SET active_revision=1")
    log2.recovery()
    log2.execute("UPDATE {s}source SET token=token+1,guard=uuid()::VARCHAR")
    with pytest.raises(FenceLost):
        log2.append(items, partition_day="20261106")
    assert log2.checkpoint() == 90
    assert not any(t.startswith("frames_v2_") for t in log2.catalog())


def test_sql_unit_upsert_itself_cannot_overwrite_a_terminal(log2):
    from test_unit_plans import Recording

    from cdc_flight.ingest_log2 import NULLABLE_UNIT
    from cdc_flight.unit_store import UNIT_TYPES, metadata_parameter

    recording = Recording(log2.con)
    log2.con = recording
    log2.append(build(log2), partition_day="20261106")
    sql = next(s for s in recording.statements if s.startswith("INSERT INTO") and "units_v2" in s)
    original = log2.execute("SELECT * FROM {s}units_v2").fetchone()
    incomplete = dict(zip(UNIT_TYPES, original, strict=True))
    incomplete.update(complete=False, end_lsn=None, digest=None, frame_count=0)
    log2.con.execute(sql, [metadata_parameter([incomplete], UNIT_TYPES, NULLABLE_UNIT)])
    assert log2.execute("SELECT * FROM {s}units_v2").fetchone() == original


@pytest.mark.parametrize(
    "setting",
    [
        "TimeZone",
        "DateStyle",
        "IntervalStyle",
        "extra_float_digits",
        "bytea_output",
        "lc_monetary",
        "xmlbinary",
        "xmloption",
        "server_encoding",
        "client_encoding",
    ],
)
def test_each_replication_session_guc_is_bound_before_any_frame_write(log2, monkeypatch, setting):
    import cdc_flight.ingest_backend as backend_module
    from cdc_flight.ingest_backend import Backend2

    observed = dict(log2.config.text_binding.settings)
    observed[setting] = "different"
    observed["server_version_num"] = "160000"
    monkeypatch.setattr(
        backend_module, "query", lambda *args: list(log2.config.text_binding.extensions)
    )
    with pytest.raises(SourceMismatch, match="binding_changed"):
        Backend2(log2).verify_session(observed.__getitem__, object())
    assert (
        log2.execute("SELECT state FROM {s}source").fetchone()[0] == "needs_action:binding_changed"
    )
    assert log2.checkpoint() == 90
    assert not any(t.startswith("frames_v2_") for t in log2.catalog())


def grouped_store_round_trip(con, database, lsn, *, check_plan=None, check_frame_plan=None):
    """Existing owned-fixture lane: real grouped append, two days and physical retry counts.

    The owning allowlist test can check the production snapshot plan via its
    check_plan(con, sql, params) callback, before fault injection. No connection,
    database or source-side PostgreSQL resource is created here.
    """

    from test_unit_plans import Recording

    base = replace(config(), database=database)
    cfg = IngestConfig2(
        **base.__dict__,
        text_binding=binding(),
        unit_config=UnitConfig(max_commits=2, chunk_frames=3),
    )
    grouped = IngestLog2(con, cfg)
    grouped.initialize()
    grouped.register(
        ("format2-proof", 1),
        lsn - 10,
        [
            dict(oid=101, schema="public", name="items", keys=["id"]),
            dict(oid=102, schema="public", name="other", keys=["id"]),
        ],
    )
    grouped.acquire("grouped-proof")
    try:
        spool = Spool(
            cfg,
            {101: ("id",), 102: ("id",)},
            900,
            "marker",
            binding=cfg.text_binding,
            start_lsn=lsn - 10,
            unit_config=cfg.unit_config,
            clock=lambda: 0,
        )
        import pgstream

        second_relation = pgstream.relation(
            102, "public", "other", "f", ((1, "id", 25, -1), (0, "v", 25, -1))
        )
        items = [
            item
            for wire in [REL, second_relation]
            + transaction(lsn, ("1", "é"))
            + transaction(lsn + 1, ("2", ""), change=pgstream.insert(102, ("2", "")))
            for item in spool.feed(wire)
        ]
        items.extend(spool.close())
        chunks = [item for item in items if isinstance(item, Chunk)]
        assert len(chunks) == 2
        assert grouped.append(items[:1], partition_day="20261105") == lsn - 10
        assert grouped.append(items[1:], partition_day="20261106") == lsn + 2
        reader = grouped.reader()
        units = reader.units(1, lsn - 10, lsn + 2)
        expected = [row for c in chunks for row in c.table.to_pylist()]
        assert reader.frames(units, high=lsn + 2) == expected
        assert grouped.append(items, partition_day="20261106") == lsn + 2
        assert reader.frames(units, high=lsn + 2) == expected
        assert len(reader.descriptors(units, high=lsn + 2)) == 2
        recording = Recording(grouped.con)
        grouped.con = recording
        reader.con = recording
        filtered = reader.frames(units, 101, high=lsn + 2)
        assert filtered == [r for r in expected if r["record_kind"] == 2 or r["relation_id"] == 101]
        assert len(filtered) == 3  # Exclude the other relation, retain both COMMITs.
        if check_frame_plan is not None:
            frame_sql, frame_params = recording.frame_query
            check_frame_plan(recording, frame_sql, frame_params)
        result = grouped._snapshot(chunks, [lsn - 10])
        assert {r["id"]: r["n"] for r in result[5]} == {c.id: c.count for c in chunks}
        sql, params = recording.snapshot
        evidence = check_plan(grouped.con, sql, params) if check_plan is not None else None
        # Delete only this proof's own ledger, preserving the physical rows. An
        # empty lookup must fail, rather than treat the rows as fresh payload.
        grouped.execute("DELETE FROM {s}chunks_v2 WHERE id=?", [chunks[0].id])
        with pytest.raises(ValueError, match="physical frames exist without a chunk ledger row"):
            grouped.append(items, partition_day="20261106")
        assert grouped.execute("SELECT count(*) FROM {s}frames_v2_20261105").fetchone()[0] == 3
        assert grouped.execute("SELECT count(*) FROM {s}frames_v2_20261106").fetchone()[0] == 1
        assert grouped.checkpoint() == lsn + 2
        return evidence
    finally:
        grouped.release()


def test_grouped_store_round_trip_at_unsigned_lsn(stores):
    store, _ = stores
    grouped_store_round_trip(store.con, store.database.strip('"'), 2**63 + 123)


def test_cache_bounds_protect_joint_metadata_loss_even_when_a_day_would_be_omitted(log2):
    items = build(log2)
    log2.append(items[:1], partition_day="20261106")
    log2.execute("DELETE FROM {s}chunks_v2")
    log2.execute("DELETE FROM {s}units_v2")
    log2._days["20261106", 1]["min"] = 91
    with pytest.raises(ValueError, match="bounds"):
        log2.append(items, partition_day="20261106")
    assert log2.execute("SELECT count(*) FROM {s}frames_v2_20261106").fetchone()[0] == 3
    assert log2.checkpoint() == 90


def test_the_full_text_binding_is_persisted_and_readable_without_runtime_settings(log2):
    from cdc_flight.config import ApplyConfig
    from cdc_flight.unit_store import UnitReader

    reader = UnitReader(
        log2.con, ApplyConfig(log2.config.source_id, (), database=log2.config.database)
    )
    assert reader.read_binding() == log2.config.text_binding
    log2.execute("UPDATE {s}protocol_v2 SET binding_json=NULL")
    with pytest.raises(SourceMismatch, match="stored text binding"):
        reader.head()


def test_upload_budget_covers_arrow_payload_when_null_heavy_rows_outgrow_wire():
    import pgstream

    from cdc_flight.ingest_backend import Backend2

    fields = tuple((int(i == 0), f"c{i}", 25, -1) for i in range(32))
    spool = Spool(
        config(),
        {101: ("c0",)},
        900,
        "marker",
        binding=binding(),
        start_lsn=90,
        unit_config=UnitConfig(max_commits=1),
        clock=lambda: 0,
    )
    messages = [pgstream.relation(101, "public", "items", "f", fields), pgstream.begin(100, 0, 7)]
    messages.extend(pgstream.insert(101, (str(i),) + (None,) * 31) for i in range(1000))
    messages.append(pgstream.commit(100, 101, 1))
    items = [item for wire in messages for item in spool.feed(wire)]
    chunk = next(item for item in items if isinstance(item, Chunk))
    descriptors = sum(
        len(d.json.encode()) + sum(len(k.encode()) for k in d.keys) for d in chunk.descriptors
    )
    assert chunk.table.nbytes > chunk.bytes + descriptors
    assert Backend2(None).budget(chunk)[0] >= chunk.table.nbytes + descriptors


def test_duplicate_middle_ordinal_cannot_hide_a_missing_middle_frame(log2):
    log2.append(build(log2), partition_day="20261106")
    reader = log2.reader()
    units = reader.units(1, 90, 102)
    log2.execute("DELETE FROM {s}frames_v2_20261106 WHERE frame_ordinal=2")
    log2.execute(
        "INSERT INTO {s}frames_v2_20261106 SELECT * FROM {s}frames_v2_20261106 WHERE frame_ordinal=1"
    )
    # n=4, min=0 and max=3 are unchanged. Only DISTINCT detects [0,1,1,3].
    with pytest.raises(ValueError, match="gap or duplicate"):
        reader.frames(units, high=102)


def test_unit_pages_preserve_contiguous_cuts_and_filtered_provenance(log2):
    log2.activate_config(UnitConfig(revision=2, max_commits=1, chunk_frames=3))
    items = build(
        log2,
        [REL]
        + transaction(100, ("1", "one"))
        + transaction(101, ("2", "two"))
        + transaction(102, ("3", "three")),
    )
    log2.append(items, partition_day="20261106")
    reader, boundary, cuts = log2.reader(), 90, []
    while boundary < 103:
        page = reader.units(1, boundary, 103, max_units=1)
        assert len(page) == 1 and page[0].start == boundary
        frames = reader.frames(page, 101, high=103)
        assert len(frames) == 2 and frames[-1]["record_kind"] == 2
        assert all(r["unit_start_lsn"] == page[0].start for r in frames)
        events = reader.events(page, 101, high=103)
        assert len(events) == 1 and events[0]["commit_lsn"] == frames[-1]["commit_lsn"]
        descriptors = reader.descriptors(page, high=103)
        event = events[0]
        assert descriptors[(101, event["schema_version"], event["key_layout_hash"])].names == (
            "id",
            "v",
        )
        cuts.append((page[0].start, page[0].end))
        boundary = page[0].end
    assert cuts == [(90, 101), (101, 102), (102, 103)]
    log2.execute("DELETE FROM {s}units_v2 WHERE unit_start_lsn=101")
    assert reader.units(1, 90, 103, max_units=1)[0].end == 101
    with pytest.raises(ValueError, match="gap"):
        reader.units(1, 101, 103, max_units=1)
