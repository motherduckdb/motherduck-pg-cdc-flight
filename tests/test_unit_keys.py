"""Key observations never rewrite an open unit's registered projection."""

import json
from types import SimpleNamespace

import pytest
from test_ingest_log2 import build, log2  # noqa: F401
from test_phase1 import pg_phase1  # noqa: F401

from cdc_flight.format2 import Chunk
from cdc_flight.ingest_log import SourceMismatch
from cdc_flight.spool2 import Spool


def row(log):
    names = "key_columns,catalog_key,key_version,key_checked_lsn,pending_keys_v2,held_reason"
    return dict(
        zip(
            names.split(","),
            log.execute(f"SELECT {names} FROM {{s}}tables WHERE relation_id=101").fetchone(),
        )
    )


def history(log):
    return log.execute(
        "SELECT version,key_columns,cut_lsn,new_from_lsn,epoch FROM {s}key_changes ORDER BY version"
    ).fetchall()


def test_b_observed_in_the_middle_of_an_open_u_keeps_the_prior_cut_and_replay(log2):  # noqa: F811
    items = build(log2)
    log2.record_keys({101: ["id"]}, 90, 95)
    log2.append(items[:1], partition_day="20261106")
    log2.record_keys({101: ["v"]}, 90, 105)
    pending = row(log2)
    assert pending["key_checked_lsn"] == 90
    assert pending["key_version"] == 0 and pending["held_reason"] is None
    assert json.loads(pending["key_columns"]) == ["id"]
    assert json.loads(pending["pending_keys_v2"]) == [
        dict(epoch=1, keys=["v"], cut_lsn=90, observed_lsn=105)
    ]
    assert build(log2, recovery=log2.recovery()) == items
    log2.append(items[1:], partition_day="20261106")
    log2.record_keys({101: ["v"]}, 102, 110)
    active = row(log2)
    assert active["pending_keys_v2"] is None and active["key_checked_lsn"] == 90
    assert log2.key_columns()[101] == ("v",)
    assert history(log2) == [(1, '["v"]', 90, 105, 1)]
    # Every chunk emitted by the old run retains A. Only a NEW spool uses B.
    assert all(d.keys == ("id",) for c in items if isinstance(c, Chunk) for d in c.descriptors)
    s = Spool(
        log2.config,
        log2.key_columns(),
        900,
        "marker",
        binding=log2.config.text_binding,
        start_lsn=102,
        unit_config=log2.config.unit_config,
        clock=lambda: 0,
    )
    from test_spool2 import REL, collect, transaction

    output = collect(s, [REL] + transaction(110))
    assert all(d.keys == ("v",) for c in output if isinstance(c, Chunk) for d in c.descriptors)


def test_a_b_a_observations_are_two_versions_and_activation_retry_is_idempotent(log2):  # noqa: F811
    items = build(log2)
    log2.record_keys({101: ["id"]}, 90, 95)
    log2.append(items[:1], partition_day="20261106")
    for keys, observed in [(["v"], 105), (["v"], 106), (["id"], 107)]:
        log2.record_keys({101: keys}, 90, observed)
    assert len(json.loads(row(log2)["pending_keys_v2"])) == 2
    assert row(log2)["key_checked_lsn"] == 90
    log2.append(items[1:], partition_day="20261106")
    log2.record_keys({101: ["id"]}, 102, 110)
    assert history(log2) == [(1, '["v"]', 90, 105, 1), (2, '["id"]', 90, 107, 1)]
    log2.record_keys({101: ["id"]}, 102, 111)
    assert len(history(log2)) == 2


@pytest.mark.parametrize(
    "live", [dict(active=True, frame_count=0), dict(active=False, frame_count=2)]
)
def test_even_an_unpersisted_live_tail_defers_activation(log2, live):  # noqa: F811
    log2._spool = SimpleNamespace(**live)
    log2.record_keys({101: ["v"]}, 90, 110)
    assert row(log2)["key_version"] == 0 and row(log2)["pending_keys_v2"] is not None
    log2._spool = None
    log2.record_keys({101: ["v"]}, 90, 111)
    assert row(log2)["key_version"] == 1 and log2.key_columns()[101] == ("v",)


def test_validation_restoration_never_clears_through_cut_zero_or_an_open_prefix(log2):  # noqa: F811
    log2.record_keys({101: ["id"]}, 90, 95)
    items = build(log2)
    log2.append(items[:1], partition_day="20261106")
    log2.record_tables({101: ("identity_not_full", "invalid")})
    log2.record_keys({101: ["id"]}, 90, 100)
    log2.record_tables(present=[101])
    log2.record_keys({101: ["id"]}, 90, 105)
    assert row(log2)["held_reason"] == "identity_not_full" and not history(log2)
    log2.append(items[1:], partition_day="20261106")
    log2.record_keys({101: ["id"]}, 102, 110)
    assert row(log2)["held_reason"] is None
    assert history(log2) == [(1, '["id"]', 90, 105, 1)]


def test_pending_history_is_finalized_in_the_old_epoch_before_the_reset(log2):  # noqa: F811
    log2.record_keys({101: ["id"]}, 90, 95)
    log2.append(build(log2)[:1], partition_day="20261106")
    log2.record_keys({101: ["v"]}, 90, 105)
    log2.begin_epoch(2, 200, "skip", log2.slot_name)
    assert history(log2) == [(1, '["v"]', 90, 105, 1), (2, '["v"]', 200, 200, 2)]
    assert row(log2)["pending_keys_v2"] is None and log2.key_columns()[101] == ("v",)


def test_a_key_cut_inside_a_unit_is_rejected_at_write_time(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    with pytest.raises(SourceMismatch, match="complete unit end"):
        log2.record_keys({101: ["id"]}, 101, 110)
    log2.execute("UPDATE {s}tables SET key_checked_lsn=101,key_epoch=1")
    with pytest.raises(SourceMismatch, match="complete unit end"):
        log2.record_keys({101: ["v"]}, 102, 110)
    assert not history(log2)


def test_activation_rolls_back_with_its_guard_and_invalidates_the_projection_cache(
    log2,  # noqa: F811
    monkeypatch,
):
    assert log2.key_columns()[101] == ("id",)
    execute = log2.execute

    def fault(sql, args=None):
        if "UPDATE {s}tables SET catalog_key=?" in sql:
            raise RuntimeError("activation fault")
        return execute(sql, args)

    monkeypatch.setattr(log2, "execute", fault)
    with pytest.raises(RuntimeError, match="activation fault"):
        log2.record_keys({101: ["v"]}, 90, 110)
    monkeypatch.setattr(log2, "execute", execute)
    assert not history(log2) and row(log2)["pending_keys_v2"] is None
    assert log2.key_columns()[101] == ("id",)
    log2.record_keys({101: ["v"]}, 90, 110)
    assert log2.key_columns()[101] == ("v",)


def test_malformed_keys_fail_before_the_guard(log2, monkeypatch):  # noqa: F811
    monkeypatch.setattr(log2, "guard", lambda: pytest.fail("guard called"))
    with pytest.raises(ValueError, match="unique"):
        log2.record_keys({101: ["id", "id"]}, 90, 110)


@pytest.mark.parametrize("action", ["add", "remove"])
@pytest.mark.parametrize("dry_run", [True, False])
def test_partial_membership_refuses_before_apply_fence_or_preview_spend(
    log2,  # noqa: F811
    action,
    dry_run,
    monkeypatch,
):
    from cdc_flight import membership

    log2.append(build(log2)[:1], partition_day="20261106")
    log2.release()

    def forbidden(*args, **kwargs):
        pytest.fail("a partial membership action reached an external read/fence/spend")

    monkeypatch.setattr(membership, "resolve", forbidden)
    apply = SimpleNamespace(active_generation=forbidden, state=forbidden)
    options = dict(dry_run=dry_run, check=forbidden, consume=forbidden)
    if action == "remove":
        result = membership.remove_table(log2, apply, "public.items", force=True, **options)
    else:
        result = membership.add_table(log2, apply, None, "pub", "public.items", **options)
    assert result.status == "drain_required"
    assert result.observed["unit_position"]["open_unit_anchor"] == 90
    assert log2.token is None and row(log2)["held_reason"] is None


def test_adopting_an_unmanaged_table_updates_the_registered_projection(log2):  # noqa: F811
    log2.execute("UPDATE {s}tables SET held_reason='unmanaged',key_checked_lsn=90,key_epoch=1")
    with log2.transaction():
        log2.guard()
        log2.activate_member(101, "public", "items", '["v"]', "unmanaged")
    log2._clear_registry()
    assert log2.key_columns()[101] == ("v",)
    assert history(log2) == [(1, '["v"]', 90, 90, 1)]
    assert row(log2)["held_reason"] is None


@pytest.mark.postgres
def test_new_identity_k_updates_and_deletes_keep_capturing_with_an_old_projection(
    stores,
    pg_phase1,  # noqa: F811
):
    """Real wire K tuples under B are captured under A, never applied under A.

    SQL peek is non-acknowledging: feed the first segment then the later transactions
    from a fresh peek, retaining session Relation messages. No slot is advanced.
    The consumer/resync half is covered by Apply2/Backfill2's integration tests.
    """
    from dataclasses import replace

    from psycopg2 import sql
    from test_format2_protocol import binding, config

    from cdc_flight.format2 import OLD_KEY, UnitConfig
    from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
    from cdc_flight.pgoutput import decode
    from cdc_flight.preflight import catalog_key, query

    source, _ = pg_phase1
    source.execute("CREATE TABLE others (id int PRIMARY KEY)")
    source.execute("ALTER TABLE others REPLICA IDENTITY FULL")
    source.execute(
        sql.SQL("ALTER PUBLICATION {} ADD TABLE others").format(sql.Identifier(source.publication))
    )
    oid, other = [
        int(query(source.con, "SELECT %s::regclass::oid", [source.schema + "." + table])[0][0])
        for table in ("items", "others")
    ]
    initial = int(
        query(
            source.con,
            "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
            [source.slot],
        )[0][0]
    )
    base = replace(config(), database=stores[0].database.strip('"'))
    cfg = IngestConfig2(
        **base.__dict__, text_binding=binding(), unit_config=UnitConfig(chunk_frames=1)
    )
    log = IngestLog2(stores[0].con, cfg)
    log.initialize()
    log.register(
        ("system", 1),
        initial,
        [
            dict(oid=o, schema=source.schema, name=n, keys=["id"])
            for o, n in ((oid, "items"), (other, "others"))
        ],
    )
    log.acquire("key change")
    try:
        log.record_keys({oid: ["id"], other: ["id"]}, initial, initial)
        spool = Spool(
            cfg,
            log.key_columns(),
            900,
            "marker",
            binding=cfg.text_binding,
            start_lsn=initial,
            unit_config=cfg.unit_config,
            clock=lambda: 0,
        )
        log._spool = spool
        source.execute("INSERT INTO items VALUES (1,'10','old')")
        source.execute("INSERT INTO others VALUES (1)")
        first = source.peek()
        last = max(decode(w)["lsn"] for w in first if decode(w)["kind"] == "commit")
        chunks = [item for wire in first for item in spool.feed(wire)]
        log.append(chunks, partition_day="20261106")
        assert log.unit_position()["partial"]
        source.execute("ALTER TABLE items DROP CONSTRAINT items_pkey")
        source.execute("ALTER TABLE items ALTER COLUMN value SET NOT NULL")
        source.execute("ALTER TABLE items ADD PRIMARY KEY (value)")
        source.execute("ALTER TABLE items REPLICA IDENTITY DEFAULT")
        observed = {oid: catalog_key(source.con, oid), other: ["id"]}
        log.record_keys(observed, initial, last)
        assert log.key_columns()[oid] == ("id",)
        source.execute("UPDATE items SET value='11' WHERE value='10'")
        source.execute("DELETE FROM items WHERE value='11'")
        source.execute("INSERT INTO others VALUES (2)")
        new = False
        later = []
        for wire in source.peek():
            message = decode(wire)
            if message["kind"] == "begin":
                new = message["final_lsn"] > last
            if new or message["kind"] == "relation":
                later.extend(spool.feed(wire))
        later.extend(spool.close())
        log.append(later, partition_day="20261106")
        high = log.checkpoint()
        log.record_keys(observed, high, high)
        assert log.key_columns()[oid] == ("value",)
        reader = log.reader()
        units = reader.units(1, initial, high)
        events = reader.events(units, oid, high=high)
        changed = [e for e in events if e["op"] in ("update", "delete")]
        assert len(changed) == 2 and all(e["old_tuple_kind"] == OLD_KEY for e in changed)
        assert all(e["old_key"]["values"] == [None] for e in changed)
        others = reader.events(units, other, high=high)
        assert [e["new"]["values"] for e in others] == [["1"], ["2"]]
        assert (
            log.execute("SELECT cut_lsn FROM {s}key_changes WHERE relation_id=?", [oid]).fetchone()[
                0
            ]
            == initial
        )
    finally:
        log.release()
