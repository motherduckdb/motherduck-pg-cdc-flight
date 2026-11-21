"""The v2 expiry seam holds the real mirror before releasing its whole-unit pin."""

import pgstream
import pytest
from test_apply2 import OID, OTHER_TABLE, TABLE, Env, ins

from cdc_flight.compact import CompactConfig, run_compaction
from cdc_flight.retention import table_owner


@pytest.mark.parametrize("crash_at", ["before_mirror_hold", "between_expiry_steps"])
def test_expiry_holds_the_mirror_before_pin_release_and_recovers_after_a_crash(
    stores, monkeypatch, crash_at
):
    env = Env(stores)
    env.apply.start_from_log(TABLE)
    env.apply.start_from_log(OTHER_TABLE)
    day = ["20260101"]
    append = env.log.append

    def on_day(items, **kw):
        return append(items, partition_day=day[0])

    monkeypatch.setattr(env.log, "append", on_day)
    env.commit(ins("1", "one"))
    env.commit(ins("2", "two"))
    assert env.apply.apply_table(TABLE, "expiry test").status == "caught_up"
    pinned = env.pin()
    for index, name in enumerate(("20260102", "20260103"), 3):
        day[0] = name
        env.commit(ins(str(index * 2), name))
        env.commit(ins(str(index * 2 + 1), name))
    assert env.apply.apply_table(OTHER_TABLE, "keeper").status == "caught_up"
    retained = env.retention()
    retained.execute(
        "UPDATE {s}retention_pins SET updated_at=current_timestamp-INTERVAL '400 hours' "
        "WHERE relation_id=?",
        [OID],
    )
    # Expiry may pass a pending key cut after releasing this table's pin.
    # Freeze the actual earlier verified cut, then persist an open unit under A.
    env.log.execute("UPDATE {s}tables SET key_checked_lsn=? WHERE relation_id=?", [pinned, OID])
    active = env.lsn
    env.feed([pgstream.begin(active, -5, 7), ins("90", "open"), ins("91", "open")])
    env.log.acquire("pending key")
    try:
        env.log.record_keys({OID: ["v"]}, env.log.checkpoint(), active)
    finally:
        env.log.release()
    endpoint = env.log.execute("SELECT * FROM {s}units_v2 WHERE end_lsn=?", [pinned]).fetchone()
    assert endpoint is not None
    config = CompactConfig(
        env.config.source_id,
        database=env.config.database,
        mirror_database=env.config.mirror_database,
    )
    report = run_compaction(config, env.store.con, retention=retained)
    assert report.would_expire == 1
    assert env.state()["state"] == "live" and env.pin() == pinned

    def crash(stage):
        if stage == crash_at:
            raise RuntimeError("expiry crash")

    from dataclasses import replace

    with pytest.raises(RuntimeError, match="expiry crash"):
        run_compaction(replace(config, expire="on"), env.store.con, retention=retained, fault=crash)
    assert env.state()["state"] == (
        "needs_resync" if crash_at == "between_expiry_steps" else "live"
    )
    assert retained.pin(table_owner(OID)) is not None
    # This is crash recovery, not a repeat of a failed acceptance measurement.
    final = run_compaction(replace(config, expire="on"), env.store.con, retention=retained)
    assert final.expired == 1 and retained.pin(table_owner(OID)) is None
    assert env.state()["state"] == "needs_resync"
    assert env.apply.apply_table(TABLE, "expiry test").status == "needs_resync"
    assert "20260102" in final.days
    assert env.mirror() == {"1": "one", "2": "two"}
    assert (
        env.log.execute("SELECT * FROM {s}units_v2 WHERE end_lsn=?", [pinned]).fetchone()
        == endpoint
    )
    assert not env.log.execute(
        "SELECT count(*) FROM {s}chunks_v2 WHERE unit_start_lsn=?", [endpoint[1]]
    ).fetchone()[0]
    # Closing the newer open unit and guarded finish must not wedge on the old cut.
    tail = list(env.spool.feed(pgstream.commit(active, active + 5, -4)))
    tail.extend(env.spool.close())
    env.log.acquire("activate after expiry")
    try:
        high = env.log.append(tail, partition_day="20260103")
        assert (
            env.log.finish(
                "activate after expiry",
                {OID: ["v"]},
                high,
                high,
                "drained",
                None,
                lambda status: "",
            )
            == "drained"
        )
    finally:
        env.log.release()
    assert env.log.key_columns()[OID] == ("v",)
    assert env.log.execute(
        "SELECT cut_lsn FROM {s}key_changes WHERE relation_id=?", [OID]
    ).fetchall() == [(pinned,)]
    assert env.apply.apply_table(TABLE, "after activation").status == "needs_resync"


def test_stale_unit_planning_is_a_consumer_resync_outcome_not_an_uncaught_gap(stores):
    from test_apply2 import START
    from test_backfill2 import BEnv

    from cdc_flight.backfill import GenerationFailed

    env = BEnv(stores)
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "one"))
    env.commit(ins("2", "two"))
    # A stale consumer may still hold a previously read low when history expires.
    # Simulate that changed floor while leaving metadata so the floor is load-bearing.
    env.log.execute("UPDATE {s}retention SET floor_epoch=1,floor_lsn=?", [START])
    result = env.apply.apply_table(TABLE, "stale floor")
    assert result.status == "needs_resync"
    assert "retention" in env.state()["detail"] or "history" in env.state()["detail"]
    assert env.state()["applied"] == START and env.mirror() == {}
    with pytest.raises(GenerationFailed, match="history was dropped"):
        env.backfill.find_marker(env.gen())
