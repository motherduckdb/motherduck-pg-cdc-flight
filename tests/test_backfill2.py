"""Format 2 (active since the format-2 cutover): Backfill2's two log reads, H (find_marker) and catch-up,
over complete units. Local stores, the real IngestLog2.append and Spool (test_apply2's
Env). The PostgreSQL generation flow around them is v1's, unchanged."""

from dataclasses import replace

import pgstream
import pytest
from test_apply2 import HEARTBEAT, OID, START, Env, ins

from cdc_flight.backfill import GenerationFailed
from cdc_flight.backfill2 import Backfill2, backfill_for
from cdc_flight.config import BackfillConfig
from cdc_flight.ingest_log import SourceMismatch


def marker(generation, row_id="backfill:101"):
    """Backfill's heartbeat write. Its `backfill:` id makes Spool2 close the unit at
    that COMMIT; another id stands in for a persisted unit with later members."""
    return pgstream.insert(HEARTBEAT, (row_id, generation))


class BEnv(Env):
    def __init__(self, stores, **kwargs):
        super().__init__(stores, **kwargs)
        c = self.config
        self.bconfig = BackfillConfig(
            c.source_id,
            c.tables,
            database=c.database,
            mirror_database=c.mirror_database,
            max_batch_events=c.max_batch_events,
            allow_local=True,
        )
        self.backfill = Backfill2(self.store.con, self.bconfig, retention=self.retention())
        self.backfill.initialize()

    def gen(self, generation="g1", bound=0, low=START):
        return dict(generation_id=generation, epoch=1, pin_lsn=low, wal_before_marker=bound)

    def staging(self, keys=("id",)):
        return dict(target=None, columns=None, keys=keys, schema="public", epoch=1)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_h_is_the_end_of_the_unit_holding_the_marker_with_its_later_members(stores):
    env = BEnv(stores, max_commits=3)
    env.commit(ins("1", "a"))
    c2 = env.commit(marker("g1", row_id="other"))  # no forced close at this COMMIT
    c3 = env.commit(ins("3", "c"))  # a later member of the marker's unit
    env.commit(ins("4", "d"))
    (start, end, _), *_ = env.units()
    assert start == START and end == c3 + 5  # the unit holds commits 1-3
    assert env.backfill.find_marker(env.gen(bound=c2)) == end  # not the marker commit's end


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_backfills_own_marker_closes_its_unit_at_the_marker_commit(stores):
    env = BEnv(stores, max_commits=3)
    env.commit(ins("1", "a"))
    c2 = env.commit(marker("g1"))
    env.commit(ins("3", "c"))
    env.commit(ins("4", "d"))
    env.commit(ins("5", "e"))
    first = env.units()[0]
    assert first[1] == c2 + 5  # Spool2 closed the unit at the marker's COMMIT
    assert env.backfill.find_marker(env.gen(bound=c2)) == first[1]


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_marker_before_the_wal_bound_or_of_another_generation_is_ignored(stores):
    env = BEnv(stores, max_commits=1)
    early = env.commit(marker("g1"))
    env.commit(marker("g0"))
    bound = env.lsn
    assert env.backfill.find_marker(env.gen(bound=bound)) is None  # not yet written
    # The search resumes after the units it already searched (complete units are
    # immutable), and finds the marker once its unit completes.
    assert env.backfill._searched["g1"] == env.units()[-1][1]
    late = env.commit(marker("g1"))
    found = env.backfill.find_marker(env.gen(bound=bound))
    assert found == [u for u in env.units() if u[0] <= late][-1][1]
    assert early < bound <= late


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_catch_up_replays_whole_units_through_the_unchanged_fold(stores):
    env = BEnv(stores, max_commits=2)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.commit(pgstream.update(OID, ("1", "z")))
    env.commit(ins("3", "c"))
    gen = dict(env.gen(), relation_id=OID)
    units = env.units()
    chosen, boundary, columns, images = env.backfill._catch_up_batch(
        gen, env.staging(), START, units[-1][1]
    )
    assert boundary == units[-1][1] and len(chosen) == 4
    assert [c.name for c in columns] == ["id", "v"]
    assert {k[0]: i.values["v"] for k, i in images.items()} == {
        "1": "z",
        "2": "b",
        "3": "c",
    }


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_catch_up_fails_the_generation_on_a_block_a_key_change_or_a_binding_refusal(stores):
    env = BEnv(stores, max_commits=2)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    gen, high = dict(env.gen(), relation_id=OID), env.units()[-1][1]
    with pytest.raises(GenerationFailed, match="key changed"):
        env.backfill._catch_up_batch(gen, env.staging(keys=("v",)), START, high)
    # Staging built from the first unit has v; a later unit's relation drops it.
    _, _, columns, _ = env.backfill._catch_up_batch(gen, env.staging(), START, high)
    dropped = pgstream.relation(OID, "public", "items", "f", ((1, "id", 25, -1),))
    env.commit(dropped, pgstream.insert(OID, ("4",)))
    env.commit(pgstream.insert(OID, ("5",)))
    with pytest.raises(GenerationFailed, match="needs_action"):
        env.backfill._catch_up_batch(
            gen, dict(env.staging(), columns=columns), high, env.units()[-1][1]
        )
    high = env.units()[-1][1]
    env.store.con.execute(f"UPDATE {env.log.prefix}protocol_v2 SET binding_json='{{}}'")
    with pytest.raises(GenerationFailed, match="binding"):
        env.backfill._catch_up_batch(gen, env.staging(), START, high)
    with pytest.raises(GenerationFailed, match="binding"):
        env.backfill.find_marker(env.gen())


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_backfill_for_dispatches_on_the_stored_format(stores):
    env = BEnv(stores)
    got = backfill_for(env.store.con, env.bconfig, retention_v2=env.retention)
    assert type(got) is Backfill2 and got.format_version == 2
    other = replace(env.bconfig, source_id="absent_v1", mirror_database="absent_v1_m")
    calls = []
    with pytest.raises(SourceMismatch, match="unsupported source protocol"):
        backfill_for(env.store.con, other, retention_v2=lambda: calls.append(1))
    assert calls == []
