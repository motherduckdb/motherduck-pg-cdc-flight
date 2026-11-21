"""Format 2 (active since the format-2 cutover): Apply2 over the grouped log's complete units.

Local stores; the log is written through the real IngestLog2.append from a real Spool,
so units, chunks, descriptors and the recorded epoch start are the production ones.
"""

import json
import re
from dataclasses import replace

import duckdb
import pgstream
import pytest
from test_format2_protocol import binding
from test_format2_protocol import config as f2_config

from cdc_flight.apply2 import Apply2, consumer_for
from cdc_flight.apply_run import ApplyRun
from cdc_flight.config import ApplyConfig, quoted
from cdc_flight.format2 import UnitConfig
from cdc_flight.ingest_log import SourceMismatch
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.retention2 import Retention2
from cdc_flight.spool2 import Spool

OID, TABLE, START = 101, "public.items", 90
OTHER, OTHER_TABLE = 102, "public.others"
HEARTBEAT = 900  # public.md_cdc_heartbeat (BackfillConfig's default): id, marker
HB_COLUMNS = ((1, "id", 25, -1), (0, "marker", 25, -1))
COLUMNS = ((1, "id", 25, -1), (0, "v", 25, -1))


class Env:
    def __init__(self, stores, *, max_commits=2, fold="sql", source="f2", **apply_options):
        """`source`: the source id (its own schema), so one store can hold several."""
        store, _ = stores
        self.store = store
        database = store.database.strip('"')
        base = replace(f2_config(), database=database, source_id=source)
        self.log_config = IngestConfig2(
            **base.__dict__,
            text_binding=replace(binding(), source_id=source),
            unit_config=UnitConfig(max_commits=max_commits, chunk_frames=3),
        )
        self.log = IngestLog2(store.con, self.log_config)
        self.log.initialize()
        self.log.register(
            ("system", 1),
            START,
            [
                dict(oid=OID, schema="public", name="items", keys=["id"], empty=True),
                dict(oid=OTHER, schema="public", name="others", keys=["id"], empty=True),
                dict(oid=HEARTBEAT, schema="public", name="md_cdc_heartbeat", keys=["id"]),
            ],
        )
        self.spool = Spool(
            self.log_config,
            {OID: ("id",), OTHER: ("id",), HEARTBEAT: ("id",)},
            900,
            "marker",
            binding=self.log_config.text_binding,
            start_lsn=START,
            unit_config=self.log_config.unit_config,
            clock=lambda: 0,
        )
        self.lsn = 100
        # What ingest reads from the catalog after each run (a test changes it).
        self.catalog_keys = {OID: ["id"], OTHER: ["id"], HEARTBEAT: ["id"]}
        self.config = ApplyConfig(
            source,
            (TABLE, OTHER_TABLE),
            database=database,
            mirror_database=store.mirror_database.strip('"'),
            **apply_options,
        )
        self.apply = Apply2(store.con, self.config, retention=self.retention(), fold=fold)
        self.apply.initialize()
        self.feed(
            [
                pgstream.relation(OID, "public", "items", "f", COLUMNS),
                pgstream.relation(OTHER, "public", "others", "f", COLUMNS),
                pgstream.relation(HEARTBEAT, "public", "md_cdc_heartbeat", "d", HB_COLUMNS),
            ]
        )

    def retention(self):
        c = self.config
        return Retention2(self.store.con, c.database, c.source_id, c.lease_seconds)

    def feed(self, messages, close=False):
        items = [i for wire in messages for i in self.spool.feed(wire)]
        if close:
            items += list(self.spool.close())
        if items:
            self.log.acquire("writer")
            try:
                self.log.append(items, partition_day="20261106")
                # As ingest does after each run: the catalog key held through the
                # checkpoint (a unit end on format 2).
                self.log.record_keys(
                    dict(self.catalog_keys), self.log.checkpoint(), self.log.checkpoint()
                )
            finally:
                self.log.release()

    def commit(self, *changes):
        """One source transaction; returns its commit LSN."""
        lsn = self.lsn
        self.lsn += 10
        self.feed([pgstream.begin(lsn, -5, 7), *changes, pgstream.commit(lsn, lsn + 5, -4)])
        return lsn

    def mirror(self, name="items"):
        """The mirror table's rows; {} before apply first created it."""
        try:
            self.store.con.execute(f"SELECT 1 FROM {self.apply.mirror}.public.{name} LIMIT 0")
        except duckdb.CatalogException:
            return {}
        return dict(
            self.store.con.execute(
                f"SELECT id,v FROM {self.apply.mirror}.public.{name} ORDER BY id"
            ).fetchall()
        )

    def state(self, oid=OID):
        row = self.store.con.execute(
            f"SELECT applied_through_lsn,state,blocked_commit_lsn,blocked_unit_start_lsn,detail "
            f"FROM {self.apply.mirror}._cdc.apply_state WHERE relation_id=?",
            [oid],
        ).fetchone()
        return dict(zip(("applied", "state", "blocked", "unit", "detail"), row))

    def pin(self):
        return self.store.con.execute(
            f"SELECT pinned_lsn FROM {self.log.prefix}retention_pins WHERE owner='table:{OID}'"
        ).fetchone()[0]

    def units(self):
        return self.store.con.execute(
            f"SELECT unit_start_lsn,end_lsn,digest FROM {self.log.prefix}units_v2 "
            "WHERE complete ORDER BY 1"
        ).fetchall()


def ins(key, value):
    return pgstream.insert(OID, (key, value))


@pytest.fixture
def env(stores):
    return Env(stores)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_log_only_start_records_the_same_epoch_start_in_state_and_pin(env):
    env.apply.start_from_log(TABLE)
    assert env.state()["applied"] == env.pin() == START
    assert env.apply.retention.initial_cut(1) == START
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    assert env.units()[0][0] == START  # the first unit starts on the recorded cut


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_whole_units_apply_across_pages_and_count(stores):
    env = Env(stores, max_batch_events=1)  # one unit per page: several pages
    env.apply.start_from_log(TABLE)
    for k in range(6):
        env.commit(ins(str(k), f"v{k}"))
    units = env.units()
    assert len(units) == 3
    result = env.apply.apply_table(TABLE, "run")
    assert result.status == "caught_up", result
    assert result.boundary == units[-1][1] == env.state()["applied"] == env.pin()
    assert result.units == 3 and result.events == 6
    assert env.mirror() == {str(k): f"v{k}" for k in range(6)}
    # No mirror state (awaiting its first generation): units is unknown, not 0.
    waiting = env.apply.apply_table(OTHER_TABLE, "run")
    assert waiting.status == "awaiting_generation" and waiting.units is None
    # Idle with a state: 0 units committed.
    assert env.apply.apply_table(TABLE, "idle").units == 0


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_consumer_for_dispatches_on_the_stored_format(env):
    """consumer_for stays as a one-release alias returning the (format-2) ApplyRun;
    a store without protocol_v2 is refused, never served a v1 consumer."""
    got = consumer_for(env.store.con, env.config, retention_v2=env.retention)
    assert type(got) is ApplyRun and got.format_version == 2
    other = replace(env.config, source_id="absent_v1", mirror_database="absent_v1_m")
    calls = []
    with pytest.raises(SourceMismatch, match="unsupported source protocol"):
        consumer_for(env.store.con, other, retention_v2=lambda: calls.append(1))
    assert calls == []  # refused before any Retention2 is built


def blocked_unit(env):
    """Unit 1: commits 1, 2. Unit 2: commit 3, then commit 4 whose relation drops v."""
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.commit(ins("3", "c"))
    dropped = pgstream.relation(OID, "public", "items", "f", COLUMNS[:1])
    c4 = env.commit(dropped, pgstream.insert(OID, ("4",)))
    first, second = env.units()
    return first, second, c4


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_blocking_commit_holds_before_its_whole_unit(env):
    first, second, c4 = blocked_unit(env)
    result = env.apply.apply_table(TABLE, "run")
    assert result.status == "needs_action" and result.boundary == second[0] == first[1]
    # Commit 3 is in the blocked unit: never applied, though it folds cleanly alone
    # (a mutant that keeps the partially folded images would apply it).
    assert env.mirror() == {"1": "a", "2": "b"}
    state = env.state()
    assert (state["applied"], state["blocked"], state["unit"]) == (second[0], c4, second[0])
    assert "dropped" in state["detail"] and env.pin() == second[0]
    assert result.units == 1


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_quarantine_skips_the_whole_unit_and_advances_through_it(env):
    first, second, c4 = blocked_unit(env)
    env.apply.apply_table(TABLE, "run")
    m = f"{env.apply.mirror}._cdc"
    generation, epoch = env.store.con.execute(
        f"SELECT generation_id,epoch FROM {m}.apply_state"
    ).fetchone()
    # What table_actions.quarantine records (its own tests cover the action).
    env.store.con.execute(
        f"INSERT INTO {m}.quarantine (relation_id,generation_id,epoch,commit_lsn,reason,actor,"
        "run_id,created_at,unit_start_lsn,unit_end_lsn,unit_digest) "
        "VALUES (?,?,?,?,'test','t','q',current_timestamp,?,?,?)",
        [OID, generation, epoch, c4, *second],
    )
    env.store.con.execute(
        f"UPDATE {m}.apply_state SET state='live',blocked_commit_lsn=NULL,"
        "blocked_unit_start_lsn=NULL,detail=NULL"
    )
    result = env.apply.apply_table(TABLE, "run2")
    assert result.status == "caught_up" and result.boundary == second[1] == env.pin()
    assert env.mirror() == {"1": "a", "2": "b"}  # commit 3 is skipped with its unit
    # A quarantine row for another unit identity (digest) does not skip.
    assert env.apply._skip_set([{"s": second[0], "e": second[1], "d": "x"}]) != {tuple(second)}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_reader_binding_refusal_holds_needs_action(env):
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.store.con.execute(f"UPDATE {env.log.prefix}protocol_v2 SET binding_json='{{}}'")
    result = env.apply.apply_table(TABLE, "run")
    assert result.status == "needs_action" and result.boundary == START
    state = env.state()
    assert state["state"] == "needs_action" and "binding" in state["detail"]
    assert state["blocked"] is None and state["unit"] is None and env.mirror() == {}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_unit_under_another_key_layout_holds_needs_resync_at_its_start(env):
    env.apply.start_from_log(TABLE)
    c1 = env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    # The mirror was built under another key than the units' descriptors carry.
    env.store.con.execute(f"UPDATE {env.apply.mirror}._cdc.apply_state SET key_columns='[\"v\"]'")
    result = env.apply.apply_table(TABLE, "run")
    assert result.status == "needs_resync" and result.boundary == START
    state = env.state()
    assert (state["blocked"], state["unit"]) == (c1, START) and env.mirror() == {}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.parametrize("fold", ["sql", "python"])
def test_a_poison_unit_is_isolated_by_whole_units(stores, fold):
    """A REAL poison (commit 3's integer cell is not a number: the MERGE cast raises), on
    either fold: the whole-unit bisect applies unit 1 and holds before unit 2."""
    env = Env(stores, fold=fold)
    env.feed(
        [pgstream.relation(OID, "public", "items", "f", ((1, "id", 25, -1), (0, "v", 23, -1)))]
    )
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "1"))
    env.commit(ins("2", "2"))
    c3 = env.commit(ins("3", "not-a-number"))
    env.commit(ins("4", "4"))
    first, second = env.units()
    result = env.apply.apply_table(TABLE, "run")
    assert result.status == "needs_action" and result.boundary == second[0]
    assert env.mirror() == {"1": 1, "2": 2}  # commit 4 shares the poison unit
    state = env.state()
    assert (state["applied"], state["blocked"], state["unit"]) == (second[0], c3, second[0])
    assert state["detail"].startswith("ConversionException applying transaction")
    if fold == "sql":
        assert env.apply.fold_stats == {"sql": 1}  # the SQL MERGE failed, then the bisect


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_cells_truncate_and_an_empty_projection_through_the_unchanged_fold(env):
    """NULL, unchanged TOAST (inherited from the mirror row), update, delete; a
    multi-relation TRUNCATE holds each listed table exactly like v1 (truncate is a
    needs_action hold); a table with no events in a page still advances."""
    env.apply.start_from_log(TABLE)
    env.apply.start_from_log(OTHER_TABLE)
    env.commit(ins("1", "a"), ins("2", None))
    env.commit(pgstream.update(OID, ("1", "u")), pgstream.delete(OID, ("2", None), identity="K"))
    env.commit(pgstream.update(OID, ("1", pgstream.UNCHANGED)))  # TOAST: keeps "u"
    env.commit(ins("3", "c"))
    result = env.apply.apply_table(TABLE, "run")
    assert result.status == "caught_up", result
    assert env.mirror() == {"1": "u", "3": "c"}
    # The other table had no events: it still advances through every complete unit.
    other = env.apply.apply_table(OTHER_TABLE, "run")
    assert other.status == "caught_up" and other.boundary == result.boundary
    assert other.events == 0 and env.state(OTHER)["applied"] == result.boundary
    truncate = env.commit(pgstream.truncate([OID, OTHER]))
    env.commit(ins("4", "d"))
    for oid, table in ((OID, TABLE), (OTHER, OTHER_TABLE)):
        held = env.apply.apply_table(table, "run2")
        state = env.state(oid)
        # TRUNCATE needs a new generation (mirror.fold), for every listed table, held
        # before the whole unit holding it.
        assert held.status == state["state"] == "needs_resync", (table, held)
        assert state["blocked"] == truncate and state["unit"] == state["applied"]
        assert state["applied"] == result.boundary


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_key_bound_that_is_not_a_unit_end_fails_closed(env):
    """Apply2 inherits v1's next_step bound unchanged: high = min(confirmed, the key
    limit). Format 2's writer records key_checked_lsn and cut_lsn only at unit ends
    (or the epoch start); a bound inside a unit is a writer bug, and the reader
    refuses it before anything is applied (never a silent prefix)."""
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.commit(ins("3", "c"))
    env.commit(ins("4", "d"))
    first, second = env.units()
    inside = second[0] + 1
    env.store.con.execute(
        f"UPDATE {env.log.prefix}tables SET key_checked_lsn=? WHERE relation_id=?", [inside, OID]
    )
    with pytest.raises(ValueError, match="straddle|does not reach"):
        env.apply.apply_table(TABLE, "run")
    assert env.state()["applied"] == START and env.mirror() == {}
    # At a unit end the same bound applies exactly up to it.
    env.store.con.execute(
        f"UPDATE {env.log.prefix}tables SET key_checked_lsn=? WHERE relation_id=?",
        [first[1], OID],
    )
    result = env.apply.apply_table(TABLE, "run2")
    assert result.status == "caught_up" and result.boundary == first[1]
    assert env.mirror() == {"1": "a", "2": "b"}


def pending(env):
    return env.store.con.execute(
        f"SELECT pending_keys_v2,key_version,key_checked_lsn,state,held_reason "
        f"FROM {env.log.prefix}tables WHERE relation_id=?",
        [OID],
    ).fetchone()


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_pending_key_change_stops_apply_at_the_frozen_cut_then_holds_at_it(env):
    """K1/K2 (consumer side): observed in the middle of an open unit, the key change is
    queued; key_checked_lsn stays at the prior verified cut, so Apply2 applies exactly
    up to it with no hold (no held_reason). After activation at the next boundary the
    mirror's version is behind: Apply2 holds needs_resync exactly at that cut."""
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    (_, cut, _), *_ = env.units()
    # A three-row commit emits a chunk (chunk_frames=3) before its unit closes: a
    # durable open unit when this run's key observation arrives.
    env.catalog_keys = {**env.catalog_keys, OID: ["v"]}
    env.commit(ins("3", "c"), ins("4", "d"), ins("5", "e"))
    assert env.log.unit_position()["partial"]
    queue, version, checked, state, held = pending(env)
    assert queue is not None and json.loads(queue)[0]["cut_lsn"] == cut
    assert (version, checked, held) == (0, cut, None)
    result = env.apply.apply_table(TABLE, "run")
    assert result.status == "caught_up" and result.boundary == cut
    assert env.mirror() == {"1": "a", "2": "b"} and env.state()["state"] == "live"
    # One run that completes the open unit AND leaves the next one open: the
    # checkpoint moves past the cut while the queue is still pending. key_checked_lsn
    # must not follow it, so Apply2 still stops at the frozen cut.
    c6, c7 = env.lsn, env.lsn + 10
    env.lsn += 20
    env.feed(
        [pgstream.begin(c6, -5, 7), ins("6", "f"), pgstream.commit(c6, c6 + 5, -4)]
        + [pgstream.begin(c7, -5, 7), ins("7", "g"), ins("8", "h"), ins("9", "i")]
        + [pgstream.commit(c7, c7 + 5, -4)]
    )
    assert env.log.unit_position()["partial"] and env.log.checkpoint() > cut
    queue, version, checked, state, held = pending(env)
    assert queue is not None and (version, checked) == (0, cut)
    again = env.apply.apply_table(TABLE, "run1b")
    assert again.status == "caught_up" and again.boundary == cut
    # The open unit closes; the queue activates at that boundary with the frozen cut.
    env.commit(ins("10", "j"))
    assert not env.log.unit_position()["partial"]
    queue, version, checked, state, held = pending(env)
    assert (queue, version, checked, state) == (None, 1, cut, "needs_resync")
    held_result = env.apply.apply_table(TABLE, "run2")
    assert held_result.status == "needs_resync" and held_result.boundary == cut
    assert env.mirror() == {"1": "a", "2": "b"}  # nothing past the cut, under either key


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_after_activation_the_registered_projection_matches_the_next_spools_units(env):
    """K3: activation rewrote the registered key_columns; a spool built from the
    registry afterwards (the next ingest run) writes descriptors under the new key, and
    a generation built under that key (emulated: what a resync publishes) applies them,
    while one still under the old key holds before them."""
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.catalog_keys = {**env.catalog_keys, OID: ["v"]}
    env.commit(ins("3", "c"))
    env.commit(ins("4", "d"))  # the unit closes: no open unit, activation at this run end
    registered = env.store.con.execute(
        f"SELECT key_columns FROM {env.log.prefix}tables WHERE relation_id=?", [OID]
    ).fetchone()[0]
    assert json.loads(registered) == ["v"]
    # The next run's spool reads its keys from the registry.
    keys = {
        oid: tuple(json.loads(k))
        for oid, k in env.store.con.execute(
            f"SELECT relation_id,key_columns FROM {env.log.prefix}tables"
        ).fetchall()
    }
    start = env.log.checkpoint()
    if env.spool.active or env.spool.frame_count:
        env.feed([], close=True)
    env.spool = Spool(
        env.log_config,
        keys,
        900,
        "marker",
        binding=env.log_config.text_binding,
        start_lsn=start,
        unit_config=env.log_config.unit_config,
        clock=lambda: 0,
    )
    env.feed([pgstream.relation(OID, "public", "items", "f", COLUMNS)])
    env.commit(ins("7", "x"))
    env.commit(ins("8", "y"))
    # Emulate the resync's publication: a generation under the new key at `start`.
    m = f"{env.apply.mirror}._cdc"
    env.store.con.execute(
        f"UPDATE {m}.apply_state SET key_columns='[\"v\"]',key_version=1,"
        "applied_through_lsn=?,state='live' WHERE relation_id=?",
        [start, OID],
    )
    gen = env.store.con.execute(
        f"SELECT generation_id FROM {m}.apply_state WHERE relation_id=?", [OID]
    ).fetchone()[0]
    env.apply.retention.transfer(OID, gen, 1, start)
    result = env.apply.apply_table(TABLE, "run")
    assert result.status == "caught_up" and result.boundary == env.log.checkpoint(), result
    # The same units under the old key's mirror would hold before them (K3 backstop).
    env.store.con.execute(
        f"UPDATE {m}.apply_state SET key_columns='[\"id\"]',applied_through_lsn=?,"
        "state='live' WHERE relation_id=?",
        [start, OID],
    )
    assert env.apply.apply_table(TABLE, "run2").status == "needs_resync"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_legacy_or_unknown_store_is_refused_before_any_mirror_write(stores):
    """Construction does no I/O; check_format() and initialize() refuse a store
    without format-2 protocol before any mirror DDL (action wrappers call check_format
    first)."""
    store, _ = stores
    config = ApplyConfig(
        "nolog",
        ("public.items",),
        database=store.database.strip('"'),
        mirror_database=store.mirror_database.strip('"'),
    )
    apply = ApplyRun(store.con, config)  # no I/O
    with pytest.raises(SourceMismatch):
        apply.check_format()
    with pytest.raises(SourceMismatch):
        apply.initialize()
    schemas = store.con.execute(
        "SELECT count(*) FROM duckdb_schemas() WHERE database_name=? AND schema_name='_cdc'",
        [config.mirror_database],
    ).fetchone()[0]
    assert schemas == 0  # nothing was created in the mirror


class LocalMD:
    """duckdb.connect for cli: every `md:<db>` names a local file under `root`;
    CREATE DATABASE (MotherDuck-only) creates nothing until the file is attached."""

    def __init__(self, root):
        self.root, self.real = root, duckdb.connect

    def path(self, name):
        return str(self.root / f"{name.split('?')[0] or 'workspace'}.duckdb")

    def __call__(self, target, *args, **kwargs):
        if not target.startswith("md:"):
            return self.real(target, *args, **kwargs)
        con = self.real(self.path(target[3:]))
        outer = self

        class Con:
            def execute(self, statement, *params):
                if statement.startswith("CREATE DATABASE"):
                    return con
                statement = re.sub(r"'md:(\w+)'", lambda m: f"'{outer.path(m[1])}'", statement)
                return con.execute(statement, *params)

            def __getattr__(self, name):
                return getattr(con, name)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                con.close()

        return Con()


@pytest.mark.parametrize("command", ["apply", "backfill"])
def test_the_cli_refuses_a_legacy_store_before_any_mirror_ddl(tmp_path, monkeypatch, command):
    """cli.apply_main/backfill_main construct the format-2 consumers: a store without
    protocol_v2 fails the run with SourceMismatch and creates no `_cdc` schema."""
    from types import SimpleNamespace

    from cdc_flight import cli

    duckdb.connect(str(tmp_path / "legacy_log.duckdb")).close()  # a store with no protocol_v2
    for name, value in dict(
        CDC_SOURCE_ID="legacy",
        CDC_TABLES='["public.items"]',
        CDC_DATABASE="legacy_log",
        CDC_MIRROR_DATABASE="legacy_mirror",
        CDC_OPS_DATABASE="legacy_ops",
        CDC_PG_DSN="host=127.0.0.1 port=1 dbname=unused",  # never connected
        CDC_BACKFILL_START="1",
    ).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(cli.duckdb, "connect", LocalMD(tmp_path))
    args = SimpleNamespace(command=command, table="public.items", abandon=False)
    run = cli.apply_main if command == "apply" else cli.backfill_main
    with pytest.raises(SourceMismatch, match="unsupported source protocol"):
        run(args)
    with duckdb.connect(str(tmp_path / "legacy_mirror.duckdb")) as mirror:
        assert mirror.execute(
            "SELECT count(*) FROM duckdb_schemas() WHERE schema_name='_cdc'"
        ).fetchone() == (0,)


# The remote whole-unit quarantine node ----------------

INT_COLUMNS = ((1, "id", 25, -1), (0, "n", 23, -1))  # n: integer (type OID 23)


def poisoned_unit_env(stores):
    """(env, unit, blocked_commit): public.others held needs_action before a REAL poison
    unit, with no write-path patch. Unit 1 = two clean commits; unit 2 = a clean commit
    then one whose integer cell is non-numeric text, so the mirror's MERGE cast raises
    ConversionException and the whole-unit bisect holds before unit 2. `unit` is its
    (start, end, digest, member_count); `blocked_commit` is what apply_state names.
    A fresh format-2 schema (source `f2`) in the store's own database, on any backend."""
    env = Env(stores, max_commits=2)
    env.feed([pgstream.relation(OTHER, "public", "others", "f", INT_COLUMNS)])
    env.apply.start_from_log(OTHER_TABLE)
    for key, value in (("1", "10"), ("2", "20"), ("3", "30"), ("4", "not-a-number")):
        env.commit(oth_int(key, value))
    first, second = env.units()
    result = env.apply.apply_table(OTHER_TABLE, "poison")
    assert result.status == "needs_action" and result.boundary == second[0] == first[1]
    state = env.state(OTHER)
    assert state["unit"] == second[0] and state["detail"].startswith("ConversionException")
    (member_count,) = env.store.con.execute(
        f"SELECT member_count FROM {env.log.prefix}units_v2 WHERE unit_start_lsn=?",
        [second[0]],
    ).fetchone()
    return env, (*second, member_count), state["blocked"]


def oth_int(key, value):
    return pgstream.insert(OTHER, (key, value))


def fingerprint(con, relation, exclude=()):
    """(rows, order-independent hash) of a relation, read on `con`: a fresh instance in
    the remote node. A sum, not bit_xor, so two identical rows never cancel out.
    `exclude`: columns that may change without changing the facts."""
    drop = f" EXCLUDE ({','.join(exclude)})" if exclude else ""
    return tuple(
        con.execute(
            f"SELECT count(*),coalesce(sum(hash(t)),0) FROM (SELECT *{drop} FROM {relation}) t"
        ).fetchone()
    )


def mirror_snapshot(con, mirror_database):
    """The four facts an action writes in its one mirror transaction."""
    m = f"{quoted(mirror_database)}._cdc."
    return {
        table: fingerprint(con, m + table)
        for table in ("previews", "quarantine", "apply_state", "runs")
    }


def log_snapshot(con, database, source_id):
    """The change log a mirror action must never write: the source row (guard included),
    the unit, chunk and descriptor ledgers, the frame partitions, and every frame day."""
    s = f"{quoted(database)}.{quoted(source_id)}."
    days = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name=? "
            "AND table_name LIKE 'frames\\_v2\\_%' ESCAPE '\\' ORDER BY 1",
            [database, source_id],
        ).fetchall()
    ]
    names = ("source", "units_v2", "chunks_v2", "descriptors_v2", "frame_partitions_v2", *days)
    return {name: fingerprint(con, s + name) for name in names}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_poisoned_unit_env_holds_on_a_real_cast_and_quarantine_skips_it(stores):
    """Local twin of the remote node's setup: the fixture's real poison, its unit and
    blocked commit; the core quarantine leaves the log unwritten and the checkpoint at
    the unit's start; the next apply passes the unit's end without any of its rows."""
    from cdc_flight import table_actions
    from cdc_flight.action_flight import unit_literal

    env, unit, blocked = poisoned_unit_env(stores)
    start, end, digest, members = unit
    assert members == 2 and start < blocked < end
    rows = env.store.con.execute(
        f"SELECT id,n FROM {env.apply.mirror}.public.others ORDER BY id"
    ).fetchall()
    assert rows == [("1", 10), ("2", 20)]  # unit 1 applied; commit 3 shares the poison unit
    con, db, mirror = env.store.con, env.config.database, env.config.mirror_database
    log_before = log_snapshot(con, db, "f2")
    mirror_before = mirror_snapshot(con, mirror)
    assert fingerprint(con, f"{env.log.prefix}units_v2") == log_before["units_v2"]
    literal = unit_literal(OTHER_TABLE, 1, start, end)
    result = table_actions.quarantine(
        env.apply,
        OTHER_TABLE,
        run_id="q",
        actor="t",
        expected_epoch=1,
        confirm=literal,
        unit_start_lsn=start,
        unit_end_lsn=end,
    )
    assert result.status == "done", result
    assert log_snapshot(con, db, "f2") == log_before  # the log is never written
    after = mirror_snapshot(con, mirror)
    assert after["quarantine"] != mirror_before["quarantine"]
    assert after["apply_state"] != mirror_before["apply_state"]
    q = con.execute(
        f"SELECT unit_start_lsn,unit_end_lsn,unit_digest,commit_lsn "
        f"FROM {quoted(mirror)}._cdc.quarantine"
    ).fetchone()
    assert q == (start, end, digest, blocked)
    state = env.state(OTHER)
    assert (state["state"], state["applied"], state["unit"]) == ("live", start, None)
    nxt = env.apply.apply_table(OTHER_TABLE, "after")
    assert nxt.status == "caught_up" and nxt.boundary == end
    assert con.execute(
        f"SELECT id FROM {env.apply.mirror}.public.others ORDER BY id"
    ).fetchall() == [("1",), ("2",)]
