"""Format-2 action-Flight wrappers (active since the format-2 cutover).

One suite, parametrized over the core it wraps: `fake` implements the
agreed core signatures exactly (the table_actions and lifecycle cores) so the
wrappers can be tested while the cores are written; `real` runs the same assertions on
the committed cores and is a hard gate before the PR (no `real` skips). The previews and
their compare-and-set are always real: a local DuckDB table and action_flight's own SQL.
"""

import uuid
from contextlib import contextmanager

import duckdb
import pytest
from test_action_flight import Trigger
from test_phase1 import pg_phase1  # noqa: F401 (fixture, for the real epoch core)

from cdc_flight import action_base as base
from cdc_flight import action_flight as af
from cdc_flight import quarantine_flight as qf
from cdc_flight import table_actions
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import ApplyConfig, lsn_text
from cdc_flight.table_actions import ActionResult, Refused, parse_lsn

TABLE = "public.orders"
OWNER = af.Owner(
    "src",
    backfill_flight=str(uuid.UUID(int=1)),
    compact_flight=str(uuid.UUID(int=2)),
)
START, END = (5 << 32) | 0x10, (5 << 32) | 0x90
BLOCKED = (5 << 32) | 0x40


def unit_observation(**changes):
    observed = dict(
        state="needs_action",
        generation=1,
        epoch=1,
        applied=START,
        token=None,
        blocked=BLOCKED,
        detail="poison",
        source_held=None,
        format_version=2,
        source_epoch=1,
        generation_id=1,
        unit_start_lsn=START,
        unit_end_lsn=END,
        unit_commit_count=3,
        unit_event_count=7,
        unit_digest="d1" * 32,
        first_commit_lsn=START + 8,
        last_commit_lsn=END - 8,
        config_revision=1,
        blocked_commit_lsn=BLOCKED,
        consistent=True,
    )
    observed.update(changes)
    return observed


class FakeApply:
    """A format-2 apply as the wrappers see it: execute() with {m}, transaction(), its
    config, and the class attribute format_version. Previews live in real DuckDB."""

    format_version = 2

    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("CREATE SCHEMA m")
        self.con.execute(af.PREVIEWS_DDL.replace("{s}", "m."))
        self.config = ApplyConfig("src", (), mirror_database="src_mirror")

    def execute(self, sql, params=None):
        return self.con.execute(sql.replace("{m}", "m."), params or [])

    @contextmanager
    def transaction(self):
        self.con.execute("BEGIN")
        try:
            yield
        except BaseException:
            self.con.execute("ROLLBACK")
            raise
        self.con.execute("COMMIT")

    def previews(self):
        return self.con.execute("SELECT id,used_at IS NOT NULL FROM m.previews").fetchall()


class FakeCore:
    """The table_actions core on format 2, to the agreed signatures and refusals:
    observe -> (relation_id, observed); quarantine(..., commit_lsn=None, unit_start_lsn,
    unit_end_lsn, confirm, consume=) spending in the same transaction; retry(...,
    expected_unit=)."""

    kind = "fake"

    def __init__(self, monkeypatch):
        self.apply, self.table, self.epoch = FakeApply(), TABLE, 1
        self.start, self.end, self.blocked = START, END, BLOCKED
        self.observed, self.done, self.observes, self.retried = unit_observation(), [], 0, None
        self.calls = 0  # quarantine library calls: refusals the wrapper owns never reach it
        monkeypatch.setattr(table_actions, "observe", self.observe)
        monkeypatch.setitem(table_actions.ACTIONS, "quarantine", self.quarantine)
        monkeypatch.setitem(table_actions.ACTIONS, "retry", self.retry)

    def set(self, **fields):
        self.observed.update(fields)

    def drop(self, name):
        del self.observed[name]

    def view(self):
        return dict(self.observed)

    def quarantined(self):
        return list(self.done)

    def previews(self):
        return self.apply.previews()

    def observe(self, apply, table):
        self.observes += 1
        return 101, dict(self.observed)

    def quarantine(
        self,
        apply,
        table,
        *,
        run_id,
        actor,
        expected_epoch,
        confirm,
        commit_lsn=None,
        unit_start_lsn=None,
        unit_end_lsn=None,
        dry_run=False,
        consume=None,
        fault=None,  # the real core's commit stages; the fake has no transaction
    ):
        self.calls += 1
        observed = dict(self.observed)
        shown = {k: v for k, v in observed.items() if k != "detail"}
        literal = af.unit_literal(
            table,
            observed.get("source_epoch"),
            observed.get("unit_start_lsn"),
            observed.get("unit_end_lsn"),
        )
        if commit_lsn is not None:
            return ActionResult("refused", table, f"name the unit: {literal}", [], shown)
        if (unit_start_lsn, unit_end_lsn) != (
            observed.get("unit_start_lsn"),
            observed.get("unit_end_lsn"),
        ):
            return ActionResult("refused", table, "the blocked unit changed", [], shown)
        if dry_run:
            return ActionResult("would", table, f"type: {literal}", [], shown | {"detail": None})
        if confirm != literal or expected_epoch != observed.get("source_epoch"):
            return ActionResult("refused", table, "confirmation", [], shown)
        try:
            with apply.transaction():
                consume(observed)
                self.done.append((table, unit_start_lsn, unit_end_lsn))
        except Refused as exc:
            return ActionResult("refused", table, str(exc), [], shown)
        return ActionResult("done", table, "quarantined the whole unit", [f"resync {table}"], shown)

    def retry(
        self,
        apply,
        table,
        *,
        run_id,
        actor,
        expected_blocked_lsn,
        expected_unit=None,
        dry_run=False,
        consume=None,
        fault=None,
    ):
        self.retried = expected_unit
        shown = {k: v for k, v in self.observed.items() if k != "detail"}
        return ActionResult("would" if dry_run else "done", table, "retry", [], shown)


class RealCore:
    """The committed table_actions core on a real Apply2 mirror held before one whole
    unit (their poisoned fixture: units (1,2),(3,4), commit 3 fails to stage). set() and
    drop() overlay the core's observation, as the core's own stale test does, so a value a
    real store can't change on cue (a digest, a member count) can still move."""

    kind = "real"

    def __init__(self, stores, monkeypatch):
        from test_apply2 import TABLE as REAL_TABLE
        from test_apply2 import Env
        from test_table_actions_v2 import poisoned

        self.env = Env(stores)
        blocked, (start, end, _) = poisoned(self.env, monkeypatch)
        self.apply, self.table, self.epoch = self.env.apply, REAL_TABLE, 1
        self.start, self.end, self.blocked = start, end, blocked
        self.overlay, self.dropped, self.observes, self.retried = {}, set(), 0, None
        self.calls = 0
        real_observe, real_retry = table_actions.observe, table_actions.ACTIONS["retry"]
        real_quarantine = table_actions.ACTIONS["quarantine"]

        def quarantine(*args, **kwargs):
            self.calls += 1
            return real_quarantine(*args, **kwargs)

        def observe(apply, table):
            self.observes += 1
            relation_id, observed = real_observe(apply, table)
            if observed is not None:
                observed = dict(observed, **self.overlay)
                for name in self.dropped:
                    observed.pop(name, None)
            return relation_id, observed

        def retry(*args, expected_unit=None, **kwargs):
            self.retried = expected_unit
            return real_retry(*args, expected_unit=expected_unit, **kwargs)

        monkeypatch.setattr(table_actions, "observe", observe)
        monkeypatch.setitem(table_actions.ACTIONS, "retry", retry)
        monkeypatch.setitem(table_actions.ACTIONS, "quarantine", quarantine)

    def set(self, **fields):
        self.overlay.update(fields)

    def drop(self, name):
        self.dropped.add(name)

    def view(self):
        return table_actions.observe(self.apply, self.table)[1]

    def _mirror(self, sql):
        return self.env.store.con.execute(sql.format(m=f"{self.apply.mirror}._cdc.")).fetchall()

    def quarantined(self):
        rows = self._mirror("SELECT unit_start_lsn,unit_end_lsn FROM {m}quarantine")
        return [(self.table, start, end) for start, end in rows]

    def previews(self):
        return self._mirror("SELECT id,used_at IS NOT NULL FROM {m}previews")


@pytest.fixture(params=["fake", "real"])
def core(request, monkeypatch):
    if request.param == "real":
        if not hasattr(table_actions, "unit_confirmation"):
            pytest.skip("the table_actions format-2 core is not on this branch")
        return RealCore(request.getfixturevalue("stores"), monkeypatch)
    return FakeCore(monkeypatch)


# Faults a real store can't produce on cue: the committed core always reports every
# promised field, so removing one only exercises the wrapper's own refusal (fake). Plan
# These are the only `real` skips allowed.
UNPRODUCIBLE_ON_REAL = {
    "generation_id",
    "unit_commit_count",
    "unit_digest",
    "config_revision",
}


def literal(core, epoch=None):
    return af.unit_literal(core.table, core.epoch if epoch is None else epoch, core.start, core.end)


def quarantine(core, *, dry_run, preview=None, confirm=None, epoch=None):
    environ = dict(
        CDC_ACTION="quarantine",
        CDC_TABLE=core.table,
        CDC_EXPECTED_EPOCH=str(core.epoch if epoch is None else epoch),
        CDC_CONFIRM=confirm or literal(core),
        CDC_DRY_RUN="1" if dry_run else "0",
    )
    if preview:
        environ["CDC_PREVIEW_ID"] = preview
    inputs = af.parse(environ, qf.QUARANTINE)
    return af.perform(
        core.apply,
        inputs,
        run_id=str(uuid.uuid4()),
        actor="flight:test",
        owner=OWNER,
        trigger=Trigger(),
    )


def test_the_wrapper_literal_is_the_cores_literal(core):
    if core.kind == "real":
        assert literal(core) == table_actions.unit_confirmation(
            core.table, core.epoch, core.start, core.end
        )
    assert (
        literal(core)
        == f"QUARANTINE {core.table} UNIT 1 {lsn_text(core.start)}..{lsn_text(core.end)}"
    )


def test_a_dry_run_then_the_exact_unit_literal_quarantines_the_whole_unit(core):
    dry = quarantine(core, dry_run=True)
    assert dry["status"] == "would" and dry["preview_id"], dry
    done = quarantine(core, dry_run=False, preview=dry["preview_id"])
    assert done["status"] == "done", done
    assert core.quarantined() == [(core.table, core.start, core.end)]
    assert core.previews() == [(dry["preview_id"], True)]
    # Observed values stay raw integers in the result, as on format 1.
    assert done["observed"]["unit_start_lsn"] == core.start
    assert isinstance(done["observed"]["unit_digest"], str)


@pytest.mark.parametrize("variant", ["case", "spacing", "zero", "epoch", "commit"])
def test_any_literal_but_the_exact_built_one_is_refused_and_writes_nothing(core, variant):
    exact = literal(core)
    confirm = {
        "case": exact.lower(),
        "spacing": exact.replace(" UNIT", "  UNIT"),
        "zero": exact.replace(
            f" {lsn_text(core.start)}..",
            f" {lsn_text(core.start)[:2]}0{lsn_text(core.start)[2:]}..",
        ),
        "epoch": literal(core, epoch=2),
        "commit": f"QUARANTINE {core.table} {lsn_text(core.blocked)}",
    }[variant]
    assert confirm != exact
    dry = quarantine(core, dry_run=True)
    calls = core.calls
    out = quarantine(core, dry_run=False, preview=dry["preview_id"], confirm=confirm)
    assert out["status"] == "refused" and exact in out["detail"], out
    assert core.calls == calls  # refused by the wrapper's exact compare, before the core
    assert core.quarantined() == [] and core.previews() == [(dry["preview_id"], False)]


def test_quarantine_has_no_commit_key_and_ignores_a_stale_one(core):
    """The static Spec has no CDC_COMMIT_LSN; a leftover value in the environment is
    not a declared key, so it never reaches the core."""
    assert "CDC_COMMIT_LSN" not in FLIGHT_CONFIG["quarantine"]
    assert qf.QUARANTINE.keys == FLIGHT_CONFIG["quarantine"]
    environ = dict(
        CDC_ACTION="quarantine",
        CDC_TABLE=core.table,
        CDC_EXPECTED_EPOCH=str(core.epoch),
        CDC_CONFIRM=literal(core),
        CDC_DRY_RUN="1",
        CDC_COMMIT_LSN=lsn_text(core.blocked),
    )
    assert "commit_lsn" not in af.parse(environ, qf.QUARANTINE).values


@pytest.mark.parametrize(
    "field,delta",
    [
        ("unit_start_lsn", 1),
        ("unit_end_lsn", 1),
        ("unit_commit_count", 1),
        ("unit_digest", None),
        ("generation_id", 1),
        ("config_revision", 1),
    ],
)
def test_a_unit_identity_change_after_the_dry_run_makes_the_preview_stale(core, field, delta):
    """Both runs carry the same inputs (the preview hash covers them, CDC_CONFIRM
    included), typed for the unit as it will be seen. On the fake only the promised
    decision (the CAS) can then refuse; the real core may refuse first itself. Either
    way nothing is written and the preview stays unspent."""
    before = core.view()
    if delta is None or isinstance(before[field], str):
        value = "f" * 64 if delta is None else before[field] + "-moved"
    else:
        value = before[field] + delta
    moved = dict(before, **{field: value})
    # The fake types the moved unit's literal in both runs so only the CAS can refuse; the
    # real core checks the literal in its dry run too, so there both runs type today's.
    unit = moved if core.kind == "fake" else before
    confirm = af.unit_literal(core.table, core.epoch, unit["unit_start_lsn"], unit["unit_end_lsn"])
    dry = quarantine(core, dry_run=True, confirm=confirm)
    core.set(**{field: value})
    out = quarantine(core, dry_run=False, preview=dry["preview_id"], confirm=confirm)
    assert out["status"] == "refused", (field, out)
    if core.kind == "fake":
        assert af.NO_PREVIEW in out["detail"], (field, out)
    assert core.quarantined() == []
    assert (dry["preview_id"], True) not in core.previews()


def test_a_new_epoch_after_the_dry_run_is_refused_before_the_cas(core):
    """The confirmation is built from the observed source_epoch, so a moved
    epoch is refused by its own check first; source_epoch in the unit CAS is then a second
    line, not separately reachable here."""
    dry = quarantine(core, dry_run=True)
    core.set(source_epoch=2)
    calls = core.calls
    out = quarantine(core, dry_run=False, preview=dry["preview_id"])
    assert core.calls == calls  # the wrapper's own epoch check, before the core
    assert out["status"] == "refused" and "expected epoch 1, found 2" in out["detail"], out
    assert core.quarantined() == [] and core.previews() == [(dry["preview_id"], False)]


def test_only_format2_is_promised():
    """Format 2 only: a store that doesn't declare format 2, or an observation reporting
    another format, refuses; there are no plain format-1 fields any more."""
    observed = dict(unit_observation())
    v2 = type("V2", (), {"format_version": 2})()
    assert af.decision("pause", observed, store=v2)["format_version"] == 2
    for version in (1, 3):
        with pytest.raises(Refused, match=f"unsupported store format {version}"):
            af.decision("pause", dict(observed, format_version=version), store=v2)
    # The store is required; without one (None) the decision refuses.
    for store in (type("Legacy", (), {"format_version": 1})(), type("Plain", (), {})(), None):
        with pytest.raises(Refused, match="only format 2 stores are served"):
            af.decision("pause", observed, store=store)
    with pytest.raises(TypeError):
        base.decision("pause", observed, af.DECISION_FIELDS)  # no store argument at all


@pytest.mark.parametrize("missing", af.UNIT_FIELDS)
def test_a_format2_observation_missing_a_promised_field_refuses_before_any_write(core, missing):
    if core.kind == "real" and missing in UNPRODUCIBLE_ON_REAL:
        pytest.skip("a committed core always reports this field (UNPRODUCIBLE_ON_REAL)")
    core.drop(missing)
    out = quarantine(core, dry_run=True)
    assert out["status"] == "refused" and missing in out["detail"], out
    assert core.previews() == []


def test_an_unknown_format_is_refused_before_any_write(core):
    core.set(format_version=3)
    out = quarantine(core, dry_run=True)
    assert out["status"] == "refused" and core.calls == 0
    assert core.previews() == []


def test_retry_names_the_observed_whole_unit(core):
    environ = dict(
        CDC_ACTION="retry",
        CDC_TABLE=core.table,
        CDC_EXPECTED_BLOCKED_LSN=lsn_text(core.blocked),
        CDC_DRY_RUN="1",
    )
    out = af.perform(
        core.apply,
        af.parse(environ, af.TABLE_ACTION),
        run_id=str(uuid.uuid4()),
        actor="flight:test",
        owner=OWNER,
        trigger=Trigger(),
    )
    assert out["status"] == "would" and core.retried == (core.start, core.end), out


def test_a_store_declaring_format1_is_refused_before_any_preview(core):
    """The legacy refusal at the wrapper: an apply whose class declares format 1 writes
    no preview, whatever it observes."""
    core.apply.format_version = 1
    out = quarantine(core, dry_run=True)
    # The wrapper's store check, or (real core) unit_arguments on the core's format-1
    # observation: either way refused before any preview.
    assert out["status"] == "refused" and "format" in out["detail"], out
    assert core.previews() == []


def test_the_longest_unit_literal_fits_the_confirmation_limit():
    table = f"{'s' * 63}.{'t' * 63}"
    longest = af.unit_literal(table, 2**32 - 1, 2**64 - 1, 2**64 - 1)
    assert len(longest) <= 200
    assert qf._confirm(longest) == ("confirm", longest)


def test_every_reported_key_is_typed_once(core):
    """LSN_KEYS and INT_KEYS are disjoint, and every key a format-2 observation reports
    is either typed or one of the known non-numeric fields; an unknown key fails here,
    so a new core key can't be rendered as the wrong kind."""
    assert not af.LSN_KEYS & af.INT_KEYS
    known = {
        "state",
        "detail",
        "token",
        "source_held",
        "unit_digest",
        "consistent",
        "binding_hash",
        "partial",
        "generation",
        "epoch",
        "applied",
        "blocked",
        "capture_mode",  # None (keyed) or "append"
        "hold_code",  # the current hold's code, or None
    }
    for key in core.view():
        assert key in af.LSN_KEYS | af.INT_KEYS | known, key
    assert "durable_frame_end" in af.INT_KEYS and "durable_frame_end" not in af.LSN_KEYS


def test_specs_keys_and_decision_fields_are_pinned():
    """The persisted preview hash covers these fields: a change is a deliberate one."""
    assert af.DECISION_FIELDS == {
        "pause": ("state", "generation", "epoch", "format_version", "generation_id"),
        "resume": ("state", "generation", "epoch", "format_version", "generation_id"),
        "retry": ("state", "generation", "epoch", "blocked", *af.UNIT_FIELDS),
        "resync": (
            "state",
            "generation",
            "active_generation",
            "format_version",
            "generation_id",
            "capture_mode",
            "ignore_deletes",
        ),
        "resync-all": ("needs_resync",),
        "start-from-log": ("capture_mode", "generation", "format_version"),
        "start-snapshot": ("generation", "active_generation", "capture_mode", "source_held"),
        "accept-types": ("state", "generation", "epoch", "blocked", *af.UNIT_FIELDS, "type_digest"),
        "compact-now": (),
        "quarantine": ("state", "generation", "epoch", "blocked", *af.UNIT_FIELDS),
    }
    assert qf.QUARANTINE.uses == {"quarantine": ("CDC_TABLE", "CDC_EXPECTED_EPOCH", "CDC_CONFIRM")}
    assert qf.QUARANTINE.optional == {}


# Membership (the core: add_table/remove_table keep their signatures; a format-2
# observation carries the unit position, and drain_required returns before any fence,
# consume or registry change). ----------------------------------------------------------

from cdc_flight import membership  # noqa: E402
from cdc_flight import membership_flight as mf  # noqa: E402

MEMBER_OWNER = mf.Owner(
    "src",
    slot="s",
    publication="p",
    heartbeat="public.pulse",
    backfill_flight=str(uuid.UUID(int=1)),
)


def position(**changes):
    value = dict(
        format_version=2,
        source_epoch=1,
        checkpoint=END,
        confirmed_end=END,
        open_unit_anchor=None,
        partial=False,
        open_chunks=0,
        active_commit_lsn=None,
        last_member_commit_lsn=None,
        last_member_end_lsn=None,
        durable_frame_end=0,
        config_revision=1,
        binding_hash="b" * 64,
    )
    value.update(changes)
    return value


class FakeLog(FakeApply):
    """The change log as the membership wrapper sees it: execute() with {s}, a
    transaction, and refuse_lifecycle()."""

    def __init__(self):
        super().__init__()
        self.con.execute("CREATE SCHEMA s")
        self.con.execute(af.PREVIEWS_DDL.replace("{s}", "s."))

    def execute(self, sql, params=None):
        return self.con.execute(sql.replace("{s}", "s.").replace("{m}", "m."), params or [])

    def refuse_lifecycle(self):
        return None

    def previews(self):
        return self.con.execute("SELECT id,used_at IS NOT NULL FROM s.previews").fetchall()


class FakeMembership:
    """The membership core to its committed shape: the format-2 unit position is
    nested under observed["unit_position"], and drain_required returns before any fence,
    consume or registry change."""

    kind = "fake"

    def __init__(self, monkeypatch):
        self.log, self.apply, self.table = FakeLog(), None, TABLE
        self.position, self.done, self.consumed = position(), [], 0
        monkeypatch.setattr(membership, "remove_table", self.remove_table)
        monkeypatch.setattr(membership, "add_table", self.add_table)

    def add_table(
        self, log, apply, pg, publication, table, *, dry_run=False, check=None, consume=None, wait=0
    ):
        unit = dict(self.position)
        if unit.get("partial"):  # before any read, fence, consume or registry change
            detail = "an open unit must drain under its recorded configuration first"
            drain = dict(unit_position=unit)
            return membership.MembershipResult("drain_required", table, detail, [], drain)
        observed = dict(oid=101, published=True, by_oid=None, holder=None, unit_position=unit)
        if dry_run:
            return membership.MembershipResult("would", table, "add", [], observed)
        try:
            with log.transaction():
                check(observed)
                consume(observed)
                self.done.append(("add", table))
        except Refused as exc:
            return membership.MembershipResult("refused", table, str(exc), [], observed)
        return membership.MembershipResult("added", table, "added", [], observed)

    def set(self, **fields):
        self.position.update(fields)

    def drop(self, name):
        del self.position[name]

    def removed(self):
        return list(self.done)

    def previews(self):
        return self.log.previews()

    def remove_table(
        self, log, apply, table, *, force=False, dry_run=False, check=None, consume=None, wait=0
    ):
        unit = dict(self.position)
        if unit.get("partial"):  # before any fence, consume or registry change
            detail = "an open unit must drain under its recorded configuration first"
            observed = dict(unit_position=unit)
            return membership.MembershipResult("drain_required", table, detail, [], observed)
        observed = dict(row=dict(relation_id=101, state="live"), active_generation=None)
        observed["unit_position"] = unit
        if dry_run:
            return membership.MembershipResult("would", table, "remove", [], observed)
        try:
            with log.transaction():
                check(observed)
                consume(observed)
                self.consumed += 1
                self.done.append(table)
        except Refused as exc:
            return membership.MembershipResult("refused", table, str(exc), [], observed)
        return membership.MembershipResult("removed", table, "removed", [], observed)


class RealMembership:
    """The committed membership core on a real IngestLog2 and Apply2.
    set()/drop() overlay the core's own read of the unit position, as the table-action
    suite does, so an open unit or a moved checkpoint can appear on cue."""

    kind = "real"

    def __init__(self, stores, monkeypatch):
        from test_apply2 import TABLE as REAL_TABLE
        from test_apply2 import Env

        self.env = Env(stores)
        self.log, self.apply, self.table = self.env.log, self.env.apply, REAL_TABLE
        assert af.ensure_previews(self.log)
        self.overlay, self.dropped = {}, set()
        read = self.log.unit_position

        def unit_position():
            value = dict(read(), **self.overlay)
            for name in self.dropped:
                value.pop(name, None)
            return value

        monkeypatch.setattr(self.log, "unit_position", unit_position)

    def set(self, **fields):
        self.overlay.update(fields)

    def drop(self, name):
        self.dropped.add(name)

    def removed(self):
        rows = [r for r in self.log.registry() if r["relation_id"] == 101]
        return [self.table] if rows and rows[0]["held_reason"] == "removed" else []

    def previews(self):
        return self.log.execute("SELECT id,used_at IS NOT NULL FROM {s}previews").fetchall()


@pytest.fixture(params=["fake", "real"])
def member_core(request, monkeypatch):
    if request.param == "real":
        if not hasattr(membership, "_unit_observation"):
            pytest.skip("the format-2 membership core is not on this branch")
        return RealMembership(request.getfixturevalue("stores"), monkeypatch)
    return FakeMembership(monkeypatch)


def remove(core, *, dry_run, preview=None):
    environ = dict(
        CDC_ACTION="remove",
        CDC_TABLE=core.table,
        CDC_FORCE="1",  # a live, captured table is removed only when forced and confirmed
        CDC_CONFIRM=mf.confirmation(core.table),
        CDC_DRY_RUN="1" if dry_run else "0",
    )
    if preview:
        environ["CDC_PREVIEW_ID"] = preview
    return mf.perform(
        core.log,
        core.apply,
        None,
        mf.parse(environ, mf.MEMBERSHIP),
        run_id=str(uuid.uuid4()),
        actor="flight:test",
        owner=MEMBER_OWNER,
        trigger=Trigger(),
        wait=0,
    )


def open_unit(core):
    checkpoint = core.log.unit_position()["checkpoint"] if core.kind == "real" else END
    core.set(partial=True, open_unit_anchor=checkpoint, open_chunks=2)


def test_membership_dry_run_then_real_run_at_a_unit_boundary(member_core):
    dry = remove(member_core, dry_run=True)
    assert dry["status"] == "would" and dry["preview_id"], dry
    done = remove(member_core, dry_run=False, preview=dry["preview_id"])
    assert done["status"] == "done" and done["result"] == "removed", done
    assert member_core.removed() == [member_core.table]
    assert member_core.previews() == [(dry["preview_id"], True)]


def test_a_dry_run_while_a_unit_is_open_records_no_preview(member_core):
    """Nothing actionable to confirm, so no preview a later real run
    could match once the unit drains."""
    open_unit(member_core)
    out = remove(member_core, dry_run=True)
    assert out["status"] == "busy" and out["result"] == "drain_required", out
    assert "preview_id" not in out and member_core.previews() == []


def test_a_real_run_while_a_unit_is_open_spends_nothing(member_core):
    dry = remove(member_core, dry_run=True)
    open_unit(member_core)
    out = remove(member_core, dry_run=False, preview=dry["preview_id"])
    assert out["status"] == "busy" and out["result"] == "drain_required", out
    assert member_core.removed() == []
    assert member_core.previews() == [(dry["preview_id"], False)]


@pytest.mark.parametrize(
    "field,delta",
    [
        ("checkpoint", 0x40),
        ("source_epoch", 1),
        ("config_revision", 1),
        ("open_unit_anchor", None),  # an anchor recorded without a durable prefix yet
    ],
)
def test_a_moved_unit_position_makes_the_membership_preview_stale(member_core, field, delta):
    dry = remove(member_core, dry_run=True)
    current = member_core.log.unit_position() if member_core.kind == "real" else position()
    value = current["checkpoint"] if delta is None else current[field] + delta
    member_core.set(**{field: value})
    out = remove(member_core, dry_run=False, preview=dry["preview_id"])
    assert out["status"] == "refused" and af.NO_PREVIEW in out["detail"], (field, out)
    assert member_core.removed() == []
    assert member_core.previews() == [(dry["preview_id"], False)]


# The committed membership core decides drain_required from `partial` itself, so it can't
# report a position without it (the allowed list above).
MEMBER_UNPRODUCIBLE_ON_REAL = {"partial"}


@pytest.mark.parametrize("missing", mf.POSITION + ("config_revision",))
def test_a_membership_observation_missing_a_promised_field_records_nothing(member_core, missing):
    if member_core.kind == "real" and missing in MEMBER_UNPRODUCIBLE_ON_REAL:
        pytest.skip("the committed core reads this field itself (MEMBER_UNPRODUCIBLE_ON_REAL)")
    member_core.drop(missing)
    out = remove(member_core, dry_run=True)
    assert out["status"] == "refused" and missing in out["detail"], out
    assert member_core.previews() == []


class FakeAddCore:
    """The fake membership core's add path behind the shared add interface."""

    kind = "fake"

    def __init__(self, monkeypatch):
        self.member = FakeMembership(monkeypatch)
        self.log, self.apply, self.table = self.member.log, None, TABLE
        self.owner, self.pg = MEMBER_OWNER, (lambda: None)

    def set(self, **fields):
        self.member.set(**fields)

    def open_unit(self):
        self.member.set(partial=True, open_unit_anchor=END, open_chunks=2)

    def drain(self):
        self.member.set(partial=False, open_unit_anchor=None, open_chunks=0)

    def complete_unit(self):
        self.member.set(checkpoint=self.member.position["checkpoint"] + 0x40)

    def added(self):
        return ("add", self.table) in self.member.done

    def previews(self):
        return self.log.previews()


class RealAddCore:
    """The committed membership core adding a real table on PostgreSQL 55443: a
    second table in the source's schema, put in the publication by the test acting as
    the DBA. The open unit is genuine (an ingest run crashed mid-unit), the drain and a
    completed unit are real ingest runs. set() overlays the log's unit position."""

    kind = "real"

    def __init__(self, stores, pg_phase1, monkeypatch):  # noqa: F811
        from test_ingest_batching import small_transactions
        from test_ingest_pipeline import prepared

        from cdc_flight.ingest import run_once
        from cdc_flight.preflight import connect

        # Units close only at a run's heartbeat marker (or its end), so every batch
        # appended before it ends inside the open unit. With 10-commit units, PostgreSQL
        # 13/14's empty members (other sessions' commits) could keep a unit below one
        # chunk: its chunks then arrive with its close and no append ends inside it.
        self.source, self.config, self.log, _ = prepared(
            stores, pg_phase1, monkeypatch, "v2", max_commits=1_000_000
        )
        self.small, self.run_once, self.next_id = small_transactions, run_once, 1
        schema = self.source.schema
        self.table = f"{schema}.extra"
        self.source.execute(f"CREATE TABLE {schema}.extra (id integer PRIMARY KEY, v text)")
        self.source.execute(f"ALTER TABLE {schema}.extra REPLICA IDENTITY FULL")  # TOAST-capable
        self.source.execute(f"ALTER PUBLICATION {self.config.publication} ADD TABLE {schema}.extra")
        self.apply, self.pg = None, (lambda: connect(self.config))
        assert af.ensure_previews(self.log)
        self.owner = mf.Owner(
            self.config.source_id,
            slot=self.config.slot,
            publication=self.config.publication,
            heartbeat=self.config.heartbeat,
            backfill_flight=str(uuid.UUID(int=1)),
        )
        self.overlay = {}
        read = self.log.unit_position
        monkeypatch.setattr(self.log, "unit_position", lambda: dict(read(), **self.overlay))
        # Ingest lists a newly published table on its own (held, unmanaged until an add),
        # so let it see the table once now: later runs then move only the unit position,
        # never the registry rows a preview also promises.
        self.drain()
        rows = [r for r in self.log.registry() if r["table_name"] == "extra"]
        assert rows and rows[0]["held_reason"] is not None, rows

    def commit(self, count):
        self.small(self.source, count, start=self.next_id)
        self.next_id += count

    def set(self, **fields):
        self.overlay.update(fields)

    def open_unit(self):
        self.commit(120)

        def crash(stage):
            if stage == "after_append" and self.log.unit_position()["partial"]:
                raise RuntimeError("crashed with a durable open prefix")

        with pytest.raises(RuntimeError, match="durable open prefix"):
            self.run_once(self.config, self.log, fault=crash)
        assert self.log.unit_position()["partial"]

    def drain(self):
        assert self.run_once(self.config, self.log).status == "drained"
        assert not self.log.unit_position()["partial"]

    def complete_unit(self):
        before = self.log.unit_position()["checkpoint"]
        self.commit(15)
        self.drain()
        assert self.log.unit_position()["checkpoint"] > before

    def added(self):
        """Registered and captured: ingest may already list a newly published table, but
        held (unmanaged) until an add."""
        rows = [
            r for r in self.log.registry() if f"{r['schema_name']}.{r['table_name']}" == self.table
        ]
        return bool(rows) and rows[0]["held_reason"] is None

    def previews(self):
        return self.log.execute("SELECT id,used_at IS NOT NULL FROM {s}previews").fetchall()


@pytest.fixture(params=["fake", "real"])
def add_core(request, monkeypatch):
    if request.param == "real":
        if not hasattr(membership, "_unit_observation"):
            pytest.skip("the format-2 membership core is not on this branch")
        stores = request.getfixturevalue("stores")
        if stores[0].remote:
            pytest.skip("local stores only")
        return RealAddCore(stores, request.getfixturevalue("pg_phase1"), monkeypatch)
    return FakeAddCore(monkeypatch)


def add(core, *, dry_run, preview=None):
    environ = dict(CDC_ACTION="add", CDC_TABLE=core.table, CDC_DRY_RUN="1" if dry_run else "0")
    if preview:
        environ["CDC_PREVIEW_ID"] = preview
    return mf.perform(
        core.log,
        core.apply,
        core.pg,
        mf.parse(environ, mf.MEMBERSHIP),
        run_id=str(uuid.uuid4()),
        actor="flight:test",
        owner=core.owner,
        trigger=Trigger(),
        wait=0,
    )


def test_an_add_while_a_unit_is_open_is_drain_required(add_core):
    add_core.open_unit()
    out = add(add_core, dry_run=True)
    assert out["status"] == "busy" and out["result"] == "drain_required", out
    assert "preview_id" not in out and add_core.previews() == [] and not add_core.added()


def test_an_add_after_the_drain_spends_its_preview(add_core):
    add_core.open_unit()
    add_core.drain()
    dry = add(add_core, dry_run=True)
    assert dry["status"] == "would", dry.get("detail")
    done = add(add_core, dry_run=False, preview=dry["preview_id"])
    assert done["status"] == "done", done
    assert add_core.added() and add_core.previews() == [(dry["preview_id"], True)]


@pytest.mark.parametrize("move", ["unit_completes", "config_revision"])
def test_an_add_preview_goes_stale_when_the_unit_position_moves(add_core, move):
    dry = add(add_core, dry_run=True)
    assert dry["status"] == "would", dry.get("detail")
    if move == "unit_completes":
        add_core.complete_unit()
    else:
        add_core.set(config_revision=99)
    out = add(add_core, dry_run=False, preview=dry["preview_id"])
    assert out["status"] == "refused", (move, out)
    if add_core.kind == "fake":
        assert af.NO_PREVIEW in out["detail"], out
    assert not add_core.added() and add_core.previews() == [(dry["preview_id"], False)]


def test_membership_decision_fields_are_pinned():
    assert mf.DECISION_FIELDS == {
        "add": ("oid", "published", "by_oid", "holder", *mf.POSITION, "config_revision"),
        "remove": ("row", "active_generation", *mf.POSITION, "config_revision"),
    }


# Epoch (the core: preview_epoch/begin_epoch keep their signatures; on format 2
# they capture the unit position themselves and enforce every prefix bound; the new
# epoch is anchored where PostgreSQL actually advanced). --------------------------------

from cdc_flight import epoch_flight as ef  # noqa: E402
from cdc_flight.epoch import EpochError, EpochResult  # noqa: E402

EPOCH_OWNER = ef.Owner("src", slot="s", publication="p", heartbeat="public.pulse")
TARGET = END + 0x1000
OPEN = dict(
    partial=True,
    open_unit_anchor=END,
    open_chunks=2,
    active_commit_lsn=END + 0x30,
    last_member_end_lsn=END + 0x28,
    durable_frame_end=9,
)


class FakeEpochLog(FakeLog):
    def position(self):
        return 1, None, END

    def guard(self):
        return None


class FakeEpoch:
    def __init__(self):
        self.position, self.targets, self.claimed = position(**OPEN), [], 0
        self.refusal, self.short = None, 0  # short: how far the source's WAL is below the target

    def observed(self, to_lsn):
        return dict(
            epoch=1,
            reason="skip",
            slot="s",
            checkpoint=END,
            slot_present=True,
            slot_active=False,
            slot_lost=False,
            to_lsn=to_lsn,
            slot_confirmed=END,
            abandoned_bytes=to_lsn - END,
            abandoned_bytes_upper_bound=to_lsn - END,
            abandoned_bytes_current=to_lsn - END - self.short,
            **{k: v for k, v in self.position.items() if k != "checkpoint"},
        )

    def preview_epoch(self, config, log, reason, *, to_lsn=None):
        self.targets.append(to_lsn)
        if self.refusal:
            raise EpochError(self.refusal)
        return self.observed(to_lsn)

    def begin_epoch(
        self,
        config,
        log,
        new_epoch,
        reason,
        *,
        to_lsn=None,
        fault=None,
        claim=None,
        wait=0,
        started_by=None,
        deadline=None,
        **_,
    ):
        self.targets.append(to_lsn)
        if self.refusal:
            raise EpochError(self.refusal)
        claim(self.observed(to_lsn))  # spends the preview, or raises Refused
        self.claimed += 1
        actual = to_lsn + 0x10
        result = EpochResult("begun", new_epoch, "s", actual, (END, actual))
        # The committed core's v2 fields, set here so the fake also runs on bases whose
        # EpochResult predates them.
        object.__setattr__(result, "requested_to_lsn", to_lsn)
        object.__setattr__(result, "clamped", False)
        object.__setattr__(
            result,
            "details",
            dict(
                abandoned_unit_start_lsn=END,
                abandoned_frame_end=9,
                abandoned_chunk_count=2,
                abandoned_first_commit_lsn=END + 0x8,
                abandoned_last_commit_lsn=END + 0x20,
                abandoned_active_commit_lsn=END + 0x30,
                abandoned_into_epoch=new_epoch,
                abandoned_reason="skip",
            ),
        )
        return result


class FakeEpochCore:
    """The fake epoch core behind the shared interface the tests use."""

    kind = "fake"

    def __init__(self, monkeypatch):
        self.fake, self.log, self.config = FakeEpoch(), FakeEpochLog(), None
        self.owner, self.monkeypatch = EPOCH_OWNER, monkeypatch
        monkeypatch.setattr(ef, "preview_epoch", self.fake.preview_epoch)
        monkeypatch.setattr(ef, "begin_epoch", self.fake.begin_epoch)

    def target(self):
        return TARGET

    def beyond_flush(self):
        """A target PostgreSQL clamps: here the fake core reports the clamp."""
        begin = self.fake.begin_epoch

        def clamped(*args, **kwargs):
            result = begin(*args, **kwargs)
            object.__setattr__(result, "start_lsn", TARGET - 0x100)  # PG landed at its flush
            object.__setattr__(result, "clamped", True)  # and the core says so
            return result

        self.monkeypatch.setattr(ef, "begin_epoch", clamped)
        self.fake.position["clamped"] = True  # the core's preview reports the clamp
        return TARGET

    def bad_targets(self):
        self.fake.refusal = "the target is behind the durable prefix (core)"
        return [END, END - 1, END + 0x10]

    def set(self, **fields):
        self.fake.position.update(fields)

    def drop(self, name):
        del self.fake.position[name]

    def begun(self):
        return self.fake.claimed > 0

    def previews(self):
        return self.log.previews()


class RealEpochCore:
    """The committed epoch core on real PostgreSQL and a real IngestLog2, with a
    genuine durable open prefix: an ingest run is crashed after the first collected batch
    that ends inside a unit. set()/drop() overlay the core's own read of the position."""

    kind = "real"

    def __init__(self, stores, pg_phase1, monkeypatch):  # noqa: F811
        from test_ingest_batching import small_transactions
        from test_ingest_pipeline import prepared

        from cdc_flight.ingest import run_once
        from cdc_flight.preflight import query

        # Units close only at a run's heartbeat marker (or its end), so every batch
        # appended before it ends inside the open unit. With 10-commit units, PostgreSQL
        # 13/14's empty members (other sessions' commits) could keep a unit below one
        # chunk: its chunks then arrive with its close and no append ends inside it.
        self.source, self.config, self.log, _ = prepared(
            stores, pg_phase1, monkeypatch, "v2", max_commits=1_000_000
        )
        self.query, self.monkeypatch = query, monkeypatch
        small_transactions(self.source, 120)

        def crash(stage):
            if stage == "after_append" and self.log.unit_position()["partial"]:
                raise RuntimeError("crashed with a durable open prefix")

        with pytest.raises(RuntimeError, match="durable open prefix"):
            run_once(self.config, self.log, fault=crash)
        self.position = self.log.unit_position()
        assert self.position["partial"] and self.position["open_chunks"] > 0, self.position
        assert af.ensure_previews(self.log)
        self.owner = ef.Owner(
            self.config.source_id,
            slot=self.config.slot,
            publication=self.config.publication,
            heartbeat=self.config.heartbeat,
        )
        self.overlay, self.dropped = {}, set()
        read = self.log.unit_position

        def unit_position():
            value = dict(read(), **self.overlay)
            for name in self.dropped:
                value.pop(name, None)
            return value

        monkeypatch.setattr(self.log, "unit_position", unit_position)

    def flush(self):
        sql = "SELECT pg_current_wal_flush_lsn()-'0/0'::pg_lsn"
        return int(self.query(self.source.con, sql)[0][0])

    def target(self):
        return self.flush()  # beyond every durable member; at or below flush: exact

    def beyond_flush(self):
        # 1 GiB past the flush: parallel workers sharing this PostgreSQL write WAL too,
        # and a nearer target could be overtaken before the click (then nothing clamps).
        return self.flush() + (1 << 30)

    def bad_targets(self):
        """At and below the checkpoint, and inside the durable prefix: at an active
        member's commit record (strictly refused) or, between members, just below the
        last member's end (its bound is inclusive)."""
        p = self.position
        inside = p["active_commit_lsn"]
        if inside is None:
            inside = p["last_member_end_lsn"] - 1
        return [p["checkpoint"], p["checkpoint"] - 1, inside]

    def set(self, **fields):
        self.overlay.update(fields)

    def drop(self, name):
        self.dropped.add(name)

    def begun(self):
        return self.log.execute("SELECT epoch FROM {s}source WHERE id=1").fetchone()[0] > 1

    def previews(self):
        return self.log.execute("SELECT id,used_at IS NOT NULL FROM {s}previews").fetchall()


@pytest.fixture(params=["fake", "real"])
def epoch_core(request, monkeypatch):
    if request.param == "real":
        if "clamped" not in EpochResult.__dataclass_fields__:
            pytest.skip("the format-2 epoch core is not on this branch")
        stores = request.getfixturevalue("stores")
        if stores[0].remote:
            pytest.skip("local stores only")
        return RealEpochCore(stores, request.getfixturevalue("pg_phase1"), monkeypatch)
    return FakeEpochCore(monkeypatch)


def skip(core, *, dry_run, preview=None, to_lsn=None):
    to_lsn = core.target() if to_lsn is None else to_lsn
    environ = dict(
        CDC_ACTION="skip-to-lsn",
        CDC_EXPECTED_EPOCH="1",
        CDC_TO_LSN=lsn_text(to_lsn),
        CDC_CONFIRM=ef.confirmation(core.owner.source_id, "skip-to-lsn", 1, to_lsn),
        CDC_DRY_RUN="1" if dry_run else "0",
    )
    if preview:
        environ["CDC_PREVIEW_ID"] = preview
    return ef.perform(
        core.log,
        core.config,
        ef.parse(environ, ef.EPOCH),
        run_id=str(uuid.uuid4()),
        actor="flight:test",
        owner=core.owner,
        wait=0,
    )


def test_a_skip_abandons_the_whole_open_unit_and_reports_the_actual_anchor(epoch_core):
    target = epoch_core.target()
    dry = skip(epoch_core, dry_run=True, to_lsn=target)
    assert dry["status"] == "would" and "abandoned whole" in dry["detail"], dry
    checkpoint = epoch_core.position["checkpoint"] if epoch_core.kind == "real" else END
    upper = target - checkpoint
    assert f"at most {upper} bytes of WAL (to the typed target)" in dry["detail"], dry["detail"]
    assert f"; {upper} bytes exist now" in dry["detail"], dry["detail"]  # an interior target
    done = skip(epoch_core, dry_run=False, preview=dry["preview_id"], to_lsn=target)
    assert done["status"] == "done", done
    if epoch_core.kind == "fake":
        assert done["start_lsn"] == lsn_text(TARGET + 0x10)  # where PostgreSQL advanced
        assert done["requested_to_lsn"] == lsn_text(TARGET)
        assert done["clamped"] is False and "clamped" not in done["detail"]
    else:  # an interior target (at or below flush) is exact
        assert done["start_lsn"] == lsn_text(target) and "requested_to_lsn" not in done
    unit = epoch_core.position if epoch_core.kind == "real" else OPEN
    assert done["abandoned_unit_start_lsn"] == unit["open_unit_anchor"], done
    assert done["abandoned_frame_end"] == unit["durable_frame_end"], done
    actual = parse_lsn(done["start_lsn"])
    assert done["abandoned_bytes_effective"] == actual - checkpoint, done
    assert epoch_core.begun()
    assert epoch_core.previews() == [(dry["preview_id"], True)]


@pytest.mark.parametrize(
    "field,delta",
    [
        ("durable_frame_end", 1),
        ("open_chunks", 1),
        ("last_member_end_lsn", 0x20),
        ("active_commit_lsn", 0x20),
        ("open_unit_anchor", 0x10),
        ("source_epoch", 1),
    ],
)
def test_a_grown_prefix_makes_the_epoch_preview_stale(epoch_core, field, delta):
    """On the fake only the CAS can refuse; the real core may refuse the grown prefix
    itself first. Either way the transition doesn't begin and the preview stays unspent."""
    dry = skip(epoch_core, dry_run=True)
    current = epoch_core.position if epoch_core.kind == "real" else epoch_core.fake.position
    base = current[field]
    if base is None:  # between members the active commit is null: grow from the last end
        base = current["last_member_end_lsn"]
    epoch_core.set(**{field: base + delta})
    out = skip(epoch_core, dry_run=False, preview=dry["preview_id"])
    assert out["status"] == "refused", (field, out)
    if epoch_core.kind == "fake":
        assert af.NO_PREVIEW in out["detail"], (field, out)
    assert not epoch_core.begun() and epoch_core.previews() == [(dry["preview_id"], False)]


def test_the_wrapper_leaves_every_target_bound_to_the_core(epoch_core, monkeypatch):
    """No wrapper pre-check that could disagree with the core; each
    target reaches the core unchanged and the core's refusal is shown as it is."""
    seen, real = [], ef.preview_epoch

    def spied(config, log, reason, *, to_lsn=None):
        seen.append(to_lsn)
        return real(config, log, reason, to_lsn=to_lsn)

    monkeypatch.setattr(ef, "preview_epoch", spied)
    for to_lsn in epoch_core.bad_targets():
        out = skip(epoch_core, dry_run=True, to_lsn=to_lsn)
        assert seen[-1] == to_lsn
        assert out["status"] == "refused", (to_lsn, out)
    assert epoch_core.previews() == []


# The committed epoch core reads these itself while previewing (the prefix bounds and
# the epoch check), so it can't report a position without them.
EPOCH_UNPRODUCIBLE_ON_REAL = {"source_epoch", "last_member_end_lsn", "active_commit_lsn"}


@pytest.mark.parametrize("missing", ef._UNIT)
def test_an_epoch_observation_missing_a_promised_field_records_nothing(epoch_core, missing):
    if epoch_core.kind == "real" and missing in EPOCH_UNPRODUCIBLE_ON_REAL:
        pytest.skip("the committed core reads this field itself (EPOCH_UNPRODUCIBLE_ON_REAL)")
    epoch_core.drop(missing)
    out = skip(epoch_core, dry_run=True)
    assert out["status"] == "refused" and missing in out["detail"], out
    assert epoch_core.previews() == []


def test_a_target_beyond_the_sources_wal_is_clamped_and_surfaced(epoch_core):
    """A skip beyond the current flush position is clamped to it (it
    skips all WAL that exists, never more); the result and audit say so."""
    target = epoch_core.beyond_flush()
    dry = skip(epoch_core, dry_run=True, to_lsn=target)
    # The preview never promises the requested range as abandoned bytes.
    checkpoint = epoch_core.position["checkpoint"] if epoch_core.kind == "real" else END
    assert dry["status"] == "would", dry
    assert f"({target - checkpoint} bytes" not in dry["detail"], dry["detail"]
    assert (
        "current WAL" in dry["detail"]
        and f"at most the requested {lsn_text(target)}" in (dry["detail"])
    )
    done = skip(epoch_core, dry_run=False, preview=dry["preview_id"], to_lsn=target)
    assert done["status"] == "done" and done["clamped"] is True, done
    assert done["requested_to_lsn"] == lsn_text(target) and "clamped" in done["detail"]
    assert done["start_lsn"] != lsn_text(target)


def test_the_current_bytes_are_informational_and_may_grow_up_to_the_typed_target(epoch_core):
    """The decision: a clamped skip's anchor is the source's WAL at the
    click, so what exists may grow between the dry run and the click, bounded by the typed
    target. abandoned_bytes_current is shown, labelled, but never promised: the click still
    spends the preview."""
    target = epoch_core.beyond_flush()
    dry = skip(epoch_core, dry_run=True, to_lsn=target)
    assert dry["status"] == "would" and "bytes exist now" in dry["detail"], dry
    assert "abandoned_bytes_current" not in ef.DECISION_FIELDS["skip-to-lsn"]
    if epoch_core.kind == "real":
        from test_ingest_batching import small_transactions

        small_transactions(epoch_core.source, 20, start=10_000)  # the source's WAL grows
    else:
        epoch_core.fake.short -= 0x40
    done = skip(epoch_core, dry_run=False, preview=dry["preview_id"], to_lsn=target)
    assert done["status"] == "done" and done["clamped"] is True, done
    assert epoch_core.previews() == [(dry["preview_id"], True)]


def test_epoch_decision_fields_are_pinned():
    common = ("epoch", "reason", "slot", "checkpoint", "slot_present", "slot_active", "slot_lost")
    assert ef.DECISION_FIELDS == {
        "recover-slot": (*common, "new_slot", "new_slot_present", *ef._UNIT),
        "skip-to-lsn": (*common, "to_lsn", "slot_confirmed", "abandoned_bytes", *ef._UNIT),
    }


# Delete source (the core; its drop list stays PostgreSQL objects only). -----------

from cdc_flight import delete_source_flight as df  # noqa: E402
from cdc_flight.delete_source import DeleteResult  # noqa: E402

DELETE_OWNER = df.Owner("src", slot="s", publication="p", heartbeat="public.pulse")
PG_KINDS = {"slot", "publication", "heartbeat"}


class FakeDelete:
    def __init__(self):
        self.position, self.deleted = position(), 0

    def source_row(self, log):
        return 1, "s", None, (True, True, True), []

    def observed(self):
        return dict(
            epoch=1,
            lifecycle=None,
            slot="s",
            slot_active=False,
            retained_bytes=0,
            drop=[dict(kind="slot", name="s"), dict(kind="publication", name="p")],
            dba=[],
        )  # PostgreSQL objects only, as the committed core reports

    def preview_delete(self, config, log):
        return self.observed()

    def delete_source(self, config, log, *, run_id, claim=None, wait=0, started_by=None):
        observed = self.observed()
        claim(observed)  # spends the preview, or raises Refused
        self.deleted += 1
        return DeleteResult("deleted", "deleted", observed["drop"], [])


class FakeDeleteCore:
    kind = "fake"

    def __init__(self, monkeypatch):
        self.fake, self.log, self.config, self.owner = FakeDelete(), FakeLog(), None, DELETE_OWNER
        for name in ("source_row", "preview_delete", "delete_source"):
            monkeypatch.setattr(df, name, getattr(self.fake, name))
        self.log.unit_position = lambda: dict(self.fake.position)  # read by the wrapper

    def current(self):
        return dict(self.fake.position)

    def set(self, **fields):
        self.fake.position.update(fields)

    def drop(self, name):
        del self.fake.position[name]

    def deleted(self):
        return self.fake.deleted > 0

    def previews(self):
        return self.log.previews()


class RealDeleteCore:
    """The committed delete-source core on real PostgreSQL and a real IngestLog2
    (the objects this code created: slot, publication, heartbeat). set()/drop() overlay
    the log's unit position, which the wrapper reads on format 2."""

    kind = "real"

    def __init__(self, stores, pg_phase1, monkeypatch):  # noqa: F811
        from test_ingest_pipeline import prepared

        _, self.config, self.log, _ = prepared(stores, pg_phase1, monkeypatch, "v2")
        assert af.ensure_previews(self.log)
        self.owner = df.Owner(
            self.config.source_id,
            slot=self.config.slot,
            publication=self.config.publication,
            heartbeat=self.config.heartbeat,
        )
        self.overlay, self.dropped = {}, set()
        read = self.log.unit_position

        def unit_position():
            value = dict(read(), **self.overlay)
            for name in self.dropped:
                value.pop(name, None)
            return value

        monkeypatch.setattr(self.log, "unit_position", unit_position)

    def current(self):
        return self.log.unit_position()

    def set(self, **fields):
        self.overlay.update(fields)

    def drop(self, name):
        self.dropped.add(name)

    def deleted(self):
        row = self.log.execute("SELECT lifecycle FROM {s}source WHERE id=1").fetchone()
        return row[0] in ("deleting", "deleted")

    def previews(self):
        return self.log.execute("SELECT id,used_at IS NOT NULL FROM {s}previews").fetchall()


@pytest.fixture(params=["fake", "real"])
def delete_core(request, monkeypatch):
    if request.param == "real":
        from cdc_flight.ingest_log2 import IngestLog2

        if not hasattr(IngestLog2, "unit_position"):
            pytest.skip("the format-2 lifecycle core is not on this branch")
        stores = request.getfixturevalue("stores")
        if stores[0].remote:
            pytest.skip("local stores only")
        return RealDeleteCore(stores, request.getfixturevalue("pg_phase1"), monkeypatch)
    return FakeDeleteCore(monkeypatch)


def delete(core, *, dry_run, preview=None):
    environ = dict(
        CDC_ACTION="delete-source",
        CDC_EXPECTED_EPOCH="1",
        CDC_CONFIRM=df.confirmation(core.owner.source_id),
        CDC_DRY_RUN="1" if dry_run else "0",
    )
    if preview:
        environ["CDC_PREVIEW_ID"] = preview
    return df.perform(
        core.log,
        core.config,
        df.parse(environ, df.DELETE),
        run_id=str(uuid.uuid4()),
        actor="flight:test",
        owner=core.owner,
        wait=0,
    )


def test_delete_source_dry_then_real_on_format2_drops_postgres_objects_only(delete_core):
    dry = delete(delete_core, dry_run=True)
    assert dry["status"] == "would" and {d["kind"] for d in dry["drop"]} <= PG_KINDS, dry
    done = delete(delete_core, dry_run=False, preview=dry["preview_id"])
    assert done["status"] == "done" and delete_core.deleted(), done
    assert {d["kind"] for d in done["dropped"]} <= PG_KINDS
    assert delete_core.previews() == [(dry["preview_id"], True)]


@pytest.mark.parametrize(
    "field,delta",
    [("checkpoint", 0x40), ("source_epoch", 1), ("open_unit_anchor", None), ("partial", None)],
)
def test_a_moved_unit_position_makes_the_delete_preview_stale(delete_core, field, delta):
    dry = delete(delete_core, dry_run=True)
    current = delete_core.current()
    if field == "partial":
        value = not current["partial"]
    elif field == "open_unit_anchor":
        value = current["checkpoint"]
    else:
        value = current[field] + delta
    delete_core.set(**{field: value})
    out = delete(delete_core, dry_run=False, preview=dry["preview_id"])
    assert out["status"] == "refused" and af.NO_PREVIEW in out["detail"], (field, out)
    assert not delete_core.deleted()
    assert delete_core.previews() == [(dry["preview_id"], False)]


@pytest.mark.parametrize("missing", df.DECISION_FIELDS["delete-source"][6:])
def test_a_delete_observation_missing_a_promised_field_records_nothing(delete_core, missing):
    delete_core.drop(missing)
    out = delete(delete_core, dry_run=True)
    assert out["status"] == "refused" and missing in out["detail"], out
    assert delete_core.previews() == []


def test_delete_decision_fields_are_pinned():
    assert df.DECISION_FIELDS == {
        "delete-source": (
            "epoch",
            "lifecycle",
            "slot",
            "slot_active",
            "drop",
            "dba",
            "format_version",
            "source_epoch",
            "checkpoint",
            "partial",
            "open_unit_anchor",
        )
    }


def test_a_colliding_membership_observation_key_refuses(member_core):
    """A position key that shadows a different core value is refused, never
    silently promised."""
    member_core.set(active_generation="shadow")
    out = remove(member_core, dry_run=True)
    assert out["status"] == "refused" and "conflicting observation key" in out["detail"], out
    assert member_core.previews() == []


def test_a_colliding_delete_observation_key_refuses(delete_core):
    delete_core.set(slot="shadow")
    out = delete(delete_core, dry_run=True)
    assert out["status"] == "refused" and "conflicting observation key slot" in out["detail"], out
    assert delete_core.previews() == []


# The table-action and quarantine Flights refuse a store that isn't
# format 2 before any write, through main() itself. ------------------------------------


class _LocalMD:
    """md: names mapped to local files: connect and ATTACH only, never MotherDuck."""

    def __init__(self, files):
        self.files, self.real = files, duckdb.connect  # before it's patched

    def connect(self, address, *args, **kwargs):
        name = address.removeprefix("md:").split("?")[0]
        return _MappedCon(self.real(self.files[name]), self.files)


class _MappedCon:
    def __init__(self, con, files):
        self.con, self.files = con, files

    def execute(self, statement, parameters=None):
        import re

        attach = re.fullmatch(r"ATTACH IF NOT EXISTS 'md:(\w+)' AS (\"?\w+\"?)", statement)
        if attach:
            statement = f"ATTACH IF NOT EXISTS '{self.files[attach[1]]}' AS {attach[2]}"
        if statement.startswith("CREATE SNAPSHOT"):
            return None
        return self.con.execute(statement, parameters or [])

    def __getattr__(self, name):
        return getattr(self.con, name)


def _fingerprint(path):
    with duckdb.connect(path) as con:
        return con.execute(
            "SELECT schema_name,table_name,estimated_size FROM duckdb_tables() ORDER BY ALL"
        ).fetchall()


@pytest.mark.parametrize(
    "spec,environ",
    [
        (None, dict(CDC_ACTION="pause", CDC_TABLE="public.items", CDC_EXPECTED_STATE="live")),
        (
            qf.QUARANTINE,
            dict(
                CDC_ACTION="quarantine",
                CDC_TABLE="public.items",
                CDC_EXPECTED_EPOCH="1",
                CDC_CONFIRM="QUARANTINE public.items UNIT 1 0/1..0/2",
            ),
        ),
    ],
    ids=["table-action", "quarantine"],
)
def test_main_refuses_a_legacy_store_before_any_write(tmp_path, monkeypatch, capsys, spec, environ):
    """A change log without protocol_v2 (a format-1 store: its source registry only) and
    an existing mirror: main() refuses, and neither database changes, previews included."""
    import json

    owner = af.Owner(
        "src",
        backfill_flight=str(uuid.UUID(int=1)),
        compact_flight=str(uuid.UUID(int=2)),
        ops_database="ops_absent",
    )
    # Each file is named as its database, so its catalog has the production name.
    names = (owner.database, owner.mirror_database, owner.ops_database)
    files = {name: str(tmp_path / f"{name}.duckdb") for name in names}
    with duckdb.connect(files[owner.database]) as con:
        con.execute("CREATE SCHEMA src")
        con.execute("CREATE TABLE src.source (id INTEGER, epoch INTEGER)")
        con.execute("INSERT INTO src.source VALUES (1, 1)")
    with duckdb.connect(files[owner.mirror_database]) as con:
        con.execute("CREATE TABLE public_items (id INTEGER)")
    before = {name: _fingerprint(path) for name, path in files.items() if name != "ops_absent"}
    monkeypatch.setattr(af.duckdb, "connect", _LocalMD(files).connect)
    code = af.main(owner, dict(environ, CDC_DRY_RUN="1"), spec)
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == 2 and out["status"] == "refused", out
    assert "not a format-2 store" in out["detail"], out
    monkeypatch.undo()
    after = {name: _fingerprint(path) for name, path in files.items() if name != "ops_absent"}
    assert after == before
