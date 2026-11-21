"""State-independent invariant audits of a format-2 change log and its mirror.

Read-only and bounded: one READ ONLY transaction, always rolled back, of at most
three SELECTs (the catalog, the change log, the mirror). Each audit answers
`pass`, `fail` or `inconclusive`; an audit whose precondition does not hold (an
old store, a missing table or column, a deleting source, a busy connection, the
budget) is `inconclusive`, never `pass`, and a report is `ok` only when it has
findings and every one passes.

Positions are compared as (epoch, lsn) tuples, as retention does. The pair audits
compare two snapshots taken at stable points (`previous`); they hold only between
points where no writer is mid-transaction. These audits judge positions, the
retention floor and leases. They do not judge which states or transitions are
legal (the S3/S4 command specs do).

No Flight imports this module. Details name positions, owners and relation or
generation ids only, never connection strings or row data.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal

import duckdb

from cdc_flight.config import quoted
from cdc_flight.generations import ACTIVE

Verdict = Literal["pass", "fail", "inconclusive"]

# Above this many complete or abandoned units or leased/active generations
# a snapshot is not carried whole: those audits are inconclusive.
ROW_CAP = 5000

LOG_TABLES = {
    "protocol_v2": ("format_version", "min_reader"),
    "source": (
        "epoch",
        "confirmed_lsn",
        "open_unit_anchor",
        "token",
        "holder",
        "expires_at",
        "guard",
        "lifecycle",
    ),
    "source_epochs": ("epoch", "start_lsn"),
    "units_v2": (
        "epoch",
        "unit_start_lsn",
        "end_lsn",
        "complete",
        "last_commit",
        "abandoned_into_epoch",
    ),
    "frame_partitions_v2": ("partition_day", "epoch", "max_unit", "status"),
}
RETENTION_TABLES = {
    "retention": ("floor_epoch", "floor_lsn", "token", "holder", "expires_at", "guard"),
    "retention_pins": ("owner", "generation_id", "source_epoch", "pinned_lsn", "expires_at"),
}
APPLY_TABLES = {
    "apply_state": (
        "relation_id",
        "generation_id",
        "epoch",
        "applied_through_lsn",
        "token",
        "holder",
        "expires_at",
    ),
}
BLOCKED_TABLES = {
    "apply_state": ("state", "blocked_commit_lsn", "blocked_unit_start_lsn"),
}
# Lease and progress columns a backfill adds (GENERATION_COLUMNS). Without them no
# generation can hold a lease or make progress: C1m and C2m then cover apply_state
# only, and C4 and B6 are inconclusive.
GENERATION_TABLES = {
    "generations": (
        "generation_id",
        "state",
        "pin_lsn",
        "catchup_lsn",
        "token",
        "holder",
        "expires_at",
        "updated_at",
    ),
}

LOG_AUDITS = ("A1", "A2", "A3a", "A4", "A5", "C1", "C3", "C5")
LOG_PAIR_AUDITS = ("A6", "B1", "B4", "B5", "C2")
MIRROR_AUDITS = ("B3", "B7", "C1m", "C4")
MIRROR_PAIR_AUDITS = ("B2", "B6", "C2m")
NAMES = {
    "A1": "floor_below_pins",
    "A2": "floor_below_checkpoint",
    "A3a": "floor_pair",
    "A3b": "floor_shape",
    "A4": "pins_are_cuts",
    "A5": "dropped_days_below_floor",
    "A6": "floor_forward",
    "B1": "checkpoint_forward",
    "B2": "applied_forward",
    "B3": "applied_within_log",
    "B4": "pins_forward",
    "B5": "units_immutable",
    "B6": "catchup_forward",
    "B7": "blocked_pair",
    "C1": "lease_pair",
    "C1m": "lease_pair_mirror",
    "C2": "token_forward",
    "C2m": "token_forward_mirror",
    "C3": "guard_present",
    "C4": "generation_lease_scope",
    "C5": "pin_expires_unused",
}


@dataclass(frozen=True)
class Finding:
    audit: str
    verdict: Verdict
    detail: str = ""

    @property
    def name(self):
        return NAMES.get(self.audit, self.audit)


@dataclass(frozen=True)
class Budget:
    statements: int = 8
    seconds: float = 5.0


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    statements: int = 0
    elapsed_s: float = 0.0

    @property
    def ok(self):
        return bool(self.findings) and all(f.verdict == "pass" for f in self.findings)

    def verdict(self, audit):
        verdicts = {f.verdict for f in self.findings if f.audit == audit}
        for v in ("fail", "inconclusive", "pass"):
            if v in verdicts:
                return v
        return None

    def failures(self):
        return [f for f in self.findings if f.verdict != "pass"]

    def render(self):
        lines = [
            f"{len(self.findings)} findings, {self.statements} statements, {self.elapsed_s:.3f} s"
        ]
        lines += [
            f"  {f.verdict.upper():12} {f.audit} {f.name}: {f.detail}" for f in self.failures()
        ]
        return "\n".join(lines)


@dataclass(frozen=True)
class Snapshot:
    """The values the pair audits compare at the next stable point."""

    checkpoint: tuple[int, int] | None = None
    floor: tuple[int, int] | None = None
    tokens: dict[str, int] = field(default_factory=dict)
    pins: dict[str, tuple] = field(default_factory=dict)  # owner -> (generation, epoch, lsn)
    units: dict[tuple, tuple] | None = None  # (epoch, start) -> (end, complete, last, abandoned)
    applied: dict[int, tuple] | None = None  # relation -> (generation, epoch, lsn, token)
    generations: dict[str, tuple] | None = None  # id -> (pin_lsn, catchup_lsn, token)


class _Run:
    def __init__(self, con, budget):
        self.con, self.budget = con, budget
        self.report = Report()
        self.started = time.monotonic()

    def add(self, audit, verdict, detail=""):
        self.report.findings.append(Finding(audit, verdict, detail))

    def inconclusive(self, audits, detail):
        for audit in audits:
            self.add(audit, "inconclusive", detail)

    def over_budget(self):
        return (
            self.report.statements >= self.budget.statements
            or time.monotonic() - self.started > self.budget.seconds
        )

    def select(self, sql, parameters=()):
        self.report.statements += 1
        return self.con.execute(sql, list(parameters)).fetchone()


def audit(
    con,
    database,
    source_id,
    mirror=None,
    *,
    lease_seconds=None,
    previous=None,
    shape=False,
    budget=Budget(),
):
    """Audit the change log `database`.`source_id` (and the mirror database `mirror`'s
    `_cdc` metadata when given). `previous` is an earlier Snapshot of the same store;
    `shape=True` also runs A3b (only between a drop and its prune). `lease_seconds`
    bounds C4 (the largest generation lease the store's runs use). Returns (Report,
    Snapshot); the Snapshot is None when the change log could not be read."""
    run = _Run(con, budget)
    snapshot = None
    try:
        con.execute("BEGIN TRANSACTION READ ONLY")
    except duckdb.TransactionException as exc:
        if "within a transaction" not in str(exc):
            raise
        run.inconclusive(_requested(mirror, previous, shape), "connection busy: in a transaction")
        run.report.elapsed_s = time.monotonic() - run.started
        return run.report, None
    try:
        snapshot = _audit(run, database, source_id, mirror, lease_seconds, previous, shape)
    except duckdb.Error as exc:
        # The store is unavailable (a lost connection, a timeout, a server error): the
        # audits not yet answered are inconclusive, and no snapshot is carried.
        answered = {f.audit for f in run.report.findings}
        missing = [a for a in _requested(mirror, previous, shape) if a not in answered]
        run.inconclusive(missing, f"unavailable: {type(exc).__name__}")
        snapshot = None
    finally:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass  # a broken connection has no transaction left to roll back
        run.report.elapsed_s = time.monotonic() - run.started
    return run.report, snapshot


def _requested(mirror, previous, shape):
    audits = list(LOG_AUDITS) + (["A3b"] if shape else [])
    if previous is not None:
        audits += LOG_PAIR_AUDITS
    if mirror is not None:
        audits += MIRROR_AUDITS + (MIRROR_PAIR_AUDITS if previous is not None else ())
    return audits


def _columns(run, database, source_id, mirror):
    run.report.statements += 1
    rows = run.con.execute(
        "SELECT schema_name,table_name,column_name FROM duckdb_columns() "
        "WHERE (database_name=? AND schema_name=?) OR (database_name=? AND schema_name='_cdc')",
        [database, source_id, mirror or ""],
    ).fetchall()
    log, mir = {}, {}
    for schema, table, column in rows:
        (log if schema == source_id else mir).setdefault(table, set()).add(column)
    return log, mir


def _has(present, tables):
    return all(set(cols) <= present.get(table, set()) for table, cols in tables.items())


def _audit(run, database, source_id, mirror, lease_seconds, previous, shape):
    log_audits = list(LOG_AUDITS) + (["A3b"] if shape else [])
    log_pairs = list(LOG_PAIR_AUDITS) if previous is not None else []
    mirror_audits = list(MIRROR_AUDITS) if mirror is not None else []
    mirror_pairs = list(MIRROR_PAIR_AUDITS) if mirror is not None and previous is not None else []
    present, mirror_present = _columns(run, database, source_id, mirror)
    if not _has(present, LOG_TABLES):
        run.inconclusive(
            log_audits + log_pairs + mirror_audits + mirror_pairs,
            "not a format-2 change log (tables or columns missing)",
        )
        return None
    if run.over_budget():
        run.inconclusive(log_audits + log_pairs + mirror_audits + mirror_pairs, "budget")
        return None
    retention = _has(present, RETENTION_TABLES)
    s = f"{quoted(database)}.{quoted(source_id)}."
    row = run.select(_log_sql(s, retention), [ROW_CAP + 1])
    protocol, source, epochs, units, ret, pins, dropped, evidence = row
    if protocol is None or protocol["format_version"] != 2 or protocol["min_reader"] > 2:
        run.inconclusive(
            log_audits + log_pairs + mirror_audits + mirror_pairs,
            "unsupported or missing protocol_v2 row",
        )
        return None
    if source is None:
        run.inconclusive(log_audits + log_pairs + mirror_audits + mirror_pairs, "no source row")
        return None
    if source["lifecycle"]:
        run.inconclusive(
            log_audits + log_pairs + mirror_audits + mirror_pairs,
            f"source lifecycle is {source['lifecycle']}",
        )
        return None
    if source["epoch"] is None or source["confirmed_lsn"] is None:
        run.inconclusive(
            log_audits + log_pairs + mirror_audits + mirror_pairs, "source not registered"
        )
        return None
    checkpoint = (source["epoch"], source["confirmed_lsn"])
    epoch_starts = {e["epoch"]: e["start_lsn"] for e in epochs or []}
    pins = pins or []
    if retention and ret is None:
        retention = False
    floor = None
    if retention and ret["floor_epoch"] is not None and ret["floor_lsn"] is not None:
        floor = (ret["floor_epoch"], ret["floor_lsn"])
    units_map = None
    if units is not None and len(units) <= ROW_CAP:
        units_map = {
            (u["epoch"], u["start"]): (
                u["end_lsn"],
                u["complete"],
                u["last_commit"],
                u["abandoned"],
            )
            for u in units
        }
    elif units is None:
        units_map = {}

    _log_audits(
        run, source, checkpoint, epoch_starts, retention, ret, floor, pins, dropped, evidence, shape
    )
    tokens = {"source": source["token"]}
    if retention:
        tokens["retention"] = ret["token"]
    pin_map = {p["owner"]: (p["generation_id"], p["epoch"], p["lsn"]) for p in pins}
    if previous is not None:
        _log_pairs(
            run, previous, checkpoint, epoch_starts, retention, floor, tokens, pin_map, units_map
        )

    applied = generations = None
    if mirror is not None:
        if run.over_budget():
            run.inconclusive(mirror_audits + mirror_pairs, "budget")
        elif not _has(mirror_present, APPLY_TABLES):
            run.inconclusive(mirror_audits + mirror_pairs, "mirror apply_state missing or old")
        else:
            m = f"{quoted(mirror)}._cdc."
            leased = _has(mirror_present, GENERATION_TABLES)
            # Format 2's blocked unit start (V2_COLUMNS): without it, B7 is inconclusive.
            blocked = _has(mirror_present, BLOCKED_TABLES)
            parameters = [lease_seconds or 0, *ACTIVE, ROW_CAP + 1] if leased else []
            states, gens = run.select(_mirror_sql(m, leased, blocked), parameters)
            states, gens = states or [], gens or []
            applied = {
                a["relation_id"]: (a["generation_id"], a["epoch"], a["applied"], a["token"])
                for a in states
            }
            if leased and len(gens) <= ROW_CAP:
                generations = {
                    g["generation_id"]: (g["pin_lsn"], g["catchup_lsn"], g["token"]) for g in gens
                }
            _mirror_audits(run, checkpoint, states, gens, lease_seconds, leased, blocked)
            if previous is not None:
                _mirror_pairs(run, previous, applied, generations, leased)
    return Snapshot(checkpoint, floor, tokens, pin_map, units_map, applied, generations)


def _log_sql(s, retention):
    # One row of nested values, so the change log is one statement.
    protocol = f"(SELECT struct_pack(format_version,min_reader) FROM {s}protocol_v2 WHERE id=1)"
    source = (
        "(SELECT struct_pack(epoch,confirmed_lsn,open_unit_anchor,token,lifecycle,"
        "holder_set:=holder IS NOT NULL,expires_set:=expires_at IS NOT NULL,"
        f"guard_null:=guard IS NULL) FROM {s}source WHERE id=1)"
    )
    epochs = f"(SELECT list(struct_pack(epoch,start_lsn)) FROM {s}source_epochs)"
    units = (
        "(SELECT list(struct_pack(epoch:=epoch::BIGINT,start:=unit_start_lsn,end_lsn,complete,"
        "last_commit,abandoned:=abandoned_into_epoch)) FROM (SELECT * FROM "
        f"{s}units_v2 WHERE complete OR abandoned_into_epoch IS NOT NULL "
        "ORDER BY epoch DESC,unit_start_lsn DESC LIMIT ?))"
    )
    if not retention:
        return f"SELECT {protocol},{source},{epochs},{units},NULL,NULL,NULL,NULL"
    ret = (
        "(SELECT struct_pack(floor_epoch,floor_lsn,token,holder_set:=holder IS NOT NULL,"
        f"expires_set:=expires_at IS NOT NULL,guard_null:=guard IS NULL) FROM {s}retention "
        "WHERE id=1)"
    )
    pins = (
        "(SELECT list(struct_pack(owner,generation_id,epoch:=source_epoch,lsn:=pinned_lsn,"
        "expires_set:=expires_at IS NOT NULL,"
        f"cut:=EXISTS (SELECT 1 FROM {s}source_epochs e WHERE e.epoch=p.source_epoch "
        "AND e.start_lsn=p.pinned_lsn) OR EXISTS (SELECT 1 FROM "
        f"{s}units_v2 u WHERE u.epoch=p.source_epoch AND u.complete "
        f"AND u.end_lsn=p.pinned_lsn)) ORDER BY owner) FROM {s}retention_pins p)"
    )
    # Dropped days whose cut is above the floor (or with no floor). A day's cut is its
    # highest unit's END when that unit is complete (drop_partition's high), else the
    # unit's start; a unit already pruned was at or below the floor, so its start serves.
    dropped = (
        "(SELECT list(struct_pack(day:=d.partition_day,epoch:=d.epoch::BIGINT,cut:=d.cut)) "
        "FROM (SELECT p.partition_day,p.epoch,CASE WHEN u.complete THEN u.end_lsn "
        f"ELSE p.max_unit END AS cut FROM {s}frame_partitions_v2 p LEFT JOIN {s}units_v2 u "
        "ON u.epoch=p.epoch AND u.unit_start_lsn=p.max_unit "
        "WHERE p.status='dropped' AND p.max_unit IS NOT NULL) d, "
        f"{s}retention r WHERE r.id=1 AND (r.floor_epoch IS NULL OR d.epoch>r.floor_epoch "
        "OR (d.epoch=r.floor_epoch AND d.cut>r.floor_lsn)))"
    )
    # A3b: the floor is an epoch start, a complete unit end or an abandoned unit's start.
    evidence = (
        f"(SELECT EXISTS (SELECT 1 FROM {s}source_epochs e WHERE e.epoch=r.floor_epoch "
        "AND e.start_lsn=r.floor_lsn) OR EXISTS (SELECT 1 FROM "
        f"{s}units_v2 u WHERE u.epoch=r.floor_epoch AND ((u.complete AND u.end_lsn=r.floor_lsn) "
        "OR (u.abandoned_into_epoch IS NOT NULL AND u.unit_start_lsn=r.floor_lsn))) "
        f"FROM {s}retention r WHERE r.id=1)"
    )
    return f"SELECT {protocol},{source},{epochs},{units},{ret},{pins},{dropped},{evidence}"


def _mirror_sql(m, leased, blocked):
    active = ",".join("?" * len(ACTIVE))  # S3's spec may add an active state
    held = (
        ",state,blocked_commit:=blocked_commit_lsn,blocked_start:=blocked_unit_start_lsn"
        if blocked
        else ""
    )
    states = (
        "(SELECT list(struct_pack(relation_id,generation_id,epoch,applied:=applied_through_lsn,"
        "token,holder_set:=holder IS NOT NULL,expires_set:=expires_at IS NOT NULL"
        f"{held}) "
        f"ORDER BY relation_id) FROM {m}apply_state)"
    )
    # Only rows that can hold or have held a lease: active, or with a lease column set.
    gens = (
        "(SELECT list(struct_pack(generation_id,state,pin_lsn,catchup_lsn,token,"
        "holder_set:=holder IS NOT NULL,expires_set:=expires_at IS NOT NULL,"
        "updated_null:=updated_at IS NULL,"
        # C4's bound, compared on the server: no timestamp crosses to the client.
        "late:=expires_at>updated_at+?*INTERVAL '1 second')) FROM (SELECT * FROM "
        f"{m}generations WHERE state IN ({active}) OR holder IS NOT NULL OR expires_at IS NOT NULL "
        "ORDER BY generation_id LIMIT ?))"
    )
    return f"SELECT {states},{gens if leased else 'NULL'}"


def _pos(epoch, lsn):
    return f"({epoch},{lsn})"


def _check(run, audit, failures):
    if failures:
        run.add(audit, "fail", "; ".join(failures[:5]) + (" …" if len(failures) > 5 else ""))
    else:
        run.add(audit, "pass")


def _log_audits(
    run, source, checkpoint, epoch_starts, retention, ret, floor, pins, dropped, evidence, shape
):
    no_retention = ["A1", "A2", "A3a", "A4", "A5", "C5"] + (["A3b"] if shape else [])
    if not retention:
        run.inconclusive(no_retention, "retention not initialized")
    else:
        protected = [(p["epoch"], p["lsn"], p["owner"]) for p in pins]
        if source["open_unit_anchor"] is not None:
            protected.append((source["epoch"], source["open_unit_anchor"], "open unit anchor"))
        _check(
            run,
            "A1",
            []
            if floor is None
            else [
                f"floor {_pos(*floor)} not below {owner} {_pos(e, lsn)}"
                for e, lsn, owner in protected
                if floor >= (e, lsn)
            ],
        )
        _check(
            run,
            "A2",
            []
            if floor is None or floor < checkpoint
            else [f"floor {_pos(*floor)} not below checkpoint {_pos(*checkpoint)}"],
        )
        pair = (ret["floor_epoch"] is None) == (ret["floor_lsn"] is None)
        a3a = [] if pair else [f"floor half set: {_pos(ret['floor_epoch'], ret['floor_lsn'])}"]
        if pair and floor is not None and floor[0] not in epoch_starts:
            a3a.append(f"floor epoch {floor[0]} has no source_epochs row")
        _check(run, "A3a", a3a)
        if shape:
            if floor is None:
                run.add("A3b", "pass")
            elif evidence:
                run.add("A3b", "pass")
            else:
                run.add(
                    "A3b",
                    "inconclusive",
                    f"no epoch start or unit at floor {_pos(*floor)} (pruned?)",
                )
        _check(
            run,
            "A4",
            [
                f"{p['owner']} at {_pos(p['epoch'], p['lsn'])} is not an epoch "
                "start or a complete unit end"
                for p in pins
                if not p["cut"]
            ],
        )
        _check(
            run,
            "A5",
            [
                f"dropped day {d['day']} epoch {d['epoch']} has a cut at "
                f"{d['cut']} above the floor "
                f"{'(none)' if floor is None else _pos(*floor)}"
                for d in dropped or []
            ],
        )
        _check(
            run,
            "C5",
            [
                f"{p['owner']} has expires_at set (no writer sets it)"
                for p in pins
                if p["expires_set"]
            ],
        )
    lease = [("source", source)] + ([("retention", ret)] if retention else [])
    _check(
        run,
        "C1",
        [
            f"{name} lease half set (holder {'set' if row['holder_set'] else 'NULL'}"
            f", expires_at {'set' if row['expires_set'] else 'NULL'})"
            for name, row in lease
            if row["holder_set"] != row["expires_set"]
        ],
    )
    c3 = []
    if source["guard_null"] and source["token"] != 0:
        c3.append(f"source guard NULL with token {source['token']}")
    if retention and ret["guard_null"]:
        c3.append("retention guard NULL")
    _check(run, "C3", c3)


def _log_pairs(run, previous, checkpoint, epoch_starts, retention, floor, tokens, pins, units):
    if previous.checkpoint is None:
        run.inconclusive(LOG_PAIR_AUDITS, "previous snapshot has no change-log values")
        return
    if not retention:
        run.inconclusive(["A6", "B4"], "retention not initialized")
    else:
        a6 = []
        if previous.floor is not None and (floor is None or floor < previous.floor):
            a6.append(
                f"floor moved from {_pos(*previous.floor)} to "
                f"{'NULL' if floor is None else _pos(*floor)}"
            )
        _check(run, "A6", a6)
        b4 = []
        for owner, (generation, epoch, lsn) in previous.pins.items():
            now = pins.get(owner)
            # A vanished owner (release, expire, transfer sweep, remove-table, sweep_pins) and
            # a transfer to another generation (which may lower the pin) are not decreases.
            if now is None or now[0] != generation:
                continue
            if (now[1], now[2]) < (epoch, lsn):
                b4.append(f"{owner} moved from {_pos(epoch, lsn)} to {_pos(now[1], now[2])}")
        _check(run, "B4", b4)
    b1 = []
    if checkpoint < previous.checkpoint:
        b1.append(f"checkpoint moved from {_pos(*previous.checkpoint)} to {_pos(*checkpoint)}")
    missing = [
        e for e in range(previous.checkpoint[0] + 1, checkpoint[0] + 1) if e not in epoch_starts
    ]
    if missing:
        b1.append(f"epochs {missing} have no source_epochs row")
    _check(run, "B1", b1)
    if units is None or previous.units is None:
        run.inconclusive(["B5"], f"more than {ROW_CAP} complete or abandoned units")
    else:
        b5 = []
        for key, before in previous.units.items():
            now = units.get(key)
            if now is None:
                end = before[0] if before[0] is not None else key[1]
                if floor is None or (key[0], end) > floor:
                    b5.append(f"unit {_pos(*key)} vanished above the floor")
            elif now != before:
                b5.append(f"unit {_pos(*key)} changed after it was complete or abandoned")
        _check(run, "B5", b5)
    c2 = [
        f"{name} token moved from {previous.tokens[name]} to {token}"
        for name, token in tokens.items()
        if name in previous.tokens and token < previous.tokens[name]
    ]
    _check(run, "C2", c2)


def _blocked_pair(a):
    """B7 for one apply_state row: the blocked commit and its unit start are NULL
    together; when set, the start is the applied position and at or below the commit.
    A hold written before the start column existed (commit set, start NULL, not live)
    is legacy and allowed."""
    commit, start = a["blocked_commit"], a["blocked_start"]
    if commit is None and start is None:
        return None
    if start is None:
        return None if a["state"] != "live" else "live with a blocked commit and no unit start"
    if commit is None:
        return f"blocked unit start {start} without a blocked commit"
    if start > commit:
        return f"blocked unit start {start} above the blocked commit {commit}"
    if start != a["applied"]:
        return f"blocked unit start {start} is not the applied position {a['applied']}"
    return None


def _mirror_audits(run, checkpoint, states, gens, lease_seconds, leased, blocked):
    _check(
        run,
        "B3",
        [
            f"relation {a['relation_id']} applied {_pos(a['epoch'], a['applied'])} beyond the "
            f"checkpoint {_pos(*checkpoint)}"
            for a in states
            if a["epoch"] is not None
            and a["applied"] is not None
            and (a["epoch"], a["applied"]) > checkpoint
        ],
    )
    if blocked:
        b7 = [(a["relation_id"], _blocked_pair(a)) for a in states]
        _check(run, "B7", [f"relation {rel}: {why}" for rel, why in b7 if why])
    else:
        run.add("B7", "inconclusive", "apply_state has no blocked_unit_start_lsn (old mirror)")
    capped = len(gens) > ROW_CAP
    c1 = [
        f"relation {a['relation_id']} lease half set"
        for a in states
        if a["holder_set"] != a["expires_set"]
    ]
    c1 += [
        f"generation {g['generation_id']} lease half set"
        for g in gens[:ROW_CAP]
        if g["holder_set"] != g["expires_set"]
    ]
    if capped and not c1:
        run.add("C1m", "inconclusive", f"more than {ROW_CAP} leased or active generations")
    else:
        _check(run, "C1m", c1)
    if not leased:
        run.add("C4", "inconclusive", "generations has no lease columns (no backfill yet)")
        return
    if lease_seconds is None:
        run.add("C4", "inconclusive", "no lease_seconds bound given")
        return
    if capped:
        run.add("C4", "inconclusive", f"more than {ROW_CAP} leased or active generations")
        return
    c4, unknown = [], []
    for g in gens:
        if g["state"] in ACTIVE or not g["expires_set"]:
            continue
        if g["updated_null"]:
            unknown.append(g["generation_id"])
        elif g["late"]:
            # acquire and guard require ACTIVE; the last guard shares the state change's
            # transaction (and current_timestamp), so a lease past this was taken after it.
            c4.append(
                f"generation {g['generation_id']} ({g['state']}) lease runs past its "
                f"last update + {lease_seconds} s"
            )
    if c4:
        _check(run, "C4", c4)
    elif unknown:
        run.add("C4", "inconclusive", f"ended generations {unknown[:5]} have no updated_at")
    else:
        run.add("C4", "pass")


def _mirror_pairs(run, previous, applied, generations, leased):
    if previous.applied is None:
        run.inconclusive(MIRROR_PAIR_AUDITS, "previous snapshot has no mirror values")
        return
    b2, c2 = [], []
    for relation, (generation, epoch, lsn, token) in previous.applied.items():
        now = applied.get(relation)
        # A retired relation's row is deleted; a publication starts a new generation
        # (its position resets to the marker or the epoch start).
        if now is None or now[0] != generation:
            continue
        if lsn is not None and (now[1] is None or (now[1], now[2]) < (epoch, lsn)):
            b2.append(
                f"relation {relation} moved from {_pos(epoch, lsn)} to "
                f"{_pos(now[1], now[2])} in generation {generation}"
            )
        if token is not None and (now[3] is None or now[3] < token):
            c2.append(f"relation {relation} token moved from {token} to {now[3]}")
    _check(run, "B2", b2)
    if not leased:
        run.add("B6", "inconclusive", "generations has no progress columns (no backfill yet)")
        _check(run, "C2m", c2)
        return
    if generations is None or previous.generations is None:
        run.inconclusive(["B6"], f"more than {ROW_CAP} leased or active generations")
        if c2:
            _check(run, "C2m", c2)
        else:
            run.add("C2m", "inconclusive", f"more than {ROW_CAP} leased or active generations")
        return
    b6 = []
    for generation, (pin_lsn, catchup, token) in previous.generations.items():
        now = generations.get(generation)
        if now is None:  # ended and released: no longer leased or active
            continue
        if now[0] != pin_lsn:
            b6.append(f"generation {generation} pin_lsn changed from {pin_lsn} to {now[0]}")
        if catchup is not None and (now[1] is None or now[1] < catchup):
            b6.append(f"generation {generation} catchup moved from {catchup} to {now[1]}")
        if token is not None and (now[2] is None or now[2] < token):
            c2.append(f"generation {generation} token moved from {token} to {now[2]}")
    _check(run, "B6", b6)
    _check(run, "C2m", c2)
