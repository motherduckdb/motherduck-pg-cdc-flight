"""The apply driver: one run over a source's tables (the apply command only).

`ApplyRun` adds the run loop, poison isolation and the idle check to `Apply`. It lives
apart from apply.py because backfill imports apply.py (Backfill subclasses Apply) but
never runs the apply loop.

Format-2 batches take one of two paths, chosen per table and page inside `_read_batch`:
- the SQL fold (sql_fold.py), the default for the common case: one plan statement (the
  page's frame checks plus what the decision needs), then one MERGE whose source folds the
  stored frames where they are. No frame reaches the client.
- the Python fold (the frames read as rows, mirror2/mirror.fold, a staged MERGE), for every
  batch the plan rules out of the common case: quarantined units, a blocking or malformed
  event, a key change, more than one schema version or key layout, a key layout other than
  the mirror's, a reconcile refusal. That path raises, holds and bisects exactly as before.
`fold="python"` (constructor only; tests use it as the oracle) forces the Python path.
The SQL path's per-table pieces (the batch types, the decision, the MERGE write) live in
sql_path.py; the schedule (the run loop, the page plan and its cache) stays here.
"""

import time
import uuid
from dataclasses import replace

import duckdb

from cdc_flight import catalog_checks, sql_fold
from cdc_flight.apply import (
    DATA_ERRORS,
    POISON,
    Apply,
    LeaseBusy,
    TableResult,
    _DryRun,
    key_limit_from,
    logger,
    next_step,
    source_from,
)
from cdc_flight.generation_spec import InvariantViolation
from cdc_flight.ingest_common import SourceMismatch, append_refusal
from cdc_flight.mirror import Blocked
from cdc_flight.retention import HistoryDropped, PinMissing, is_conflict, refuse_lifecycle
from cdc_flight.spool2 import logical_events
from cdc_flight.sql_path import (
    CACHE_CAP_BYTES,
    PageFacts,
    SqlBatch,
    SqlImages,
    SqlWrite,
    decide,
    versions_of,
)


class UnitBatch:
    """One table's reads for one page: the units, its logical events, descriptors."""

    def __init__(self, page, events, descriptors, skip):
        self.page, self.events, self.descriptors, self.skip = page, events, descriptors, skip


class Counted:
    """`usable` for a SQL batch: _apply_table only takes len() (events) and the distinct
    commit_lsn values (transactions) from it."""

    def __init__(self, events, commits):
        self.events, self.commits = events, commits

    def __len__(self):
        return self.events

    def __iter__(self):
        return ({"commit_lsn": i} for i in range(self.commits))


class PagePlan:
    """A page's plan for the run: its descriptors, frames SQL, and per-relation facts.
    `batched`: the batched statement succeeded (an absent relation then has no events)."""

    def __init__(self, descriptors, frames):
        self.descriptors, self.frames = descriptors, frames
        self.rows, self.layouts_ambiguous, self.batched = {}, set(), False
        self.judged = frozenset()  # the relations the batched statement judged
        self.oids = frozenset(oid for (oid, _, _) in descriptors)  # relations in the page


# Errors from the batched page plan that fall back to per-table planning: the planner or
# the engine's capacity, never a stop (InterruptException), I/O, a transaction conflict or
# a check failure, which propagate or map as before.
# InternalException can invalidate the DuckDB database; the per-table fallback then fails
# loudly, which is the intended outcome.
BATCH_FALLBACK = (
    duckdb.BinderException,
    duckdb.ParserException,
    duckdb.NotImplementedException,
    duckdb.OutOfMemoryException,
    duckdb.InternalException,
)


class _Open:
    """One table's state across the run's rounds, under its lease (_schedule)."""

    def __init__(self, table, run_id, relation_id, token, current, applied, renewed):
        self.table, self.run_id, self.relation_id, self.token = table, run_id, relation_id, token
        self.current, self.committed = current, applied
        self.events = self.transactions = 0
        self.status, self.detail = None, ""  # status: None while the table steps
        self.pinned = None  # (generation, epoch, lsn) the table pin is known to hold
        self.unit_ends = set()  # unit ends paged for this table (its units count)
        self.index = None  # position in the run's table list (results keep its order)
        self.result = None
        self.closed = False
        self.renewed = renewed  # when the lease was last taken or renewed (monotonic)


class ApplyRun(SqlWrite, Apply):
    def __init__(self, con, config, *, retention=None, fold="sql"):
        if fold not in ("sql", "python"):
            raise ValueError("fold must be 'sql' or 'python'")
        super().__init__(con, config, retention=retention)
        self.fold = fold
        # Per batch: "sql", or "fallback:<reason>" (the Python path). Never persisted.
        self.fold_stats = {}
        self._sql_batch = None  # the SQL batch being written (isolate_poison re-reads it)
        self._skip = ()  # the current page's quarantine skip set
        self._unit_ends = set()  # unit ends paged by the table being stepped (_Open's)
        # The schedule's rounds; two-step rounds are those where a lagging group paged twice.
        self.round_stats = dict(
            rounds=0, max_round_seconds=0.0, two_step_rounds=0, max_two_step_round_seconds=0.0
        )
        self._cache = None  # (page key, {oid: [events]}, descriptors): LRU 1, one run
        self.cache_stats = {"fills": 0, "hits": 0, "uncached": 0}
        # The SQL fold's page plans ({page key: PagePlan}): one per checkpoint group, one run.
        self._plan_cache = None
        self.plan_stats = {"fills": 0, "hits": 0, "batched_failed": 0}
        self._run_tables = None  # the tables of the current run() (None: config.tables)

    def _redo_on_python(self, relation_id, token, current, error):
        """A data error from the SQL MERGE (rolled back): read the same page on the Python
        path, fold it and write it once. That path's own outcome stands: a commit (with any
        hold it cuts at), its baseline precheck's hold (the SQL MERGE may fail on a cast
        before its ERROR clause fires), or a data error, bisected as today. The re-read's
        holds are mapped here as _apply_table maps reads; a TransactionException from it
        propagates (reads take no retention guard, so it isn't a conflict to retry).
        Returns isolate_poison's (usable, blocked, committed)."""
        batch = self._sql_batch
        try:
            events, descriptors = self._page_events(relation_id, batch.page, batch.high)
        except (PinMissing, HistoryDropped) as exc:
            blocked = Blocked("needs_resync", str(exc), code="history_lost")
            self.hold(relation_id, token, current, blocked)
            return [], blocked, current["applied"]
        except SourceMismatch as exc:
            blocked = Blocked("needs_action", str(exc), code="source_mismatch")
            self.hold(relation_id, token, current, blocked)
            return [], blocked, current["applied"]
        python = UnitBatch(batch.page, events, descriptors, self._skip)
        usable, columns, images, boundary, blocked = ApplyRun._fold_read_batch(
            self, relation_id, current, python
        )
        try:
            self.write(relation_id, token, current, columns, images, boundary, blocked)
        except Blocked as exc:
            self.hold(relation_id, token, current, exc)  # as _apply_table does
            return [], exc, current["applied"]
        except DATA_ERRORS as exc:
            return self.isolate_poison(relation_id, token, current, usable, exc)
        logger.info(
            "apply: the Python fold committed a batch the SQL MERGE refused (%s)",
            type(error).__name__,
        )
        return usable, blocked, boundary

    def _probe(self, relation_id, token, current, events):
        """True when applying exactly these events succeeds; always rolled back."""
        _, columns, images, _ = self.fold_batch(relation_id, current, events)
        try:
            self.write(relation_id, token, current, columns, images, 0, None, dry_run=True)
        except _DryRun:
            return True
        except (Blocked, *DATA_ERRORS):
            return False
        return True

    def isolate_poison(self, relation_id, token, current, usable, error):
        """v1's bisect, by whole units: apply the clean units before the poison unit
        and hold at its start. The hold names the unit's first commit for this table
        (the commit diagnosis; quarantine and retry act on the whole unit).

        A data error from the SQL fold's MERGE first redoes the batch on the Python fold
        (_redo_on_python), so the batch resolves exactly as that path would."""
        if isinstance(usable, Counted):
            return self._redo_on_python(relation_id, token, current, error)
        starts = sorted({e["unit_start_lsn"] for e in usable})

        def prefix(count):
            return [
                e for e in usable if count == len(starts) or e["unit_start_lsn"] < starts[count]
            ]

        good, bad = 0, len(starts)
        while bad - good > 1:
            middle = (good + bad) // 2
            if self._probe(relation_id, token, current, prefix(middle)):
                good = middle
            else:
                bad = middle
        poison_unit = starts[good]
        clean = prefix(good)
        commit = min(e["commit_lsn"] for e in usable if e["unit_start_lsn"] == poison_unit)
        _, columns, images, _ = self.fold_batch(relation_id, current, clean)
        blocked = Blocked(
            "needs_action", POISON.format(type(error).__name__, commit), commit, code="poison"
        )
        blocked.unit_start_lsn = poison_unit
        self.write(relation_id, token, current, columns, images, poison_unit, blocked)
        return clean, blocked, poison_unit

    # Driver ---------------------------------------------------------------

    def _open(self, table, run_id):
        """A table's run start: the head shortcuts that need no lease, then its lease.
        Returns a finished TableResult, or the _Open state of a table to step. A finished
        table with a mirror state counts 0 units (it paged nothing)."""
        head = self.head(table)  # before the lease
        state = self._open_head(head, table, run_id)
        if isinstance(state, TableResult) and head["current"] is not None:
            state = replace(state, units=0)
        return state

    def _open_head(self, head, table, run_id):
        # A deleting or deleted source applies nothing (read with the head: no
        # extra round trip), before the idle and held-table shortcuts.
        refuse_lifecycle(self.config.source_id, (head["source"] or {}).get("lifecycle"))
        relation_id, current = head["relation_id"], head["current"]
        if head["key"] and head["key"]["held_reason"]:
            # The source table is gone, renamed or removed (the registry holds it; see
            # preflight.classify). Reported, never a failure, and never the idle path.
            applied = current["applied"] if current else 0
            key = head["key"]
            detail = f"source table {key['held_reason']}: {key['held_detail'] or ''}".strip()
            return TableResult(table, "source_missing", applied, detail=detail)
        key = head["key"]
        refusal = key and append_refusal(
            table,
            key["capture_mode"],
            key["key_columns"],
            key["catalog_key"],
            key["held_reason"],
            append_ok=True,
        )
        if not refusal and key and current and current["capture_mode"] != key["capture_mode"]:
            # The registry and the generation disagree on the mode (a remove and re-add in
            # the other mode, before its new generation): never applied in either mode.
            refusal = (
                f"{table}: registered for {key['capture_mode'] or 'keyed'} capture, but its "
                f"mirror generation is {current['capture_mode'] or 'keyed'}: start a new one"
            )
        if refusal:
            # An unknown capture mode, an inconsistent registry, or a mode mismatch:
            # never applied here, never written, reported as needing an operator.
            applied = current["applied"] if current else 0
            return TableResult(table, "needs_action", applied, detail=refusal)
        if current is None:
            return TableResult(table, "awaiting_generation", 0)
        if current["state"] != "live":
            return TableResult(table, current["state"], current["applied"])
        if head["held"]:
            return TableResult(table, "busy", current["applied"])
        if self.idle(head):
            # Nothing to apply, hold or repair: no lease needed. Same decision as the
            # loop head's first iteration (next_step) plus a pin already at applied.
            result = TableResult(table, "caught_up", current["applied"])
            self._record(run_id, relation_id, result)
            return result
        try:
            token = self.acquire(relation_id, run_id, token=current["token"])
        except (LeaseBusy, duckdb.TransactionException) as exc:
            if isinstance(exc, duckdb.TransactionException) and "conflict" not in str(exc).lower():
                raise
            return TableResult(table, "busy", current["applied"])
        return _Open(
            table, run_id, relation_id, token, current, current["applied"], time.monotonic()
        )

    def _renew(self, t):
        """Keep a run-long lease alive: every write renews it (guard), and a
        table that steps without writing for a third of the lease renews it here, so no
        round or idle stretch outlives it. No statement in the steady state."""
        now = time.monotonic()
        if now - t.renewed >= self.config.lease_seconds / 3:
            self.guard(t.relation_id, t.token)  # raises FenceLost when the lease is gone
            t.renewed = now

    def _step(self, t, round_high, cap, fault):
        """One page of an open table, read and acted on under its lease: today's loop
        iteration. `round_high` is the round's confirmed position (None: this table's read
        sets it), `cap` the next higher group's checkpoint (None: the top group).
        Returns the round's confirmed position after this table's read; sets t.status
        when the table is finished."""
        relation_id, token = t.relation_id, t.token
        self._unit_ends = t.unit_ends  # _fold_read_batch's per-table unit ends
        self._renew(t)
        head = self.head(t.table)
        current = t.current = head["current"]
        t.committed = current["applied"]
        pin = head["pin"]
        if pin and pin["generation"] == current["generation"]:
            t.pinned = (pin["generation"], pin["epoch"], pin["lsn"])
        epoch, confirmed = source_from(head["source"], self.config.epoch, self.config.source_id)
        # The round shares one confirmed position (its page keys line up; S2a): the first
        # table's read, never one read later in the round.
        round_high = confirmed if round_high is None else min(round_high, confirmed)
        changed, limit = self._limit(head, current, confirmed)
        step, value = next_step(current, epoch, confirmed, changed, limit)
        if step == "hold" and changed and current["capture_mode"] == "append":
            # An append table's catalog hold: needs_action, left by start-from-log.
            reason = head["append_hold"]["reason"]
            value = Blocked(
                "needs_action",
                catalog_checks.hold_detail(reason, append=True) or reason,
                code=reason,  # an APPEND_REASONS key
            )
        elif step == "hold" and changed and head.get("cut_reason"):
            # A catalog hold (catalog_checks) at the key-change cut: say which DDL.
            detail = catalog_checks.hold_detail(head["cut_reason"])
            code = head["cut_reason"] if detail else value.code  # a REASONS key
            value = Blocked(value.state, detail or value.reason, code=code)
        if step == "hold":
            self.hold(relation_id, token, current, value)
            t.status, t.detail = value.state, value.reason
            return round_high
        if step == "done":
            t.status = "caught_up"
            return round_high
        # This table's own key limit stays its own (never past a pending key cut).
        high = min(value, round_high, cap) if cap is not None else min(value, round_high)
        if high <= current["applied"]:
            return round_high  # at the round's position: steps again next round
        try:
            try:
                self.ensure_pin(relation_id, current, pin=pin)
            except InvariantViolation as exc:
                # The pin transfer (rolled back) found the registry inconsistent (oracle
                # S4): this table alone is held, the others run on. Only ensure_pin's: a
                # future invariant elsewhere isn't labelled a pin's.
                blocked = Blocked("needs_action", str(exc), code="pin_invariant")
                self.hold(relation_id, token, current, blocked)
                t.status, t.detail = blocked.state, blocked.reason
                return round_high
            # Quarantined commits of this generation are skipped (table_actions).
            batch = self._read_batch(relation_id, current, high, head["skip"])
        except (PinMissing, HistoryDropped) as exc:
            blocked = Blocked("needs_resync", str(exc), code="history_lost")
            self.hold(relation_id, token, current, blocked)
            t.status, t.detail = blocked.state, blocked.reason
            return round_high
        except Blocked as exc:
            # Never raised by v1's reads; format 2's reader refusals (apply2).
            self.hold(relation_id, token, current, exc)
            t.status, t.detail = exc.state, exc.reason
            return round_high
        except duckdb.TransactionException as exc:
            # Completing a transfer contends on the retention guard.
            if not is_conflict(exc):
                raise
            t.status = "busy"
            return round_high
        usable, columns, images, boundary, blocked = self._fold_read_batch(
            relation_id, current, batch
        )
        try:
            self.write(relation_id, token, current, columns, images, boundary, blocked, fault=fault)
            t.committed = boundary  # write returns only after its COMMIT
            t.renewed = time.monotonic()  # the write's guard renewed the lease
        except Blocked as exc:
            # Raised before any effect (e.g. no baseline row); nothing applied.
            blocked = exc
            self.hold(relation_id, token, current, exc)
            usable = []
        except DATA_ERRORS as exc:
            usable, blocked, t.committed = self.isolate_poison(
                relation_id, token, current, usable, exc
            )
        if self.advance_pin(relation_id, current["generation"], current["epoch"], t.committed):
            t.pinned = (current["generation"], current["epoch"], t.committed)
        t.events += len(usable)
        t.transactions += len({e["commit_lsn"] for e in usable})
        if blocked:
            t.status, t.detail = blocked.state, blocked.reason
        return round_high

    def _close(self, t, *, record=True):
        """A table's run end (done, held, capped, busy): the pin repair, its runs row and
        its lease release. The checkpoint is the last one read or committed under the
        lease; the pin repair also fixes a pin a previous run failed to advance."""
        try:
            if record:
                current = t.current
                if t.pinned != (current["generation"], current["epoch"], t.committed):
                    self.advance_pin(
                        t.relation_id, current["generation"], current["epoch"], t.committed
                    )
                result = TableResult(
                    t.table, t.status, t.committed, t.events, t.transactions, t.detail
                )
                self._record(t.run_id, t.relation_id, result)
                units = sum(1 for end in t.unit_ends if end <= t.committed)
                t.result = replace(result, units=units)
        finally:
            t.closed = True
            try:
                self.release(t.relation_id, t.token)
            except Exception:
                logger.error("apply lease release failed; it expires automatically")

    def run(self, *, fault=None, on_table=None, tables=None, run_id=None):
        """v1's run, with the page cache scoped to it: cleared when the run ends, however
        it ends."""
        self._run_tables = tuple(tables or self.config.tables)
        self.reader.commit_times = {}  # this run's unit times, read by its ops report
        try:
            return self._run_tables_loop(
                fault=fault, on_table=on_table, tables=tables, run_id=run_id
            )
        finally:
            self._cache = self._plan_cache = None
            self._run_tables = None

    def apply_table(self, table, run_id, *, deadline=None, fault=None):
        """One table alone: the run's schedule over that table (a round is one page). The
        result also counts the whole units committed: every page read starts at the
        table's checkpoint, so each recorded unit end lies above the starting position."""
        return self._schedule([table], run_id, deadline=deadline, fault=fault)[0]

    def _read_batch(self, relation_id, current, high, skip):
        """One table's page. units() runs for every table, so the retention floor is
        checked at that table's own checkpoint in the same statement. On the SQL path the
        plan statement decides (sql_fold.plan_sql); otherwise, or when it rules the page out,
        the page's frames and descriptors are read once per run for every table planning the
        identical page (the cache), or per table above the admission cap. Quarantine skips
        apply later, per table, to the unfiltered split. Every read happens here, inside
        _apply_table's exception mapping."""
        epoch, applied = current["epoch"], current["applied"]
        self._skip = skip
        try:
            page = self.reader.units(
                epoch,
                applied,
                high,
                max_events=self.config.max_batch_events,
                max_bytes=self.config.max_batch_bytes,
            )
            if self.fold == "sql" and current["capture_mode"] != "append":
                # An append table never takes the SQL fold (keyless design S-2).
                batch = self._plan(relation_id, current, page, high, skip)
                if batch is not None:
                    return batch
            events, descriptors = self._page_events(relation_id, page, high)
        except SourceMismatch as exc:
            self._cache = self._plan_cache = None
            raise Blocked("needs_action", str(exc), code="source_mismatch") from exc
        except BaseException:
            self._cache = self._plan_cache = None  # never serve a page whose read failed
            raise
        return UnitBatch(page, events, descriptors, skip)

    def _page_events(self, relation_id, page, high):
        """(this table's logical events, the page's descriptors) on the Python path."""
        key = self._page_key(page, high)
        if self._cache is not None and self._cache[0] == key:
            self.cache_stats["hits"] += 1
            split, descriptors = self._cache[1], self._cache[2]
            return split.get(relation_id, []), descriptors
        if sum(u.bytes for u in page) <= CACHE_CAP_BYTES:
            self._cache = None
            frames = self.reader.frames(page, None, high=high)
            descriptors = self.reader.descriptors(page, high=high)
            split = {}
            for event in logical_events(frames):
                split.setdefault(event["relation_id"], []).append(event)
            # Stored only after every read and the split succeeded: no partial entry.
            # Every table planning this page is served the SAME event dicts: they
            # must never be mutated in place (_present/to_v1 copy before fold_batch
            # writes full_old; a truncate is presented as a new dict too).
            self._cache = (key, split, descriptors)
            self.cache_stats["fills"] += 1
            return split.get(relation_id, []), descriptors
        self.cache_stats["uncached"] += 1
        events = self.reader.events(page, relation_id, high=high)
        return events, self.reader.descriptors(page, high=high)

    def _fallback(self, reason):
        key = "fallback:" + reason
        self.fold_stats[key] = self.fold_stats.get(key, 0) + 1
        logger.info("apply: Python fold for this batch (%s)", reason)
        return None

    def _plan(self, relation_id, current, page, high, skip):
        """The SQL path's read: a SqlBatch, or None to read the page on the Python path.

        The page's facts come from one plan statement per page per run, shared by every
        table planning the identical page (_page_plan). The decision is this table's own,
        made every time from its `current` (read under its lease): its quarantine skips, its
        descriptor, key layout and reconcile against its own mirror columns."""
        if not page:
            return self._fallback("empty_page")
        if any((u.start, u.end, u.digest) in skip for u in page):
            return self._fallback("quarantine")
        plan = self._page_plan(relation_id, page, high)
        if relation_id in plan.layouts_ambiguous:
            return self._fallback("layouts")  # one version under two key layouts
        facts = plan.rows.get(relation_id)
        if facts is None:
            if plan.batched and (relation_id in plan.judged or relation_id not in plan.oids):
                # No events for this table in the page: judged and absent, or no descriptor
                # in the page at all. spool2 keeps every EVENT's relation descriptor and
                # every relation a TRUNCATE lists in the unit's chunks (spool2.py, the
                # descriptor_keys of a chunk), so a relation without one has no events here.
                facts = PageFacts(0, 0, [], None, None, None, 0)
            else:  # unbatched page, or a table the batched plan didn't judge
                facts = self._plan_single(relation_id, page, high, plan)
                if facts is None:
                    return self._fallback("layouts")
        reason, columns, layout = decide(
            relation_id, facts, plan.descriptors, current["columns"], current["keys"]
        )
        if reason is not None:
            return self._fallback(reason)
        self.fold_stats["sql"] = self.fold_stats.get("sql", 0) + 1
        return SqlBatch(page, high, plan.frames, facts.events, facts.commits, columns, layout)

    def _page_plan(self, relation_id, page, high):
        """The page's facts for every table of the run: one descriptors read and one plan
        statement (sql_fold.page_plan_sql) per page per run. Served from the run cache to
        every later table planning the identical page; the cache key is _page_key, as the
        Python fold's page cache. Each table's own units() (its floor, under its lease) ran
        before this, and each table's retention pin, at or below its checkpoint and so
        below the page, keeps the page's units from being dropped while any table that
        plans it is behind (the window guard between the cached checks and a later table's
        MERGE). A planner or engine-capacity error from the batched statement (never a stop,
        an I/O or transaction error, or a check failure) leaves the page unbatched: each
        table then plans alone (_plan_single), as before."""
        key = self._page_key(page, high)
        cache = self._plan_cache if self._plan_cache is not None else {}
        if key in cache:
            plan = cache[key]
            if relation_id in plan.rows or (
                plan.batched and (relation_id in plan.judged or relation_id not in plan.oids)
            ):
                self.plan_stats["hits"] += 1
            return plan
        self._plan_cache = cache
        descriptors = self.reader.descriptors(page, high=high)
        epoch, low, end = self.reader._bounds(page, high)
        days = sorted({d for u in page for d in u.days})
        frames = sql_fold.frames_sql(days, epoch, low, end)
        plan = PagePlan(descriptors, frames)
        names = set(self._run_tables or self.config.tables)
        seen, relations, judged = {}, [], {relation_id}
        for (oid, version, _), desc in descriptors.items():
            if oid != relation_id and f"{desc.namespace}.{desc.table_name}" not in names:
                continue
            if oid != relation_id and not desc.keys:
                # A keyless descriptor is never a fold input: an empty key would fold its
                # rows into one. Per descriptor: a relation with a keyed version too stays
                # judged (its keyless events read as an unknown version), harmless since its
                # own apply refuses it at head (append_refusal).
                continue
            judged.add(oid)
            if (oid, version) in seen:
                plan.layouts_ambiguous.add(oid)
                continue
            seen[(oid, version)] = desc
        for (oid, version), desc in seen.items():
            if oid in plan.layouts_ambiguous:
                continue
            positions = tuple(desc.names.index(k) + 1 for k in desc.keys)
            relations.append((oid, version, len(desc.names), positions))
        only = judged - plan.layouts_ambiguous
        plan.judged = frozenset(only)
        try:
            row = self.execute(sql_fold.page_plan_sql(frames, relations, only)).fetchone()
        except BATCH_FALLBACK as exc:
            self.plan_stats["batched_failed"] += 1
            self.fold_stats["fallback:batched_plan"] = (
                self.fold_stats.get("fallback:batched_plan", 0) + 1
            )
            logger.warning(
                "apply: batched page plan failed (%s); planning per table", type(exc).__name__
            )
            self._store_plan(key, plan)  # unbatched: no rows, each table plans alone
            return plan
        self.reader.check_frames(page, high, *row[:4])
        for r in row[4] or ():
            plan.rows[r["relation_id"]] = PageFacts(
                r["events"],
                r["commits"],
                r["layouts"] or [],
                r["block_at"],
                r["block_reason"],
                r["change_at"],
                r["key_changes"],
            )
        plan.batched = True
        self._store_plan(key, plan)
        self.plan_stats["fills"] += 1
        return plan

    def _store_plan(self, key, plan):
        """Keep a page's plan for the run: one slot per checkpoint group (_schedule
        steps up to one group per open table, each on its own page key), oldest out.
        A PagePlan holds no frames, so a slot costs only its facts."""
        cache = self._plan_cache if self._plan_cache is not None else {}
        cache.pop(key, None)
        cache[key] = plan
        while len(cache) > max(1, len(self._run_tables or self.config.tables)):
            cache.pop(next(iter(cache)))
        self._plan_cache = cache

    def _plan_single(self, relation_id, page, high, plan):
        """One table's facts alone (sql_fold.plan_sql's statement), after a batched
        plan failed for this page. None: one version under two key layouts."""
        versions = versions_of(plan.descriptors, relation_id)
        if versions is None:
            return None
        row = self.execute(sql_fold.plan_sql(plan.frames, relation_id, versions)).fetchone()
        self.reader.check_frames(page, high, *row[:4])
        facts = PageFacts(*row[4:])
        plan.rows[relation_id] = facts
        return facts

    def _page_key(self, page, high):
        """The page's identity: every unit's (epoch, start, end, terminal digest), the
        snapshotted high, and the stored binding and configuration revision the reader
        validated with this table's units() call."""
        head, binding = self.reader._head, self.reader._binding
        return (
            tuple((u.epoch, u.start, u.end, u.digest) for u in page),
            high,
            binding.digest.hex() if binding is not None else None,
            head.config_revision if head is not None else None,
        )

    def _key_blocked_unit(self, current, batch, relation_id):
        """The first unit whose events carry a key layout other than the mirror's, as
        (unit start, Blocked), or None. Key changes activate only at unit boundaries,
        so a mixed unit is never folded under either key."""
        event = self._key_mismatch(
            current["keys"], batch.events, batch.descriptors, relation_id, self._quarantined(batch)
        )
        if event is None:
            return None
        if current["capture_mode"] == "append":
            # Can't happen: ingest writes an append table's frames keyless (its key map is
            # '[]' and never activated). If it ever did, resync is refused for append,
            # so hold needs_action with the way out instead.
            return event["unit_start_lsn"], Blocked(
                "needs_action",
                "an insert-only table's frames carry a key: remove the table and add it again",
                event["commit_lsn"],
                code="append_keyed_frames",
            )
        return event["unit_start_lsn"], Blocked(
            "needs_resync",
            "source replica identity key changed; resync",
            event["commit_lsn"],
            code="key_changed",
        )

    def _fold_units(self, relation_id, current, batch, units_before):
        """fold_batch over the events of the units starting before `units_before`
        (None: every unit), from the pre-batch state."""
        quarantined = self._quarantined(batch)  # once per batch, not once per event
        kept = [
            e
            for e in batch.events
            if (units_before is None or e["unit_start_lsn"] < units_before)
            and e["unit_start_lsn"] not in quarantined
        ]
        presented, descriptors = self._present(kept, batch.descriptors, relation_id)
        return self.fold_batch(relation_id, current, presented, descriptors=descriptors)

    @staticmethod
    def _quarantined(batch):
        return {u.start for u in batch.page if (u.start, u.end, u.digest) in batch.skip}

    def _fold_read_batch(self, relation_id, current, batch):
        page = batch.page
        self._unit_ends.update(u.end for u in page)
        if isinstance(batch, SqlBatch):
            self._sql_batch = batch
            images = SqlImages(batch, relation_id)
            return Counted(batch.events, batch.commits), batch.columns, images, page[-1].end, None
        cut = None  # (unit start, Blocked): hold before this whole unit
        keyed = self._key_blocked_unit(current, batch, relation_id)
        if keyed:
            cut = keyed
        usable, columns, images, blocked = self._fold_units(
            relation_id, current, batch, cut[0] if cut else None
        )
        if blocked:
            start = self._unit_of(batch, blocked.commit_lsn)
            if cut is None or start < cut[0]:
                cut = (start, blocked)
        if cut is None:
            return usable, columns, images, page[-1].end, None
        start, blocked = cut
        # Re-fold from the pre-batch state over the whole units before the cut: a
        # commit-level prefix of the blocked unit is never applied.
        usable, columns, images, again = self._fold_units(relation_id, current, batch, start)
        if again:
            raise AssertionError("units before a blocked unit cannot block")
        blocked.unit_start_lsn = start
        return usable, columns, images, start, blocked

    @staticmethod
    def _unit_of(batch, commit_lsn):
        for event in batch.events:
            if event["commit_lsn"] == commit_lsn:
                return event["unit_start_lsn"]
        raise ValueError("a blocking commit outside the batch's events")

    @staticmethod
    def _limit(head, current, confirmed):
        """(changed, limit) for next_step. An append table has no key, so no key gate
        (ingest never key-tracks it, and its key_changes rows, such as an epoch
        transition's, are never read): up to the confirmed position. An epoch change
        still holds it (next_step)."""
        if current["capture_mode"] == "append":
            hold = head.get("append_hold")
            if hold is not None:  # a catalog hold (catalog_checks): stop at its cut
                return True, hold["cut"]
            return False, confirmed
        return key_limit_from(head["key"], head["cut"], current["key_version"], current["epoch"])

    def idle(self, head):
        """True when the loop head would end at once ("done") and the table pin is
        already at the checkpoint: there is nothing to apply, hold or repair."""
        current, pin = head["current"], head["pin"]
        epoch, confirmed = source_from(head["source"], self.config.epoch, self.config.source_id)
        changed, limit = self._limit(head, current, confirmed)
        step, _ = next_step(current, epoch, confirmed, changed, limit)
        return (
            step == "done"
            and pin is not None
            and (
                pin["generation"],
                pin["epoch"],
                pin["lsn"],
            )
            == (current["generation"], current["epoch"], current["applied"])
        )

    def _record(self, run_id, relation_id, result):
        self.execute(
            "INSERT INTO {m}runs (run_id,relation_id,status,boundary,detail,updated_at) "
            "VALUES (?,?,?,?,?,current_timestamp) ON CONFLICT DO UPDATE SET "
            "status=excluded.status,boundary=excluded.boundary,detail=excluded.detail,"
            "updated_at=excluded.updated_at",
            [run_id, relation_id, result.status, result.boundary, result.detail],
        )

    def _run_tables_loop(self, *, fault=None, on_table=None, tables=None, run_id=None):
        """Apply every table (`tables`, default CDC_TABLES; the CLI passes the registry's
        through table_set.run) under one run id and deadline, page-major (_schedule).
        `on_table(event, value)` is called with ("start", table) when a table opens and
        ("end", result) when it finishes, for progress lines; like `fault`, it must only
        print and never raise."""
        run_id = run_id or uuid.uuid4().hex  # the CLI passes the id its status row uses
        deadline = time.monotonic() + self.config.runtime_seconds
        return self._schedule(
            list(tables or self.config.tables),
            run_id,
            deadline=deadline,
            fault=fault,
            on_table=on_table,
        )

    def _schedule(self, tables, run_id, *, deadline=None, fault=None, on_table=None):
        """The page-major schedule. Every table opens first: its lease is taken once, for the run. Then in
        rounds, each open table steps one page, grouped by checkpoint: a group's high is
        the round's confirmed position, capped at the next higher group's checkpoint (and
        at the table's own key limit), so a lagging group lands exactly on the next one and
        from then on they share every page key (one descriptors read and one plan per page
        per run). No table waits for a laggard: every group below confirmed steps each
        round, and a lower group steps twice (capped), so it closes on the next group by a
        page a round. A round starts only if the previous one would still fit before the deadline
        (the first always runs), and once begun it is finished, so a run overruns by at
        most one round. A finished table (done, held, busy) closes at once: its runs row,
        pin repair and lease release. On an unexpected error every open table is closed
        (as `capped`, its committed pages kept) and the error is raised."""
        results, opened, failing = [None] * len(tables), [], None
        try:
            for i, table in enumerate(tables):
                if on_table:
                    on_table("start", table)
                state = self._open(table, run_id)
                if isinstance(state, TableResult):
                    results[i] = state
                    if on_table:
                        on_table("end", state)
                else:
                    state.index = i
                    opened.append(state)
            last = None  # the previous round's seconds
            while True:
                active = [t for t in opened if not t.closed and t.status is None]
                if not active:
                    break
                now = time.monotonic()
                # The first round runs unless the deadline has passed; a later one only if
                # it would fit, judged by the previous round's length (P-2).
                if deadline is not None and now + (last or 0) >= deadline:
                    for t in active:
                        t.status = "capped"
                    self._finish(opened, results, on_table)
                    break
                # Groups by checkpoint, stepped top-down. The top group pages up to the
                # round's confirmed position; each lower group then pages up to the next
                # group's checkpoint as it stands after that group's step. A page key
                # includes its high, so a capped group never shares the group above's
                # pages: a lower group takes up to two capped steps, gaining a page a round
                # on the group above until it lands on its checkpoint and merges (it never
                # passes it).
                groups = {}
                for t in sorted(active, key=lambda t: t.index):
                    groups.setdefault(t.committed, []).append(t)
                round_high, cap, two_steps = None, None, False
                for checkpoint in sorted(groups, reverse=True):
                    members = groups[checkpoint]
                    for second in (False, True) if cap is not None else (False,):
                        for t in members:
                            if t.status is None and not (second and t.committed >= cap):
                                failing = t
                                two_steps |= second
                                round_high = self._step(t, round_high, cap, fault)
                    stepped = [t.committed for t in members if t.status is None]
                    cap = min(stepped) if stepped else checkpoint
                self.round_stats["rounds"] += 1
                last = time.monotonic() - now
                self.round_stats["max_round_seconds"] = max(
                    self.round_stats["max_round_seconds"], last
                )
                failing = None  # closing a finished table never blames the last stepper
                if two_steps:  # a lagging group paged twice: the budget's worst case
                    self.round_stats["two_step_rounds"] += 1
                    self.round_stats["max_two_step_round_seconds"] = max(
                        self.round_stats["max_two_step_round_seconds"], last
                    )
                self._finish(opened, results, on_table)
        except BaseException:
            for t in opened:
                if not t.closed:
                    if t.status is None:
                        t.status, t.detail = "capped", "run stopped: another table failed"
                    try:
                        # The failing table writes no runs row (as before); every table's
                        # lease is released.
                        self._close(t, record=t is not failing)
                    except Exception:
                        logger.error("apply: closing %s after a failure failed", t.table)
            raise
        if self.round_stats["rounds"]:
            r = self.round_stats
            logger.info(
                "apply: %d rounds, longest %.1f s; %d two-step rounds, longest %.1f s",
                r["rounds"],
                r["max_round_seconds"],
                r["two_step_rounds"],
                r["max_two_step_round_seconds"],
            )
        return results

    def _finish(self, opened, results, on_table):
        """Close every open table that finished this round."""
        for t in opened:
            if not t.closed and t.status is not None:
                self._close(t)
                results[t.index] = t.result
                if on_table:
                    on_table("end", t.result)
