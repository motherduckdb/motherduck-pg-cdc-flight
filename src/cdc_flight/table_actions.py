"""Apply-side operator actions on one mirror table: pause, resume, retry, quarantine.

Phase 3. Each action is idempotent and guarded:
- the caller's expected value (the state, or the blocked commit and epoch) must match
  what is read, or the action is refused (a stale screen never acts);
- a dry run reads and validates only: no lease, no write;
- the real action takes the table's apply lease from the observed token (any run since
  the read makes it `busy`), then in one mirror transaction: guard, its writes, a
  compare-and-set state change (Apply.set_state) and a `runs` audit row. A moved row
  rolls everything back (`refused`).

Quarantine is the repair action: it skips one poison transaction for the current
generation of one table, recorded in the mirror's `quarantine` table (its own audit).
On a format-2 mirror (an Apply2, `format_version == 2`) it skips one WHOLE unit for the
table and generation instead: the confirmation names the unit
(`QUARANTINE <table> UNIT <epoch> <start>..<end>`), a commit literal is refused, and
the blocked commit is kept only as a diagnosis.
Rows that commit touched may diverge until a resync, which rebuilds from the source
and ignores quarantine. The library is shared by `cdc-flight-tables` and Phase 3's
table-action Flights; no pipeline command imports it.
"""

import hashlib
import json
import re

# Re-exported: the action Flights import these from the dependency-free module, and
# table_actions.Refused is action_types.Refused (one class, one identity).
from cdc_flight.action_types import ActionResult, Refused, lsn_or_none, parse_lsn  # noqa: F401
from cdc_flight.apply import POISON, ApplyError, LeaseBusy
from cdc_flight.config import lsn_text
from cdc_flight.mirror import descriptor_columns

GAP = "rows touched by commit {} in {} may diverge until a resync"
POISON_DETAIL = re.compile("^" + POISON.format(r"\w+", r"(\d+)") + "$")
# An append table's hold on an UPDATE or DELETE (append_apply.append_batch).
APPEND_HOLD = re.compile(r"^append_(update|delete): ")


class _Moved(Exception):
    """The row changed between the read and the guarded write: roll back."""


# mirror.reconcile's holds: a mirror schema the operator can repair, then retry. Any
# other hold comes from the event itself (resync is the way out). A test keeps this
# list equal to reconcile's messages.
SCHEMA_HOLD = re.compile(
    r"^(duplicate column names in relation descriptor"
    r"|source column uses reserved prefix .*"
    r"|replica identity column missing from relation"
    r"|replica identity NOTHING cannot be applied"
    r"|replica identity columns differ from registered key"
    r"|column .* (dropped or renamed|changed type( \(.*)?))$"
)


def not_poison(table, observed, shown):
    """Quarantine's refusal for any hold but a poison transaction, with the way out:
    retry after a repair for a schema hold or a hold with no blocked commit (a mirror row
    to restore), else resync."""
    detail = observed["detail"] or observed["state"]
    if observed["blocked"] is None:
        steps = [f"retry {table} --blocked-lsn none", f"resync {table}"]
    elif SCHEMA_HOLD.match(detail):
        steps = [f"retry {table}", f"resync {table}"]
    else:
        steps = [f"resync {table}"]
    return ActionResult(
        "refused",
        table,
        f"only a poison transaction can be quarantined; this table is held on: {detail}",
        steps,
        shown,
    )


def confirmation(table, commit_lsn):
    """The literal quarantine requires, from the canonical form of the blocked LSN."""
    return f"QUARANTINE {table} {lsn_text(commit_lsn)}"


def unit_confirmation(table, epoch, start, end):
    """Format 2's literal: the whole blocked unit, built from the observation."""
    return f"QUARANTINE {table} UNIT {epoch} {lsn_text(start)}..{lsn_text(end)}"


# Format 2's blocked unit, read with the mirror row (one statement; a complete unit only).
UNIT_SELECT = (
    ",s.blocked_unit_start_lsn,u.end_lsn,u.member_count,u.event_count,u.digest,"
    "u.first_commit,u.last_commit,u.config_revision"
)
UNIT_JOIN = (
    " LEFT JOIN {s}units_v2 u ON u.epoch=s.epoch AND u.unit_start_lsn=s.blocked_unit_start_lsn "
    "AND u.complete"
)
UNIT_KEYS = (
    "unit_start_lsn",
    "unit_end_lsn",
    "unit_commit_count",
    "unit_event_count",
    "unit_digest",
    "first_commit_lsn",
    "last_commit_lsn",
    "config_revision",
)


def v2(apply):
    """A format-2 mirror (Apply2): a class attribute, so a v1 action reads nothing more."""
    return getattr(apply, "format_version", 1) == 2


def observe(apply, table):
    """(relation_id, observed) in one read, the token and blocked commit included, so
    the expected-value checks and the lease all refer to the same row version."""
    # Pause, resume, retry and quarantine are key-agnostic state moves: an append
    # (insert-only) table takes them too (resync refuses it separately).
    relation_id, mode = apply.registered(table, append_ok=True)
    held = apply.execute(
        "SELECT held_reason FROM {s}tables WHERE relation_id=?", [relation_id]
    ).fetchone()
    two = v2(apply)
    # The hold's code while it is the current one (hold_code_at = updated_at).
    code = apply._optional_state()["hold_code"]
    code = f"CASE WHEN s.hold_code_at=s.updated_at THEN s.{code} END" if code != "NULL" else code
    row = apply.execute(
        "SELECT s.state,s.generation_id,s.epoch,s.applied_through_lsn,s.token,"
        f"s.blocked_commit_lsn,s.detail,{code}"
        + (UNIT_SELECT if two else "")
        + " FROM {m}apply_state s"
        + (UNIT_JOIN if two else "")
        + " WHERE s.relation_id=?",
        [relation_id],
    ).fetchone()
    if row is None:
        return relation_id, None
    state, generation, epoch, applied, token, blocked, detail, hold_code = row[:8]
    observed = dict(
        state=state,
        generation=generation,
        epoch=epoch,
        applied=applied,
        token=token,
        blocked=blocked,
        detail=detail,
        hold_code=hold_code,
        source_held=held[0] if held else None,
        capture_mode=mode,
    )
    if two:
        unit = dict(zip(UNIT_KEYS, row[8:]))
        observed.update(
            format_version=2,
            source_epoch=epoch,
            generation_id=generation,
            blocked_commit_lsn=blocked,
            **unit,
            # While held on a commit, the checkpoint is its unit's start (Apply2), and
            # the unit is complete in the log. Anything else is refused, never guessed.
            consistent=blocked is None
            or (unit["unit_start_lsn"] == applied and unit["unit_end_lsn"] is not None),
        )
    return relation_id, observed


def _shown(observed):
    return {k: v for k, v in (observed or {}).items() if k != "detail"}


def _act(
    apply,
    table,
    relation_id,
    observed,
    *,
    action,
    run_id,
    actor,
    state,
    detail,
    extra=None,
    consume=None,
    fault=None,
):
    """Lease from the observed token, then one guarded mirror transaction: `consume`
    (an action Flight spends its preview; raises Refused), `extra` (quarantine's row),
    the compare-and-set and the audit row. `fault(stage)` (tests only; no-op default):
    "before_action_commit" after every write, before COMMIT (all of them roll back), and
    "after_action_commit" once COMMIT returned (a lost response: the action is done)."""
    try:
        token = apply.acquire(relation_id, run_id, token=observed["token"])
    except LeaseBusy:
        return ActionResult(
            "busy", table, "an apply run holds this table; retry", [], _shown(observed)
        )
    try:
        with apply.transaction():
            apply.guard(relation_id, token)
            if consume is not None:
                consume(observed)
            if extra is not None:
                extra(token)
            # An action moves a table to paused or back to live: its hold code follows.
            code = "paused" if state == "paused" else None
            if not apply.set_state(
                relation_id, token, observed, state=state, detail=detail, code=code
            ):
                raise _Moved
            apply.execute(
                "INSERT INTO {m}runs (run_id,relation_id,status,boundary,detail,updated_at) "
                "VALUES (?,?,?,?,?,current_timestamp)",
                [run_id, relation_id, action, observed["applied"], f"{action} by {actor}"],
            )
            if fault:
                fault("before_action_commit")
        if fault:
            fault("after_action_commit")
    except _Moved:
        return ActionResult(
            "refused",
            table,
            "the table's state changed since it was read; look again",
            [],
            _shown(observed),
        )
    except Refused as exc:
        return ActionResult("refused", table, str(exc), [], _shown(observed))
    finally:
        apply.release(relation_id, token)
    return None


def _start(apply, table):
    try:
        relation_id, observed = observe(apply, table)
    except ApplyError as exc:
        return None, None, ActionResult("refused", table, str(exc))
    if observed is None:
        return (
            None,
            None,
            ActionResult("refused", table, "the table has no mirror yet; run a backfill"),
        )
    if observed["source_held"]:
        return (
            None,
            None,
            ActionResult(
                "refused",
                table,
                f"the source table is held ({observed['source_held']}); see cdc-flight-tables",
                [],
                _shown(observed),
            ),
        )
    return relation_id, observed, None


def _stale(table, observed, expected_state):
    if expected_state is not None and expected_state != observed["state"]:
        return ActionResult(
            "refused",
            table,
            f"state changed: expected {expected_state}, found {observed['state']}",
            [],
            _shown(observed),
        )
    return None


def pause(
    apply, table, *, run_id, actor, expected_state=None, dry_run=False, consume=None, fault=None
):
    """live → paused. Apply skips a paused table; its pin stays (and is subject to
    retention expiry), and ingest keeps logging."""
    relation_id, observed, refused = _start(apply, table)
    if refused:
        return refused
    if refused := _stale(table, observed, expected_state):
        return refused
    shown = _shown(observed)
    if observed["state"] == "paused":
        return ActionResult("already", table, "already paused", [], shown)
    if observed["state"] != "live":
        return ActionResult(
            "refused",
            table,
            f"only a live table can be paused; it is {observed['state']}",
            [],
            shown,
        )
    if apply.active_generation(relation_id):
        return ActionResult("refused", table, "a backfill is running for this table", [], shown)
    if dry_run:
        return ActionResult("would", table, "pause: apply skips it until resumed", [], shown)
    return _act(
        apply,
        table,
        relation_id,
        observed,
        action="pause",
        run_id=run_id,
        actor=actor,
        consume=consume,
        fault=fault,
        state="paused",
        detail=f"paused by {actor} (run {run_id})",
    ) or ActionResult(
        "done",
        table,
        "paused: its pin keeps the log, which retention may expire",
        [f"resume {table} before its pin expires"],
        shown,
    )


def resume(
    apply, table, *, run_id, actor, expected_state=None, dry_run=False, consume=None, fault=None
):
    """paused → live. After a retention expiry the table is needs_resync: refused."""
    relation_id, observed, refused = _start(apply, table)
    if refused:
        return refused
    if refused := _stale(table, observed, expected_state):
        return refused
    shown = _shown(observed)
    if observed["state"] == "live":
        return ActionResult("already", table, "already live", [], shown)
    if observed["state"] != "paused":
        steps = [f"resync {table}"] if observed["state"] == "needs_resync" else []
        return ActionResult(
            "refused",
            table,
            f"only a paused table can be resumed; it is {observed['state']}",
            steps,
            shown,
        )
    if dry_run:
        return ActionResult(
            "would", table, "resume: apply continues from its checkpoint", [], shown
        )
    return _act(
        apply,
        table,
        relation_id,
        observed,
        action="resume",
        run_id=run_id,
        actor=actor,
        consume=consume,
        fault=fault,
        state="live",
        detail=None,
    ) or ActionResult("done", table, "resumed: the next apply run catches it up", [], shown)


MISSING = object()  # an argument the caller must pass explicitly on format 2


def retry(
    apply,
    table,
    *,
    run_id,
    actor,
    expected_blocked_lsn,
    expected_unit=MISSING,
    dry_run=False,
    consume=None,
    fault=None,
):
    """needs_action → live, after the mirror (or the data) was repaired. Apply re-reads
    from its checkpoint; a transaction that still fails is isolated again. Format 2:
    `expected_unit` ((start, end) of the blocked unit, or None for a hold without one)
    is required, and apply replays that whole unit."""
    relation_id, observed, refused = _start(apply, table)
    if refused:
        return refused
    shown = _shown(observed)
    if v2(apply) and observed["state"] != "live":
        found = (
            None
            if observed["unit_start_lsn"] is None
            else (observed["unit_start_lsn"], observed["unit_end_lsn"])
        )
        if expected_unit is MISSING:
            return ActionResult(
                "refused", table, "format 2: name the blocked unit (expected_unit)", [], shown
            )
        if not observed["consistent"]:
            return ActionResult(
                "refused",
                table,
                "the held unit is inconsistent; resync",
                [f"resync {table}"],
                shown,
            )
        if expected_unit != found:
            return ActionResult(
                "refused",
                table,
                f"state changed: expected blocked unit {_unit_text(expected_unit)}, "
                f"found {_unit_text(found)}",
                [],
                shown,
            )
    if observed["state"] == "live":
        return ActionResult("already", table, "already live", [], shown)
    if observed["state"] != "needs_action":
        return ActionResult(
            "refused",
            table,
            f"only a needs_action table can be retried; it is {observed['state']}",
            [],
            shown,
        )
    if observed["blocked"] != expected_blocked_lsn:
        found = lsn_or_none(observed["blocked"])
        return ActionResult(
            "refused",
            table,
            f"state changed: expected blocked commit "
            f"{lsn_or_none(expected_blocked_lsn)}, found {found}",
            [],
            shown,
        )
    if dry_run:
        text = "retry: apply re-reads from its checkpoint"
        if v2(apply) and observed["unit_start_lsn"] is not None:
            text += f", replaying the whole unit {_unit_text(expected_unit)}"
        return ActionResult("would", table, text, [], shown)
    return _act(
        apply,
        table,
        relation_id,
        observed,
        action="retry",
        run_id=run_id,
        actor=actor,
        consume=consume,
        fault=fault,
        state="live",
        detail=None,
    ) or ActionResult(
        "done", table, "retrying: the next apply run re-reads from its checkpoint", [], shown
    )


def quarantine(
    apply,
    table,
    *,
    run_id,
    actor,
    commit_lsn=None,
    expected_epoch,
    confirm,
    unit_start_lsn=None,
    unit_end_lsn=None,
    dry_run=False,
    consume=None,
    fault=None,
):
    """Skip one poison transaction (the table's blocked commit) for this generation, and
    return the table to live. Refused for any other kind of hold. Format 2: one whole
    unit (`unit_start_lsn`, `unit_end_lsn`; `commit_lsn` must be None)."""
    if v2(apply):
        return _quarantine_unit(
            apply,
            table,
            run_id=run_id,
            actor=actor,
            commit_lsn=commit_lsn,
            expected_epoch=expected_epoch,
            confirm=confirm,
            unit=(unit_start_lsn, unit_end_lsn),
            dry_run=dry_run,
            consume=consume,
            fault=fault,
        )
    relation_id, observed, refused = _start(apply, table)
    if refused:
        return refused
    shown = _shown(observed)
    gap = GAP.format(lsn_text(commit_lsn), table)
    if observed["state"] == "live":
        done = apply.execute(
            "SELECT count(*) FROM {m}quarantine WHERE relation_id=? AND generation_id=? "
            "AND epoch=? AND commit_lsn=?",
            [relation_id, observed["generation"], observed["epoch"], commit_lsn],
        ).fetchone()[0]
        if done:
            return ActionResult(
                "already", table, f"already quarantined; {gap}", [f"resync {table}"], shown
            )
    if observed["state"] != "needs_action":
        return ActionResult(
            "refused",
            table,
            f"only a table held on a poison transaction can be quarantined; "
            f"it is {observed['state']}",
            [],
            shown,
        )
    if observed["blocked"] is None:
        return not_poison(table, observed, shown)
    match = POISON_DETAIL.match(observed["detail"] or "")
    if not match or int(match.group(1)) != observed["blocked"]:
        return not_poison(table, observed, shown)
    if commit_lsn != observed["blocked"] or expected_epoch != observed["epoch"]:
        return ActionResult(
            "refused",
            table,
            f"state changed: the blocked commit is {lsn_text(observed['blocked'])} "
            f"in epoch {observed['epoch']}",
            [],
            shown,
        )
    if confirm != confirmation(table, observed["blocked"]):
        return ActionResult(
            "refused",
            table,
            f'type the confirmation exactly: "{confirmation(table, observed["blocked"])}"',
            [],
            shown,
        )
    if dry_run:
        return ActionResult(
            "would",
            table,
            f"quarantine poison commit {lsn_text(commit_lsn)} ({observed['detail']}); {gap}",
            [f"resync {table} afterwards"],
            shown,
        )
    reason = f"{observed['detail']}; {gap}"

    def record(token):
        # Only while the row still blocks on this commit under this lease.
        count = apply.execute(
            "INSERT INTO {m}quarantine (relation_id,generation_id,epoch,commit_lsn,reason,actor,"
            "run_id,created_at) SELECT relation_id,generation_id,epoch,"
            "blocked_commit_lsn,?,?,?,current_timestamp FROM {m}apply_state "
            "WHERE relation_id=? AND token=? AND blocked_commit_lsn=? AND generation_id=? "
            "AND epoch=?",
            [
                reason,
                actor,
                run_id,
                relation_id,
                token,
                commit_lsn,
                observed["generation"],
                observed["epoch"],
            ],
        ).fetchone()[0]
        if count != 1:
            raise _Moved

    return _act(
        apply,
        table,
        relation_id,
        observed,
        action="quarantine",
        run_id=run_id,
        actor=actor,
        consume=consume,
        fault=fault,
        state="live",
        detail=None,
        extra=record,
    ) or ActionResult(
        "done",
        table,
        f"quarantined commit {lsn_text(commit_lsn)}; {gap}",
        [f"resync {table}"],
        shown,
    )


def _unit_text(unit):
    return "none" if unit is None else f"{lsn_text(unit[0])}..{lsn_text(unit[1])}"


UNIT_GAP = "rows touched by unit {} ({} commits) in {} may diverge until a resync"
APPEND_GAP = (
    "the inserts of unit {} ({} commits) in {} are not in the mirror: a recorded gap until "
    "a resync (an exact re-snapshot)"
)


def _quarantine_unit(
    apply,
    table,
    *,
    run_id,
    actor,
    commit_lsn,
    expected_epoch,
    confirm,
    unit,
    dry_run,
    consume,
    fault=None,
):
    """Format 2's quarantine: one whole unit for one table and generation. Every refusal
    happens before the lease and any write (the spend included)."""
    relation_id, observed, refused = _start(apply, table)
    if refused:
        return refused
    shown = _shown(observed)
    held = (observed["unit_start_lsn"], observed["unit_end_lsn"])
    if commit_lsn is not None:
        return ActionResult(
            "refused",
            table,
            "format 2 quarantines a whole unit, not one commit: preview again and confirm "
            f'"{unit_confirmation(table, observed["epoch"], *held)}"'
            if held[0] is not None and held[1] is not None
            else "format 2 quarantines a whole unit, not one commit",
            [],
            shown,
        )
    if None in unit:
        return ActionResult(
            "refused",
            table,
            "format 2: name the blocked unit (unit_start_lsn, unit_end_lsn)",
            [],
            shown,
        )
    append = observed.get("capture_mode") == "append"
    if append:
        # An insert-only table's rebuild is an exact re-snapshot (keyless phase 2): its
        # resync. Until then the unit's inserts are a recorded gap.
        gap = APPEND_GAP.format(_unit_text(unit), observed["unit_commit_count"], table)
        after = [f"resync {table}"]
    else:
        gap = UNIT_GAP.format(_unit_text(unit), observed["unit_commit_count"], table)
        after = [f"resync {table}"]
    if observed["state"] == "live":
        done = apply.execute(
            "SELECT count(*) FROM {m}quarantine WHERE relation_id=? AND generation_id=? "
            "AND epoch=? AND unit_start_lsn=? AND unit_end_lsn=?",
            [relation_id, observed["generation"], observed["epoch"], *unit],
        ).fetchone()[0]
        if done:
            return ActionResult("already", table, f"already quarantined; {gap}", after, shown)
    if observed["state"] != "needs_action":
        return ActionResult(
            "refused",
            table,
            f"only a table held on a poison unit can be quarantined; it is {observed['state']}",
            [],
            shown,
        )
    if observed["blocked"] is None:
        return not_poison(table, observed, shown)
    match = POISON_DETAIL.match(observed["detail"] or "")
    # An append table's UPDATE/DELETE hold is quarantined the same way (keyless design
    # D8): the whole unit goes, its inserts with it (a recorded gap).
    held_append = observed.get("capture_mode") == "append" and APPEND_HOLD.match(
        observed["detail"] or ""
    )
    if not held_append and (not match or int(match.group(1)) != observed["blocked"]):
        return not_poison(table, observed, shown)
    if not observed["consistent"]:
        return ActionResult(
            "refused", table, "the held unit is inconsistent; resync", [f"resync {table}"], shown
        )
    if unit != held or expected_epoch != observed["epoch"]:
        return ActionResult(
            "refused",
            table,
            f"state changed: the blocked unit is {_unit_text(held)} in epoch {observed['epoch']}",
            [],
            shown,
        )
    literal = unit_confirmation(table, observed["epoch"], *held)
    if confirm != literal:
        return ActionResult(
            "refused", table, f'type the confirmation exactly: "{literal}"', [], shown
        )
    radius = (
        f"the whole unit {_unit_text(held)} is skipped for this table and generation: "
        f"{observed['unit_commit_count']} commits "
        f"({lsn_text(observed['first_commit_lsn'])}..{lsn_text(observed['last_commit_lsn'])}), "
        f"held on commit {lsn_text(observed['blocked'])}"
    )
    if dry_run:
        return ActionResult(
            "would",
            table,
            f"quarantine: {radius} ({observed['detail']}); {gap}",
            [f"{step} afterwards" for step in after],
            shown,
        )
    reason = f"{observed['detail']}; {gap}"

    def record(token):
        # Only while the row still blocks on this unit, with the same members and
        # terminal digest, under this lease.
        count = apply.execute(
            "INSERT INTO {m}quarantine (relation_id,generation_id,epoch,commit_lsn,reason,actor,"
            "run_id,created_at,unit_start_lsn,unit_end_lsn,unit_digest) "
            "SELECT s.relation_id,s.generation_id,s.epoch,s.blocked_commit_lsn,?,?,?,"
            "current_timestamp,u.unit_start_lsn,u.end_lsn,u.digest FROM {m}apply_state s "
            "JOIN {s}units_v2 u ON u.epoch=s.epoch AND u.unit_start_lsn=s.blocked_unit_start_lsn "
            "AND u.complete WHERE s.relation_id=? AND s.token=? AND s.generation_id=? "
            "AND s.epoch=? AND s.blocked_commit_lsn=? AND s.blocked_unit_start_lsn=? "
            "AND s.applied_through_lsn=s.blocked_unit_start_lsn AND u.end_lsn=? "
            "AND u.digest=? AND u.member_count=?",
            [
                reason,
                actor,
                run_id,
                relation_id,
                token,
                observed["generation"],
                observed["epoch"],
                observed["blocked"],
                held[0],
                held[1],
                observed["unit_digest"],
                observed["unit_commit_count"],
            ],
        ).fetchone()[0]
        if count != 1:
            raise _Moved

    return _act(
        apply,
        table,
        relation_id,
        observed,
        action="quarantine",
        run_id=run_id,
        actor=actor,
        consume=consume,
        fault=fault,
        state="live",
        detail=None,
        extra=record,
    ) or ActionResult("done", table, f"quarantined: {radius}; {gap}", after, shown)


# accept-types ----------------------------------------------------------------------------

ACCEPT_WARNING = (
    "accept only if every existing value was kept; a value-changing USING rewrite needs a resync"
)


def _type_hold(observed):
    """A needs_action hold on a source type change under the same mirror type: by its
    current hold code, or reconcile's text when no code was recorded."""
    if observed["state"] != "needs_action":
        return False
    if observed.get("hold_code") is not None:
        return observed["hold_code"] == "type_changed"
    return "changed type (its source type" in (observed.get("detail") or "")


def type_change(apply, relation_id, observed):
    """(version, [[column, stored, current]], refusal text or None) for the blocked commit:
    its own events' schema version (one version, or refused), read from the
    blocked unit, against the mirror's stored columns."""
    start, end, blocked = (
        observed.get("unit_start_lsn"),
        observed.get("unit_end_lsn"),
        observed["blocked"],
    )
    if start is None or end is None or blocked is None:
        return None, [], "the hold has no complete blocked unit; look again"
    reader = apply.reader
    page = reader.units(observed["epoch"], start, end)
    events = [e for e in reader.events(page, relation_id, high=end) if e["commit_lsn"] == blocked]
    keys = {(e["schema_version"], e["key_layout_hash"]) for e in events if e["op"] != "truncate"}
    if len(keys) != 1:
        return None, [], "the blocked commit has no single schema version for this table; resync"
    (version, key_hash) = keys.pop()
    desc = reader.descriptors(page, high=end).get((relation_id, version, key_hash))
    if desc is None:
        return None, [], "the blocked commit's descriptor is not in the log; resync"
    current = {c.name: c for c in descriptor_columns(desc.json)}
    changes = []
    for column in apply.state(relation_id)["columns"] or ():
        other = current.get(column.name)
        if other is None or (other.type_oid, other.typmod) == (column.type_oid, column.typmod):
            continue
        if other.dtype != column.dtype:
            return None, [], f"column {column.name!r} changed mirror type too; resync"
        changes.append(
            [column.name, [column.type_oid, column.typmod], [other.type_oid, other.typmod]]
        )
    if not changes:
        return version, [], "no column's source type differs from the mirror's; retry"
    return version, changes, None


def accept_confirmation(table, version, observed, changes):
    """The exact text a real accept-types needs: names the change, its schema version and
    the blocked unit (a digest), so it acts only on what the dry run showed."""
    data = [table, version, observed.get("unit_start_lsn"), observed.get("unit_end_lsn"), changes]
    digest = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:12]
    return f"ACCEPT TYPES {table} {digest}", digest


def accept_types(
    apply, table, *, run_id, actor, dry_run=False, confirm=None, consume=None, fault=None, **_
):
    """Accept a held table's source retype: rewrite the changed columns' stored source type
    (OID, typmod) to the blocked commit's, so the next Retry reconciles it. The table stays
    needs_action until that Retry. A dry run returns the columns and the confirmation."""
    relation_id, observed, refused = _start(apply, table)
    if refused:
        return refused
    if not _type_hold(observed):
        detail = "accept-types fixes only a source type held under the same mirror type"
        return ActionResult("refused", table, detail, [], _shown(observed))
    version, changes, problem = type_change(apply, relation_id, observed)
    if problem:
        return ActionResult("refused", table, problem, [], _shown(observed))
    text, digest = accept_confirmation(table, version, observed, changes)
    observed.update(type_digest=digest, type_changes=changes, confirm=text)
    shown = _shown(observed)
    if dry_run:
        what = f"accept {len(changes)} column type change(s) of {table}: {ACCEPT_WARNING}"
        return ActionResult("would", table, what, [f"type exactly: {text}"], shown)
    if confirm != text:
        return ActionResult("refused", table, f"type exactly: {text}", [], shown)
    stored = apply.state(relation_id)["columns"]
    new = {name: now for name, _, now in changes}
    rewritten = [
        [c.name, c.dtype, *new[c.name]]
        if c.name in new
        else [c.name, c.dtype, c.type_oid, c.typmod]
        for c in stored
    ]
    old = [[c.name, c.dtype, c.type_oid, c.typmod] for c in stored]
    try:
        token = apply.acquire(relation_id, run_id, token=observed["token"])
    except LeaseBusy:
        return ActionResult("busy", table, "an apply run holds this table; retry", [], shown)
    try:
        with apply.transaction():
            apply.guard(relation_id, token)
            if consume is not None:
                consume(observed)
            count = apply.execute(
                "UPDATE {m}apply_state SET columns=? WHERE relation_id=? AND token=? "
                "AND columns=? AND blocked_commit_lsn IS NOT DISTINCT FROM ?",
                [json.dumps(rewritten), relation_id, token, json.dumps(old), observed["blocked"]],
            ).fetchone()[0]
            if count != 1:
                raise _Moved
            apply.execute(
                "INSERT INTO {m}runs (run_id,relation_id,status,boundary,detail,updated_at) "
                "VALUES (?,?,?,?,?,current_timestamp)",
                [run_id, relation_id, "accept-types", observed["applied"], f"{text} by {actor}"],
            )
            if fault:
                fault("before_action_commit")
    except _Moved:
        detail = "the table's state changed since it was read; run a new dry run"
        return ActionResult("refused", table, detail, [], shown)
    except Refused as exc:
        return ActionResult("refused", table, str(exc), [], shown)
    finally:
        apply.release(relation_id, token)
    return ActionResult(
        "done", table, f"accepted {len(changes)} column type change(s)", [f"retry {table}"], shown
    )


def start_from_log(apply, table, **options):
    """An append (insert-only) table's first generation, from the log at S
    (append_action, imported only here)."""
    from cdc_flight.append_action import start_from_log_action

    return start_from_log_action(apply, table, **options)


ACTIONS = {
    "pause": pause,
    "resume": resume,
    "retry": retry,
    "quarantine": quarantine,
    "start-from-log": start_from_log,
    "accept-types": accept_types,
}
