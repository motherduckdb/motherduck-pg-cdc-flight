"""The table action that starts an append (insert-only) table's mirror from the log
(keyless design Q3). Imported lazily by table_actions.start_from_log, so a command that
never starts a generation (table-membership) doesn't load it."""

from cdc_flight.action_types import ActionResult, Refused
from cdc_flight.append_apply import start_decision
from cdc_flight.apply import ApplyError


def start_from_log_action(apply, table, *, run_id, actor, dry_run=False, consume=None, fault=None):
    """An append (insert-only) table's generation, from the log at S, the current durable
    unit boundary (Apply.start_append): its first, or one replacing a generation that
    can't apply (append_apply.start_decision); never a healthy one. Rows already in the table are never
    copied: the dry run says so, and the confirmed preview is the owner's acceptance.
    A keyed table starts with a backfill instead (resync)."""
    try:
        relation_id, mode = apply.registered(table, append_ok=True)
    except ApplyError as exc:
        return ActionResult("refused", table, str(exc))
    observed = dict(
        capture_mode=mode,
        generation=None,
        format_version=2 if getattr(apply, "format_version", 1) == 2 else 1,
    )
    if mode != "append":
        return ActionResult(
            "refused",
            table,
            "start-from-log is for insert-only (append) tables; a keyed table starts with a "
            "backfill (resync)",
            [],
            observed,
        )
    held = apply.execute(
        "SELECT held_reason FROM {s}tables WHERE relation_id=?", [relation_id]
    ).fetchone()
    if held and held[0]:
        return ActionResult("refused", table, f"the source table is held ({held[0]})", [], observed)
    decision, old, why = start_decision(apply, relation_id, repair=not dry_run)
    observed["generation"] = old
    if decision == "already":
        return ActionResult(
            "already", table, "it already has a live mirror generation", [], observed
        )
    if decision == "paused":
        return ActionResult(
            "refused", table, "it is paused: resume it instead", [f"resume {table}"], observed
        )
    replaces = (
        f"; it replaces the current generation ({why}): its mirror table is renamed aside, "
        "rows kept"
        if decision == "replace"
        else ""
    )
    if dry_run:
        return ActionResult(
            "would",
            table,
            "start the mirror from the log at the current position: only rows inserted from "
            "then on are captured (rows already in the table are not copied)" + replaces,
            [],
            observed,
        )
    try:
        generation, epoch, start, kept = apply.start_append(
            table,
            accept_incomplete=True,
            claim=consume and (lambda: consume(observed)),
            audit=(run_id, f"start-from-log by {actor}"),
            run_id=run_id,
        )
    except (Refused, ApplyError) as exc:
        return ActionResult("refused", table, str(exc), [], observed)
    observed.update(generation=generation, epoch=epoch, start_lsn=start, kept_mirror=kept)
    return ActionResult(
        "done",
        table,
        f"started from the log at epoch {epoch}, lsn {start}: rows inserted before it are "
        "not in the mirror" + (f"; the previous mirror is kept as {kept}" if kept else ""),
        [],
        observed,
    )
