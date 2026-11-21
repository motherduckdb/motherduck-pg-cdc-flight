"""Delete a source: stop capturing it, for good.

The source moves `deleting` -> `deleted` (`{s}source.lifecycle`). From the first step on,
every Flight refuses it: new code reads `lifecycle`, and builds from before `lifecycle`
are fenced by `slot_name = 'deleted:<source_id>'`, which their identifier check rejects
before any PostgreSQL I/O. The real slot name moves to `deleted_slot_name`.

Only what this code created is dropped: the slot, publication and heartbeat whose
`created_*` flag is set (preflight with create_objects, or a recover's new slot), plus any
pending `<slot>_e<n>` slot an interrupted recover left. A source registered before these flags
(flags NULL) drops nothing: the result prints the DDL for the DBA. Objects this role can't
drop (no REPLICATION for a slot, not the owner of the publication or the heartbeat) also
go to the DBA list, found by the dry run. A slot an earlier recover replaced
and couldn't drop is this source's too: a recover's `<slot>_e<n>` slot is
dropped like a pending one; any other (the first slot, whose ownership the recover's flag
overwrote, or one with a reader attached) goes to the DBA list. The mirrors, the change
log and the registry are kept. A deleted source id is final.

Order: lease (waited), read-only observe, then one guarded transaction: the caller's
claim (an action Flight's preview), `deleting` and the tombstone. Then the drops, each
idempotent; a reader still attached to the slot is `waiting` (the state stays
`deleting`, and every Flight is already refused, so it stops). Then `deleted`. A re-run of
a `deleting` source continues from the drops.
"""

import re
from dataclasses import dataclass, field

import psycopg2.errors
from psycopg2 import sql

from cdc_flight import lifecycle_commands as lc
from cdc_flight.config import table_name
from cdc_flight.ingest_log import leased
from cdc_flight.lifecycle_spec import check as check_lifecycle
from cdc_flight.lifecycle_spec import command as spec_command
from cdc_flight.lifecycle_spec import lifecycle as lifecycle_of
from cdc_flight.lifecycle_spec import lifecycle_sql
from cdc_flight.positions import current_lsn, role_of
from cdc_flight.preflight import connect, query

TOMBSTONE = "deleted:{}"


@dataclass(frozen=True)
class DeleteResult:
    status: str  # deleted, already, waiting
    lifecycle: str | None
    dropped: list = field(default_factory=list)
    dba: list = field(default_factory=list)  # DDL for objects this code may not drop
    detail: str = ""
    retained_bytes: int | None = None


def source_row(log):
    """(epoch, real slot, lifecycle, created flags, previous slots) of the registered
    source. The previous slots are those the recovers replaced."""
    row = log.execute(
        "SELECT epoch,coalesce(deleted_slot_name,slot_name,json_extract_string(binding,'$.slot')),"
        "lifecycle,created_slot,created_publication,created_heartbeat,"
        "(SELECT list(DISTINCT e.previous_slot_name) FROM {s}source_epochs e "
        "WHERE e.reason='slot_lost' AND e.previous_slot_name IS NOT NULL) "
        "FROM {s}source WHERE id=1"
    ).fetchone()
    if row is None:
        raise RuntimeError("source is not registered")
    epoch, slot, lifecycle, *flags, previous = row
    created = dict(zip(("slot", "publication", "heartbeat"), (bool(f) for f in flags)))
    return epoch, slot, lifecycle, created, sorted(previous or ())


# Whether this role may drop a replication slot: the REPLICATION attribute, a superuser,
# or (Amazon RDS) holding rds_replication's privileges (USAGE: membership that inherits;
# a NOINHERIT member doesn't count), where every login role's rolreplication is false. The
# role is matched by oid, so a server without rds_replication reads false. Under-claiming
# only leaves the slot to the DBA; over-claiming would fail the drop.
CAN_REPLICATE = (
    "SELECT r.rolreplication OR r.rolsuper OR EXISTS (SELECT 1 FROM pg_roles g "
    "WHERE g.rolname='rds_replication' AND pg_has_role(r.oid,g.oid,'USAGE')) "
    "FROM pg_roles r WHERE r.rolname=current_user"
)


def _recover_slot(name, base):
    """A recover's `<base>_e<n>` name, exactly (never `<base>2` or `<base>_e7x`)."""
    return re.fullmatch(re.escape(base) + r"_e[0-9]+", name) is not None


def observe(pg, config, epoch, slot, lifecycle, created, previous=()):
    """Read only: what a delete would drop and what it would leave for the DBA. The
    decision fields an action Flight's preview hashes (no retained-WAL bytes: they move).
    `retained_bytes` sums this source's present slots."""
    database = query(pg, "SELECT current_database()")[0][0]
    replication = query(pg, CAN_REPLICATE)[0][0]
    slots = {
        name: (plugin, db, active, retained)
        for name, plugin, db, active, retained in query(
            pg,
            "SELECT slot_name,plugin,database,active,"
            f"{current_lsn(role_of(config.heartbeat_writer))}-restart_lsn FROM pg_replication_slots",
        )
    }
    current = slots.get(slot)

    def droppable(name):  # pgoutput, this database, no reader attached
        plugin, db, active, _ = slots[name]
        return plugin == "pgoutput" and db == database and not active

    pending = sorted(
        name
        for name in slots
        if name != slot and _recover_slot(name, config.slot) and droppable(name)
    )
    # Recovers' previous slots still present and not already pending go to the DBA.
    leftovers = sorted(n for n in set(previous) if n in slots and n != slot and n not in pending)
    publication = query(
        pg,
        "SELECT pg_has_role(current_user,pubowner,'USAGE') FROM pg_publication WHERE pubname=%s",
        [config.publication],
    )
    heartbeat = query(
        pg,
        "SELECT pg_has_role(current_user,c.relowner,'USAGE') FROM pg_class c "
        "WHERE c.oid=to_regclass(%s)",
        [config.heartbeat],
    )
    drop, dba = [], []

    def decide(kind, name, present, ours, can, ddl):
        if not present:
            return
        (drop if ours and can else dba).append(dict(kind=kind, name=name) if ours and can else ddl)

    decide(
        "slot",
        slot,
        current is not None,
        created["slot"]
        and current is not None
        and current[0] == "pgoutput"
        and current[1] == database,
        replication,
        f"SELECT pg_drop_replication_slot('{slot}');",
    )
    for name in pending:  # only an interrupted recover makes these: ours
        decide("slot", name, True, True, replication, f"SELECT pg_drop_replication_slot('{name}');")
    for name in leftovers:
        decide("slot", name, True, False, False, f"SELECT pg_drop_replication_slot('{name}');")
    decide(
        "publication",
        config.publication,
        bool(publication),
        created["publication"],
        bool(publication and publication[0][0]),
        f'DROP PUBLICATION "{config.publication}";',
    )
    schema, name = table_name(config.heartbeat)
    decide(
        "heartbeat",
        config.heartbeat,
        bool(heartbeat),
        created["heartbeat"],
        bool(heartbeat and heartbeat[0][0]),
        f'DROP TABLE "{schema}"."{name}";',
    )
    if config.heartbeat_writer == "external":
        # A standby source: it owns only its slot on the standby. The heartbeat
        # job, publication and table are the DBA's, on the primary; never assumed ours.
        job = f"md_cdc_heartbeat_{config.source_id}"
        primary = [d for d in dba if not d.startswith("SELECT pg_drop_replication_slot")]
        dba = [d for d in dba if d not in primary] + [
            f"on the primary: SELECT cron.unschedule('{job}'); (or stop the heartbeat job)",
            *(f"on the primary: {d}" for d in primary),
        ]
    return dict(
        epoch=epoch,
        lifecycle=lifecycle,
        slot=slot,
        slot_active=bool(current and current[2]),
        retained_bytes=_retained(slots, [slot, *pending, *leftovers]),
        drop=drop,
        dba=dba,
    )


def _retained(slots, names):
    retained = [int(slots[n][3]) for n in names if n in slots and slots[n][3] is not None]
    return sum(retained) if retained else None


def preview_delete(config, log):
    """A dry run: read only, no lease."""
    epoch, slot, lifecycle, created, previous = source_row(log)
    pg = connect(config)
    try:
        return observe(pg, config, epoch, slot, lifecycle, created, previous)
    finally:
        pg.close()


def _drop(pg, item):
    if item["kind"] == "slot":
        query(
            pg,
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name=%s",
            [item["name"]],
        )
    elif item["kind"] == "publication":
        query(pg, sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(item["name"])))
    else:
        query(
            pg,
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(*table_name(item["name"]))),
        )


def delete_source(config, log, *, run_id, claim=None, wait=0, started_by=None):
    """Delete the source; see the module docstring. Returns a DeleteResult."""
    log.allow_lifecycle = True  # this Flight finishes its own `deleting`

    def action(_run):
        epoch, slot, lifecycle, created, previous = source_row(log)
        if lifecycle == "deleted":
            return DeleteResult("already", "deleted", detail="the source was deleted")
        pg = connect(config)
        try:
            observed = observe(pg, config, epoch, slot, lifecycle, created, previous)
            if lifecycle is None:
                check_lifecycle("delete_start", lifecycle_of(lifecycle))
                with log.transaction():
                    log.guard()
                    if claim is not None:
                        claim(observed)
                    # The spec: registered -> deleting, under the lease (count 1).
                    lc.statement(
                        log,
                        "delete_start",
                        "lifecycle_update",
                        "UPDATE {s}source SET lifecycle='deleting',deleted_slot_name=?,"
                        "slot_name=?,deleted_run_id=?,deleted_started_by=? WHERE id=1 AND "
                        + lifecycle_sql(spec_command("delete_start").from_states),
                        [slot, TOMBSTONE.format(config.source_id), run_id, started_by],
                    )
                # The lease's guard compares the slot name it read: now the tombstone.
                log.slot_name = TOMBSTONE.format(config.source_id)
            if observed["slot_active"]:
                return DeleteResult(
                    "waiting",
                    "deleting",
                    dba=observed["dba"],
                    detail=f"an ingest reader is still attached to {slot}; it is refused from "
                    "now on, so run delete-source again to finish",
                    retained_bytes=observed["retained_bytes"],
                )
            dropped = []
            for item in observed["drop"]:
                try:
                    _drop(pg, item)
                except psycopg2.errors.ObjectInUse:
                    return DeleteResult(
                        "waiting",
                        "deleting",
                        dropped,
                        observed["dba"],
                        f"{item['name']} is in use; run delete-source again to finish",
                        observed["retained_bytes"],
                    )
                dropped.append(item)
            # The lifecycle now: deleting, written above or read at the start (a re-run).
            check_lifecycle("delete_finish", lifecycle_of(lifecycle or "deleting"))
            with log.transaction():
                log.guard()
                # The spec: deleting -> deleted, under the lease (count 1).
                lc.statement(
                    log,
                    "delete_finish",
                    "lifecycle_update",
                    "UPDATE {s}source SET lifecycle='deleted',deleted_at=current_timestamp "
                    "WHERE id=1 AND " + lifecycle_sql(spec_command("delete_finish").from_states),
                )
            return DeleteResult("deleted", "deleted", dropped, observed["dba"])
        finally:
            pg.close()

    result = leased(log, action, wait)
    if result is None:
        lifecycle = source_row(log)[2]
        return DeleteResult("waiting", lifecycle, detail="ingest holds the lease; retry")
    return result
