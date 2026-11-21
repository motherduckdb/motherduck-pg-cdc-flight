"""Exact snapshot backfill of an append (keyless insert-only) table
option A, D3, and the phase 2 plan. Imported lazily by Backfill for append tables only.

Without a key a row committed in [L, H) could be read twice, so the copy is ONE exported
snapshot read in one run. A temporary slot's EXPORT_SNAPSHOT gives the consistent point
C: the snapshot holds exactly the commits whose commit record starts before C, and the
log's frames carry that same commit lsn. A fresh connection imports the snapshot, then
the slot's connection closes at once (the slot goes; the import keeps the snapshot).
Catch-up appends this relation's frames with commit_lsn >= C through H, by apply's
append rules; publication keeps a replaced mirror aside. A run that ends during the
copy fails the generation (its snapshot is gone), hence the size limit."""

import tempfile
import threading
import time
import uuid
from pathlib import Path

import duckdb
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2 import errors, sql

from cdc_flight import generation_commands as commands
from cdc_flight.append_apply import (
    _keep_mirror,
    backfill_running,
    kept_refusal,
    passed_catalog_version,
    start_decision,
    write_append,
)
from cdc_flight.apply import DATA_ERRORS, ApplyError, LeaseBusy, column_json, create_mirror_table
from cdc_flight.backfill import BackfillResult, GenerationFailed, standby_backfill
from cdc_flight.config import quoted, table_name
from cdc_flight.generations import ACTIVE
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.mirror import RESERVED_PREFIX, Blocked, cast_sql, type_kept
from cdc_flight.pg_connect import check_formats, connect, query
from cdc_flight.positions import PRIMARY, current_sql
from cdc_flight.retention import HistoryDropped, PinMissing, generation_owner, is_conflict

MODE = "append_snapshot"
# The slot wait (Q5; below every lease TTL, which is at least 900 s) and the copy rate
# assumed when checking, after the slot, that the copy fits the run (provisional).
SLOT_WAIT_SECONDS = 300
COPY_RATE = 4 * 1024 * 1024
SLOT_PREFIX = "cdc_snap_"
CAPABILITY = "the backfill's role lacks replication capability (needed for the snapshot's slot)"


def connect_refusal(exc):
    """A replication connection's failure, by its message (connect errors carry no
    SQLSTATE): a missing capability, a full server, or anything else."""
    text = str(exc).strip()
    lower = text.lower()
    if "permission" in lower or "replication role" in lower or "superuser" in lower:
        return CAPABILITY
    if "max_wal_senders" in lower or "too many" in lower or "slots are in use" in lower:
        return "no free replication slot or walsender for the snapshot; retry later"
    return "replication connection failed: " + (text.splitlines() or ["unknown"])[0]


def lsn_int(text):
    high, low = text.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def source_refusal(pg, config, relation_id, columns=None):
    """Why the source table can't be snapshotted, or None: kind, identity, publication,
    and (inside the snapshot) the generation's columns."""
    found = query(pg, "SELECT relkind,relreplident FROM pg_class WHERE oid=%s", [relation_id])
    if not found or tuple(found[0]) != ("r", "f"):
        return "needs a non-partitioned table with REPLICA IDENTITY FULL"
    if not query(
        pg,
        "SELECT 1 FROM pg_publication_rel r JOIN pg_publication p ON p.oid=r.prpubid "
        "WHERE p.pubname=%s AND r.prrelid=%s",
        [config.publication, relation_id],
    ):
        return f"is not in the publication {config.publication}"
    if columns is not None and [
        tuple(r)
        for r in query(
            pg,
            "SELECT attname,atttypid,atttypmod FROM pg_attribute WHERE attrelid=%s AND attnum>0 "
            "AND NOT attisdropped AND attgenerated='' ORDER BY attnum",
            [relation_id],
        )
    ] != [(c.name, c.type_oid, c.typmod) for c in columns]:
        return "changed its columns"
    return None


def refusal(backfill, table, relation_id):
    """Why an append snapshot can't start, or None; nothing is written."""
    config, pg = backfill.config, backfill.pg
    held = backfill.execute(
        "SELECT held_reason,held_detail FROM {s}tables WHERE relation_id=?", [relation_id]
    ).fetchone()
    if held and held[0]:
        return f"source table {held[0]} ({held[1]}); backfill refused"
    if backfill_running(backfill, relation_id):
        return "already has an active backfill"
    decision = start_decision(backfill, relation_id)[0]
    if decision == "paused":
        return "is paused: resume it first"
    # Any mirror this publication would keep aside, the retired relation's under a
    # recreated table's name too: at most one kept-aside table.
    kept = kept_refusal(backfill, *table_name(table))
    if kept and (decision != "start" or _mirror_exists(backfill, *table_name(table))):
        return kept
    why = source_refusal(pg, config, relation_id)
    if why:
        return why
    size = query(pg, "SELECT pg_table_size(%s)", [relation_id])[0][0]
    if size > config.append_snapshot_max_bytes:
        return (
            f"is {size} bytes, over the append snapshot limit {config.append_snapshot_max_bytes}"
            " (CDC_APPEND_SNAPSHOT_MAX_BYTES): use start-from-log, or raise the limit"
        )
    if int(query(pg, "SHOW server_version_num")[0][0]) < 170000:
        threshold = query(pg, "SHOW old_snapshot_threshold")[0][0]
        if threshold not in ("-1", "-1min"):
            return f"needs old_snapshot_threshold=-1 (it is {threshold}), or use start-from-log"
    # Capability is probed, never read from pg_roles (managed providers).
    # The probe briefly takes a walsender of its own.
    try:
        connect(config, replication=True, keepalive=True).close()
    except psycopg2.Error as exc:
        return connect_refusal(exc)
    free = query(
        pg,
        "SELECT least(current_setting('max_replication_slots')::int-(SELECT count(*) FROM "
        "pg_replication_slots),current_setting('max_wal_senders')::int-(SELECT count(*) "
        "FROM pg_stat_replication))",
    )[0][0]
    return None if free > 0 else "no free replication slot or walsender; retry later"


def start(backfill, table, relation_id, run_id, *, deadline=None, fault=None):
    """Pin L, then one mirror transaction: the claim, the generation (leased by this run
    from its insert) and its empty append staging table."""
    why = refusal(backfill, table, relation_id)
    if why:
        raise ApplyError(f"{table}: {why}")
    # The recorded mode, in the position read: the generation's finality mode.
    epoch, low, writer = backfill.start_position()
    backfill.check_writer(table, writer)
    schema, name = table_name(table)
    columns, _ = backfill.catalog(relation_id, table)
    if any(c.name.startswith(RESERVED_PREFIX) for c in columns):
        raise ApplyError(f"{table}: reserved column prefix")
    generation = uuid.uuid4().hex
    staging = f"{name}__gen_{generation[:12]}"
    backfill.pin_generation(table, relation_id, generation, epoch, low, deadline)
    if fault:
        fault("after_pin")
    try:
        with backfill.transaction():
            commands.inserted(
                backfill,
                "create_append",
                "claim_insert",
                "INSERT INTO {m}backfills (relation_id,generation_id) VALUES (?,?)",
                [relation_id, generation],
            )
            commands.inserted(
                backfill,
                "create_append",
                "generation_insert",
                "INSERT INTO {m}generations (generation_id,relation_id,mode,epoch,pin_lsn,"
                "state,created_at,staging,key_columns,columns,chunks,rows,scan_done,catchup_lsn,"
                "token,holder,expires_at,guard,updated_at,key_version,copy_started,"
                "deletes_ignored,finality_mode) VALUES (?,?,?,?,?,'snapshotting',"
                "current_timestamp,?,'[]',?,0,0,false,?,1,?,current_timestamp+?*INTERVAL "
                "'1 second',uuid()::VARCHAR,current_timestamp,0,false,0,?)",
                [generation, relation_id, MODE, epoch, low, staging, column_json(columns), low]
                + [run_id, backfill.config.lease_seconds, writer],
            )
            target = f"{backfill.mirror}.{quoted(schema)}.{quoted(staging)}"
            current = dict(target=target, keys=(), schema=schema, capture_mode="append")
            create_mirror_table(backfill.con, backfill.mirror, current, columns)
    except (duckdb.ConstraintException, duckdb.TransactionException) as exc:
        if isinstance(exc, duckdb.TransactionException) and not is_conflict(exc):
            raise
        backfill._release_quietly(generation_owner(generation), generation)
        raise ApplyError(f"{table} already has an active backfill") from exc
    return backfill.generation(generation), 1


def _create_slot(repl, slot, wait):
    """CREATE_REPLICATION_SLOT under an idempotent cancel timer: (C, snapshot)."""
    done = threading.Event()
    timer = threading.Timer(max(wait, 0.1), lambda: done.is_set() or repl.cancel())
    timer.daemon = True
    timer.start()
    try:
        with repl.cursor() as cur:
            cur.execute(
                f"CREATE_REPLICATION_SLOT {slot} TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT"
            )
            _, point, snapshot, _ = cur.fetchone()
    except errors.QueryCanceled as exc:
        raise GenerationFailed(
            f"a long-running source transaction delayed the snapshot past {int(wait)} s "
            "(see pg_stat_activity and pg_prepared_xacts); retry later"
        ) from exc
    except errors.InsufficientPrivilege as exc:
        raise GenerationFailed(CAPABILITY) from exc
    except (errors.ConfigurationLimitExceeded, errors.ObjectNotInPrerequisiteState) as exc:
        raise GenerationFailed(
            f"the server refused the snapshot's slot ({type(exc).__name__}): no free "
            "replication slot or walsender, or a setting it needs; retry later"
        ) from exc
    finally:
        done.set()
        timer.cancel()
    return lsn_int(point), snapshot


def copy(backfill, table, gen, token, *, deadline=None, fault=None):
    """Slot, import, close the slot's connection, copy in batches, then C and
    `catching_up` in one commit. A failure anywhere fails the generation."""
    config, generation_id = backfill.config, gen["generation_id"]
    with backfill.held(generation_id, token) as lease:
        lease.progress_scan(gen, copy_started=True)
    gen = backfill.generation(generation_id)
    wait = min(getattr(backfill, "slot_wait", SLOT_WAIT_SECONDS), config.lease_seconds - 1)
    if deadline is not None:
        wait = min(wait, deadline - time.monotonic())
    if wait <= 0:
        raise GenerationFailed("no time left in this run for the snapshot copy")
    repl = imp = None
    try:
        try:
            repl = connect(config, replication=True, keepalive=True)
        except psycopg2.Error as exc:
            raise GenerationFailed(connect_refusal(exc)) from exc
        snapshot_lsn, snapshot = _create_slot(repl, SLOT_PREFIX + generation_id[:12], wait)
        if fault:
            fault("after_slot")
        if snapshot_lsn < gen["pin_lsn"]:
            raise GenerationFailed("the snapshot's consistent point is below the pin")
        imp = connect(config, keepalive=True)  # autocommit: no transaction open yet
        check_formats(lambda n: query(imp, "SHOW " + n)[0][0])
        # An explicit REPEATABLE READ BEGIN, no transaction open before it.
        imp.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        cur = imp.cursor()
        cur.execute("SET TRANSACTION SNAPSHOT %s", [snapshot])
        cur.execute("SET LOCAL idle_in_transaction_session_timeout=0")
        repl.close()  # at once: the slot and its catalog_xmin go
        repl = None
        if fault:
            fault("after_import")
        why = source_refusal(imp, config, gen["relation_id"], gen["columns"])
        if why:
            raise GenerationFailed(f"before the snapshot, the source table {why}")
        if deadline is not None:
            size = query(imp, "SELECT pg_table_size(%s)", [gen["relation_id"]])[0][0]
            if time.monotonic() + size / COPY_RATE > deadline:
                raise GenerationFailed("the copy can't finish in this run: use start-from-log")
        gen = _copy_rows(backfill, imp, cur, table, gen, token, deadline, fault)
        wal = query(backfill.pg, current_sql(PRIMARY))[0][0]
        with backfill.held(generation_id, token) as lease:
            lease.finish_scan(
                gen,
                scan_done=True,
                snapshot_lsn=snapshot_lsn,
                marker=generation_id,
                wal_before_marker=int(wal),
            )
        imp.commit()  # releases the snapshot's xmin
        if fault:
            fault("after_scan_done")
    except (errors.QueryCanceled, errors.SnapshotTooOld, errors.UndefinedTable) as exc:
        raise GenerationFailed(f"snapshot copy stopped: {type(exc).__name__}") from exc
    except psycopg2.Error as exc:  # a lost source connection mid-copy
        raise GenerationFailed(f"snapshot copy failed: {type(exc).__name__}") from exc
    except DATA_ERRORS as exc:  # a value staging can't take
        raise GenerationFailed(f"{type(exc).__name__} in the snapshot copy") from exc
    finally:
        for con in (repl, imp):
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass


def _copy_rows(backfill, imp, cur, table, gen, token, deadline, fault):
    """The snapshot's rows in batches (chunk_rows, max_chunk_bytes), each committed to
    staging under the generation guard, which renews the lease. Snapshot rows: NULL
    commit lsn and time, their row number as the ordinal. Each batch's FETCHes run
    under min(chunk timeout, time left)."""
    config, columns = backfill.config, gen["columns"]
    select = sql.SQL(",").join(sql.SQL("{}::text").format(sql.Identifier(c.name)) for c in columns)
    staging = backfill.staging_current(gen, table_name(table)[0])["target"]
    names = ",".join(quoted(c.name) for c in columns)
    casts = ",".join(cast_sql(f"c{i}", c.dtype) for i, c in enumerate(columns))
    with imp.cursor(name=f"cdc_snap_{uuid.uuid4().hex}") as rows_cur:
        rows_cur.execute(
            sql.SQL("SELECT {} FROM ONLY {}").format(select, sql.Identifier(*table_name(table)))
        )
        done = False
        while not done:
            seconds = config.chunk_timeout_seconds
            if deadline is not None:
                seconds = min(seconds, deadline - time.monotonic())
            if seconds <= 0:
                raise GenerationFailed("the copy didn't finish in one run: use start-from-log")
            cur.execute(f"SET LOCAL statement_timeout={int(seconds * 1000)}")
            batch, size = [], 0
            while len(batch) < config.chunk_rows and size < config.max_chunk_bytes:
                fetched = rows_cur.fetchmany(min(1000, config.chunk_rows - len(batch)))
                if not fetched:
                    done = True
                    break
                batch += fetched
                size += sum(len(v.encode()) for row in fetched for v in row if v is not None)
            if not batch:
                break
            before = gen["rows"]
            data = {
                f"c{i}": pa.array([r[i] for r in batch], pa.string()) for i in range(len(columns))
            }
            data["_s_ord"] = pa.array(range(before + 1, before + len(batch) + 1), pa.uint64())
            with tempfile.TemporaryDirectory(prefix="cdc_snap_") as directory:
                path = str(Path(directory) / "copy.parquet")
                pq.write_table(pa.table(data), path)
                with backfill.held(gen["generation_id"], token) as lease:
                    backfill.con.execute(
                        f"INSERT INTO {staging} ({names},_cdc_commit_lsn,_cdc_event_ordinal,"
                        f"_cdc_updated_at) SELECT {casts},NULL,_s_ord,NULL FROM read_parquet(?)",
                        [path],
                    )
                    lease.progress_scan(gen, chunks=gen["chunks"] + 1, rows=before + len(batch))
            if fault:
                fault("after_chunk_commit")
            gen = backfill.generation(gen["generation_id"])
    return gen


def catch_up(backfill, table, gen, token, *, deadline, fault=None):
    """Append this relation's logged changes with commit_lsn >= C through H into staging,
    in batches each committed with its catch-up checkpoint. True once at H."""
    schema, _ = table_name(table)
    relation_id, floor, high = gen["relation_id"], gen["snapshot_lsn"], gen["marker_lsn"]
    (ignore_deletes,) = backfill.execute(
        "SELECT coalesce(ignore_deletes,false) FROM {s}tables WHERE relation_id=?", [relation_id]
    ).fetchone()
    while gen["catchup_lsn"] < high:
        if deadline is not None and time.monotonic() >= deadline:
            return False
        current = dict(
            backfill.staging_current(gen, schema),
            capture_mode="append",
            ignore_deletes=ignore_deletes,
        )
        try:
            page = backfill._page(gen["epoch"], gen["catchup_lsn"], high)
            events = backfill.reader.events(page, relation_id, high=high)
            descriptors = backfill.reader.descriptors(page, high=high)
        except (SourceMismatch, HistoryDropped, PinMissing) as exc:
            raise GenerationFailed(str(exc)) from exc
        presented, v1 = backfill._present(events, descriptors, relation_id)
        # The exact cut: commits before C are in the snapshot. A TRUNCATE
        # before C is dropped too, so it never empties staging.
        presented = [e for e in presented if e["commit_lsn"] >= floor]
        _, columns, rows, blocked = backfill.fold_batch(
            relation_id, current, presented, descriptors=v1
        )
        if blocked:  # every hold fails the generation (append_update, schema, ...)
            raise GenerationFailed(f"{blocked.state}: {blocked.reason} at {blocked.commit_lsn}")
        changes = dict(
            catchup_lsn=page[-1].end,
            columns=column_json(columns),
            deletes_ignored=(gen["deletes_ignored"] or 0) + rows.deletes_ignored,
            last_truncate_lsn=rows.truncate_lsn or gen["last_truncate_lsn"],
        )
        lease = backfill.lease(gen["generation_id"], token)
        try:
            write_append(
                backfill,
                current,
                columns,
                rows,
                guard=lease.guard,
                commit_state=lambda: lease.progress_catchup(gen, **changes),
                fault=fault,
                dry_run=False,
                count_state=False,  # counted on the generation, not the live row
            )
        except Blocked as exc:
            raise GenerationFailed(f"{exc.state}: {exc.reason}") from exc
        except DATA_ERRORS as exc:  # as the keyed catch-up
            raise GenerationFailed(
                f"{type(exc).__name__} in catch-up units {page[0].start}..{page[-1].end}"
            ) from exc
        gen = backfill.generation(gen["generation_id"])
    return True


def run_table(backfill, table, relation_id, *, run_id, deadline=None, fault=None, start_new=True):
    """Backfill.run_table for an append table; a refusal to start is `refused`."""
    gen, token = backfill.active(relation_id), None
    if gen is None and not start_new:
        return BackfillResult(table, "", "idle")
    if gen is None:
        try:
            gen, token = start(backfill, table, relation_id, run_id, deadline=deadline, fault=fault)
        except LeaseBusy:
            return BackfillResult(table, "", "busy")
        except ApplyError as exc:
            gen = backfill.active(relation_id)
            if gen is None:
                return BackfillResult(table, "", "refused", detail=str(exc))
    generation_id = gen["generation_id"]
    if gen["mode"] != MODE:
        raise ApplyError(f"{table}: its active generation isn't an append snapshot")
    if token is None:
        try:
            token = backfill.acquire_generation(generation_id, run_id, token=gen["token"])
        except (LeaseBusy, duckdb.TransactionException) as exc:
            if isinstance(exc, duckdb.TransactionException) and "conflict" not in str(exc).lower():
                raise
            return backfill.result(table, generation_id, "busy")
    waited = None
    # A standby source (the generation's recorded mode): the package-only module.
    standby = standby_backfill() if backfill.external(gen) else None
    try:
        if standby:
            standby.check_identity(backfill)
        while True:
            gen = backfill.generation(generation_id)
            if backfill.source_position()[0] != gen["epoch"]:
                raise GenerationFailed("source epoch changed")
            if gen["state"] in ACTIVE:
                backfill.require_pin(gen)
            if gen["state"] == "snapshotting":
                if gen["copy_started"]:  # its snapshot died with the run that took it
                    raise GenerationFailed("the snapshot copy was interrupted; start over")
                if standby:
                    stop = standby.append_copy(
                        backfill, table, gen, token, deadline=deadline, fault=fault
                    )
                    if stop is not None:
                        return stop
                else:
                    copy(backfill, table, gen, token, deadline=deadline, fault=fault)
            elif gen["marker_lsn"] is None:
                high = standby.find_past(backfill, gen) if standby else backfill.find_marker(gen)
                if high is None:
                    if waited is None:
                        if not standby:
                            backfill.write_marker(gen)
                        waited = time.monotonic() + backfill.config.marker_wait_seconds
                        if deadline is not None:
                            waited = min(waited, deadline)
                    if time.monotonic() >= waited:
                        status = "waiting_past_replay" if standby else "waiting_for_marker"
                        return backfill.result(table, generation_id, status)
                    time.sleep(min(2, backfill.config.marker_wait_seconds))
                    continue
                verify_source(backfill, table, gen)
                catalog_change(backfill, gen, high)
                backfill.fail_if_held(gen)
                with backfill.held(generation_id, token) as lease:
                    lease.progress_catchup(gen, marker_lsn=high)
                if fault:
                    fault("after_marker")
            elif backfill.fail_if_held(gen) or not catch_up(
                backfill, table, gen, token, deadline=deadline, fault=fault
            ):
                return backfill.result(table, generation_id, "catching_up")
            else:
                if fault:
                    fault("after_catch_up")
                gen = backfill.generation(generation_id)
                backfill.fail_if_held(gen)
                if not backfill.publish(table, gen, token, run_id, fault=fault):
                    return backfill.result(table, generation_id, "busy_publish")
                return backfill.result(table, generation_id, "published")
    except GenerationFailed as exc:
        backfill.end(backfill.generation(generation_id), token, "failed", exc.reason)
        return backfill.result(table, generation_id, "failed")
    finally:
        try:
            backfill.release_generation(generation_id, token)
        except Exception:
            pass


def verify_source(backfill, table, gen):
    """At the marker: kind, identity and the generation's columns (added ones are fine:
    catch-up adds them, as apply does)."""
    try:
        current = {c.name: c for c in backfill.catalog(gen["relation_id"], table)[0]}
    except ApplyError as exc:
        raise GenerationFailed(str(exc)) from exc
    for column in gen["columns"]:
        other = current.get(column.name)
        # The source type itself, not only the mirror's (type_kept).
        if other is None or other.dtype != column.dtype or not type_kept(column, other):
            raise GenerationFailed(f"source schema changed during backfill: column {column.name!r}")
    why = source_refusal(backfill.pg, backfill.config, gen["relation_id"])
    if why:
        raise GenerationFailed(f"during backfill, the source table {why}")


def _mirror_exists(backfill, schema, name):
    return backfill.con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name=? "
        "AND table_name=?",
        [backfill.config.mirror_database, schema, name],
    ).fetchone()[0]


def catalog_change(backfill, gen, high):
    """A catalog hold (a reasoned key_changes row) cut in (L, high] for this
    relation fails the generation: its snapshot or catch-up may predate the change.
    Read at the marker and again inside publication's transaction (a row commits in
    ingest's finish, after its cut is durable)."""
    row = backfill.execute(
        "SELECT reason FROM {s}key_changes WHERE relation_id=? AND reason IS NOT NULL "
        "AND epoch=? AND cut_lsn>? AND cut_lsn<=? ORDER BY version LIMIT 1",
        [gen["relation_id"], gen["epoch"], gen["pin_lsn"], high],
    ).fetchone()
    if row:
        raise GenerationFailed(f"source changed during backfill: {row[0]}")


def keep_aside(backfill, schema, name, old):
    """Publication's replacement of an append mirror: renamed aside, inside its
    transaction, refused again there while another is kept aside (Q3b). The bound is
    checked at start and here; a kept table created by hand between them, under another
    name than `<name>__gen_<8 hex>`, isn't counted."""
    kept = kept_refusal(backfill, schema, name)
    if kept and _mirror_exists(backfill, schema, name):
        raise GenerationFailed(kept)
    try:
        return _keep_mirror(backfill, schema, name, old)
    except ApplyError as exc:
        raise GenerationFailed(str(exc)) from exc


def apply_state_values(backfill, gen):
    """(ignore_deletes, deletes_ignored, last_truncate_lsn, key_version) for publication,
    inside its transaction: the catalog-hold check again, and the key version past every
    catalog hold cut at or before C (the rule shared with start_append; with that check
    the same number as at or before H)."""
    catalog_change(backfill, gen, gen["marker_lsn"])
    (ignore,) = backfill.execute(
        "SELECT coalesce(ignore_deletes,false) FROM {s}tables WHERE relation_id=?",
        [gen["relation_id"]],
    ).fetchone()
    version = passed_catalog_version(
        backfill, gen["relation_id"], gen["epoch"], gen["snapshot_lsn"]
    )
    return ignore, gen["deletes_ignored"] or 0, gen["last_truncate_lsn"], version
