"""CLI and Flight entrypoint. Secrets come from environment, never command arguments."""

import argparse
import functools
import json
import logging
import os
import re
import time
import uuid
from dataclasses import asdict, replace

import duckdb
import psycopg2
from psycopg2.extensions import parse_dsn

from cdc_flight import explain, flightlog, ops
from cdc_flight.chain import (
    ApplyChain,
    BackfillChain,
    ConfigError,
    after_backfill,
    after_ingest,
    blank_to_unset,
    flag,
    run_flight,
)
from cdc_flight.config import (
    ApplyConfig,
    BackfillConfig,
    IngestConfig,
    identifier,
    quoted,
)
from cdc_flight.config import table_name as split_table
from cdc_flight.flightlog import Run


def process_age():
    """Seconds since this process started (Linux), or None."""
    try:
        with open("/proc/self/stat") as f:
            started = int(f.read().rsplit(")", 1)[1].split()[19])  # field 22, starttime
        with open("/proc/uptime") as f:
            uptime = float(f.read().split()[0])
        return uptime - started / os.sysconf("SC_CLK_TCK")
    except Exception:
        return None


def connect_timing(con, wall):
    """The ingest's `@flight-log timing connect` line: the MotherDuck connect and attach,
    the process's age at that point (startup and imports before it), and the client
    settings the ingest ceiling depends on. Log-only; never raises."""
    try:
        threads, threshold = con.execute(
            "SELECT current_setting('threads'), current_setting('checkpoint_threshold')"
        ).fetchone()
    except Exception:
        threads = threshold = None
    flightlog.timing(
        "connect",
        wall=wall,
        age=process_age(),
        threads=threads,
        checkpoint_threshold=threshold,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=[
            "preflight",
            "ingest",
            "apply",
            "start-from-log",
            "backfill",
            "new-epoch",
            "compact",
            "table-action",
            "table-membership",
            "quarantine",
            "epoch",
            "delete-source",
        ],
    )
    parser.add_argument("--create-objects", action="store_true")
    parser.add_argument("--table", help="schema.table for start-from-log or backfill")
    parser.add_argument(
        "--abandon", action="store_true", help="backfill: abandon the active generation"
    )
    parser.add_argument(
        "--accept-incomplete",
        action="store_true",
        help="start-from-log on a table not observed empty: mirror only later changes",
    )
    parser.add_argument("--epoch", type=int, help="new-epoch: the epoch to begin")
    parser.add_argument("--reason", choices=["slot_lost", "skip"], help="new-epoch: why")
    parser.add_argument("--to-lsn", help="new-epoch --reason skip: the LSN to skip to (X/Y)")
    parser.add_argument("--owner", help="action Flights: the stub's owner settings (JSON)")
    args = parser.parse_args()
    if args.command == "table-action":
        return table_action_main(args)  # its config holds only the action's inputs
    if args.command == "table-membership":
        return table_membership_main(args)
    if args.command == "quarantine":
        return quarantine_main(args)
    if args.command == "epoch":
        return epoch_main(args)
    if args.command == "delete-source":
        return delete_source_main(args)
    # Flights declare every key with a "" default; "" means unset.
    blank_to_unset()
    required()
    require_token()
    if args.command in ("apply", "start-from-log"):
        return apply_main(args)
    if args.command == "compact":
        return compact_main()
    if args.command == "backfill":
        return backfill_main(args)
    # Each command imports its own modules, before any I/O (tests/authority_scan.py
    # follows these imports for each command's authority).
    from cdc_flight import epoch, ops_ingest, pg_ca
    from cdc_flight.bootstrap import bootstrap_log
    from cdc_flight.ingest import observe_after_failure, run_once
    from cdc_flight.runtime import open_log
    from cdc_flight.standby import WaitingForPrimary

    config = IngestConfig.from_env()
    try:
        chain = ApplyChain.from_env() if args.command == "ingest" else None
    except ConfigError as exc:
        raise SystemExit(str(exc)) from None
    try:
        parse_dsn(config.dsn)
    except psycopg2.ProgrammingError as exc:
        # A malformed DSN echoes itself in the message; report the type only.
        raise SystemExit(f"invalid PostgreSQL DSN ({type(exc).__name__})") from None
    if args.command == "preflight":
        with duckdb.connect("md:") as bootstrap:
            bootstrap.execute(f"CREATE DATABASE IF NOT EXISTS {quoted(config.database)}")
    connecting = time.perf_counter()
    con = duckdb.connect(f"md:{config.database}?attach_mode=single")
    if args.command == "ingest":
        connect_timing(con, time.perf_counter() - connecting)
    started, observed, run, info, slot, log = time.monotonic(), {}, None, None, None, None
    try:
        if args.command == "new-epoch":
            log = open_log(con, config)
            return new_epoch_main(args, log.config, log, epoch)
        run = Run()
        create = (
            args.create_objects or os.getenv("CDC_CREATE_OBJECTS") == "1"
        ) and args.command == "preflight"

        def prepare(fault):
            nonlocal log, config
            log, checked = bootstrap_log(con, config, create_objects=create, fault=fault)
            config = log.config
            return checked

        try:
            if args.command == "preflight":
                # Bootstrap commits registration and ownership together. The graph's
                # register step reports that completed operation, without a second write.
                info = explain.preflight(run, prepare, lambda info: None)
            else:
                log = open_log(con, config)
                config = log.config
                slot = log.registered_slot()
                info = explain.step(run, "preflight", lambda: prepare(None))
        except psycopg2.ProgrammingError as exc:
            # A malformed DSN echoes itself in the message; report the type only.
            raise SystemExit(f"invalid PostgreSQL DSN ({type(exc).__name__})") from None
        if args.command == "preflight":
            print(json.dumps(dict(status="ready", source=config.source_id, warnings=info.warnings)))
        else:
            before = log.checkpoint()
            result, _ = explain.ingest(
                run,
                config,
                log,
                info,
                functools.partial(run_once, observe=observed),
                chain,
                lambda target, o: run_flight(con, target, o),
                after_ingest,
                before,
            )
            observed.update(
                status=result.status, transactions=result.transactions, chunks=result.chunks
            )
            print(json.dumps(asdict(result)))
    except WaitingForPrimary as exc:
        # A standby slot's bounded wait ran out: retryable, nothing
        # registered. Exit 3, as an action Flight's busy (action_base.EXIT), so the run
        # shows FAILED: someone must start the primary's heartbeat job.
        observed.update(status="waiting_for_primary")
        print(
            json.dumps(
                dict(status="waiting_for_primary", source=config.source_id, message=str(exc))
            )
        )
        raise SystemExit(3) from None
    except BaseException as exc:
        observed.update(
            status="failed", error=type(exc).__name__, step=run.failed_node if run else None
        )
        if args.command == "ingest" and log is not None:
            observe_after_failure(config, slot, observed)
        raise
    finally:
        try:
            if args.command == "ingest":
                observed["duration_s"] = round(time.monotonic() - started, 3)
                observed.update(pg_ca.fields())  # the run row's TLS (never a status upsert)
                if log is not None:
                    try:
                        position = log.unit_position()
                        # A concurrent later writer may have moved the store. Keep this
                        # run's own collected checkpoint, epoch and counts authoritative.
                        observed.update({key: position[key] for key in ops_ingest.UNIT_POSITION})
                    except Exception as exc:
                        logging.getLogger(__name__).warning(
                            "unit position read failed (%s)", type(exc).__name__
                        )
                ops_ingest.report_ingest(
                    con,
                    config,
                    observed,
                    remaining=config.runtime_seconds - (time.monotonic() - started),
                    database=OPS_DATABASE(),
                )
            elif args.command == "preflight":
                ops_ingest.report_preflight(
                    con,
                    config,
                    observed.get("run_id") or uuid.uuid4().hex,
                    observed.get("status") or "ready",
                    error=observed.get("error"),
                    warnings=info.warnings if info else (),
                    remaining=config.runtime_seconds - (time.monotonic() - started),
                    database=OPS_DATABASE(),
                )
        except Exception as exc:  # the status write never fails the run
            ops.status_write_failed(exc)
        finally:
            con.close()


def require_token():
    token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")
    if not token:
        raise SystemExit("MOTHERDUCK_TOKEN is required; browser login is disabled")
    os.environ["MOTHERDUCK_TOKEN"] = token


def table_action_main(args):
    """The table-action Flight. Owner settings come only from the
    stub's OWNER (--owner), never from config: a runner may override every key the
    Flight declares, and it declares only the action's inputs (action_flight.KEYS)."""
    from cdc_flight import action_flight

    try:
        owner = action_flight.Owner.from_dict(json.loads(args.owner or "{}"))
    except (TypeError, ValueError):
        raise SystemExit("table-action needs valid owner settings (--owner)") from None
    require_token()
    raise SystemExit(action_flight.main(owner))


def table_membership_main(args):
    """The table-membership Flight: add or remove a table. Owner
    settings come only from the stub's OWNER, as for table-action; it is the one action
    Flight that holds the source's PostgreSQL secret (the variable OWNER names)."""
    from cdc_flight import membership_flight

    try:
        owner = membership_flight.Owner.from_dict(json.loads(args.owner or "{}"))
    except (TypeError, ValueError):
        raise SystemExit("table-membership needs valid owner settings (--owner)") from None
    require_token()
    raise SystemExit(membership_flight.main(owner))


def quarantine_main(args):
    """The quarantine Flight: owner settings from the stub's OWNER."""
    from cdc_flight import quarantine_flight

    try:
        owner = quarantine_flight.Owner.from_dict(json.loads(args.owner or "{}"))
    except (TypeError, ValueError):
        raise SystemExit("quarantine needs valid owner settings (--owner)") from None
    require_token()
    raise SystemExit(quarantine_flight.main(owner))


def epoch_main(args):
    """The epoch Flight: recover a lost slot or skip to an LSN. Owner
    settings come only from the stub's OWNER; the secret by the variable it names."""
    from cdc_flight import epoch_flight

    try:
        owner = epoch_flight.Owner.from_dict(json.loads(args.owner or "{}"))
    except (TypeError, ValueError):
        raise SystemExit("epoch needs valid owner settings (--owner)") from None
    require_token()
    raise SystemExit(epoch_flight.main(owner))


def delete_source_main(args):
    """The delete-source Flight. Owner settings come only from the
    stub's OWNER; the secret by the variable it names."""
    from cdc_flight import delete_source_flight

    try:
        owner = delete_source_flight.Owner.from_dict(json.loads(args.owner or "{}"))
    except (TypeError, ValueError):
        raise SystemExit("delete-source needs valid owner settings (--owner)") from None
    require_token()
    raise SystemExit(delete_source_flight.main(owner))


def OPS_DATABASE():  # noqa: N802
    """The operations status database (an owner setting): CDC_OPS_DATABASE."""
    return identifier(os.getenv("CDC_OPS_DATABASE") or ops.DATABASE)


def new_epoch_main(args, config, log, epoch):
    """Begin the next source epoch (see epoch.py). Every mirror then needs a resync."""

    if args.epoch is None or args.reason is None:
        raise SystemExit("new-epoch needs --epoch and --reason")
    to_lsn = None
    if args.reason == "skip":
        if not args.to_lsn or not re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", args.to_lsn):
            raise SystemExit("--reason skip needs --to-lsn X/Y")
        high, low = args.to_lsn.split("/")
        to_lsn = int(high, 16) << 32 | int(low, 16)
        print(
            f"skipping WAL [{epoch.lsn_text(log.checkpoint())}, {args.to_lsn}): it is never ingested"
        )
    elif args.to_lsn:
        raise SystemExit("--to-lsn is only for --reason skip")
    else:
        print(
            f"creating slot {config.slot}_e{args.epoch} if needed: if this does not finish, "
            "retry the same command (or drop that slot), or it retains WAL"
        )
    try:
        # CDC_EPOCH names the epoch before or after the transition; neither must block it.
        log = type(log)(log.con, replace(config, epoch=None))
        result = epoch.begin_epoch(log.config, log, args.epoch, args.reason, to_lsn=to_lsn)
    except psycopg2.ProgrammingError as exc:
        raise SystemExit(f"invalid PostgreSQL DSN ({type(exc).__name__})") from None
    print(json.dumps(asdict(result)))
    if result.status != "busy":
        print(f"run ingest soon: slot {result.slot} retains WAL until ingest consumes it")


def required():
    """Config is run-overridable input: check required keys and the table list's shape
    here, with a message naming the key, before config.py parses them. CDC_TABLES is
    optional (unset, the registry is the table set), but never an explicit []."""
    if not os.getenv("CDC_SOURCE_ID"):
        raise SystemExit("CDC_SOURCE_ID is required")
    try:
        tables = json.loads(os.getenv("CDC_TABLES") or '["unset"]')
    except ValueError:
        tables = None
    if not tables or not isinstance(tables, list) or not all(isinstance(t, str) for t in tables):
        raise SystemExit('CDC_TABLES must be a non-empty JSON list of "schema.table" strings')
    # CDC_DSN_ENV names the variable holding the DSN. It is run-overridable, and a
    # DSN parse error echoes the variable's value, so it may only name a *_DSN
    # variable, never a runtime one such as MOTHERDUCK_TOKEN.
    dsn_env = os.getenv("CDC_DSN_ENV", "")
    if dsn_env and (
        not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,126}_DSN", dsn_env)
        or dsn_env.upper().startswith("MOTHERDUCK_")
    ):
        raise SystemExit("CDC_DSN_ENV must name a variable ending in _DSN")


def member(table, config, key, what=None):
    """A table input: `schema.table`, and one of CDC_TABLES when that's set. Unset,
    the registry decides (start and start-from-log refuse an unregistered table)."""
    try:
        split_table(table)
    except ValueError:
        raise SystemExit(f'{key} must be a "schema.table" name') from None
    if table not in (config.tables or (table,)):
        raise SystemExit(f"{what or key} must be one of CDC_TABLES")


def apply_failed(results):
    """Apply exits 2 for a table that needs an operator; a held source table
    (source_missing) is reported only."""
    return any(r.status in ("needs_action", "needs_resync") for r in results)


def apply_stats(apply):
    """One apply run's counters for its log: page plans (fills, hits, batched failures),
    the schedule's rounds, the fold path per batch and the frame cache. Counts only."""
    return {
        "apply_stats": dict(
            plan=dict(apply.plan_stats),
            rounds=dict(apply.round_stats),
            fold=dict(apply.fold_stats),
            cache=dict(apply.cache_stats),
        )
    }


def apply_main(args):
    """Apply never connects to PostgreSQL. Each correctness-critical write
    transaction touches only the mirror database; retention pins are written to
    cdc_internal in separate transactions, so the change log stays attached
    read-write."""
    from cdc_flight import ops_apply, table_set
    from cdc_flight.apply_run import ApplyRun
    from cdc_flight.releases import record_release

    config = ApplyConfig.from_env()
    if args.command == "start-from-log":
        member(args.table or "", config, "--table")  # before any connection
    with duckdb.connect("md:") as bootstrap:
        bootstrap.execute(f"CREATE DATABASE IF NOT EXISTS {quoted(config.mirror_database)}")
    con = duckdb.connect(f"md:{config.mirror_database}?attach_mode=single")
    started, results, run_id, failure = time.monotonic(), None, uuid.uuid4().hex, {}
    run = apply = None
    try:
        run = Run()  # Flight Explainer lines; see flights/charts/apply.mmd

        def attach():
            con.execute(f"ATTACH IF NOT EXISTS 'md:{config.database}' AS {quoted(config.database)}")
            con.execute("SET TimeZone='UTC'")
            apply = ApplyRun(con, config)
            apply.initialize()
            record_release(apply.execute, "apply", os.environ)  # D5: inside a Flight only
            return apply

        apply = explain.step(run, "attach", attach)
        if args.command == "start-from-log":
            generation = apply.start_from_log(args.table, accept_incomplete=args.accept_incomplete)
            print(json.dumps(dict(status="published", table=args.table, generation=generation)))
        else:
            results = explain.apply(
                run,
                lambda f: table_set.run(apply, f, run, run_id=run_id),
                config.mirror_database,
            )
            print(json.dumps([asdict(result) for result in results]))
            # The run's schedule and fold counters, values-free (no "status" key: never a
            # result line): plan fills/hits, rounds, fold paths, cache use.
            print(json.dumps(apply_stats(apply)))
            if apply_failed(results):
                raise SystemExit(2)
    except BaseException as exc:
        failure = dict(status="failed", error=type(exc).__name__, step=run and run.failed_node)
        if isinstance(exc, SystemExit) and exc.code == 2:
            failure = dict(status="needs_operator")  # held tables: reported, not a crash
        raise
    finally:
        try:
            if args.command == "apply":
                ops_apply.report_apply(
                    con,
                    config,
                    run_id,
                    results or (),
                    **failure,
                    remaining=config.runtime_seconds - (time.monotonic() - started),
                    database=OPS_DATABASE(),
                    # The unit times this run's reads checked ("current as of").
                    times=apply.reader.commit_times if apply is not None else None,
                )
        except Exception as exc:  # the status write never fails the run
            ops.status_write_failed(exc)
        finally:
            con.close()


def compact_main():
    """Compaction reads and writes only cdc_internal: no PostgreSQL and no secret.
    Exits 2 when a published table has lost its pin (nothing more drops until it's
    fixed) and 3 when a drop would raise the floor past the ingest checkpoint; busy
    and capped runs exit 0, since the next run continues."""
    from cdc_flight import ops_compact
    from cdc_flight.compact import CompactConfig, run_compaction
    from cdc_flight.generation_spec import InvariantViolation
    from cdc_flight.retention import FloorAboveCheckpoint

    try:
        config = CompactConfig.from_env()
    except ValueError as exc:
        raise SystemExit(f"invalid compaction config: {exc}") from None
    con = duckdb.connect(f"md:{config.database}?attach_mode=single")
    started, observed = time.monotonic(), {}
    try:
        con.execute("SET TimeZone='UTC'")
        run = Run()  # Flight Explainer lines; see flights/charts/compact.mmd
        try:
            result = run_compaction(
                config, con, on_event=explain.compaction_events(run), observe=observed
            )
        except (FloorAboveCheckpoint, InvariantViolation) as exc:
            # An invariant broke: nothing was dropped or pruned past it, or a generation
            # expiry could not fail was left as it was (after the rest of the run). Exit 3,
            # distinct from held (2), so the run's status names it.
            explain.failed(run, exc)
            print(json.dumps(dict(status="invariant", error=type(exc).__name__)))
            raise SystemExit(3) from None
        except Exception as exc:
            explain.failed(run, exc)
            raise
        print(json.dumps(asdict(result)))
        if result.status == "held":
            raise SystemExit(2)
    finally:
        try:
            if "result" in observed:
                ops_compact.report_compaction(
                    con,
                    config,
                    observed["result"],
                    observed.get("as_of"),
                    observed.get("summary"),
                    remaining=config.runtime_seconds - (time.monotonic() - started),
                    database=OPS_DATABASE(),
                )
        except Exception as exc:  # the status write never fails the run
            ops.status_write_failed(exc)
        finally:
            con.close()


def backfill_main(args):
    """One table per run. Flights pass inputs as config, so the table and the abandon
    flag also come from CDC_BACKFILL_TABLE and CDC_BACKFILL_ABANDON=1. Inside a Flight
    the run re-triggers itself (resume only) until the generation is published,
    failed or idle; see chain.after_backfill."""
    from cdc_flight import ops_apply, pg_ca
    from cdc_flight.backfill import Backfill
    from cdc_flight.pg_connect import query
    from cdc_flight.releases import record_release

    config = BackfillConfig.from_env()
    table = args.table or os.getenv("CDC_BACKFILL_TABLE", "")
    member(table, config, "--table" if args.table else "CDC_BACKFILL_TABLE", "backfill table")
    try:
        abandon = args.abandon or flag(os.environ, "CDC_BACKFILL_ABANDON", False)
        chain = BackfillChain.from_env(table)
    except ConfigError as exc:
        raise SystemExit(str(exc)) from None
    with duckdb.connect("md:") as bootstrap:
        bootstrap.execute(f"CREATE DATABASE IF NOT EXISTS {quoted(config.mirror_database)}")
    con = duckdb.connect(f"md:{config.mirror_database}?attach_mode=single")
    started, totals, run_id, failure = time.monotonic(), {}, uuid.uuid4().hex, {}
    run, result, backfill = None, None, None
    try:
        try:
            con.execute(f"ATTACH IF NOT EXISTS 'md:{config.database}' AS {quoted(config.database)}")
            con.execute("SET TimeZone='UTC'")
            backfill = Backfill(con, config)
            backfill.initialize()
            record_release(backfill.execute, "backfill", os.environ)  # D5: Flight only
            if abandon:
                generation = backfill.abandon(table)
                print(json.dumps(dict(status="abandoned", table=table, generation=generation)))
                failure = dict(status="abandoned")
                return
            deadline = time.monotonic() + config.runtime_seconds
            run = Run()  # Flight Explainer lines; see flights/charts/backfill.mmd
            gen = backfill.active(backfill.registered(table, append_ok=True)[0])
            total = None
            if gen is None and chain.start or gen and explain.resume_point(gen) == "scan":
                total = explain.estimate_chunks(backfill.pg, table, config.chunk_rows, query)
            schema, name = split_table(table)
            result = explain.backfill(
                run,
                gen,
                total,
                f"{config.mirror_database}.{schema}.{name}",
                lambda fault: backfill.run_table(
                    table, run_id=run_id, deadline=deadline, start=chain.start, fault=fault
                ),
                start=chain.start,
            )
            if total is not None and result.generation:
                totals[result.generation] = total
            # `fold`: catch-up batches by path ("sql", "fallback:<reason>").
            print(json.dumps(dict(asdict(result), fold=backfill.fold_stats)))
            if result.status == "busy":
                print(
                    json.dumps(
                        dict(
                            status="busy",
                            table=table,
                            detail="another run or an unexpired lease holds this generation; "
                            "its chain continues, or rerun with CDC_BACKFILL_START=0 after "
                            "the lease expires"
                            if result.generation
                            # No generation: the retention guard stayed busy at start.
                            else "the retention guard stayed busy; nothing started, so "
                            "rerun with CDC_BACKFILL_START=1",
                        )
                    )
                )
            # The ingest checkpoint, so waiting on a draining ingest isn't a stall.
            ingest_lsn = backfill.source_position()[1]
        finally:
            # run_table released the generation lease; close the snapshot source
            # before the next run can start.
            if backfill is not None:
                backfill.close()
        if result.status in ("failed", "refused"):
            explain.backfill_outcome(run, result, "stopped", {})
            raise SystemExit(2)
        launched = {}
        outcome, counts = after_backfill(
            result,
            chain,
            explain.traced_trigger(
                run, "retrigger", "backfill", lambda target, o: run_flight(con, target, o), launched
            ),
            ingest_lsn=ingest_lsn,
        )
        explain.backfill_outcome(run, result, outcome, launched)
        if outcome in ("stalled", "waiting", "trigger_failed"):
            detail = {
                "stalled": "neither the generation nor ingest moved; check ingest",
                "waiting": "ingest moved but the generation did not; check apply leases "
                "and this table's key checks",
                "trigger_failed": "the re-trigger failed",
            }[outcome]
            print(
                json.dumps(
                    dict(
                        status="retrigger_failed" if outcome == "trigger_failed" else "stalled",
                        bound=outcome,
                        detail=detail,
                        table=table,
                        generation=result.generation,
                        last_status=result.status,
                        **counts,
                    )
                )
            )
            raise SystemExit(3)
    except BaseException as exc:
        failure = dict(status="failed", error=type(exc).__name__, step=run and run.failed_node)
        raise
    finally:
        try:
            ops_apply.report_backfill(
                con,
                config,
                run_id,
                totals=totals,
                table=table,
                generation=result.generation if result else None,
                **(failure or dict(status=result.status if result else "done")),
                remaining=config.runtime_seconds - (time.monotonic() - started),
                database=OPS_DATABASE(),
                tls=pg_ca.fields(),
                fold=backfill.fold_stats if backfill is not None else None,
                detail=(result.detail or None) if result else None,
            )
        except Exception as exc:  # the status write never fails the run
            ops.status_write_failed(exc)
        finally:
            con.close()


if __name__ == "__main__":
    main()
