"""`cdc-flight-tables`: the operator commands, run outside a Flight (no Flight command
imports this module). add/remove go through the membership core; pause, resume, retry and
quarantine through table_actions, on the observed blocked unit."""

import json
import uuid

import duckdb

from cdc_flight.config import quoted, table_name
from cdc_flight.membership import add_table, remove_table


def main():
    """`cdc-flight-tables add|remove --table s.t [--force]`: operator commands, run
    outside a Flight (no Flight command imports them). Same environment as the Flights:
    CDC_SOURCE_ID, the DSN secret and MOTHERDUCK_TOKEN (CDC_TABLES optional).

    `pause|resume|retry|quarantine` (table_actions) act on the mirror only: no
    PostgreSQL and no DSN; CDC_MIRROR_DATABASE names the mirror if not the default."""
    import argparse
    import os
    from dataclasses import asdict

    from cdc_flight.apply import Apply
    from cdc_flight.chain import blank_to_unset
    from cdc_flight.cli import required
    from cdc_flight.config import ApplyConfig, IngestConfig
    from cdc_flight.preflight import connect
    from cdc_flight.runtime import open_log

    parser = argparse.ArgumentParser(prog="cdc-flight-tables")
    parser.add_argument(
        "action",
        choices=[
            "add",
            "remove",
            "pause",
            "resume",
            "retry",
            "quarantine",
            "releases",
            "types",
            "accept-types",
        ],
    )
    parser.add_argument("--table", help="schema.table (every action but releases and types)")
    parser.add_argument(
        "--not-deployed",
        metavar="COMMAND",
        help="releases: declare that no Flight runs COMMAND for this source, so append "
        "mode's add doesn't wait for its version stamp (a later Flight run replaces it)",
    )
    parser.add_argument(
        "--force", action="store_true", help="remove: also a table that still exists"
    )
    parser.add_argument(
        "--mode",
        choices=["keyed", "append"],
        default="keyed",
        help="add: keyed (default), or append: insert-only capture of a keyless table",
    )
    parser.add_argument(
        "--ignore-deletes",
        action="store_true",
        help="add --mode append: skip DELETEs (counted) instead of holding the table",
    )
    parser.add_argument("--expected-state", help="pause/resume: refuse unless the state is this")
    parser.add_argument(
        "--blocked-lsn", help='retry: the blocked commit, X/Y, or "none" for a hold without one'
    )
    parser.add_argument("--epoch", type=int, help="quarantine: the table's source epoch")
    parser.add_argument(
        "--confirm",
        help='quarantine: "QUARANTINE <schema.table> UNIT <epoch> <X/Y>..<X/Y>"; accept-types: '
        '"ACCEPT TYPES <schema.table> <digest>"; as the dry run shows',
    )
    parser.add_argument("--dry-run", action="store_true", help="validate and report only")
    args = parser.parse_args()
    if args.action in ("releases", "types"):
        if args.table:
            raise SystemExit(f"--table is not used by {args.action}")
    elif not args.table:
        raise SystemExit("--table is required")
    elif args.not_deployed:
        raise SystemExit("--not-deployed is only for releases")
    if args.action != "add" and (args.mode != "keyed" or args.ignore_deletes):
        raise SystemExit("--mode and --ignore-deletes are only for add")
    if args.ignore_deletes and args.mode != "append":
        raise SystemExit("--ignore-deletes is only for --mode append")
    blank_to_unset()
    required()
    if not (os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")):
        raise SystemExit("MOTHERDUCK_TOKEN is required; browser login is disabled")
    if args.action == "releases":
        return _releases(args)
    if args.action in ("types", "accept-types"):
        return _types(args)
    table_name(args.table)
    if args.action not in ("add", "remove"):
        return _table_action(args)
    config, apply_config = IngestConfig.from_env(), ApplyConfig.from_env()
    con = duckdb.connect(f"md:{apply_config.mirror_database}?attach_mode=single")
    pg = None
    try:
        con.execute(f"ATTACH IF NOT EXISTS 'md:{config.database}' AS {quoted(config.database)}")
        con.execute("SET TimeZone='UTC'")
        log = open_log(con, config)  # the stored format-2 store; anything else refuses
        apply = Apply(con, apply_config)
        apply.initialize()
        if args.action == "remove":
            result = remove_table(log, apply, args.table, force=args.force)
        else:
            pg = connect(config)
            result = add_table(
                log,
                apply,
                pg,
                config.publication,
                args.table,
                mode=args.mode,
                ignore_deletes=args.ignore_deletes,
            )
    finally:
        if pg is not None:
            pg.close()
        con.close()
    print(json.dumps(asdict(result)))
    if result.status == "refused":
        raise SystemExit(2)
    if result.status == "busy":
        raise SystemExit(3)


def _releases(args):
    """`releases [--not-deployed COMMAND]`: each stamped command's status, after
    recording the declaration if given. Outside a Flight: no stamp is written here."""
    import getpass

    from cdc_flight.config import IngestConfig
    from cdc_flight.releases import ReleaseError, declare_not_deployed, release_status
    from cdc_flight.runtime import open_log

    config = IngestConfig.from_env()
    con = duckdb.connect(f"md:{config.database}?attach_mode=single")
    try:
        con.execute("SET TimeZone='UTC'")
        log = open_log(con, config)
        log.initialize()  # adds the declaration's columns to a guard-release store
        if args.not_deployed:
            try:
                declare_not_deployed(log.execute, args.not_deployed, f"cli:{getpass.getuser()}")
            except ReleaseError as exc:
                raise SystemExit(str(exc)) from None
        print(json.dumps(release_status(log.execute)))
    finally:
        con.close()


def _types(args):
    """`types`: the pre-deploy type identity report (read-only, every mirror table).
    `accept-types --table T [--dry-run] [--confirm TEXT]`: accept one table's source
    retype (type_identity.accept). The source's catalog is read with the DSN secret."""
    from cdc_flight import type_identity
    from cdc_flight.apply import Apply
    from cdc_flight.config import ApplyConfig, IngestConfig
    from cdc_flight.preflight import connect

    config, apply_config = IngestConfig.from_env(), ApplyConfig.from_env()
    con = duckdb.connect(f"md:{apply_config.mirror_database}?attach_mode=single")
    pg = None
    try:
        con.execute(f"ATTACH IF NOT EXISTS 'md:{config.database}' AS {quoted(config.database)}")
        con.execute("SET TimeZone='UTC'")
        apply = Apply(con, apply_config)
        pg = connect(config)
        if args.action == "types":
            print(json.dumps(type_identity.report(apply, pg)))
            return
        result = type_identity.accept(
            apply, pg, args.table, confirm=args.confirm, dry_run=args.dry_run
        )
    finally:
        if pg is not None:
            pg.close()
        con.close()
    print(json.dumps(result))
    if result["status"] == "refused":
        raise SystemExit(2)
    if result["status"] == "busy":
        raise SystemExit(3)


def _table_action(args):
    """pause, resume, retry, quarantine: inputs checked before any connection. retry and
    quarantine act on the observed blocked unit, exactly as the action Flights do
    (action_flight.unit_arguments): quarantine's --confirm must be the unit literal."""
    import getpass
    from dataclasses import asdict

    from cdc_flight import table_actions
    from cdc_flight.action_flight import Inputs, unit_arguments
    from cdc_flight.apply import Apply
    from cdc_flight.config import ApplyConfig

    inputs = {}
    try:
        if args.action == "retry":
            # "none": a hold with no blocked commit (a mirror row to repair), exactly.
            inputs["expected_blocked_lsn"] = (
                None if args.blocked_lsn == "none" else table_actions.parse_lsn(args.blocked_lsn)
            )
    except ValueError as exc:
        raise SystemExit(f"--blocked-lsn: {exc}") from None
    if args.action == "quarantine":
        if args.epoch is None or not args.confirm:
            raise SystemExit("quarantine requires --epoch and --confirm")
        inputs.update(expected_epoch=args.epoch, confirm=args.confirm)
    if args.action in ("pause", "resume"):
        inputs["expected_state"] = args.expected_state
    config = ApplyConfig.from_env()
    con = duckdb.connect(f"md:{config.mirror_database}?attach_mode=single")
    try:
        con.execute(f"ATTACH IF NOT EXISTS 'md:{config.database}' AS {quoted(config.database)}")
        con.execute("SET TimeZone='UTC'")
        apply = Apply(con, config)
        apply.initialize()
        unit, result = unit_arguments(
            apply, Inputs(args.action, args.table, dict(inputs), dry_run=args.dry_run)
        )
        if result is None:
            result = table_actions.ACTIONS[args.action](
                apply,
                args.table,
                run_id=uuid.uuid4().hex,
                actor=f"cli:{getpass.getuser()}",
                dry_run=args.dry_run,
                **inputs,
                **unit,
            )
    finally:
        con.close()
    print(json.dumps(asdict(result)))
    if result.status == "refused":
        raise SystemExit(2)
    if result.status == "busy":
        raise SystemExit(3)
