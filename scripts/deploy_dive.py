"""Deploy the operations Dive (docs/dive.md): create or update it by title, over the share
of `cdc_ops`.

    cd dive && npm ci && npm run bundle && cd ..
    python scripts/deploy_dive.py --share md:_share/cdc_ops/<id>          # dry run: prints the plan
    python scripts/deploy_dive.py --share … --expect-user <owner> --apply  # with approval only

The dry run connects to nothing and needs no token. `--expect-user` is also the owner the
Dive is built for: only that user can act from it, and everyone else sees it read-only
(dive/src/owner.ts). `--apply`, as the owner of `cdc_ops` and its Flights:
1. checks the token is set (a missing token would open a browser login) and that
   `current_user` is `--expect-user`;
2. creates `cdc_ops` if it doesn't exist (the Flights never do) and attaches it, then
   every status table and column the Dive reads, add-only (the writers' own DDL), so the
   Dive never names a missing table or column;
3. creates the Dive, or updates the one with the same title, with the bundled source
   and the share as its only resource.

Making the Dive and the share viewable by the whole org is a separate owner step (docs/dive.md).
Mirrors motherduckdb/md-migration-assessment's `publish.py` (MD_CREATE_DIVE /
MD_UPDATE_DIVE_CONTENT by title).
"""

import argparse
import json
import os
import re
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cdc_flight import ops, ops_init  # noqa: E402
from cdc_flight.chain import FLIGHT_CONFIG  # noqa: E402
from cdc_flight.config import quoted  # noqa: E402

BUNDLE = ROOT / "dive" / ".build" / "index.tsx"
PLACEHOLDER = "md:_share/cdc_ops/REPLACE_WITH_SHARE_ID"  # dive/src/share.ts
ALIAS = "ops"
TITLE = "CDC operations"
DESCRIPTION = (
    "Postgres to MotherDuck replication status from the ops share: every source with its "
    "tables, problems first; a source page (pipeline, WAL limit and slots, recovery, "
    "retention, setup check); a drawer per table. Read-only; owner-only actions come later. "
    "Metadata only."
)
SHARE = re.compile(r"md:_share/[A-Za-z0-9_]+/[0-9a-fA-F-]{36}")
# Every table the Dive reads, with the writers' own format-2 DDL (columns are only ever
# added): the deploy creates any missing table or column, so every query runs on an empty
# or an older cdc_ops (no Flight has reported yet, or one from before a column existed).
TABLES = ops_init.TABLES_V2


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


FLIGHTS_PLACEHOLDER = '"__CDC_FLIGHTS__"'  # dive/src/flights.ts, as esbuild writes it
OWNER_PLACEHOLDER = '"__CDC_OWNER__"'  # dive/src/owner.ts
FLIGHT_KINDS = {
    "table_action": "table-action",
    "table_membership": "table-membership",
    "quarantine": "quarantine",
    "epoch": "epoch",
    "delete_source": "delete-source",
}


def load_flights(path):
    """`{source: {"table_action": uuid, "table_membership": uuid}}`, validated."""
    try:
        flights = json.loads(Path(path).read_text())
    except (OSError, ValueError) as exc:
        raise SystemExit(f"--flights: can't read {path}: {type(exc).__name__}") from None
    if not isinstance(flights, dict) or not flights:
        raise SystemExit("--flights: expected a non-empty JSON object of sources")
    for source, kinds in flights.items():
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", source) or not isinstance(kinds, dict):
            raise SystemExit(f"--flights: bad source entry {source!r}")
        for kind, flight in kinds.items():
            if kind not in FLIGHT_KINDS:
                raise SystemExit(f"--flights: {source}: unknown kind {kind!r}")
            try:
                kinds[kind] = str(uuid.UUID(flight))
            except (ValueError, TypeError, AttributeError):
                raise SystemExit(f"--flights: {source}.{kind} is not a Flight id") from None
    return flights


def inject_flights(source, flights):
    if source.count(FLIGHTS_PLACEHOLDER) != 1:
        raise SystemExit("the bundle must contain the Flights placeholder exactly once")
    return source.replace(FLIGHTS_PLACEHOLDER, json.dumps(json.dumps(flights, sort_keys=True)))


def inject_owner(source, owner):
    """The owner's user name as the bundle's owner literal: the one viewer who can act."""
    if not owner or owner.startswith("__"):
        raise SystemExit(f"--expect-user: not a user name: {owner!r}")
    if source.count(OWNER_PLACEHOLDER) != 1:
        raise SystemExit("the bundle must contain the owner placeholder exactly once")
    return source.replace(OWNER_PLACEHOLDER, json.dumps(owner))


# Actions are owner-only, enforced by RUN: the grant check guards the Flights a deploy wires
# into the Dive. A read-only deploy wires none, so it has nothing to check (and says so).
NO_ACTIONS = (
    "no --flights: no action Flights wired, so the RUN grant check is skipped (read-only Dive)"
)


def check_flights(con, flights, owner):
    """Each Flight exists, is `owner`'s, declares exactly its kind's inputs, and is
    runnable by its owner only: actions are owner-only, enforced by RUN, so a RUN grant
    to anyone else refuses the deploy. Raises on a mismatch; returns warnings."""
    warnings = []
    for source, kinds in flights.items():
        for kind, flight in kinds.items():
            name = f"{source}.{kind}"
            row = con.execute(
                f"SELECT owner_name,current_version FROM MD_GET_FLIGHT(flight_id := '{flight}'::UUID)"
            ).fetchone()
            if row is None:
                raise SystemExit(f"{name}: no Flight {flight}")
            if row[0] != owner:
                raise SystemExit(f"{name}: owned by {row[0]!r}, not {owner!r}")
            keys = con.execute(
                "SELECT map_keys(config) FROM MD_GET_FLIGHT_VERSION("
                f"flight_id := '{flight}'::UUID, version_number := {int(row[1])})"
            ).fetchone()[0]
            expected = set(FLIGHT_CONFIG[FLIGHT_KINDS[kind]])
            missing, extra = sorted(expected - set(keys)), sorted(set(keys) - expected)
            if missing or extra:
                raise SystemExit(
                    f"{name}: declared inputs differ (missing {missing}: an older bundle; "
                    f"extra {extra}: a newer one)"
                )
            grants = con.execute(
                "SELECT grantee_type,grantee_name,privilege "
                f"FROM MD_LIST_FLIGHT_GRANTEES('{flight}'::UUID)"
            ).fetchall()
            others = sorted(
                f"{(kind_ or '').lower()} {who}"
                for kind_, who, privilege in grants
                if (privilege or "").lower() == "run"
                and not ((kind_ or "").lower() == "user" and who == owner)
            )
            if others:
                raise SystemExit(
                    f"{name}: granted RUN to {', '.join(others)}: actions are owner-only; "
                    f"revoke it (REVOKE RUN ON FLIGHT '{flight}' FROM …)"
                )
    return warnings


def check_share_database(share, database):
    """The deploy adds missing tables and columns to `database`, and the Dive reads the
    share: they must be the same database, or the Dive reads one the DDL never reached.
    The share URL names the share, not its database, so the share must be named after its
    database (`CREATE SHARE cdc_ops FROM cdc_ops`); this checks that name."""
    shared = share.split("/")[1]
    if shared != database:
        raise SystemExit(
            f"--share is named {shared!r}, but --database is {database!r}: the share must be "
            f"named after the database it shares (CREATE SHARE {database} FROM {database}), "
            "so the DDL reaches the database the Dive reads"
        )


def dive_source(share, path=None):
    path = path or BUNDLE
    if not SHARE.fullmatch(share):
        raise SystemExit(f"--share must look like md:_share/<database>/<uuid>, not {share!r}")
    if not path.is_file():
        raise SystemExit("the Dive is not bundled: run `npm ci && npm run bundle` in dive/")
    source = path.read_text(encoding="utf-8")
    if source.count(PLACEHOLDER) != 1:
        raise SystemExit(f"{path} must contain the share placeholder exactly once")
    return source.replace(PLACEHOLDER, share)


def create_database(con, database):
    """`cdc_ops` itself, then attached: in workspace mode `md:` attaches only the
    databases that existed at connect, and the DDL reads the client catalog."""
    statements = [
        f"CREATE DATABASE IF NOT EXISTS {quoted(database)}",
        f"ATTACH IF NOT EXISTS 'md:{database}' AS {quoted(database)}",
    ]
    for statement in statements:
        con.execute(statement)
    return statements


def create_schema(con, database="cdc_ops"):
    """The writers' add-only DDL, on an attached `database`: every table and column
    exists afterwards. Returns the statements it ran."""
    seen = []
    writer = ops.Ops(con, None, None, database)
    execute = writer.execute

    def record(statement, parameters=None):
        seen.append(statement.replace("{o}", writer.prefix))
        return execute(statement, parameters)

    writer.execute = record
    writer._schema(TABLES)
    return seen


def dive_statements(con, title, content, share):
    """The statements that create the Dive, or update the one titled `title`."""
    resources = f"[{{'url': {literal(share)}, 'alias': {literal(ALIAS)}}}]"
    rows = con.execute(f"SELECT id FROM MD_LIST_DIVES() WHERE title = {literal(title)}").fetchall()
    if len(rows) > 1:
        raise SystemExit(f"{len(rows)} Dives are titled {title!r}; pass a unique --title")
    if rows:
        dive = literal(rows[0][0])
        # count(*): the result rows carry TIMESTAMPTZ columns, which fetching would
        # convert with pytz (not a dependency); only that the update ran matters.
        return [
            f"SELECT count(*) FROM MD_UPDATE_DIVE_CONTENT(id = {dive}::UUID, "
            f"content = {literal(content)}, api_version = 1, required_resources = {resources})",
            f"SELECT count(*) FROM MD_UPDATE_DIVE_METADATA(id = {dive}::UUID, "
            f"title = {literal(title)}, description = {literal(DESCRIPTION)})",
        ]
    return [
        f"SELECT id FROM MD_CREATE_DIVE(title = {literal(title)}, content = {literal(content)}, "
        f"description = {literal(DESCRIPTION)}, api_version = 1, required_resources = {resources})"
    ]


def token():
    value = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not value:
        raise SystemExit("MOTHERDUCK_TOKEN is not set: refusing to connect to md:")
    return value


def deploy(con, *, share, title, expect_user, database, content, flights=None):
    user = con.execute("SELECT current_user").fetchone()[0]
    if user != expect_user:
        raise SystemExit(f"connected as {user!r}, not --expect-user {expect_user!r}: refusing")
    if flights:
        for warning in check_flights(con, flights, expect_user):
            print(f"warning: {warning}")
    else:
        print(NO_ACTIONS)
    ran = create_database(con, database) + create_schema(con, database)
    statements = dive_statements(con, title, content, share)
    result = None
    for statement in statements:
        result = con.execute(statement).fetchall()
    return ran, statements, result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--share", required=True, help="the cdc_ops share URL (md:_share/…)")
    parser.add_argument("--title", default=TITLE)
    parser.add_argument("--database", default=ops.DATABASE, help="the status database")
    parser.add_argument(
        "--expect-user",
        help="the owner's user name: required with --apply, the only one who can act",
    )
    parser.add_argument("--flights", help="JSON: {source: {table_action, table_membership}}")
    parser.add_argument("--apply", action="store_true", help="deploy (only with approval)")
    args = parser.parse_args(argv)
    content = dive_source(args.share)
    check_share_database(args.share, args.database)
    flights = load_flights(args.flights) if args.flights else None
    if flights:
        content = inject_flights(content, flights)
    if args.expect_user:
        content = inject_owner(content, args.expect_user)
    if not args.apply:
        print(f"dry run: Dive {args.title!r} over {args.share} as {ALIAS!r}")
        print(f"  {len(content) / 1024:.1f} KB of Dive source from {BUNDLE.relative_to(ROOT)}")
        print(f"  then: CREATE DATABASE IF NOT EXISTS {args.database}, ATTACH it")
        print(f"  then: the add-only DDL for {', '.join(TABLES)} on {args.database}")
        if flights:
            for source, kinds in sorted(flights.items()):
                for kind, flight in sorted(kinds.items()):
                    print(f"  action Flight {source}.{kind} = {flight} (checked with --apply)")
        else:
            print(f"  {NO_ACTIONS}")
        if args.expect_user and flights:
            print(f"  actions: {args.expect_user} only; read-only for every other viewer")
        elif args.expect_user:
            print(f"  owner: {args.expect_user}; no actions wired, so read-only for every viewer")
        else:
            print("  no --expect-user: read-only for every viewer")
        print("  then: MD_CREATE_DIVE, or MD_UPDATE_DIVE_CONTENT if the title exists")
        print(
            "  the share must be UPDATE AUTOMATIC (viewers then lag the Flights by about a "
            "minute); org-wide viewing is a separate owner step (docs/dive.md)"
        )
        return 0
    if not args.expect_user:
        raise SystemExit("--apply needs --expect-user")
    import duckdb

    with duckdb.connect("md:", config={"motherduck_token": token()}) as con:
        ran, statements, result = deploy(
            con,
            share=args.share,
            title=args.title,
            expect_user=args.expect_user,
            database=args.database,
            content=content,
            flights=flights,
        )
    print(f"ran {len(ran)} DDL statements; {statements[0].split('(')[0]}: {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
