"""Create or update all of a source's Flights from one source file (docs/flights.md, "Deploy
a source's Flights").

usage: uv run python scripts/deploy_flights.py <source.toml> [--adopt] [--disable-ingest]
       [--apply --expect <digest>]

Without --apply it only prints the plan: each Flight is `create`, `update: <fields>`,
`unchanged`, `opted out` or `REFUSE: <why>`, and the plan's digest. With --apply --expect
<digest> it recomputes the plan, whose digest must equal the approved one, creates and updates the Flights in dependency order (apply before
ingest, backfill and compact before the action Flights, which bake their ids), then reads
every Flight back. A second run plans `unchanged` for all.

- MotherDuck SQL functions (MD_CREATE_FLIGHT, MD_UPDATE_FLIGHT, MD_GET_FLIGHT, ...) over a
  `md:` connection with the token from the environment (motherduck_token). The token, DSNs
  and secret values are never read into output: secrets are checked by name only.
- It refuses unless md_user_info()'s organization is the file's `org`.
- Owner-only: it never grants, never deletes a Flight, never runs one, and refuses a Flight
  that has grantees or that someone else owns. A same-named Flight that isn't this
  source's package stub of that command is refused unless --adopt.
- The ingest schedule is set only when `[ingest] enabled = true`; an existing schedule is
  cleared only with --disable-ingest.
- Every Flight is a package stub pinned to `package` (build_flight.py --package), so the
  checkout must be clean at that merged commit. The install is anonymous unless
  `[secrets] package` names the Flights secret of a read-only GitHub token (a private copy
  of the repository).

The source file (TOML) holds names, never secrets; docs/deploy_flights.example.toml shows
every key.
"""

import argparse
import ast
import dataclasses
import hashlib
import importlib
import json
import os
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import build_flight  # noqa: E402

from cdc_flight.chain import FLIGHT_CONFIG  # noqa: E402
from cdc_flight.config import identifier  # noqa: E402

# Creation order: a Flight's dependencies come first (ingest bakes apply's id into its
# config; the action Flights bake backfill's and compact's into their OWNER).
ORDER = (
    "preflight",
    "apply",
    "ingest",
    "backfill",
    "compact",
    "table-action",
    "table-membership",
    "quarantine",
    "epoch",
    "delete-source",
)
DESTRUCTIVE = ("epoch", "delete-source")
INGEST_SCHEDULE = "*/3 * * * *"


@dataclasses.dataclass(frozen=True)
class Defaults:
    schedule: str | None
    max_runtime_sec: int
    instance: str | None  # None: MotherDuck's default size (never sent or compared)


# The setup table (docs/setup.md, step 5): one place, read by the tests too.
DEFAULTS = {
    "preflight": Defaults(None, 600, None),
    "ingest": Defaults(None, 900, "F16"),  # INGEST_SCHEDULE only when enabled
    "apply": Defaults("*/10 * * * *", 900, "F16"),
    "backfill": Defaults(None, 1800, None),
    "compact": Defaults("0 3 * * *", 900, None),
    "table-action": Defaults(None, 300, None),
    "table-membership": Defaults(None, 300, None),
    "quarantine": Defaults(None, 300, None),
    "epoch": Defaults(None, 600, None),
    "delete-source": Defaults(None, 600, None),
}
INSTANCES = ("F4", "F16", "F32")
CRON = re.compile(r"(\S+\s+){4}\S+")
# Values that look like a credential: the file holds names only.
SECRET_LIKE = re.compile(r"password\s*=|postgres(ql)?://|://[^/\s]*@", re.I)
# Case-sensitive shapes: a JWT (three dotted base64url parts), an AWS access key id, and any
# run of 64+ token characters (PostgreSQL identifiers stop at 63).
TOKEN_LIKE = re.compile(
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.|\bAKIA[0-9A-Z]{16}\b|[A-Za-z0-9_-]{64,}"
)


class SourceError(ValueError):
    """The source file is invalid: raised before any network call."""


@dataclasses.dataclass(frozen=True)
class FlightSettings:
    name: str
    schedule: str | None
    max_runtime_sec: int
    instance: str | None


@dataclasses.dataclass(frozen=True)
class Source:
    org: str
    source: str
    package: str
    pg_ca: str | None
    pg_secret: str
    pg_key: str
    membership_secret: str  # "" = the [postgres] secret (one role)
    membership_key: str
    package_secret: str  # "" = an anonymous install (no token)
    publication: str
    heartbeat: str
    slot: str
    tables: tuple
    # Replica capture: "self" (a primary) or "external" (a standby; the DBA's job on the
    # primary writes the heartbeat). Passed to the Flights only when external.
    heartbeat_writer: str
    heartbeat_interval_seconds: int  # 0 = unset (the Flights' default, 60)
    create_objects: bool
    ingest_enabled: bool
    destructive: bool
    flights: dict  # command -> FlightSettings

    @property
    def dsn_env(self):
        return f"{self.pg_secret}_{self.pg_key}"

    def secret_for(self, command):
        """The PostgreSQL secret a command reads: table-membership's own when set (the
        optional read-only role, docs/operations.md), else the [postgres] one."""
        if command == "table-membership" and self.membership_secret:
            return self.membership_secret
        return self.pg_secret

    def dsn_env_for(self, command):
        if command == "table-membership" and self.membership_secret:
            return f"{self.membership_secret}_{self.membership_key}"
        return self.dsn_env

    def commands(self):
        return [c for c in ORDER if self.destructive or c not in DESTRUCTIVE]


SCHEMA = {
    "": {
        "org",
        "source",
        "package",
        "pg_ca",
        "postgres",
        "secrets",
        "preflight",
        "ingest",
        "destructive",
        "flights",
    },  # fmt: skip
    "postgres": {
        "secret",
        "key",
        "membership_secret",
        "membership_key",
        "publication",
        "heartbeat",
        "heartbeat_writer",
        "heartbeat_interval_seconds",
        "slot",
    },
    "secrets": {"package"},
    "preflight": {"tables", "create_objects"},
    "ingest": {"enabled"},
    "destructive": {"enabled"},
    "flight": {"name", "schedule", "max_runtime_sec", "instance"},
}


def _keys(table, allowed, where):
    if not isinstance(table, dict):
        raise SourceError(f"{where or 'the file'} must be a table")
    unknown = set(table) - allowed
    if unknown:
        raise SourceError(f"unknown key(s) in {where or 'the file'}: {sorted(unknown)}")


def _no_secret(value, where):
    for text in value if isinstance(value, list) else [value]:
        if isinstance(text, str) and (SECRET_LIKE.search(text) or TOKEN_LIKE.search(text)):
            raise SourceError(f"{where} looks like a credential: the file holds names only")


def load_source(path):
    """The validated source file. Pure: no network, no checkout reads."""
    try:
        raw = tomllib.loads(Path(path).read_text())
    except tomllib.TOMLDecodeError as exc:
        raise SourceError(f"not valid TOML: {exc}") from None
    _keys(raw, SCHEMA[""], "")
    for section in ("postgres", "secrets", "preflight", "ingest", "destructive"):
        _keys(raw.get(section, {}), SCHEMA[section], f"[{section}]")
    flights = raw.get("flights", {})
    _keys(flights, set(ORDER), "[flights]")
    for command, table in flights.items():
        _keys(table, SCHEMA["flight"], f"[flights.{command}]")

    def walk(value, where):
        if isinstance(value, dict):
            for k, v in value.items():
                walk(v, f"{where}.{k}" if where else k)
        elif not re.fullmatch(r"flights\.[a-z-]+\.name", where):
            # Names have their own rule (letters, digits, '-', '_', '.').
            _no_secret(value, where)

    walk(raw, "")

    def text(value, where, default=None):
        if value is None:
            if default is None:
                raise SourceError(f"{where} is required")
            return default
        if not isinstance(value, str):
            raise SourceError(f"{where} must be a string")
        return value

    def flag(value, where, default):
        if value is None:
            return default
        if not isinstance(value, bool):
            raise SourceError(f"{where} must be true or false")
        return value

    try:
        source = identifier(text(raw.get("source"), "source"))
        org = text(raw.get("org"), "org")
        if not org.strip():
            raise SourceError("org is required")
        package = text(raw.get("package"), "package")
        if not re.fullmatch(r"[0-9a-f]{40}", package):
            raise SourceError("package must be a full 40-hex commit SHA merged to main")
        pg_ca = raw.get("pg_ca")
        if pg_ca is not None:
            from cdc_flight import pg_ca as cas

            if pg_ca not in cas.CAS:
                raise SourceError(f"pg_ca: unknown CA {pg_ca!r}; known: {sorted(cas.CAS)}")
        pg = raw.get("postgres", {})
        pg_secret = identifier(text(pg.get("secret"), "postgres.secret"))
        pg_key = text(pg.get("key"), "postgres.key", "DSN")
        dsn_env = f"{pg_secret}_{pg_key}"
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,126}_DSN", dsn_env):
            raise SourceError("postgres.key must end in DSN (the variable is <secret>_<key>)")
        membership_secret = text(pg.get("membership_secret"), "postgres.membership_secret", "")
        membership_key = text(pg.get("membership_key"), "postgres.membership_key", "DSN")
        if membership_secret:
            identifier(membership_secret)
            if not re.fullmatch(
                r"[A-Za-z][A-Za-z0-9_]{0,126}_DSN", f"{membership_secret}_{membership_key}"
            ):
                raise SourceError("postgres.membership_key must end in DSN")
            if membership_secret == pg_secret:
                raise SourceError("postgres.membership_secret is the replication secret: omit it")
        elif "membership_key" in pg:
            raise SourceError("postgres.membership_key needs postgres.membership_secret")
        publication = text(pg.get("publication"), "postgres.publication", "")
        slot = text(pg.get("slot"), "postgres.slot", "")
        for name, value in (("postgres.publication", publication), ("postgres.slot", slot)):
            if value:
                identifier(value)
        heartbeat = text(pg.get("heartbeat"), "postgres.heartbeat", "")
        if heartbeat and not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", heartbeat):
            raise SourceError("postgres.heartbeat must be schema.table")
        writer = text(pg.get("heartbeat_writer"), "postgres.heartbeat_writer", "self")
        if writer not in ("self", "external"):
            raise SourceError("postgres.heartbeat_writer must be self or external")
        interval = pg.get("heartbeat_interval_seconds", 0)
        if (
            not isinstance(interval, int)
            or isinstance(interval, bool)
            or not (interval == 0 or 1 <= interval <= 86400)
        ):
            raise SourceError("postgres.heartbeat_interval_seconds must be 1..86400")
        if interval and writer != "external":
            raise SourceError(
                "postgres.heartbeat_interval_seconds is for heartbeat_writer = external"
            )
        package_secret = text(raw.get("secrets", {}).get("package"), "secrets.package", "")
        if package_secret:
            identifier(package_secret)
            if package_secret in (pg_secret, membership_secret):
                raise SourceError("secrets.package must differ from the PostgreSQL secrets")
        pre = raw.get("preflight", {})
        tables = pre.get("tables", [])
        if (
            not isinstance(tables, list)
            or not tables
            or not all(isinstance(t, str) and re.fullmatch(r"[^.\s]+\.[^.\s]+", t) for t in tables)
        ):
            raise SourceError("preflight.tables must be a non-empty list of schema.table")
    except ValueError as exc:
        raise SourceError(str(exc)) from None
    settings = {}
    for command in ORDER:
        d, table = DEFAULTS[command], flights.get(command, {})
        name = text(table.get("name"), f"flights.{command}.name", f"cdc-{source}-{command}")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name):
            raise SourceError(f"flights.{command}.name: letters, digits, '-', '_', '.'")
        schedule = table.get("schedule", d.schedule)
        if command == "ingest" and "schedule" in table:
            raise SourceError("the ingest schedule is [ingest] enabled, not a flights key")
        if schedule == "":
            schedule = None
        if schedule is not None and (not isinstance(schedule, str) or not CRON.fullmatch(schedule)):
            raise SourceError(f"flights.{command}.schedule must be a 5-field cron")
        runtime = table.get("max_runtime_sec", d.max_runtime_sec)
        if not isinstance(runtime, int) or isinstance(runtime, bool) or not 60 <= runtime <= 86400:
            raise SourceError(f"flights.{command}.max_runtime_sec must be 60..86400")
        instance = table.get("instance", d.instance)
        if instance is not None and instance not in INSTANCES:
            raise SourceError(f"flights.{command}.instance must be one of {INSTANCES}")
        settings[command] = FlightSettings(name, schedule, runtime, instance)
    names = [s.name for s in settings.values()]
    if len(set(names)) != len(names):
        raise SourceError("two Flights share a name")
    return Source(
        org=org,
        source=source,
        package=package,
        pg_ca=pg_ca,
        pg_secret=pg_secret,
        pg_key=pg_key,
        membership_secret=membership_secret,
        membership_key=membership_key,
        package_secret=package_secret,
        publication=publication,
        heartbeat=heartbeat,
        slot=slot,
        tables=tuple(tables),
        heartbeat_writer=writer,
        heartbeat_interval_seconds=interval,
        create_objects=flag(pre.get("create_objects"), "preflight.create_objects", True),
        ingest_enabled=flag(raw.get("ingest", {}).get("enabled"), "ingest.enabled", False),
        destructive=flag(raw.get("destructive", {}).get("enabled"), "destructive.enabled", True),
        flights=settings,
    )


# --- the desired Flights --------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Desired:
    command: str
    name: str
    source_code: str
    requirements_txt: str
    config: dict
    secrets: tuple
    instance: str | None
    max_runtime_sec: int
    schedule: str | None
    chart: str
    pending: tuple = ()  # dependencies not created yet (plan mode): ids are placeholders


PLACEHOLDER = "00000000-0000-0000-0000-000000000000"


def config_values(src, command, ids):
    """The Flight's config: its declared template (every key, "" meaning the default) with
    the values this source sets. Action Flights take their settings in OWNER instead."""
    values = dict.fromkeys(FLIGHT_CONFIG[command], "")
    if command in build_flight.ACTION_COMMANDS:
        return values
    sets = {"CDC_SOURCE_ID": src.source}
    if command in build_flight.PG_COMMANDS:
        sets["CDC_DSN_ENV"] = src.dsn_env_for(command)
    for key, value in (
        ("CDC_PUBLICATION", src.publication),
        ("CDC_HEARTBEAT", src.heartbeat),
        ("CDC_SLOT", src.slot),
        ("CDC_HEARTBEAT_WRITER", "external" if src.heartbeat_writer == "external" else ""),
        (
            "CDC_HEARTBEAT_INTERVAL_SECONDS",
            str(src.heartbeat_interval_seconds) if src.heartbeat_interval_seconds else "",
        ),
    ):
        if value:
            sets[key] = value
    if command == "preflight":
        sets["CDC_TABLES"] = json.dumps(list(src.tables))
        if src.create_objects:
            sets["CDC_CREATE_OBJECTS"] = "1"
    if command == "ingest":
        sets["CDC_APPLY_FLIGHT_ID"] = ids.get("apply", PLACEHOLDER)
    for key, value in sets.items():
        if key in values:
            values[key] = value
    return values


def owner_values(src, command, ids):
    """The OWNER settings an action Flight bakes in: only its Owner class's fields."""
    if command not in build_flight.ACTION_COMMANDS:
        return None
    module = importlib.import_module(f"cdc_flight.{build_flight.ACTION_COMMANDS[command]}")
    names = {f.name for f in dataclasses.fields(module.Owner)}
    candidates = dict(
        source_id=src.source,
        backfill_flight=ids.get("backfill", PLACEHOLDER),
        compact_flight=ids.get("compact", PLACEHOLDER),
        dsn_env=src.dsn_env_for(command),
        publication=src.publication,
        heartbeat=src.heartbeat,
        slot=src.slot,
        heartbeat_writer="external" if src.heartbeat_writer == "external" else "",
    )
    return {k: v for k, v in candidates.items() if k in names and v != ""}


def depends(command):
    """The Flights whose ids this one bakes in."""
    if command == "ingest":
        return ("apply",)
    if command not in build_flight.ACTION_COMMANDS:
        return ()
    module = importlib.import_module(f"cdc_flight.{build_flight.ACTION_COMMANDS[command]}")
    names = {f.name for f in dataclasses.fields(module.Owner)}
    return tuple(c for c in ("backfill", "compact") if f"{c}_flight" in names)


def desired(src, command, ids, version):
    """What the Flight should be, given the ids of the Flights it depends on."""
    s = src.flights[command]
    pg_ca = src.pg_ca if command in build_flight.PG_COMMANDS else None
    token = src.package_secret or None
    code = build_flight.stub(
        command, src.package, version, owner_values(src, command, ids), pg_ca=pg_ca, token=token
    )
    secrets = [token] if token else []
    if command in build_flight.PG_COMMANDS:
        secrets.insert(0, src.secret_for(command))
    schedule = s.schedule
    if command == "ingest":
        schedule = INGEST_SCHEDULE if src.ingest_enabled else None
    return Desired(
        command=command,
        name=s.name,
        source_code=code,
        requirements_txt=build_flight.requirements(src.package, token=token),
        config=config_values(src, command, ids),
        secrets=tuple(sorted(secrets)),
        instance=s.instance,
        max_runtime_sec=s.max_runtime_sec,
        schedule=schedule,
        chart=build_flight.chart(command),
        pending=tuple(d for d in depends(command) if d not in ids),
    )


# --- MotherDuck --------------------------------------------------------------------------


def _mapping(value):
    if value is None:
        return {}
    if isinstance(value, str):
        return json.loads(value) if value else {}
    if isinstance(value, list):  # a MAP as a list of {key, value}
        return {item["key"]: item["value"] for item in value}
    return dict(value)


class MD:
    """The MotherDuck SQL functions this script uses, over one md: connection."""

    LIMIT = 10000

    def __init__(self, con):
        self.con = con

    def _rows(self, sql, params=()):
        cur = self.con.execute(sql, list(params))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]

    def user_info(self):
        row = self._rows("SELECT org_name, username FROM md_user_info()")[0]
        return row["org_name"], row["username"]

    def flights(self, owner_only):
        """Every visible Flight, in one page: MD_LIST_FLIGHTS has no stated order, so OFFSET
        paging could skip a row (and a skipped Flight would be created twice)."""
        rows = self._rows(
            "SELECT flight_id::VARCHAR AS flight_id, flight_name, owner_name FROM MD_LIST_FLIGHTS("
            '"LIMIT" := ?, "OFFSET" := 0, owner_only := ?)',
            [self.LIMIT, owner_only],
        )
        if len(rows) >= self.LIMIT or len({r["flight_id"] for r in rows}) != len(rows):
            raise SystemExit(f"refused: can't list every Flight in one page of {self.LIMIT}")
        return rows

    def state(self, flight_id):
        """(summary, current version) of a Flight."""
        # Named columns, never the timestamps: DuckDB converts TIMESTAMPTZ with pytz,
        # which this project doesn't install.
        f = self._rows(
            "SELECT flight_name, current_version, schedule_cron, schedule_status::VARCHAR AS "
            "schedule_status, status::VARCHAR AS status, owner_name "
            "FROM MD_GET_FLIGHT(flight_id := ?::UUID)",
            [flight_id],
        )[0]
        v = self._rows(
            "SELECT source_code, requirements_txt, config, flight_secret_names, instance_type, "
            "max_runtime_sec, metadata, access_token_name FROM MD_GET_FLIGHT_VERSION("
            "flight_id := ?::UUID, version_number := ?::UINTEGER)",
            [flight_id, int(f["current_version"])],
        )[0]
        return f, v

    def grantees(self, flight_id):
        return self._rows(
            "SELECT count(*) AS n FROM MD_LIST_FLIGHT_GRANTEES(?::UUID)", [flight_id]
        )[0]["n"]

    def secret_names(self):
        return {
            r["name"]
            for r in self._rows("SELECT name FROM duckdb_secrets() WHERE type = 'flights'")
        }

    def create(self, d):
        args = [
            ("name", "?", d.name),
            ("source_code", "?", d.source_code),
            ("requirements_txt", "?", d.requirements_txt),
            ("config", "?::MAP(VARCHAR, VARCHAR)", d.config),
            ("flight_secret_names", "?::VARCHAR[]", list(d.secrets)),
            ("metadata", "MAP {'chart': ?}", d.chart),
            ("max_runtime_sec", "?::UINTEGER", d.max_runtime_sec),
        ]
        if d.instance:
            args.append(("instance_type", "?", d.instance))
        if d.schedule:
            args.append(("schedule_cron", "?", d.schedule))
        sql = ", ".join(f"{k} := {p}" for k, p, _ in args)
        self._rows(
            f"SELECT flight_id::VARCHAR AS flight_id FROM MD_CREATE_FLIGHT({sql})",
            [v for _, _, v in args],
        )

    def update(self, flight_id, d, fields):
        """MD_UPDATE_FLIGHT with only `fields` (PATCH); source always with its chart."""
        cols = {
            "source_code": ("source_code", "?", d.source_code),
            "requirements_txt": ("requirements_txt", "?", d.requirements_txt),
            "config": ("config", "?::MAP(VARCHAR, VARCHAR)", d.config),
            "secrets": ("flight_secret_names", "?::VARCHAR[]", list(d.secrets)),
            "instance": ("instance_type", "?", d.instance),
            "max_runtime_sec": ("max_runtime_sec", "?::UINTEGER", d.max_runtime_sec),
            "schedule": ("schedule_cron", "?", d.schedule or ""),
            "chart": ("metadata", "MAP {'chart': ?}", d.chart),
        }
        wanted = set(fields) | ({"chart"} if "source_code" in fields else set())
        args = [("flight_id", "?::UUID", flight_id)] + [cols[f] for f in sorted(wanted)]
        sql = ", ".join(f"{k} := {p}" for k, p, _ in args)
        self._rows(
            f"SELECT flight_id::VARCHAR AS flight_id FROM MD_UPDATE_FLIGHT({sql})",
            [v for _, _, v in args],
        )


# --- plan ------------------------------------------------------------------------------


def observed(summary, version):
    """A Flight's current values, in Desired's terms."""
    metadata = _mapping(version.get("metadata"))
    return dict(
        source_code=version.get("source_code") or "",
        requirements_txt=version.get("requirements_txt") or "",
        config={k: str(v) for k, v in _mapping(version.get("config")).items()},
        secrets=tuple(sorted(version.get("flight_secret_names") or ())),
        instance=version.get("instance_type"),
        max_runtime_sec=int(version.get("max_runtime_sec") or 0),
        # MD_UPDATE_FLIGHT(schedule_cron := '') disables a schedule but keeps its cron
        # (live test, 2026-11-14): only an ACTIVE schedule counts as one.
        schedule=(summary.get("schedule_cron") or None)
        if str(summary.get("schedule_status")) == "ACTIVE"
        else None,
        chart=metadata.get("chart"),
    )


def differences(d, now):
    """The fields to update, in a fixed order."""
    out = []
    for field in (
        "source_code",
        "requirements_txt",
        "config",
        "secrets",
        "max_runtime_sec",
        "chart",
    ):
        if getattr(d, field) != now[field]:
            out.append(field)
    if d.instance is not None and d.instance != now["instance"]:
        out.append("instance")
    if d.schedule != now["schedule"]:
        out.append("schedule")
    return out


def foreign(src, command, now):
    """Why even --adopt must not take this Flight: a stub of another command, or another
    source's (its CDC_SOURCE_ID config or OWNER source_id). None if adoptable."""
    code = now["source_code"]
    stub = re.search(r"^COMMAND = '([a-z-]+)'$", code, re.M)
    if code.startswith("# cdc_flight Flight stub.") and stub and stub.group(1) != command:
        return f"it is a {stub.group(1)} stub"
    source = now["config"].get("CDC_SOURCE_ID", "")
    if source and source != src.source:
        return f"it belongs to source {source!r}"
    owner = re.search(r"^OWNER = (.+)$", code, re.M)
    if owner:
        try:
            values = json.loads(ast.literal_eval(owner.group(1)))
        except (ValueError, SyntaxError):
            return "its OWNER can't be read"
        if isinstance(values, dict) and values.get("source_id", src.source) != src.source:
            return f"it belongs to source {values['source_id']!r}"
    return None


def secret_change(src, command, now, d):
    """Why table-membership must not be re-pointed: its PostgreSQL secret differs from the
    file's. A hardened Flight (the read-only role's secret) is never moved to the
    replication secret by a run, with or without --adopt, nor the other way round: the
    file must name the secret the Flight already uses."""
    if command != "table-membership":
        return None
    # The token's secret isn't a PostgreSQL secret. Anonymous, a Flight may still carry the
    # default-named one from a token install: that run drops it.
    token = {src.package_secret or build_flight.TOKEN_SECRET}
    current = sorted(set(now["secrets"]) - token)
    wanted = sorted(set(d.secrets) - token)
    if current == wanted:
        return None
    return (
        f"it reads the PostgreSQL secret {', '.join(current) or '(none)'}, the file says "
        f"{', '.join(wanted)}: name the secret it uses ([postgres] membership_secret for a "
        "read-only role), or change the Flight by hand"
    )


def recognised(src, command, now):
    """Whether a Flight is this source's package stub of this command: the stub's COMMAND,
    and the source in its OWNER (action Flights) or its CDC_SOURCE_ID config (the rest)."""
    code = now["source_code"]
    if not code.startswith("# cdc_flight Flight stub.") or f"COMMAND = {command!r}\n" not in code:
        return False
    owner = re.search(r"^OWNER = (.+)$", code, re.M)
    if command not in build_flight.ACTION_COMMANDS:
        return owner is None and now["config"].get("CDC_SOURCE_ID") == src.source
    try:
        values = json.loads(ast.literal_eval(owner.group(1))) if owner else {}
    except (ValueError, SyntaxError):
        return False
    return isinstance(values, dict) and values.get("source_id") == src.source


@dataclasses.dataclass
class Step:
    command: str
    name: str
    action: str  # create | update | unchanged | opted out | REFUSE
    fields: list = dataclasses.field(default_factory=list)
    reason: str = ""
    flight_id: str | None = None
    pending: tuple = ()
    content: str = ""  # sha256 of the desired Flight (digest input)
    note: str = ""

    def line(self):
        what = self.action
        if self.action == "update":
            what = "update: " + ", ".join(self.fields)
        if self.note:
            what += f" ({self.note})"
        if self.action == "REFUSE":
            what = f"REFUSE: {self.reason}"
        if self.pending:
            what += f" (after {', '.join(self.pending)} exist)"
        return f"{self.command:<17} {self.name:<40} {what}"

    def key(self):
        return (
            self.command,
            self.name,
            self.action,
            tuple(self.fields),
            self.flight_id,
            self.content,
        )


def content(d):
    """sha256 over everything a Flight is created or updated with."""
    fields = dataclasses.asdict(d)
    fields["config"] = sorted(fields["config"].items())
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def digest(org, steps):
    """The approval handle: --apply --expect must match the printed plan's digest. The org
    is in it too: a plan of creates only has no Flight ids to tie it to one."""
    text = json.dumps([org, *(s.key() for s in steps)])
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def plan(md, src, version, *, adopt=False, disable_ingest=False, mine=None, everyone=None):
    """One Step per Flight of the source, in ORDER. Reads only."""
    listed = mine if mine is not None else md.flights(True)
    names = [f["flight_name"] for f in listed]
    twice = {n for n in names if names.count(n) > 1}
    mine = {f["flight_name"]: f for f in listed}
    everyone = everyone if everyone is not None else md.flights(False)
    others = {f["flight_name"]: f for f in everyone if f["flight_name"] not in mine}
    ids = {c: mine[src.flights[c].name]["flight_id"] for c in ORDER if src.flights[c].name in mine}
    steps = []
    for command in ORDER:
        name = src.flights[command].name
        if command not in src.commands():
            steps.append(
                Step(
                    command,
                    name,
                    "opted out",
                    reason="[destructive] enabled = false",
                    flight_id=ids.get(command),
                )  # fmt: skip
            )
            continue
        if name in others:
            steps.append(
                Step(command, name, "REFUSE", reason="a Flight of that name is someone else's")
            )
            continue
        if name in twice:
            steps.append(Step(command, name, "REFUSE", reason="you own two Flights of that name"))
            continue
        d = desired(src, command, ids, version)
        if name not in mine:
            steps.append(Step(command, name, "create", pending=d.pending, content=content(d)))
            continue
        fid = ids[command]
        summary, current = md.state(fid)
        now = observed(summary, current)
        if md.grantees(fid):
            steps.append(
                Step(command, name, "REFUSE", reason="it has grantees (owner-only)", flight_id=fid)
            )
            continue
        alien = foreign(src, command, now)
        if alien:
            steps.append(
                Step(command, name, "REFUSE", flight_id=fid, reason=f"{alien} (not even --adopt)")
            )
            continue
        moved = secret_change(src, command, now, d)
        if moved:
            steps.append(Step(command, name, "REFUSE", flight_id=fid, reason=moved))
            continue
        if not adopt and not recognised(src, command, now):
            steps.append(
                Step(
                    command,
                    name,
                    "REFUSE",
                    flight_id=fid,
                    reason=f"not this source's {command} package stub (pass --adopt to take it over)",
                )  # fmt: skip
            )
            continue
        fields = differences(d, now)
        if command == "ingest" and now["schedule"] and d.schedule is None and not disable_ingest:
            steps.append(
                Step(
                    command,
                    name,
                    "REFUSE",
                    flight_id=fid,
                    reason="ingest is scheduled: set [ingest] enabled = true, or pass --disable-ingest",
                )  # fmt: skip
            )
            continue
        note = ""
        if "schedule" in fields:
            note = f"schedule {now['schedule'] or 'none'} -> {d.schedule or 'none'}"
        steps.append(
            Step(
                command,
                name,
                "update" if fields else "unchanged",
                fields,
                flight_id=fid,
                pending=d.pending,
                content=content(d),
                note=note,
            )
        )
    return steps


def preconditions(md, src):
    """Refusals that stop everything: the wrong org, missing secrets."""
    org, user = md.user_info()
    if org != src.org:
        raise SystemExit(
            f"refused: this token is signed in to the organization {org!r}, the file names "
            f"{src.org!r}; nothing was read or changed"
        )
    need = {src.pg_secret} | ({src.package_secret} - {""})  # preflight always connects
    if "table-membership" in src.commands():
        need.add(src.secret_for("table-membership"))
    missing = sorted(need - md.secret_names())
    if missing:
        raise SystemExit(
            f"refused: missing Flights secret(s) {missing} (create them in the MotherDuck UI)"
        )
    return user


# --- apply and read back -----------------------------------------------------------------


def apply(md, src, version, expect, *, adopt=False, disable_ingest=False):
    """Create/update in ORDER, recomputing each Flight with the ids created so far. The plan
    (actions, fields, Flight ids and the desired content) must hash to `expect`, the digest
    the approved plan printed, or nothing is changed."""
    again = plan(md, src, version, adopt=adopt, disable_ingest=disable_ingest)
    if digest(src.org, again) != expect:
        raise SystemExit("refused: the plan differs from the approved one (digest); plan again")
    done = []
    for step in again:
        if step.action not in ("create", "update"):
            continue
        mine = {f["flight_name"]: f for f in md.flights(True)}
        ids = {
            c: mine[src.flights[c].name]["flight_id"] for c in ORDER if src.flights[c].name in mine
        }
        d = desired(src, step.command, ids, version)
        if d.pending:
            raise SystemExit(f"refused: {step.command} needs {', '.join(d.pending)} first")
        if step.action == "create":
            md.create(d)
        else:
            summary, current = md.state(step.flight_id)
            fields = differences(d, observed(summary, current))
            if fields:
                md.update(step.flight_id, d, fields)
        done.append(step.command)
    return done


def read_back(md, src, version):
    """Every Flight of the source against its desired values; (ok, rows)."""
    mine = {f["flight_name"]: f for f in md.flights(True)}
    ids = {c: mine[src.flights[c].name]["flight_id"] for c in ORDER if src.flights[c].name in mine}
    rows, ok = [], True
    for command in src.commands():
        name = src.flights[command].name
        if name not in mine:
            rows.append(dict(command=command, name=name, ok=False, missing=True))
            ok = False
            continue
        fid = ids[command]
        summary, current = md.state(fid)
        d = desired(src, command, ids, version)
        fields = differences(d, observed(summary, current))
        check = dict(
            fields=not fields,
            grantees=md.grantees(fid) == 0,
            status=str(summary.get("status", "ACTIVE")) == "ACTIVE",
        )
        row = dict(command=command, name=name, id=fid, version=int(summary["current_version"]),
                   source_sha256=hashlib.sha256(current.get("source_code", "").encode()).hexdigest(),
                   check=check, ok=all(check.values()))  # fmt: skip
        if fields:
            row["differs"] = fields
        ok &= row["ok"]
        rows.append(row)
    return ok, rows


def connect():
    if not (os.getenv("motherduck_token") or os.getenv("MOTHERDUCK_TOKEN")):
        raise SystemExit("set motherduck_token (the Flights owner's token) in the environment")
    import duckdb

    return duckdb.connect("md:")


def main(argv=None, md=None, version=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("source_file", type=Path)
    parser.add_argument(
        "--apply", action="store_true", help="create and update (default: plan only)"
    )
    parser.add_argument(
        "--adopt",
        action="store_true",
        help="take over same-named Flights that aren't this source's stubs",
    )
    parser.add_argument(
        "--disable-ingest", action="store_true", help="clear a live ingest schedule"
    )
    parser.add_argument("--expect", help="with --apply: the approved plan's digest")
    args = parser.parse_args(argv)
    if args.apply and not args.expect:
        raise SystemExit("--apply needs --expect <digest>: the digest the approved plan printed")
    try:
        src = load_source(args.source_file)
    except SourceError as exc:
        raise SystemExit(f"{args.source_file}: {exc}") from None
    if version is None:
        try:
            version = build_flight.pinned(src.package)
        except ValueError as exc:
            raise SystemExit(str(exc)) from None
    md = md or MD(connect())
    preconditions(md, src)
    steps = plan(md, src, version, adopt=args.adopt, disable_ingest=args.disable_ingest)
    print(f"source {src.source} in {src.org} at {src.package[:12]} (version {version}):")
    for step in steps:
        print("  " + step.line())
    refused = [s for s in steps if s.action == "REFUSE"]
    print(f"plan digest: {digest(src.org, steps)}")
    summary = dict(source=src.source, org=src.org, package=src.package, digest=digest(src.org, steps),
                   plan=[dict(command=s.command, name=s.name, action=s.action, fields=s.fields) for s in steps])  # fmt: skip
    if refused or not args.apply:
        summary["result"] = "refused" if refused else "plan"
        print(json.dumps(summary))
        return 1 if refused else 0
    summary["applied"] = apply(
        md, src, version, args.expect, adopt=args.adopt, disable_ingest=args.disable_ingest
    )
    ok, rows = read_back(md, src, version)
    summary.update(read_back=rows, result="ok" if ok else "read_back_failed")
    print(json.dumps(summary))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
