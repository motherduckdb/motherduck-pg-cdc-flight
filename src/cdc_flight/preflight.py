"""Validate explicitly selected PostgreSQL objects before provisioning CDC."""

import socket
from dataclasses import dataclass, field, replace

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn

from cdc_flight import pg_ca
from cdc_flight.config import table_name
from cdc_flight.pg_connect import (  # noqa: F401
    STATEMENT_TIMEOUT,
    PreflightError,
    catalog_key,
    check_formats,
    connect,
    connection_options,
    query,
)


@dataclass
class SourceInfo:
    identity: tuple
    initial_lsn: int
    tables: list
    heartbeat_oid: int
    warnings: list
    # Against a registry (after the first registration; see classify):
    held: dict = field(default_factory=dict)  # relation_id -> (reason, detail)
    held_names: dict = field(default_factory=dict)  # relation_id -> registered (schema, name)
    present: list = field(default_factory=list)  # registered OIDs found under their name
    unmanaged: list = field(default_factory=list)  # unregistered publication members
    stale: list = field(default_factory=list)  # CDC_TABLES names that were removed or renamed
    undeclared: list = field(default_factory=list)  # active tables CDC_TABLES omits
    # What this run created (create_objects): delete-source drops only these.
    created: dict = field(default_factory=dict)
    # External mode (a standby): its timeline, recorded at registration; None otherwise.
    timeline: int | None = None

    @property
    def keys(self):
        return {table["oid"]: tuple(table["keys"]) for table in self.tables}


def qualified(name):
    return sql.SQL(".").join(map(sql.Identifier, table_name(name)))


def ddl_plan(config, tables=None):
    """The provisioning DDL. `tables` is the publication's table list: CDC_TABLES at the
    first registration, the registry's captured tables afterwards (CDC_TABLES may be
    unset then). SQL composition stays separate from the connection secret."""
    names = [qualified(name) for name in (*(tables or config.tables), config.heartbeat)]
    return [
        sql.SQL("CREATE TABLE {} (id text PRIMARY KEY, marker text NOT NULL)").format(
            qualified(config.heartbeat)
        ),
        sql.SQL("CREATE PUBLICATION {} FOR TABLE {} WITH (publish_via_partition_root=true)").format(
            sql.Identifier(config.publication), sql.SQL(", ").join(names)
        ),
        sql.SQL("SELECT * FROM pg_create_logical_replication_slot({},'pgoutput')").format(
            sql.Literal(config.slot)
        ),
    ]


# One statement for every managed table: the key exactly as
# catalog_key reads it, plus (the end read only) the facts ingest's per-run checks compare.
_KEY_JOIN = (
    "FROM pg_class c LEFT JOIN LATERAL ("
    "SELECT array_agg(a.attname::text ORDER BY k.ord) AS key,bool_or(i.indisprimary) AS primary_key "
    "FROM (SELECT i.indexrelid FROM pg_index i WHERE i.indrelid=c.oid "
    "AND (i.indisreplident OR i.indisprimary) ORDER BY i.indisreplident DESC LIMIT 1) x "
    "JOIN pg_index i ON i.indexrelid=x.indexrelid "
    "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord) "
    "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
    "WHERE k.ord<=i.indnkeyatts) k ON TRUE "
    "WHERE c.oid=ANY(%s::oid[])"
)
CATALOG_KEYS = "SELECT c.oid::bigint,k.key " + _KEY_JOIN
CATALOG_FACTS = (
    "SELECT c.oid::bigint,k.key,k.primary_key,c.relkind::text,c.relreplident::text,"
    "c.relfilenode::bigint,"
    "(SELECT p.oid::bigint FROM pg_constraint p WHERE p.conrelid=c.oid AND p.contype='p'),"
    # The leaf set as a count and a digest: a fixed-size value however many partitions.
    "CASE WHEN c.relkind='p' THEN (SELECT count(*)::text||':'||"
    "coalesce(md5(string_agg(t.relid::bigint::text,',' ORDER BY t.relid::bigint)),'') "
    "FROM pg_partition_tree(c.oid) t WHERE t.isleaf) END,"
    "(SELECT array_agg(a.attname::text ORDER BY a.attnum) FROM pg_attribute a "
    "WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),"
    # A partitioned root's new column carries atthasmissing on its leaves only.
    "(SELECT array_agg(DISTINCT a.attname::text) FROM pg_attribute a "
    "WHERE a.attnum>0 AND NOT a.attisdropped AND a.atthasmissing AND a.attrelid IN "
    "(SELECT c.oid WHERE c.relkind<>'p' UNION ALL "
    "SELECT t.relid FROM pg_partition_tree(c.oid) t WHERE c.relkind='p' AND t.isleaf)) " + _KEY_JOIN
)


def catalog_facts(con, oids, *, keys_only=False):
    """{oid: facts} for every table in `oids` that still exists (a dropped one is
    absent), in one statement. facts: key (catalog_key's columns, or None); unless
    `keys_only` (ingest's start read): from_pk (the key is the primary key's index),
    relkind, replident, relfilenode, pk_oid (the primary key constraint), leaves (a
    partitioned table's leaf count and digest), columns (live, in attnum order) and
    missing (live columns with atthasmissing, a partitioned table's from its leaves)."""
    if not oids:
        return {}
    params = [sorted(int(o) for o in oids)]
    if keys_only:
        return {
            int(oid): dict(key=list(key) if key else None)
            for oid, key in query(con, CATALOG_KEYS, params)
        }
    rows = query(con, CATALOG_FACTS, params)
    return {
        int(oid): dict(
            key=list(key) if key else None,
            from_pk=bool(primary) if key else False,
            relkind=relkind,
            replident=replident,
            relfilenode=int(filenode) if filenode is not None else None,
            pk_oid=int(pk) if pk is not None else None,
            leaves=leaves,
            columns=list(columns or ()),
            missing=sorted(missing or ()),
        )
        for oid, key, primary, relkind, replident, filenode, pk, leaves, columns, missing in rows
    }


def check_publication_options(con, version, publication):
    """Reject publication features newer than PostgreSQL 13 that change what pgoutput
    sends. Backfill and the TOAST check read only ordinary columns of listed tables."""
    if (
        version >= 150000
        and query(
            con,
            "SELECT count(*) FROM pg_publication_namespace s JOIN pg_publication p "
            "ON p.oid=s.pnpubid WHERE p.pubname=%s",
            [publication],
        )[0][0]
    ):
        # FOR TABLES IN SCHEMA would publish tables created later.
        raise PreflightError("publication must explicitly list tables, not schemas")
    if version >= 180000:
        (generated,) = query(
            con, "SELECT pubgencols FROM pg_publication WHERE pubname=%s", [publication]
        )[0]
        if generated != "n":
            raise PreflightError("publication must use publish_generated_columns=none")


def resolve(con, schema, table):
    """(oid, relkind, replica identity, SELECT granted) of schema.table, or None."""
    rows = query(
        con,
        "SELECT c.oid,c.relkind,c.relreplident,has_table_privilege(c.oid,'SELECT') "
        "FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid "
        "WHERE n.nspname=%s AND c.relname=%s",
        [schema, table],
    )
    return rows[0] if rows else None


def relation_name(con, oid):
    """(schema, name) of a relation by OID, or None if it no longer exists."""
    rows = query(
        con,
        "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n "
        "ON c.relnamespace=n.oid WHERE c.oid=%s",
        [oid],
    )
    return tuple(rows[0]) if rows else None


class TableInvalid(PreflightError):
    """A per-table check failed. `reason` is the held reason (see classify)."""

    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason


def validate_table(con, name, found, *, managed=False):
    """Preflight's per-table checks (also add-table's). `found` is resolve()'s row.
    `managed`: a registered, captured table (classify), whose no_key advice differs."""
    schema, table = table_name(name)
    if not found:
        raise PreflightError(
            f"{name}: table does not exist or is not an ordinary/partitioned table"
        )
    if found[1] not in ("r", "p"):
        raise TableInvalid(
            "invalid_kind", f"{name}: not an ordinary or partitioned table (relkind {found[1]})"
        )
    oid, kind, identity, can_select = found
    if not can_select:
        raise no_select(con, name)
    key = catalog_key(con, oid)
    if not key or identity == "n":
        advice = keyless_advice(con, name, found, managed=managed) if not key else ""
        raise TableInvalid(
            "no_key", f"{name}: a primary key or replica identity index is required{advice}"
        )
    toast = query(
        con,
        "SELECT count(*) FROM pg_attribute WHERE attrelid=%s AND attnum>0 "
        "AND NOT attisdropped AND attgenerated='' AND attstorage!='p'",
        [oid],
    )[0][0]
    if toast and identity != "f":
        raise TableInvalid(
            "identity_not_full",
            f"{name}: set REPLICA IDENTITY FULL for TOAST-capable columns; "
            f"{OWNER}{full_statement(name)}",
        )
    if kind == "p":
        leaves = query(
            con,
            "SELECT c.oid,c.oid::regclass::text,c.relreplident,"
            "EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 "
            "AND NOT a.attisdropped AND a.attgenerated='' AND a.attstorage!='p'),"
            "EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND (i.indisprimary OR i.indisreplident)) "
            "FROM pg_partition_tree(%s) t JOIN pg_class c ON c.oid=t.relid WHERE t.isleaf",
            [oid],
        )
        for _, leaf_name, leaf_identity, leaf_toast, leaf_key in leaves:
            if not leaf_key or leaf_identity == "n":
                raise TableInvalid(
                    "leaf_invalid",
                    f"{leaf_name}: a primary key or replica identity index is required",
                )
            if leaf_toast and leaf_identity != "f":
                raise TableInvalid(
                    "leaf_invalid",
                    f"{leaf_name}: set REPLICA IDENTITY FULL on every TOAST-capable leaf "
                    f"partition; {OWNER}ALTER TABLE {leaf_name} REPLICA IDENTITY FULL;",
                )
    return dict(oid=oid, schema=schema, name=table, keys=key)


def qualifying_unique_indexes(con, oid):
    """Unique indexes PostgreSQL would accept for REPLICA IDENTITY USING INDEX: unique,
    valid, not deferrable, not partial, no expressions, every key column NOT NULL
    (INCLUDE columns aren't checked, as PostgreSQL doesn't). Smallest first (key column
    count, then size). Index names: an index lives in its table's schema,
    and REPLICA IDENTITY USING INDEX takes the bare name."""
    rows = query(
        con,
        "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
        "WHERE i.indrelid=%s "
        "AND i.indisunique AND i.indisvalid AND i.indimmediate AND i.indpred IS NULL "
        "AND i.indexprs IS NULL AND 0 <> ALL (i.indkey) AND NOT EXISTS ("
        "SELECT 1 FROM unnest(i.indkey) WITH ORDINALITY k(attnum,ord) JOIN pg_attribute a "
        "ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
        "WHERE k.ord<=i.indnkeyatts AND NOT a.attnotnull) "
        "ORDER BY i.indnkeyatts,pg_relation_size(i.indexrelid),c.relname",
        [oid],
    )
    return [r[0] for r in rows]


def sql_name(identifier):
    """A PostgreSQL identifier as the owner would type it: bare when it is plainly a
    lowercase identifier, else quoted. Approximate: a reserved word stays bare (which
    PostgreSQL may reject), and a name it quotes might not have needed quoting."""
    plain = identifier.isascii() and identifier.isidentifier() and identifier == identifier.lower()
    return identifier if plain else '"' + identifier.replace('"', '""') + '"'


def sql_table(name):
    schema, table = table_name(name)
    return f"{sql_name(schema)}.{sql_name(table)}"


def identity_statements(name, indexes):
    """The owner's statements that give a keyless table a key from a qualifying index."""
    return [
        f"ALTER TABLE {sql_table(name)} REPLICA IDENTITY USING INDEX {sql_name(i)};"
        for i in indexes
    ]


def full_statement(name):
    return f"ALTER TABLE {sql_table(name)} REPLICA IDENTITY FULL;"


# Refusals name the exact SQL a human runs with their own client: the agent and the
# Flights never need admin credentials (docs/setup.md, the DBA script).
OWNER = "ask the table's owner (the DBA) to run: "
OWNERS = "ask the tables' owner (the DBA) to run:\n"


def current_role(con):
    return sql_name(query(con, "SELECT current_user")[0][0])


def grant_select(con, name):
    return f"GRANT SELECT ON {sql_table(name)} TO {current_role(con)};"


def no_select(con, name):
    return TableInvalid(
        "no_select",
        f"{name}: the role this Flight connects as can't read it; {OWNER}{grant_select(con, name)}",
    )


def qualified_names(pairs):
    """(schema, name) pairs as the owner would type them, for a statement."""
    return ", ".join(f"{sql_name(schema)}.{sql_name(name)}" for schema, name in pairs)


def add_tables(publication, pairs):
    return f"ALTER PUBLICATION {sql_name(publication)} ADD TABLE {qualified_names(pairs)};"


def provisioning_plan(con, config, ddl, missing):
    """Who creates the missing objects, and the exact SQL. On RDS only the tables' owner
    can create a publication for them, so the owner creates the heartbeat (granting this
    role what ingest needs on it) and the publication; preflight with
    CDC_CREATE_OBJECTS=1 then creates only the slot, which needs replication rights."""
    owner = []
    if "heartbeat table" in missing:
        owner += [
            ddl[0].as_string(con) + ";",
            f"GRANT SELECT, INSERT, UPDATE ON {sql_table(config.heartbeat)} "
            f"TO {current_role(con)};",
        ]
    if "publication" in missing:
        owner.append(ddl[1].as_string(con) + ";")
    text = OWNERS + "\n".join(owner) if owner else ""
    if "replication slot" in missing:
        slot = (
            "the replication slot: run preflight with CDC_CREATE_OBJECTS=1, which creates "
            "only what is missing (this role can create the slot)"
        )
        text = f"{text}\nthen {slot}" if text else slot
    return text


def membership_plan(con, config, members, wanted, ddl):
    """The first registration's publication must list exactly the configured tables and
    the heartbeat: the statements that make it so, or the alternatives. A missing
    heartbeat (no OID yet) is created by the owner, then added."""

    def pairs(oids):
        return sorted(filter(None, (relation_name(con, oid) for oid in oids)))

    lines = []
    if None in wanted:
        lines += [
            ddl[0].as_string(con) + ";",
            f"GRANT SELECT, INSERT, UPDATE ON {sql_table(config.heartbeat)} "
            f"TO {current_role(con)};",
            add_tables(config.publication, [table_name(config.heartbeat)]),
        ]
        wanted = wanted - {None}
    if wanted - members:
        lines.append(add_tables(config.publication, pairs(wanted - members)))
    if members - wanted:
        extra = qualified_names(pairs(members - wanted))
        lines += [
            f"-- only if no other subscriber or source uses {sql_name(config.publication)}:",
            f"ALTER PUBLICATION {sql_name(config.publication)} DROP TABLE {extra};",
        ]
    return (
        "publication membership differs from the configured tables and heartbeat; "
        "use a publication of its own (CDC_PUBLICATION), or list every published table in "
        "CDC_TABLES, or " + OWNERS + "\n".join(lines)
    )


def can_provision(con, config, missing, owned_oids):
    """Whether this role can create the missing heartbeat and publication itself:
    CREATE on the heartbeat's schema, CREATE on the database, and ownership of every
    table the publication lists (PostgreSQL's rule; on RDS the replication role
    usually owns none)."""
    if (
        "heartbeat table" in missing
        and not query(
            con,
            "SELECT coalesce((SELECT has_schema_privilege(oid,'CREATE') FROM pg_namespace "
            "WHERE nspname=%s),false)",
            [table_name(config.heartbeat)[0]],
        )[0][0]
    ):
        return False
    if "publication" in missing:
        if not query(con, "SELECT has_database_privilege(current_database(),'CREATE')")[0][0]:
            return False
        if (
            owned_oids
            and not query(
                con,
                "SELECT coalesce(bool_and(pg_has_role(current_user,c.relowner,'USAGE')),true) "
                "FROM pg_class c WHERE c.oid=ANY(%s::oid[])",
                [list(owned_oids)],
            )[0][0]
        ):
            return False
    return True


def keyless_advice(con, name, found, *, managed=False):
    """What a keyless table's owner can do, appended to its `no_key` message: a
    qualifying unique index first (capture it keyed), else insert-only capture. For a
    table already captured keyed (`managed`, which is published), restoring its key is the
    fix; append capture means removing it first. One index query per such table."""
    oid, kind, identity, _ = found
    indexes = qualifying_unique_indexes(con, oid)
    if indexes:
        return (
            "; it has a unique index that can be its key, so capture it keyed: run "
            + " or ".join(identity_statements(name, indexes))
        )
    if managed:
        if kind == "p":
            return "; restore its key"  # a keyless partitioned table can't be appended
        return "; restore its key, or remove it and add it in append mode"
    if kind == "p":
        return "; a keyless partitioned table can't be captured: add a key"
    if identity == "f":
        return "; or capture it insert-only: add it in append mode"
    return (
        f"; or capture it insert-only: run {full_statement(name)} before the table joins "
        "the publication, then add it in append mode"
    )


def validate_append(con, name, found):
    """add-table's checks for append (insert-only) capture of a keyless table: ordinary
    (a keyless partitioned table is refused: FULL on the root reaches no leaf), readable,
    and REPLICA IDENTITY FULL (published without it, its UPDATE and DELETE fail on the
    source). `found` is resolve()'s row. A key that appears after add doesn't matter."""
    if not found:
        raise PreflightError(f"{name}: table does not exist or is not an ordinary table")
    oid, kind, identity, can_select = found
    if kind == "p":
        raise TableInvalid(
            "append_partitioned", f"{name}: a keyless partitioned table can't be captured"
        )
    if kind != "r":
        raise TableInvalid("invalid_kind", f"{name}: not an ordinary table (relkind {kind})")
    if not can_select:
        raise no_select(con, name)
    if identity != "f":
        raise TableInvalid(
            "append_identity",
            f"{name}: append mode needs REPLICA IDENTITY FULL; {OWNER}{full_statement(name)}",
        )
    schema, table = table_name(name)
    return dict(oid=oid, schema=schema, name=table, keys=(), append=True)


def classify(con, config, registry):
    """Configured tables against the registry, by OID (the registry is the truth).

    Each registered table not removed or unmanaged:
    - found under its registered name with its OID: present, validated as usual;
    - its OID gone: held `dropped`, or `replaced` if another table now has the name;
    - its OID under another name: held `renamed`. It still exists, so its key is
      still tracked by OID (a key change during the hold must not be missed).
    CDC_TABLES, if set, is a declaration: a removed or former name is stale (ignored
    and reported); an unknown name is refused (run add-table); an active table it
    omits is captured and reported. Unset, there's nothing to check.
    """
    heartbeat = config.heartbeat
    tables, held, present, names = [], {}, [], {}
    for row in registry:
        name = f"{row['schema_name']}.{row['table_name']}"
        if row["held_reason"] in ("removed", "unmanaged") or name == heartbeat:
            continue
        oid = row["relation_id"]
        names[oid] = (row["schema_name"], row["table_name"])
        found = resolve(con, row["schema_name"], row["table_name"])
        append = row.get("capture_mode") == "append"
        if found and found[0] == oid and append:
            # Keyless append capture: its own checks, and never key-tracked (a key
            # that appears later doesn't change the mode).
            try:
                tables.append(validate_append(con, name, found))
                present.append(oid)
            except TableInvalid as exc:
                held[oid] = (exc.reason, str(exc))
            continue
        if found and found[0] == oid:
            try:
                tables.append(validate_table(con, name, found, managed=True))
                present.append(oid)
            except TableInvalid as exc:
                # One table failing a check holds alone; the source keeps running.
                held[oid] = (exc.reason, str(exc))
                keys = catalog_key(con, oid)
                if keys:  # still tracked by OID while it has a key
                    tables.append(
                        dict(
                            oid=oid,
                            schema=row["schema_name"],
                            name=row["table_name"],
                            keys=keys,
                            held=True,
                        )
                    )
            continue
        current = relation_name(con, oid)
        if current is None:
            held[oid] = (
                ("replaced", f"{name} is now a different table")
                if found
                else ("dropped", f"{name} was dropped")
            )
        else:
            held[oid] = ("renamed", f"renamed to {current[0]}.{current[1]}")
            tables.append(
                dict(
                    oid=oid,
                    schema=current[0],
                    name=current[1],
                    keys=() if append else catalog_key(con, oid),
                    held=True,
                    append=append,
                )
            )
    # Later rows win: an active row over a removed one of the same name.
    order = sorted(registry, key=lambda r: r["held_reason"] != "removed")
    known = {
        f"{r['schema_name']}.{r['table_name']}": r for r in order if r["held_reason"] != "unmanaged"
    }
    formers = {r["former_name"] for r in registry if r["former_name"]}
    stale = []
    for name in config.tables:
        row = known.get(name)
        if row is None and name not in formers:
            raise PreflightError(f"{name} is not registered for this source; run add-table")
        if row is None or row["held_reason"] == "removed":
            stale.append(name)
    undeclared = sorted(
        name
        for name, row in known.items()
        if config.tables and row["held_reason"] is None and name not in (heartbeat, *config.tables)
    )
    return tables, held, present, stale, undeclared, names


def check(config, *, create_objects=False, slot=None, fault=None, registry=None, rehearse=False):
    """Validate the source. `slot` is the registered epoch's slot when it differs
    from the configured base name (after a slot-loss epoch transition). `fault` is
    called with a stage name after each group of checks (progress lines).

    `registry` (the ingest log's tables rows) makes the registry the truth for which
    tables are captured: a registered table that disappeared holds instead of
    refusing the source (see classify). Without it (the first registration) every
    configured table must be valid.

    `rehearse` (with create_objects): every check, including whether this role can create
    the missing objects, then return None before creating anything. A fresh store runs it
    before writing its bootstrap marker, so a refusal leaves no incomplete bootstrap."""
    if rehearse and not create_objects:
        raise ValueError("rehearse is only for create_objects")
    fault = fault or (lambda stage: None)
    if slot is not None and slot != config.slot:
        config = replace(config, slot=slot)
    options = parse_dsn(config.dsn)
    host = options.get("host", "")
    port = options.get("port", "5432")
    external = config.heartbeat_writer == "external"
    if external:
        from cdc_flight import standby  # external mode only (table-membership cuts it)
    timeline = reached = None  # external mode: from the replication session
    if external and standby.endpoint_refusal(host):
        raise PreflightError(standby.endpoint_refusal(host))
    if not config.allow_local:
        if not host or host.startswith("/") or host in ("localhost", "127.0.0.1", "::1"):
            raise PreflightError("use a direct IPv4 Postgres host reachable from the Flight region")
        if "pooler" in host.lower() or port in ("6432", "6543"):
            raise PreflightError(
                "connection poolers are unsupported; use the direct database endpoint"
            )
        # A baked Flight forces verify-full whatever the DSN says (connection_options).
        if not pg_ca.baked() and options.get("sslmode", "require") not in (
            "require",
            "verify-ca",
            "verify-full",
        ):
            raise PreflightError("TLS is required for remote Postgres connections")
        try:
            socket.getaddrinfo(host, int(port), socket.AF_INET, socket.SOCK_STREAM)
        except OSError as exc:
            raise PreflightError("direct host has no reachable IPv4 DNS address") from exc
    fault("after_endpoint")
    if pg_ca.baked():
        # Defense in depth: a baked Flight connects only with verify-full against its
        # own roots (connection_options forces it; this guards a future refactor).
        effective = connection_options(config)
        if (
            effective.get("sslmode") != "verify-full"
            or effective.get("sslrootcert") != pg_ca.ca_file()
        ):
            raise PreflightError("this Flight requires verify-full TLS with its baked CA")
    con = connect(config)
    try:
        fault("after_connect")
        version = int(query(con, "SHOW server_version_num")[0][0])
        if version < 130000:
            raise PreflightError("PostgreSQL 13 or newer is required")
        if query(con, "SHOW wal_level")[0][0] != "logical":
            raise PreflightError("set wal_level=logical and restart PostgreSQL")
        if query(con, "SHOW server_encoding")[0][0] != "UTF8":
            raise PreflightError("server_encoding must be UTF8")
        check_formats(lambda name: query(con, "SHOW " + name)[0][0])
        fault("after_server")
        # Capability testing also supports managed-provider replication roles whose
        # pg_roles.rolreplication bit is false. Fail before provisioning anything.
        try:
            repl = connect(config, replication=True)
            try:
                with repl.cursor() as cur:
                    cur.execute("IDENTIFY_SYSTEM")
                    identified = cur.fetchone()
                    system_id = identified[0]
                    if external:
                        # The timeline, and the address this session reached: the
                        # regular session must have reached the same server.
                        timeline = int(identified[1])
                        cur.execute("SELECT host(inet_server_addr()),inet_server_port()")
                        reached = cur.fetchone()

                    def show(name):
                        cur.execute("SHOW " + name)
                        return cur.fetchone()[0]

                    # pgoutput renders values in the walsender session, whose
                    # settings can differ from a regular session's (DSN options).
                    check_formats(show)
            finally:
                repl.close()
        except psycopg2.Error as exc:
            if external:
                raise PreflightError(
                    "grant replication capability and allow direct replication connections "
                    f"to the standby (on RDS, on the primary: GRANT rds_replication TO "
                    f"{current_role(con)};)"
                ) from exc
            raise PreflightError(
                "grant replication capability and allow direct replication connections"
            ) from exc
        in_recovery = query(con, "SELECT pg_is_in_recovery()")[0][0]
        if in_recovery and not external:
            raise PreflightError(
                "this server is a standby: capturing from a standby needs "
                "CDC_HEARTBEAT_WRITER=external and the heartbeat job on the primary"
            )
        if external and not in_recovery:
            raise PreflightError(
                "CDC_HEARTBEAT_WRITER=external is for a standby in this release, and this "
                "server is a primary (promotion and failover aren't supported)"
            )
        standby_warnings = standby.standby_checks(con, version, reached) if external else []
        fault("after_replication")
        tables, held, present, stale, undeclared, held_names = [], {}, [], [], [], {}
        if not registry:
            # The first registration: every configured table must be valid now, and
            # CDC_TABLES (optional afterwards; the registry is the table set) names them.
            if not config.tables:
                raise PreflightError("CDC_TABLES is required to register")
            for name in config.tables:
                tables.append(validate_table(con, name, resolve(con, *table_name(name))))
        else:
            tables, held, present, stale, undeclared, held_names = classify(con, config, registry)
        fault("after_tables")
        # The publication's tables, for the DDL plan: CDC_TABLES at the first
        # registration, else every registered table that still exists (CDC_TABLES may
        # be unset). Held ones too: a renamed table, under its current name, resumes
        # from its checkpoint when relabelled, so leaving it out would lose its changes.
        ddl = ddl_plan(config, registry and [f"{t['schema']}.{t['name']}" for t in tables])
        missing = []
        heartbeat = query(con, "SELECT to_regclass(%s)::oid", [config.heartbeat])[0][0]
        pub = query(
            con,
            "SELECT puballtables,pubinsert,pubupdate,pubdelete,pubtruncate,pubviaroot "
            "FROM pg_publication WHERE pubname=%s",
            [config.publication],
        )
        slot = query(
            con,
            "SELECT plugin,database,active,confirmed_flush_lsn,wal_status "
            "FROM pg_replication_slots WHERE slot_name=%s",
            [config.slot],
        )
        if not heartbeat:
            missing.append("heartbeat table")
        if not pub:
            missing.append("publication")
        if not slot:
            missing.append("replication slot")
        if external and (not heartbeat or not pub):
            # The DBA makes these on the primary; a standby can't create anything.
            fault("objects_missing")
            names = (registry and [f"{t['schema']}.{t['name']}" for t in tables]) or config.tables
            raise PreflightError(
                "missing "
                + ", ".join(m for m in missing if m != "replication slot")
                + "; "
                + standby.primary_setup_sql(config, names)
            )
        if missing and not create_objects:
            fault("objects_missing")
            raise PreflightError(
                "missing "
                + ", ".join(missing)
                + "; "
                + provisioning_plan(con, config, ddl, missing)
            )
        # Validate existing objects before making any new ones.
        if pub and pub[0][0]:
            raise PreflightError(
                "publication must explicitly list tables (it is FOR ALL TABLES); use a "
                "publication of its own (CDC_PUBLICATION), or, only if no other subscriber or "
                f"source uses {sql_name(config.publication)}, "
                + OWNERS
                + f"DROP PUBLICATION {sql_name(config.publication)};\n"
                + ddl[1].as_string(con)
                + ";"
            )
        if pub and not all(pub[0][1:]):
            raise PreflightError(
                "publication must publish insert/update/delete/truncate via roots; use a "
                "publication of its own (CDC_PUBLICATION), or, only if no other subscriber or "
                f"source uses {sql_name(config.publication)}, "
                + OWNER
                + f"ALTER PUBLICATION {sql_name(config.publication)} SET (publish = "
                "'insert, update, delete, truncate', publish_via_partition_root = true);"
            )
        if pub:
            check_publication_options(con, version, config.publication)
        if slot and (
            slot[0][0] != "pgoutput" or slot[0][1] != query(con, "SELECT current_database()")[0][0]
        ):
            raise PreflightError("slot must use pgoutput and belong to this database")
        if slot and slot[0][4] == "lost":
            raise PreflightError("replication slot is lost; explicit source resync is required")
        if slot and external:
            standby.standby_slot_check(con, version, config.slot)
        if not slot:
            available = query(
                con,
                "SELECT current_setting('max_replication_slots')::int-count(*) FROM pg_replication_slots",
            )[0][0]
            if available <= 0:
                raise PreflightError("increase max_replication_slots before provisioning")
        # Check every existing object before provisioning missing ones.
        if heartbeat:
            columns = query(
                con,
                "SELECT attname,atttypid,attnotnull FROM pg_attribute WHERE attrelid=%s "
                "AND attnum>0 AND NOT attisdropped ORDER BY attnum",
                [heartbeat],
            )
            primary = query(
                con,
                "SELECT array_agg(a.attname ORDER BY k.ord) "
                "FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(num,ord) "
                "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num "
                "WHERE i.indrelid=%s AND i.indisprimary GROUP BY i.indexrelid",
                [heartbeat],
            )
            if columns != [("id", 25, True), ("marker", 25, True)] or primary != [(["id"],)]:
                raise PreflightError("heartbeat requires id text PRIMARY KEY, marker text NOT NULL")
            # External mode only reads it: the primary's job writes it.
            lacking = [
                privilege
                for privilege in (("SELECT",) if external else ("SELECT", "INSERT", "UPDATE"))
                if not query(con, "SELECT has_table_privilege(%s,%s)", [heartbeat, privilege])[0][0]
            ]
            if lacking:
                raise PreflightError(
                    f"the role this Flight connects as needs {', '.join(lacking)} on the "
                    f"heartbeat table; {OWNER}GRANT {', '.join(lacking)} ON "
                    f"{sql_table(config.heartbeat)} TO {current_role(con)};"
                )
        unmanaged = []
        if pub:
            # Membership by OID: names change, OIDs don't.
            members = {
                oid
                for (oid,) in query(
                    con,
                    "SELECT r.prrelid FROM pg_publication_rel r JOIN pg_publication p "
                    "ON p.oid=r.prpubid WHERE p.pubname=%s",
                    [config.publication],
                )
            }
            active = {t["oid"] for t in tables if not t.get("held")}
            if not registry:
                if members != active | {heartbeat}:
                    raise PreflightError(
                        membership_plan(con, config, members, active | {heartbeat}, ddl)
                    )
            else:
                if heartbeat not in members:
                    raise PreflightError(
                        "the heartbeat table must stay in the publication; "
                        + OWNER
                        + add_tables(config.publication, [table_name(config.heartbeat)])
                    )
                # A table taken out of the publication holds; it doesn't stop the source.
                for t in tables:
                    if not t.get("held") and t["oid"] not in members:
                        held[t["oid"]] = (
                            "unpublished",
                            f"{t['schema']}.{t['name']} is not in the publication: "
                            + OWNER
                            + add_tables(config.publication, [(t["schema"], t["name"])])
                            + " Or remove-table.",
                        )
                tables = [t for t in tables if t["oid"] not in held or t.get("held")]
                present = [oid for oid in present if oid not in held]
                registered = {r["relation_id"] for r in registry}
                # Members nobody registered (a recreated table, a DBA's addition) are
                # registered `unmanaged` by ingest: logged, never applied, reported.
                for oid in sorted(members - registered - {heartbeat}):
                    where = relation_name(con, oid)
                    if where:
                        unmanaged.append(
                            dict(
                                oid=oid,
                                schema=where[0],
                                name=where[1],
                                keys=catalog_key(con, oid) or [],
                            )
                        )
            if version >= 150000:
                filtered = query(
                    con,
                    "SELECT r.prrelid,r.prqual IS NOT NULL,r.prattrs IS NOT NULL "
                    "FROM pg_publication_rel r JOIN pg_publication p ON p.oid=r.prpubid "
                    "WHERE p.pubname=%s AND (r.prqual IS NOT NULL OR r.prattrs IS NOT NULL)",
                    [config.publication],
                )
                if filtered and (not registry or any(o == heartbeat for o, _, _ in filtered)):
                    raise PreflightError("publication row filters and column lists are unsupported")
                for oid, row_filter, columns in filtered:
                    # Per table: that table holds. The detail names the kind, never its SQL.
                    kinds = " and ".join(
                        k for k, on in (("row filter", row_filter), ("column list", columns)) if on
                    )
                    held.setdefault(
                        oid, ("publication_filter", f"the publication has a {kinds} on this table")
                    )
                    present = [o for o in present if o != oid]
                    for t in tables:
                        if t["oid"] == oid:
                            t["held"] = True
        available_senders = query(
            con, "SELECT current_setting('max_wal_senders')::int-count(*) FROM pg_stat_replication"
        )[0][0]
        if available_senders <= 0:
            raise PreflightError("increase max_wal_senders or free a replication connection")
        owner_objects = [m for m in missing if m != "replication slot"]
        if owner_objects:
            listed = [t["oid"] for t in tables] + ([heartbeat] if heartbeat else [])
            if not can_provision(con, config, owner_objects, listed):
                fault("objects_missing")
                raise PreflightError(
                    f"this role can't create the {' and '.join(owner_objects)}; "
                    + provisioning_plan(con, config, ddl, missing)
                )
        if rehearse:
            if external and not slot:
                # Before the store's bootstrap marker: a timed-out wait
                # leaves nothing behind, so waiting_for_primary is retryable. Only with
                # create_objects (rehearse requires it).
                standby.create_standby_slot(con, config)
                return {"slot": True}
            return None
        created = {}
        if not heartbeat:
            query(con, ddl[0])
            created["heartbeat"] = True
            heartbeat = query(con, "SELECT to_regclass(%s)::oid", [config.heartbeat])[0][0]
        if not pub:
            query(con, ddl[1])
            created["publication"] = True
        if not slot and external:
            standby.create_standby_slot(con, config)
            created["slot"] = True
        elif not slot:
            query(
                con,
                "SELECT * FROM pg_create_logical_replication_slot(%s,'pgoutput')",
                [config.slot],
            )
            created["slot"] = True
        if missing:
            fault("after_objects_created")
        else:
            fault("after_objects_present")
        initial = query(
            con,
            "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
            [config.slot],
        )[0][0]
        # The slot exists now. A table empty at this moment has every earlier row's
        # delete in the log from the registered start, so a log-only generation is
        # exact for it. Registration keeps only the first observation.
        for table in tables:
            table["empty"] = query(
                con,
                sql.SQL("SELECT NOT EXISTS (SELECT 1 FROM {})").format(
                    sql.Identifier(table["schema"], table["name"])
                ),
            )[0][0]
        database_oid = query(con, "SELECT oid FROM pg_database WHERE datname=current_database()")[
            0
        ][0]
        tables.append(
            dict(
                oid=heartbeat,
                schema=table_name(config.heartbeat)[0],
                name=table_name(config.heartbeat)[1],
                keys=["id"],
                empty=None,
            )
        )
        warnings = []
        if query(con, "SHOW max_slot_wal_keep_size")[0][0] == "-1":
            warnings.append(
                "max_slot_wal_keep_size is unlimited; size source WAL retention explicitly"
            )
        warnings.append(
            "verify failover slot synchronization separately before production failover"
        )
        warnings += standby_warnings
        return SourceInfo(
            (system_id, database_oid),
            int(initial),
            tables,
            heartbeat,
            warnings,
            held=held,
            held_names={oid: held_names[oid] for oid in held if oid in held_names},
            present=present,
            unmanaged=unmanaged,
            stale=stale,
            undeclared=undeclared,
            created=created,
            timeline=timeline if external else None,
        )
    finally:
        con.close()
