"""preflight's connection layer (re-exported), shipped alone by backfill."""

import psycopg2
from psycopg2.extensions import parse_dsn
from psycopg2.extras import LogicalReplicationConnection

from cdc_flight import pg_ca
from cdc_flight.text_settings import connection_settings

KEEPALIVE = "-c tcp_keepalives_idle=30 -c tcp_keepalives_interval=10 -c tcp_keepalives_count=3"


class PreflightError(RuntimeError):
    pass


def query(con, statement, params=None):
    with con.cursor() as cur:
        cur.execute(statement, params)
        return cur.fetchall() if cur.description else []


def connection_options(config):
    """Every client's connection parameters, ingest's stream included. A --pg-ca Flight
    forces verify-full against its baked roots (explicit keywords beat PGSSLMODE etc.)."""
    options = parse_dsn(config.dsn)
    options.setdefault("connect_timeout", "10")
    options["options"] = connection_settings(config, options.get("options", ""))
    if pg_ca.baked():
        options["sslmode"] = "verify-full"
        options["sslrootcert"] = pg_ca.ca_file()
        # libpq's default floor since PostgreSQL 13; forced so a DSN can't lower it.
        options["ssl_min_protocol_version"] = "TLSv1.2"
    else:
        options.setdefault("sslmode", "disable" if config.allow_local else "require")
    pg_ca.note_options(options)
    return options


# Every regular connection's statement bound; epoch restores it after a longer wait.
STATEMENT_TIMEOUT = "30s"


def connect(config, *, replication=False, keepalive=False):
    options = connection_options(config)
    if keepalive:
        options["options"] = f"{options['options']} {KEEPALIVE}"
        options.update(keepalives="1", keepalives_idle="30", keepalives_interval="10")
    if replication:
        con = psycopg2.connect(**options, connection_factory=LogicalReplicationConnection)
        pg_ca.note_in_use(con.info.ssl_in_use)
        return con
    con = psycopg2.connect(**options)
    pg_ca.note_in_use(con.info.ssl_in_use)
    con.autocommit = True
    query(con, f"SET statement_timeout='{STATEMENT_TIMEOUT}'")
    return con


def catalog_key(con, oid):
    """The replica identity index's columns, else the primary key's."""
    rows = query(
        con,
        "SELECT array_agg(a.attname ORDER BY k.ord) FROM pg_index i "
        "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord) "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
        "WHERE i.indrelid=%s AND (i.indisreplident OR i.indisprimary) "
        "AND k.ord<=i.indnkeyatts GROUP BY i.indexrelid,i.indisreplident "
        "ORDER BY i.indisreplident DESC LIMIT 1",
        [oid],
    )
    return list(rows[0][0]) if rows else None


def check_formats(show):
    """pgoutput text values use these session formats. Apply casts expect ISO dates,
    hex bytea and round-trip floats, and never guess another style."""
    if not show("DateStyle").upper().startswith("ISO"):
        raise PreflightError("DateStyle must use ISO output for the replication role")
    if show("bytea_output") != "hex":
        raise PreflightError("bytea_output must be hex for the replication role")
    if int(show("extra_float_digits")) < 1:
        raise PreflightError("extra_float_digits must be at least 1 for exact float output")
