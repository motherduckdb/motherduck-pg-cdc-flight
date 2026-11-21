"""Replica capture: preflight's checks and provisioning for a source
that is a physical standby (CDC_HEARTBEAT_WRITER=external).

Imported by preflight.check only in external mode (and by bootstrap and the CLI), so
commands that never run a preflight (table-membership) don't load it.
"""

import psycopg2
import psycopg2.errors

from cdc_flight.pg_connect import STATEMENT_TIMEOUT, PreflightError, query
from cdc_flight.preflight import sql_name, sql_table

# Hostname parts of RDS/Aurora endpoints that aren't one instance's (reader, cluster,
# custom cluster, RDS Proxy): only these are refused.
RDS_NOT_AN_INSTANCE = (".cluster-ro-", ".cluster-custom-", ".cluster-", ".proxy-")


def endpoint_refusal(host):
    """Why an external-mode host isn't a single standby's own endpoint, or None."""
    lowered = host.lower()
    if lowered.endswith(".rds.amazonaws.com") and any(
        part in lowered for part in RDS_NOT_AN_INSTANCE
    ):
        return (
            "a standby source needs the replica instance's own endpoint, not a cluster, "
            "reader or proxy endpoint: it may move to another instance"
        )
    return None


class WaitingForPrimary(PreflightError):
    """Creating a slot on a standby waits for the primary to log a standby snapshot; the
    bounded wait ran out. Retryable: nothing is registered (status waiting_for_primary)."""


def waiting_message(config):
    return (
        f"the replication slot {config.slot} was not created within "
        f"{config.slot_wait_seconds:g} s: a slot on a standby waits for the primary to log a "
        "standby snapshot. Check the heartbeat job is running on the primary, or ask the "
        "DBA to run SELECT pg_log_standby_snapshot(); on the primary, which helps only once "
        "no transaction is open there. Then retry."
    )


def primary_setup_sql(config, tables):
    """The SQL "to run on the primary" (external mode): the heartbeat table, the
    publication and the heartbeat job. The job's row id is `external:<source>`."""
    heartbeat = sql_table(config.heartbeat)
    listed = ", ".join([*(sql_table(t) for t in tables), heartbeat])
    source = config.source_id.replace("'", "''")
    return (
        "on the primary (the standby replicates it), ask the tables' owner (the DBA) to run:\n"
        f"CREATE TABLE {heartbeat} (id text PRIMARY KEY, marker text NOT NULL);\n"
        f"CREATE PUBLICATION {sql_name(config.publication)} FOR TABLE {listed} "
        "WITH (publish_via_partition_root=true);\n"
        "-- any scheduler; pg_cron shown. One minute is the portable default.\n"
        f"SELECT cron.schedule('md_cdc_heartbeat_{source}', '* * * * *', $$ "
        f"INSERT INTO {heartbeat} (id, marker) VALUES ('external:{source}', "
        "clock_timestamp()::text) ON CONFLICT (id) DO UPDATE SET marker = excluded.marker $$);\n"
        "Grant this role SELECT on the captured tables and the heartbeat (no INSERT or UPDATE)."
    )


def standby_checks(con, version, reached):
    """A standby's prerequisites, read on the standby only; returns warnings."""
    if version < 160000:
        raise PreflightError("capturing from a standby needs PostgreSQL 16 or newer")
    if query(con, "SHOW hot_standby_feedback")[0][0] != "on":
        raise PreflightError(
            "set hot_standby_feedback=on on the standby: without it the primary can remove "
            "catalog rows the slot needs and the slot is invalidated"
        )
    warnings = []
    here = tuple(query(con, "SELECT host(inet_server_addr()),inet_server_port()")[0])
    if here[0] is None or reached[0] is None:
        warnings.append("the replication and SQL sessions' server address is unverified")
    elif here != tuple(reached):
        raise PreflightError(
            "the replication and SQL sessions reached different servers; use the standby "
            "instance's own endpoint"
        )
    if not query(con, "SHOW primary_slot_name")[0][0]:
        warnings.append(
            "primary_slot_name is empty on the standby: without a physical slot upstream the "
            "primary may remove WAL the standby still needs"
        )
    return warnings


def standby_slot_check(con, version, slot):
    """A conflict-invalidated or synced (PG17+) slot on a standby is refused, by
    its columns, never by error text."""
    columns = "conflicting," + ("invalidation_reason,synced" if version >= 170000 else "NULL,NULL")
    conflicting, reason, synced = query(
        con, f"SELECT {columns} FROM pg_replication_slots WHERE slot_name=%s", [slot]
    )[0]
    if synced:
        raise PreflightError(
            f"slot {slot} is a synced failover slot; use a slot name of this source's own"
        )
    if conflicting:
        raise PreflightError(
            f"replication slot {slot} was invalidated by a recovery conflict"
            + (f" ({reason})" if reason else "")
            + "; explicit source resync is required"
        )


def create_standby_slot(con, config):
    """The slot on a standby, under the bounded wait (CDC_SLOT_WAIT_SECONDS): a timeout
    raises WaitingForPrimary. A cancelled creation leaves no slot (it is ephemeral until
    it completes)."""
    # On an autocommit connection (pg_connect.connect) a cancelled statement
    # leaves no aborted transaction, so the SET in `finally` can run.
    if not con.autocommit:
        raise RuntimeError("create_standby_slot needs an autocommit connection")
    query(con, f"SET statement_timeout='{int(config.slot_wait_seconds * 1000)}ms'")
    try:
        query(
            con,
            "SELECT * FROM pg_create_logical_replication_slot(%s,'pgoutput')",
            [config.slot],
        )
    except psycopg2.errors.QueryCanceled as exc:
        raise WaitingForPrimary(waiting_message(config)) from exc
    except psycopg2.errors.DuplicateObject as exc:
        raise PreflightError(
            f"slot {config.slot} was created concurrently by another run; retry"
        ) from exc
    finally:
        query(con, f"SET statement_timeout='{STATEMENT_TIMEOUT}'")


def adopted_slot_warning(config):
    """A slot found at a first registration in external mode was left by an earlier,
    abandoned attempt (or made by someone else): reused, never owned."""
    return (
        f"reusing the existing slot {config.slot} on the standby, not created by this "
        "registration (an earlier attempt that stopped before registering, or someone "
        "else's): delete-source won't drop it. Through hot_standby_feedback it holds back "
        "catalog_xmin on the primary while it exists. If it isn't wanted, drop it on the "
        f"standby: SELECT pg_drop_replication_slot('{config.slot}');"
    )
