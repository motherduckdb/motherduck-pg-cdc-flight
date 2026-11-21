"""Frozen format-1 catalog, used only to prove refusal after runtime removal.

Copied from the pre-activation writer; no writer/reader or protocol factory is
provided. Production commands must never import this test reference.
"""

from cdc_flight.config import quoted

STATEMENTS = (
    "CREATE TABLE IF NOT EXISTS {s}source (id INTEGER PRIMARY KEY, epoch INTEGER, binding VARCHAR, system_id VARCHAR, database_oid UBIGINT, confirmed_lsn UBIGINT, token BIGINT, holder VARCHAR, expires_at TIMESTAMPTZ, guard VARCHAR, open_commit_lsn UBIGINT, last_capped_commit UBIGINT, last_capped_count UBIGINT, state VARCHAR DEFAULT 'ready')",
    "CREATE TABLE IF NOT EXISTS {s}batches (id VARCHAR PRIMARY KEY, epoch INTEGER, commit_lsn UBIGINT, first_ordinal UINTEGER, n UINTEGER, digest VARCHAR, chain_digest VARCHAR, partition_day VARCHAR)",
    "CREATE TABLE IF NOT EXISTS {s}txn_manifests (epoch INTEGER, commit_lsn UBIGINT, end_lsn UBIGINT, n UBIGINT, digest VARCHAR, complete BOOLEAN, PRIMARY KEY(epoch,commit_lsn))",
    "CREATE TABLE IF NOT EXISTS {s}schema_versions (epoch INTEGER, relation_id UINTEGER, version VARCHAR, descriptor VARCHAR, PRIMARY KEY(epoch,relation_id,version))",
    "CREATE TABLE IF NOT EXISTS {s}log_partitions (partition_day VARCHAR, epoch INTEGER, min_commit_lsn UBIGINT, max_commit_lsn UBIGINT, n UBIGINT, status VARCHAR, PRIMARY KEY(partition_day,epoch))",
    "CREATE TABLE IF NOT EXISTS {s}tables (relation_id UINTEGER PRIMARY KEY, schema_name VARCHAR, table_name VARCHAR, key_columns VARCHAR, state VARCHAR)",
    "CREATE TABLE IF NOT EXISTS {s}runs (run_id VARCHAR PRIMARY KEY, status VARCHAR, boundary UBIGINT, detail VARCHAR, updated_at TIMESTAMPTZ)",
    "CREATE TABLE IF NOT EXISTS {s}key_changes (relation_id UINTEGER, version INTEGER, key_columns VARCHAR, cut_lsn UBIGINT, new_from_lsn UBIGINT, PRIMARY KEY(relation_id, version))",
    "CREATE TABLE IF NOT EXISTS {s}source_epochs (epoch INTEGER PRIMARY KEY, previous_epoch INTEGER, start_lsn UBIGINT, previous_confirmed_lsn UBIGINT, reason VARCHAR, slot_name VARCHAR, previous_slot_name VARCHAR, timeline_id BIGINT, abandoned_from UBIGINT, abandoned_to UBIGINT, created_at TIMESTAMPTZ)",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS empty_after_slot BOOLEAN",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS catalog_key VARCHAR",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS key_version INTEGER",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS key_checked_lsn UBIGINT",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS key_epoch INTEGER DEFAULT 1",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS held_reason VARCHAR",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS held_detail VARCHAR",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS held_since TIMESTAMPTZ",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS held_checked_at TIMESTAMPTZ",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS former_name VARCHAR",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS retired_by UINTEGER",
    "ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS pin_state VARCHAR",
    "ALTER TABLE {s}key_changes ADD COLUMN IF NOT EXISTS epoch INTEGER DEFAULT 1",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS slot_name VARCHAR",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS lifecycle VARCHAR",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS deleted_slot_name VARCHAR",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS deleted_run_id VARCHAR",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS deleted_started_by VARCHAR",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS created_slot BOOLEAN",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS created_publication BOOLEAN",
    "ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS created_heartbeat BOOLEAN",
    "ALTER TABLE {s}log_partitions ADD COLUMN IF NOT EXISTS bytes UBIGINT",
    "ALTER TABLE {s}source_epochs ADD COLUMN IF NOT EXISTS started_by VARCHAR",
)


def create(con, config):
    prefix = f"{quoted(config.database)}.{quoted(config.source_id)}."
    con.execute(f"CREATE SCHEMA {quoted(config.database)}.{quoted(config.source_id)}")
    for statement in STATEMENTS:
        con.execute(statement.replace("{s}", prefix))
