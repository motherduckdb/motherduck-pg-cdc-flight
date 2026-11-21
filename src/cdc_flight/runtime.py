"""Public format-2 construction: explicit bootstrap, read-only existing-store open.

No caller chooses a format. Existing legacy/unknown stores refuse before any
source or PostgreSQL mutation. Control clients need neither a DSN nor a
replication connection to reconstruct the recorded binding and unit config.
"""

import json
from dataclasses import fields

from cdc_flight.config import IngestConfig, quoted
from cdc_flight.format2 import UnitConfig
from cdc_flight.ingest_common import bound
from cdc_flight.ingest_log import SourceMismatch
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.unit_store import stored_binding


def _catalog(con, config):
    return {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT table_name FROM duckdb_columns() "
            "WHERE database_name=? AND schema_name=?",
            [config.database, config.source_id],
        ).fetchall()
    }


def _configured(config, binding, unit_config, server_major):
    values = {field.name: getattr(config, field.name) for field in fields(IngestConfig)}
    return IngestConfig2(
        **values, text_binding=binding, unit_config=unit_config, server_major=server_major
    )


def open_log(con, config, *, create=False, binding=None, unit_config=None, server_major=None):
    """Open v2 using its persisted configuration, without PostgreSQL or writes.

    Empty-store creation is opt-in and requires an explicit binding, unit config
    and actual PostgreSQL major. An existing schema is never migrated or repaired.
    The returned log's config is authoritative for subsequent ingest/lifecycle
    calls; a caller's DSN, budgets and source identity remain their own inputs.
    """
    catalog = _catalog(con, config)
    if "protocol_v2" not in catalog:
        if catalog or not create:
            raise SourceMismatch("format 2 requires a fresh store; recreate and re-backfill")
        if binding is None or unit_config is None or server_major is None:
            raise ValueError("bootstrap requires binding, unit config and PostgreSQL major")
        log = IngestLog2(con, _configured(config, binding, unit_config, server_major))
        if not log.initialize():
            raise SourceMismatch("bootstrap already started; open the registered store")
        return log
    if "unit_configs_v2" not in catalog:
        raise SourceMismatch("missing immutable unit configuration")
    if create:
        raise SourceMismatch("bootstrap already started; open the registered store")
    prefix = f"{quoted(config.database)}.{quoted(config.source_id)}."
    rows = con.execute(
        "SELECT p.format_version,p.min_reader,p.binding_hash,p.active_revision,"
        "p.server_major,p.binding_json,c.binding_hash,c.config_json,"
        f"(SELECT binding FROM {prefix}source WHERE id=1) "
        f"FROM {prefix}protocol_v2 p LEFT JOIN {prefix}unit_configs_v2 c "
        "ON c.revision=p.active_revision WHERE p.id=1"
    ).fetchall()
    if len(rows) != 1:
        raise SourceMismatch("missing or ambiguous format-2 configuration")
    (
        version,
        minimum,
        digest,
        revision,
        major,
        payload,
        config_digest,
        config_json,
        source_identity,
    ) = rows[0]
    if version != 2 or minimum is None or minimum > 2:
        raise SourceMismatch("unsupported source protocol")
    if config_digest != digest or config_json is None:
        raise SourceMismatch("missing or inconsistent immutable unit configuration")
    recorded_binding = stored_binding(payload, digest, config.source_id)
    try:
        values = json.loads(config_json)
        if type(values) is not dict or set(values) != {f.name for f in fields(UnitConfig)}:
            raise ValueError("incomplete immutable unit configuration")
        recorded_config = UnitConfig(**values)
        if recorded_config.revision != revision:
            raise ValueError("unit configuration revision differs")
        configured = _configured(config, recorded_binding, recorded_config, major)
    except (ValueError, TypeError) as exc:
        raise SourceMismatch("unsupported or malformed immutable unit configuration") from exc
    if source_identity is None:
        raise incomplete_bootstrap(config)
    try:
        if type(json.loads(source_identity)) is not dict:
            raise ValueError("malformed registered source identity")
        if bound(source_identity) != bound(configured.binding()):
            raise ValueError("registered source identity differs")
    except (ValueError, TypeError) as exc:
        raise SourceMismatch("registered source identity/config mismatch") from exc
    return IngestLog2(con, configured)


def incomplete_bootstrap(config):
    return SourceMismatch(
        f"incomplete bootstrap: recreate only after inspecting slot {config.slot!r} "
        f"in pg_replication_slots and publication {config.publication!r}; "
        "an unused slot retains WAL until dropped"
    )
