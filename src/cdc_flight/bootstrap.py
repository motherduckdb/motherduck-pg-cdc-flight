"""Explicit v2 setup; existing control clients use runtime.open_log without PG I/O."""

from cdc_flight.format2 import Binding, UnitConfig
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.runtime import _catalog, _configured, open_log


def source_binding(config):
    """Observe the actual replication session, before streaming or provisioning."""
    from cdc_flight.format2 import OUTPUT_SETTINGS
    from cdc_flight.preflight import connect, connection_options, query
    from cdc_flight.replication import ReplicationStream

    regular = connect(config)
    replication = None
    try:
        replication = ReplicationStream(connection_options(config))
        settings = tuple((name, replication.show(name)) for name in OUTPUT_SETTINGS)
        extensions = tuple(
            query(regular, "SELECT extname,extversion FROM pg_extension ORDER BY extname")
        )
        major = int(replication.show("server_version_num")) // 10000
        return Binding(config.source_id, settings, extensions), major
    finally:
        if replication is not None:
            replication.close()
        regular.close()


# The stages check() reports after its rehearsal's last one (the objects node).
OBJECT_STAGES = frozenset({"objects_missing", "after_objects_created", "after_objects_present"})


def bootstrap_log(con, config, *, create_objects=False, fault=None):
    """Return (log, SourceInfo) using the same factory for CLI and embedding clients.

    Refuse legacy metadata before any PostgreSQL I/O. For a fresh store, observe
    the replication binding first, initialize a durable bootstrap marker before
    provisioning, then register at the slot's actual position. Ownership flags
    commit atomically with registration. An incomplete bootstrap refuses on retry,
    requiring an explicit recreation/object inspection rather than silently
    adopting an object another initializer may have created.

    External mode (a standby) is the exception: its rehearsal creates the slot, under
    the bounded wait, before the marker, so a wait that runs out leaves nothing behind
    and is retryable. A run that stops between that slot and the
    marker leaves a slot the retry reuses without owning it (reported with its drop).
    """
    from cdc_flight.preflight import check
    from cdc_flight.standby import adopted_slot_warning

    fault = fault or (lambda stage: None)
    rehearsed = None
    catalog = _catalog(con, config)
    if catalog and "protocol_v2" not in catalog:
        raise SourceMismatch("format 2 requires a fresh store; recreate and re-backfill")
    if catalog:
        log = open_log(con, config)
        info = check(
            log.config,
            create_objects=create_objects,
            slot=log.registered_slot(),
            fault=fault,
            registry=log.registry(),
        )
    else:
        binding, major = source_binding(config)
        unit_config = UnitConfig(chunk_bytes=config.chunk_bytes)
        configured = _configured(config, binding, unit_config, major)
        # A non-provisioning probe must not poison an empty store when objects
        # are missing. Provisioning still requires the durable marker first, so it
        # rehearses every check (including whether this role can create the objects)
        # before the marker: a refusal leaves no incomplete bootstrap behind.
        if create_objects:
            rehearsed = check(configured, create_objects=True, fault=fault, rehearse=True)
            if rehearsed:
                fault("after_standby_slot")
        else:
            info = check(configured, fault=fault)
        log = open_log(
            con,
            configured,
            create=True,
            binding=binding,
            unit_config=unit_config,
            server_major=major,
        )
        fault("after_initialize")
        if create_objects:
            # The rehearsal reported the stages up to the objects; report only theirs.
            def objects_only(stage):
                if stage in OBJECT_STAGES:
                    fault(stage)

            info = check(configured, create_objects=True, fault=objects_only)
        if rehearsed:
            info.created.update(rehearsed)  # the rehearsal made the standby slot
        elif info.timeline is not None and not info.created.get("slot"):
            info.warnings.append(adopted_slot_warning(configured))
    fault("before_register")
    log.register(
        info.identity, info.initial_lsn, info.tables, created=info.created, timeline=info.timeline
    )
    fault("after_register")
    return log, info
