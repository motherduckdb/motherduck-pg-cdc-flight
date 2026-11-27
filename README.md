# MotherDuck CDC Flight

Change data capture from PostgreSQL into MotherDuck, run as
[MotherDuck Flights](https://motherduck.com/docs/concepts/flights/). An "ingest" Flight reads the
source's logical replication stream into a durable change log in MotherDuck; and an "apply" Flight
keeps a replica of each table up to date from it. A "backfill" Flight loads or resyncs a table's
replica from a snapshot. An operations Dive shows each source's health and allows a user to perform
maintenance actions, which are also implemented as Flights.
[How it works](docs/how-it-works.md) explains the design and its guarantees.

> **Experimental.** This is an experimental release. MotherDuck doesn't provide official support for it.

## Documentation

- [Setting up a new source](docs/setup.md): a runbook an agent follows, with the steps a human takes
  marked.
- [Flights](docs/flights.md): building, deploying, scheduling and chaining them, and PostgreSQL TLS.
- [Operations](docs/operations.md): the `cdc_ops` status database and the operator-action Flights;
  [the action Flights API](docs/actions-api.md), for scripts and agents;
  [the operations Dive](docs/dive.md); [runbooks](docs/runbooks.md).
- [Tables](docs/tables.md): which tables are captured, held states, and what source DDL does.
- [Retention](docs/retention.md): compaction and consumer expiry; [upgrading](docs/upgrading.md).
- [A read replica (standby) as the source](docs/read-replica.md).
- [Running the commands by hand](docs/running.md).
- [Performance](docs/performance.md) and [limits](docs/limits.md).

To change the code, start with [DEVELOPING.md](DEVELOPING.md).

## Build and deploy

To build the Flights (package stubs pinned to a merged commit) and deploy the preflight,
ingest, apply and backfill Flights (secret, config, schedules, chaining and size: F16 is
recommended for ingest and apply), see [docs/flights.md](docs/flights.md):

```sh
for c in preflight ingest apply backfill; do uv run python scripts/build_flight.py $c artifacts/${c}_flight.py --package <SHA>; done
```

For a remote source, add `--pg-ca rds-global` to the PostgreSQL Flights (preflight, ingest, backfill) to force `verify-full`
TLS; see [PostgreSQL TLS](docs/flights.md#postgresql-tls-verify-full).

## License

Apache License, Version 2.0: see [LICENSE](LICENSE) and [NOTICE](NOTICE). Contributions are
welcome ([CONTRIBUTING.md](CONTRIBUTING.md)); report security problems as
[SECURITY.md](SECURITY.md) describes.
