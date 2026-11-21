# Flights: build, create, schedule and chain

This page covers deploying the pipeline Flights (preflight, ingest, apply and
backfill), their schedules, and how they chain. Nothing here is deployed
automatically. Creating, scheduling and running a Flight are actions in your
MotherDuck account, and they cost compute.

## Build

Every Flight is a **package Flight**. Its `requirements_txt` installs the
`motherduck-pg-cdc-flight` package from GitHub at one merged commit, and its source is a
short stub.

**Build** from a clean checkout of a commit that is merged into `main`:

```sh
git fetch origin && git switch --detach <SHA>
for c in preflight ingest apply backfill; do
  uv run python scripts/build_flight.py $c artifacts/${c}_flight.py --package <SHA>
done
```

PostgreSQL Flights also take `--pg-ca` (below). `--package` is required; `--token-secret`
is only for a [private copy](#installing-from-a-private-copy) of the repository. This writes four
files per command:
- `artifacts/<command>_flight.py`, the Flight source (the stub);
- `artifacts/<command>_flight.requirements.txt`, its `requirements_txt`;
- `artifacts/<command>_flight.config.json`, every config key that Flight declares;
- `artifacts/<command>_flight.chart.mmd`, its Flight Explainer chart (below).

`scripts/deploy_flights.py` builds and deploys the same stubs for a whole source.

The build refuses:
- a ref that isn't a full 40-hex commit SHA (no tags, branches or short SHAs);
- a commit that isn't merged into `origin/main`;
- a checkout whose `HEAD` isn't that commit, or with uncommitted changes under `src/`,
  `scripts/` or `flights/`, or in `pyproject.toml` (the stub, its owner settings, config
  template, chart and version all come from the checkout);
- a Flight config that declares the install token's variable (used only for a private
  copy, [below](#installing-from-a-private-copy)).

**Action Flights** (`table-action` and the others) are built per source, with the owner's
settings as flags (`--source`, `--backfill-flight`, `--compact-flight`, ...), which the
stub carries as an `OWNER` literal instead of config. See
[operations.md](operations.md#action-flights). PostgreSQL Flights carry `--pg-ca` as a
`PG_CA` literal. Nothing in the stub is encoded: read it directly.

## Package install

**requirements_txt**, one line, the same for every command:

```
motherduck-pg-cdc-flight @ git+https://github.com/motherduckdb/motherduck-pg-cdc-flight@<SHA>
```

- The install is anonymous: the repository is public, so no token or secret is needed.
  A private copy of the repository needs one ([Installing from a private
  copy](#installing-from-a-private-copy)).
- The package's own exact pins (`pyproject.toml`) bring the dependencies, the same ones
  as `flights/requirements.txt`. The build backend's version is pinned too
  (`hatchling==1.33.0`); its own dependencies are not.

**The stub, at start, before any work:**
1. It checks the install: the package version is the stub's `EXPECTED_VERSION`, and its
   PEP 610 `direct_url.json` says a git install from
   `https://github.com/motherduckdb/motherduck-pg-cdc-flight` (compared without userinfo) at
   `EXPECTED_SHA`. The `cdc_flight` that imports must be that installed package.
2. It logs `pg-cdc-flight-stub version_check ok version=... commit=... vcs=git source=ok`, or
   `failed` with the expected values, then exits 1. It never prints the URL or the
   environment. It's a plain log line, not an `@flight-run` line: it isn't in the charts.
3. It runs the command, with `--owner OWNER` and `pg_ca.bake(PG_CA)` when the stub carries them.

**Version numbers.** `pyproject.toml`'s version is bumped on each release merge. The
stub's `EXPECTED_VERSION` is read from the pinned commit's `pyproject.toml`. The SHA is the
integrity anchor; the version is for people.

**Install time.** Each run builds a fresh environment, downloading the dependencies, before the
stub runs. The package install adds about **1.0–1.3 s per run** at the median, measured on idle
sources.
- **Dependency downloads from PyPI occasionally stall:** about 1 in 55 measured runs took 10–53 s
  to install. On a heavily loaded source, a stall can make an ingest run miss its schedule.
- **The default ingest schedule is `*/3`** for headroom: each run's fixed setup (install, connect,
  attach) is spread over 180 s. See [performance.md](performance.md) for what a schedule sustains.

**Pinning and rollback.** A Flight version maps to one commit: the SHA in its
`requirements_txt` and the stub's `EXPECTED_SHA` must match (check both when reading a
version back). **Rolling back is deploying the previous merged commit:** run
`deploy_flights.py` with the older `package` SHA (or update a Flight to an older Flight
version). It only works while that commit can still be fetched, so `main` must be
protected against force-pushes, deletion and history rewrites.

**When installs fail.** A GitHub outage, a rate limit, a network failure or a commit
GitHub no longer serves all fail the install before any code runs: the run is FAILED with
no `@flight-run` lines. uv's error line names the cause (a timeout, 5xx or DNS error:
GitHub or the network; 429: a rate limit; 404 or "not found": the repository or commit
isn't reachable, for example a private copy without a token). Ingest stops, so the slot's
WAL grows toward `max_slot_wal_keep_size`.
- **In the Dive,** a failed install never reaches the code, so it writes no `cdc_ops` run
  row. The Flight's status turns **stale** once its newest run is older than the stale
  limit ([dive.md](dive.md)), not failed. There's no separate install-failure alert; a
  Flight's run list or logs show the FAILED runs and uv's error line.
- **There is no fallback that avoids the install** until the package is on PyPI. A rollback to an older commit fails
  the same way, since it installs from the same host. Fix the cause (wait out the
  outage; with a token, rotate it) and watch the Dive's time since the last successful
  ingest run.

### Installing from a private copy

A private copy of the repository (a fork, or a mirror) needs a read-only GitHub token at
install time. Set `[secrets] package` in the source file
([deploy_flights.example.toml](deploy_flights.example.toml)), or pass
`--token-secret <name>` to `build_flight.py`. `requirements_txt` then carries the token's
variable:

```
motherduck-pg-cdc-flight @ git+https://x-access-token:${CDC_FLIGHT_GITHUB_TOKEN}@github.com/motherduckdb/motherduck-pg-cdc-flight@<SHA>
```

- The Flight runtime installs requirements with uv. uv expands `${CDC_FLIGHT_GITHUB_TOKEN}`
  from the Flight's environment, where Flight secrets are injected. It expands only
  upper-case names, so the `cdc_flight_pkg_...` alias can't be used.
- **The token is the password, under a fixed user name.** When authentication fails,
  git prints the URL's user name in its error (uv masks its own line, not git's): a
  token in the user-name slot would be in the run's log. A password never is.
- The repository path in the URL and in the stub's install check is this repository's.
  A copy at another path needs both changed in `scripts/build_flight.py` (`REPOSITORY`).

**The secret.** A fine-grained GitHub token with access to that repository only and
permission Contents: read. The owner creates it, and stores it in the MotherDuck UI as a
Flights secret, for example `cdc_flight_pkg`, with one key, `CDC_FLIGHT_GITHUB_TOKEN`.
`deploy_flights.py` adds it to every Flight's `flight_secret_names`, next to its
PostgreSQL secret, if any, and refuses to run while it's missing.
- Attach exactly one secret with that key to a Flight. If two did, the bare variable
  might not be injected: the install would then fail on a literal `${...}`, before any
  code runs.
- Never declare `CDC_FLIGHT_GITHUB_TOKEN` (or its prefixed alias) as config; the build
  refuses it.
- The stub removes both token variables (`CDC_FLIGHT_GITHUB_TOKEN` and
  `<secret>_CDC_FLIGHT_GITHUB_TOKEN`) from its environment before its install check, so
  `cdc_flight` code never sees the token. An anonymous stub has no such step.
- An expired or revoked token fails the install like an outage does; uv's error line
  shows 401 or 403.

**Known risks of the install token.** Two risks are known and haven't been fully checked:
- **Config override.** The token sits in the URL's userinfo, so a value containing `@`
  or `/` could point the install at another server, whose build would run code with the
  Flight owner's MotherDuck token. Whether a run's config can set
  `CDC_FLIGHT_GITHUB_TOKEN` (an undeclared key) before the install is unverified. The
  defences are the distinctive key name, the build refusing a config that declares it,
  and the stub's install check (which catches mistakes, not build-time code).
- **Redaction on MotherDuck.** Locally (uv 0.13.5, git 2.53), the token appeared in no
  log line, no file in the environment and no file in uv's cache, on success or on
  failure, with the password form above. The Flight runtime's uv and git versions
  weren't checked.

**Token rotation, without downtime:**
1. create the new token, and set the dated rotation reminder (below) at the same time;
2. replace the value of the secret in place (same name, same key);
3. check that the next run of each Flight passes `version_check`;
4. revoke the old token.

Secrets are read at each run's start, so no Flight version changes.

**Expiry.** Give the token a long expiry, and keep a dated reminder to rotate it before
then. There's no automatic expiry warning.

**Moving to the anonymous install.** Remove `[secrets]` from the source file and run
`deploy_flights.py`: the plan shows `update: source_code, requirements_txt, secrets` for
every Flight, and the token's secret is detached. A secret named anything other than
`cdc_flight_pkg` makes the plan refuse `table-membership` (it can't tell that secret from a
PostgreSQL one): detach it from that Flight by hand first.

## Flight Explainer

Each Flight prints `@flight-run <node> <status> ...` lines while it runs, so
MotherDuck's Flight Explainer shows a live, step-by-step view of the run: per-step
status, timing, rows, the tables it wrote, branches taken, and the Flights it
triggered.
- The lines join to a Mermaid chart by node id. The charts are
  `flights/charts/<command>.mmd`; the build copies one next to each stub.
- **Pass the chart as the Flight's `metadata.chart`** when you create it, and again on
  every update. The field is a full replacement, so send the whole file each time.
  It is never part of the source.
  - With the MotherDuck MCP flight tools, set it in the create or update call.
  - With the MotherDuck CLI, keep it as `chart.mmd` next to `main.py`.
- **What the lines carry:** node ids, statuses, counts, table names, run numbers, and
  exception *type* names. They never carry error messages, hosts, DSNs or tokens.
  Emitting never fails a run: errors are swallowed, and a broken stdout turns it off.
- **The charts:**
  - **preflight:** each check group in turn, then the objects check, which branches
    `present`, `created` or `missing`.
  - **ingest:** the lease and slot checks (each can branch `busy`), streaming,
    appends (one line per durable batch), then one step that records keys and
    progress and releases the lease (with the run's transaction count), and the apply
    trigger. The trigger carries the triggered
    run's number.
  - **apply:** one fan-out row per table, with its events and mirror table, then
    the table's state as a branch: `current`, `awaiting_generation`, `needs_resync`
    or `needs_action`.
  - **backfill:** a new or resumed generation (resuming at the scan, the marker or
    catch-up), the snapshot as a `done / total` fraction, the marker wait, catch-up,
    publication and the re-trigger. `total` is the planner's row estimate over
    `chunk_rows`, omitted when the table was never analyzed.
- `tests/test_explainer.py` lints the charts and checks the lines of real ingest,
  apply and backfill runs against them.

## Adding and removing tables

From the Dive, the table-membership Flight does this; see
[operations.md](operations.md#the-table-membership-flight).

`cdc-flight-tables` changes which tables a source captures. It runs outside a Flight,
with the Flights' environment (`CDC_SOURCE_ID`, the DSN secret, `MOTHERDUCK_TOKEN`,
and `CDC_TABLES` if your Flights set it):

```sh
uv run --env-file .env cdc-flight-tables add --table public.orders     # then backfill it
uv run --env-file .env cdc-flight-tables remove --table public.legacy  # a held table
uv run --env-file .env cdc-flight-tables remove --table public.x --force
```

- It never alters the publication. `add` refuses a table that isn't in it, and prints
  the `ALTER PUBLICATION … ADD TABLE` for the DBA.
- With `CDC_TABLES` unset, nothing else changes: apply and backfill read the
  registry, so an added table is applied once its backfill publishes, and a removed one
  drops out.
- If your Flights set `CDC_TABLES`, update it on every one of them. Until then a removed
  or former name is ignored and reported, and an added table is captured but neither
  applied nor backfilled. Apply reports it as `not_configured`, and its pin holds the
  log.
- See [Table membership](tables.md#table-membership) for holds, renames and drop/recreate.

## Pausing, retrying and quarantining a table

The same command acts on one mirror table. These actions touch only the mirror database,
so they need no DSN. `CDC_MIRROR_DATABASE` names the mirror if it isn't the default.

```sh
uv run --env-file .env cdc-flight-tables pause  --table public.orders [--expected-state live]
uv run --env-file .env cdc-flight-tables resume --table public.orders [--expected-state paused]
uv run --env-file .env cdc-flight-tables retry  --table public.orders --blocked-lsn 0/5A3B2C10
uv run --env-file .env cdc-flight-tables quarantine --table public.orders \
    --epoch 1 --confirm "QUARANTINE public.orders UNIT 1 0/5A3B2C10..0/5A3B4F88"
```

- **pause and resume:** `pause` moves a `live` table to `paused`, and apply skips it until
  `resume`. Ingest keeps logging, and the table's pin keeps the log.
  - A paused table is subject to retention expiry, like any stalled pin. After it expires,
    the table is `needs_resync`, so `resume` is refused: run a backfill.
- **retry:** returns a `needs_action` table to `live` after you've repaired the mirror or the
  data. Apply re-reads from its checkpoint, and a transaction that still fails is isolated
  again.
  - `--blocked-lsn` is the table's `blocked_commit_lsn`. For a hold without one, such as an
    update to a row missing from the mirror (insert the row, then retry), pass
    `--blocked-lsn none`.
- **quarantine:** a table held on a value the mirror rejected is held before one whole
  unit; quarantine skips that **whole unit** for the table's current generation.
  - The rows that unit touched may diverge until a resync. A backfill rebuilds the table
    from the source and ignores the quarantine.
  - It's refused for a schema block: repair the mirror and retry, or resync.
  - `--confirm` must equal the literal the dry run prints exactly,
    `QUARANTINE <schema.table> UNIT <epoch> <X/Y>..<X/Y>`, with the LSNs in uppercase hex.
    See [operations.md](operations.md#the-quarantine-flight).
  - Retry likewise replays the whole blocked unit.
- **Every action:**
  - It's idempotent: a repeat reports `already`.
  - `--dry-run` validates and reports with no lease and no write.
  - A stale expected value is refused.
  - Each action writes an audit row to the mirror's `_cdc.runs`. Quarantine also writes a
    `_cdc.quarantine` row.
  - Exit codes: 2 when refused, 3 when an apply run holds the table.

## Secret

Preflight, ingest and backfill read PostgreSQL, and so does the table-membership
Flight (its `--dsn-env`, the same secret by default). Apply never does and needs no
secret.

1. Create a Flights secret in the MotherDuck UI, for example `cdc_pg` with one key,
   `DSN`, holding a libpq connection string with `sslmode=require`. Its value is
   injected as `cdc_pg_DSN`. A prefilled link opens the form with the name and key
   filled in, never the value:
   `https://app.motherduck.com/org/<org slug>/settings/secrets?action=create&type=flights&name=cdc_pg&params=DSN`
   ([setup.md](setup.md#4-motherduck-secrets-stop) has the DSN's form).
2. Reference the secret from the Flights that read PostgreSQL (`flight_secret_names`).
3. Set config `CDC_DSN_ENV` to `cdc_pg_DSN`, the variable that holds the DSN.

Never put the DSN in config. Config is visible to everyone who can run the Flight.

## Deploy a source's Flights

`scripts/deploy_flights.py` creates or updates all of a source's Flights from one source
file, through MotherDuck's SQL functions (`MD_CREATE_FLIGHT`, `MD_UPDATE_FLIGHT`, …) with
the owner's token (`motherduck_token`):

```sh
git fetch origin && git switch --detach <SHA>     # the file's `package`, merged to main
uv run --env-file .env python scripts/deploy_flights.py ~/cdc/shop.toml            # the plan and its digest
uv run --env-file .env python scripts/deploy_flights.py ~/cdc/shop.toml --apply --expect <digest>
```

- **The source file** (TOML, [`deploy_flights.example.toml`](deploy_flights.example.toml)
  shows every key) names the organization, the source, the package commit, the CA and
  the Flights secrets. It holds names only; a value that looks like a credential is
  refused. Keep real files outside the repository: they name your organization.
- **The plan** lists each Flight as `create`, `update: <fields>`, `unchanged`, `opted out`
  or `REFUSE: <why>`, a schedule change as `old -> new`, then `plan digest: <digest>` and
  one JSON line. Nothing changes without `--apply --expect <digest>`: the plan is computed
  again and its digest (each Flight's action, fields, id and desired content) must equal
  the approved one, so a plan that changed after approval (a new package, a Flight that
  appeared) changes nothing. The Flights are created in
  dependency order: apply before ingest (whose `CDC_APPLY_FLIGHT_ID` it is), backfill and
  compact before the action Flights (whose `OWNER` bakes their ids). Then every Flight is
  read back: source, requirements, config, secrets, size, runtime, schedule, chart, owner,
  no grantees. A second run plans `unchanged` for all.
- **Refusals:** a token signed in to another organization than the file's `org`
  (checked with `md_user_info()` before anything else); a missing Flights secret (by
  name); a same-named Flight someone else owns; a Flight with grantees; a same-named
  Flight that isn't this source's package stub of that command (`--adopt` takes it over,
  but never another source's Flight or a stub of another command).
- **Never:** a grant, a run, a deleted Flight, or a change to another source's Flights.
  Updates send only the changed fields; the chart goes with every source change.
- **Ingest's schedule** (`*/3 * * * *`) is set only with `[ingest] enabled = true`, after
  the approval in setup.md. A file with `enabled = false` refuses to clear a live
  schedule unless `--disable-ingest` is passed. Clearing sends `schedule_cron := ''`,
  which MotherDuck applies by disabling the schedule (`schedule_status` `DISABLED`; the
  cron stays): the script reads only an `ACTIVE` schedule as one. So a schedule someone
  disabled in the UI is turned on again by the next approved run whose file wants a cron;
  the plan shows it as `schedule none -> <cron>`.
- **The destructive Flights** (epoch, delete-source: the Dive's Danger zone) are created
  by default. `[destructive] enabled = false` leaves them out; existing ones are left
  alone, never deleted.
- **Rollback:** run it again with the previous `package` (it must still install: the commit reachable, and the token valid if there is one).

The defaults (`[flights.<command>]` overrides `name`, `schedule`, `instance`,
`max_runtime_sec`; names default to `cdc-<source>-<command>`):

| Flight | Schedule (UTC) | `max_runtime_sec` | Size |
|---|---|---|---|
| `preflight` | none | 600 | default |
| `ingest` | `[ingest] enabled` | 900 | F16 |
| `apply` | `*/10 * * * *` | 900 | F16 |
| `backfill` | none | 1800 | default |
| `compact` | `0 3 * * *` | 900 | default |
| `table-action` | none | 300 | default |
| `table-membership` | none | 300 | default |
| `quarantine` | none | 300 | default |
| `epoch` | none | 600 | default |
| `delete-source` | none | 600 | default |

The PostgreSQL Flights (preflight, ingest, backfill, table-membership, epoch,
delete-source) get the `[postgres]` secret and the CA; every Flight gets the package
secret. With the optional two-role hardening ([operations.md](operations.md),
table-membership), `[postgres] membership_secret` (and `membership_key`, default `DSN`)
gives table-membership alone the read-only role's secret and bakes its variable into
`OWNER`. A run never re-points table-membership to another secret, with or without
`--adopt`: if its current secret isn't the one the file names, the plan refuses it, so
a hardened Flight is never moved back to the replication secret.

Config is each Flight's declared template with `CDC_SOURCE_ID`, `CDC_DSN_ENV`,
`CDC_PUBLICATION`/`CDC_HEARTBEAT`/`CDC_SLOT` when set, `CDC_HEARTBEAT_WRITER=external` when
`[postgres] heartbeat_writer = "external"` (a read replica (standby) as the source), and for preflight `CDC_TABLES` and
`CDC_CREATE_OBJECTS`; the action Flights take theirs in `OWNER` (`heartbeat_writer` likewise,
only when external).

## PostgreSQL TLS (verify-full)

`sslmode=require` encrypts but doesn't authenticate the server. A Flight built with
`--pg-ca` authenticates it:

```sh
uv run python scripts/build_flight.py ingest artifacts/ingest_flight.py --package <SHA> --pg-ca rds-global
```

Two names: `rds-global` (every RDS region's roots) and `rds-us-east-1` (that region's
roots). Use `rds-global`.

- **What it forces.** Every PostgreSQL connection the Flight opens (preflight, ingest
  including its replication stream, backfill, and the table-membership, epoch and
  delete-source Flights) uses `sslmode=verify-full` against the named CA bundle,
  `sslrootcert` set to that bundle, and `ssl_min_protocol_version=TLSv1.2`. These are
  explicit connection keywords, so the DSN, the secret, `PGSSLMODE`/`PGSSLROOTCERT` and a
  service file can't change or lower them. Apply, compaction, table-action and quarantine
  never connect to PostgreSQL; `--pg-ca` is refused for them.
- **Not config.** The CA is a literal in the stub (`PG_CA = 'rds-global'`), baked
  before the CLI imports. Config is untrusted (anyone who can run a Flight can pass
  overrides), so no config key or environment variable can set or clear it.
- **Fail closed.** An untrusted certificate, a host name that doesn't match it, or a
  server without TLS fails the connection, and the run fails. There's no retry with
  weaker settings and no switch to turn it off. A bad CA name fails the build; a CA whose
  bytes don't match their pinned sha256 fails the run before it connects.
- **No secret change.** A secret's `sslmode=require` is overridden in code. The DSN's host
  must be the name on the server certificate (the RDS endpoint), not an IP address.
- **Before pushing a stub,** check that it's baked: it carries `PG_CA = '<name>'` in plain
  text. Each run then logs
  `postgres tls=verify-full ca=<name>` (never the DSN)
  and records `tls_mode`, `tls_ca` and `tls_in_use` in its `cdc_ops` run row
  ([operations.md](operations.md#tables)).
- **Without `--pg-ca`,** a Flight connects as before: the DSN's `sslmode` (default
  `require`) and its own `sslrootcert`, if any.
- **A live check** of a host's certificate, from a laptop with this checkout:
  `uv run python scripts/check_pg_tls.py <ENV_VAR_NAME> [CA name, default rds-global]` reads
  the DSN from that variable, connects with the baked CA (must pass, printing the TLS version), then with the host's
  IP address in place of its name (must fail the name check). It never prints the DSN.

**The CA bundles** are pinned by sha256 in `src/cdc_flight/pg_ca.py`:
- `rds-global` (`src/cdc_flight/rds_global_ca.py`): Amazon RDS's global bundle, from
  `https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem`. It holds 114
  roots, 38 regions (including AWS's "Beta us-east-1" and "Preview us-east-2" CAs) in
  three families each: RSA2048 G1 (valid to 2061-2066), RSA4096 G1 and ECC384 G1 (to
  2121-2126). At about 175 KB, its module is imported only for that name.
- `rds-us-east-1` (inline in `pg_ca.py`): the us-east-1 bundle, from
  `https://truststore.pki.rds.amazonaws.com/us-east-1/us-east-1-bundle.pem`. Its three
  roots are also in the global bundle.

**Rotation.** A server certificate renewed under the same CA family needs no change. When AWS
adds roots (a new region or CA generation), they ship in a new package release; rebuild and
redeploy **every PostgreSQL Flight** of the source from it (preflight, ingest, backfill,
table-membership, epoch, delete-source), under the usual update and rollback rules
([Updating and rolling back a Flight](#updating-and-rolling-back-a-flight)). A run whose CA
bytes don't match their pin fails before it connects.

- **Not RDS:** only the RDS bundles ship today. Another provider (Aurora uses
  the same RDS CAs; Cloud SQL, Azure, or a self-managed server) needs its own pinned
  `pg_ca.CAS` entry. The alternative, the system trust store (`sslrootcert=system`), would
  trust any public CA for a name.

## Config

Every key is declared on the Flight with a `""` default, which means "use the
built-in default". A run can override only declared keys, so declaring them all
lets an operator override any knob for one run. Fill in at least:

| Key | Value |
| --- | --- |
| `CDC_SOURCE_ID` | the source id, lowercase letters, digits and `_` |
| `CDC_TABLES` | JSON list of `"schema.table"`, for the first preflight. Optional afterwards (see below) |
| `CDC_DSN_ENV` | the secret variable, such as `cdc_pg_DSN` (not on apply) |
| `CDC_APPLY_FLIGHT_ID` | the apply Flight's id (ingest only) |

The other keys are optional:

- Shared: `CDC_DATABASE` (default `cdc_internal`), `CDC_MIRROR_DATABASE` (default
  `<source>_mirror`), `CDC_EPOCH` (unset follows the registered epoch; when set, it must match it), `CDC_RUNTIME_SECONDS` (default 540),
  `CDC_HEARTBEAT`, `CDC_SLOT`, `CDC_PUBLICATION`.
- PostgreSQL commands: `CDC_HEARTBEAT_WRITER` (`self`, the default, writes the heartbeat on
  a primary; `external` captures from a read replica whose primary runs the DBA's
  heartbeat job; see [read-replica.md](read-replica.md). Backfill (keyed and append), start-snapshot,
  resync and epoch run from the read replica: each generation records the source's mode, and
  finality is the first change-log unit past the read replica's replay position. A keyed scan
  retries a recovery conflict up to 3 times per run, halving the chunk; an append snapshot
  fails on one (resync). Epoch's skip
  clamps to the replay position) and `CDC_SLOT_WAIT_SECONDS` (default 90: how long
  preflight (and an append snapshot's or epoch's slot) waits for a read replica slot's creation
  before reporting `waiting_for_primary`
  with exit code 3, so the run shows FAILED until the primary's heartbeat job runs (or the
  DBA runs `pg_log_standby_snapshot()` with no transaction open there); nothing is
  registered and retrying is safe). The mode is recorded at registration; changing it is refused.
  `CDC_HEARTBEAT_INTERVAL_SECONDS` (default 60) declares the DBA job's interval, for the
  diagnostics' freshness display only. See [read-replica.md](read-replica.md).
- Ingest: `CDC_CHAIN_APPLY` (`0` disables chaining).
- Preflight: `CDC_CREATE_OBJECTS` (`1` creates the slot, publication and heartbeat).
- Backfill: covered in the backfill section below.

Every value is validated as untrusted input. A bad value exits with a message naming
the key.

**`CDC_TABLES` is optional once the source is registered.** The registry (see
[Table membership](tables.md#table-membership)) is the table set:
- **Unset (or `""`):** apply covers every registered table, in name order, except
  removed and unmanaged ones. Backfill accepts any registered table. Ingest captures the
  registry, as it always does.
- **Set:** a non-empty list, the same on every Flight. It restricts apply and backfill to
  its tables, and ingest checks it against the registry: an unknown name is refused, and
  a removed or omitted one is reported. Apply reports a registered table the list leaves
  out as `not_configured`.
- **An explicit `[]` is refused**, so a list emptied by mistake never means "every
  table".
- **The first preflight needs `CDC_TABLES`,** because it creates the publication from
  it.

## The Flights

| Flight | Schedule | `max_runtime_sec` | Secret |
| --- | --- | --- | --- |
| preflight | none (on demand, once per source) | 600 | yes |
| ingest | `*/3 * * * *` | 900 | yes |
| apply | `*/10 * * * *` (the safety cron) | 900 | no |
| backfill | none (on demand, then self-chained) | 1800 | yes |
| compact | `0 3 * * *` | 900 | no |

Cron is UTC, five fields. Runs may overlap, because leases make a second run exit
`busy`.

**Size: create ingest and apply as F16.** Every throughput figure in
[performance.md](performance.md) was measured on F16 (2 vCPU by quota), and ingest is CPU-bound
there. Smaller sizes weren't measured. At F4 (0.5 vCPU), ingest is expected to fall well below
the published ceilings. This is a recommendation, not a measured number. The size of
preflight, backfill and compact wasn't studied.

Keep `RUN` on these Flights with their owner, and grant it on none of them. Anyone who
can run a Flight can override its config, including its databases and tables. The
action Flights have no RUN grants either: actions are owner-only, and the Dive is
read-only for everyone else ([operations.md](operations.md#action-flights)). Compact declares
`CDC_SOURCE_ID`, `CDC_DATABASE`, `CDC_RUNTIME_SECONDS`, `CDC_MIRROR_DATABASE` and its
expiry settings (`CDC_COMPACT_EXPIRE`, default `report`; `CDC_MAX_RETAINED_LAG_HOURS`,
default 168; `CDC_LOG_STORAGE_BUDGET_GB`, default off), and reads no PostgreSQL; see
[retention.md](retention.md).

Set `max_runtime_sec` well above `CDC_RUNTIME_SECONDS`. Ingest needs room for its
60-second destination grace. Backfill needs room for a chunk started just before the
deadline (bounded by 600 seconds), the marker wait and the re-trigger delay. A run
killed by the timeout shows as FAILED and leaves its lease to expire (900 seconds).

## Chaining

A Flight starts another with `MD_RUN_FLIGHT`. The call is asynchronous, and the run
uses the target owner's token and secrets. One user should own all four Flights.
If ingest and apply have different owners, the apply owner must grant RUN to the
ingest owner. The Flights' access token must not be a read-scaling token, which
cannot run Flights.

### Ingest to apply

After an ingest run whose status is `drained`, `capped` or `needs_action` and whose
durable checkpoint advanced, ingest starts the apply Flight. It never chains after
`busy` or a failed run. The trigger is best effort: if it fails, ingest logs the
error type and still succeeds, and the apply safety cron catches up.

The heartbeat marker advances the checkpoint on every ingest run, even on an idle
source, so an idle source runs apply after every ingest run (every 3 minutes on the
default schedule). Each apply run on an
idle log does no batch work, but it is a billed run. To trade freshness for cost,
set `CDC_CHAIN_APPLY=0` and rely on the apply cron alone.

**Do not grant RUN on the ingest Flight.** A run override can change
`CDC_APPLY_FLIGHT_ID`, and the trigger runs with the owner's token. Ingest is
meant to run from its cron only.

**RUN on the backfill Flight is full operator authority** over every source the
owner's token can reach. Every declared key is overridable, including
`CDC_SOURCE_ID`, `CDC_DATABASE` and `CDC_MIRROR_DATABASE`, and the run can create a
mirror database. Don't grant it: the owner operates through the action Flights,
which resolve their inputs from registered objects instead. `CDC_DSN_ENV` may only name a
variable ending in `_DSN`, never a `MOTHERDUCK_` one, so an override cannot make a
run echo another variable such as the MotherDuck token.

### Backfill re-triggers itself

Starting a backfill is an operator action: run the backfill Flight with
`CDC_BACKFILL_TABLE=<schema.table>`. Each run advances the generation until its
deadline. Then, while the status is `snapshotting`, `catching_up`,
`waiting_for_marker`, `waiting_for_ingest`, `busy_publish`, or (a read replica source)
`waiting_for_replay`, `waiting_past_replay` or `waiting_for_primary`, the run starts the same Flight again (its
own `MOTHERDUCK_FLIGHT_ID`, never a config value).

- Every re-triggered run passes `CDC_BACKFILL_START=0`. It only resumes the active
  generation, so a retry never starts a second one.
- The chain stops on `published`, `failed` (exit 2), `idle`, or `busy`. `busy`
  means another run or an unexpired lease holds the generation, and that run's
  chain continues.
- The waiting statuses sleep `CDC_BACKFILL_RETRIGGER_DELAY_SECONDS` (default 30)
  before re-triggering, so the chain doesn't outrun the ingest cron.
- The operator's one-off overrides, such as `CDC_BACKFILL_CHUNK_ROWS` or
  `CDC_RUNTIME_SECONDS`, carry into every re-triggered run.
- A stale chain from an abandoned generation can resume a newly started generation
  of the same table. That is harmless: one chain gets `busy` and stops.

**No-progress bounds.** The generation moves when its status, chunks, rows or
marker change. `catching_up` always counts as moving. Two bounds stop the chain:

- `CDC_BACKFILL_MAX_STALLED_RUNS` (default 10): runs in a row where neither the
  generation nor the ingest checkpoint moved. This usually means ingest is down.
- `CDC_BACKFILL_MAX_WAITING_RUNS` (default 60, about 1.5 to 2 hours at the default
  delay): runs in a row where the generation didn't move, even though ingest did.
  This means the generation is stuck on something else, such as a long-held apply
  lease or this table's key check not advancing.

When a bound is hit, the run prints `{"status": "stalled", "bound": ...}` with a
detail line and exits 3, so the run shows FAILED. A failed re-trigger also exits
3, with `"status": "retrigger_failed"`. The generation keeps its state in all
cases. Each re-trigger logs the new run's id and run number. The id joins to
`query_history.user_agent` as `md-flights(<flight_id>,run_id=...)`.

**Recovery.** Fix the cause, usually a stopped ingest. Then rerun the backfill
Flight with `CDC_BACKFILL_TABLE=<table>` and `CDC_BACKFILL_START=0`. If the
previous run was killed, the rerun reports `busy` until its lease expires.

Backfill knobs: `CDC_BACKFILL_RETRIGGER` (`0` disables the chain),
`CDC_BACKFILL_MAX_STALLED_RUNS` (1..100), `CDC_BACKFILL_MAX_WAITING_RUNS`
(1..1000), `CDC_BACKFILL_RETRIGGER_DELAY_SECONDS` (0..600),
`CDC_BACKFILL_CHUNK_ROWS`, and `CDC_BACKFILL_ABANDON=1` to abandon the active
generation. `CDC_BACKFILL_STALLED_RUNS`, `CDC_BACKFILL_WAITING_RUNS` and
`CDC_BACKFILL_LAST_PROGRESS` are chain state, set by the loop.

## Updating and rolling back a Flight

Each run creates any tables or columns its code needs. Columns are only ever added, so a
store a newer Flight has run on gains them for good.
- **A Flight can be rolled back only to code that knows every column the store now has.**
  Below that, recover by rolling forward to a fixed build, not back.
  - Ingest from before the `log_partitions.bytes` column can't append to a store that has
    it.
- **Every `INSERT` names its columns,** and `tests/test_insert_columns.py` enforces it for
  every module a Flight command imports. That keeps an older Flight that's still deployed working when
  a newer one adds a column.
- **Update all of a source's Flights from one commit.** Save each one's current source and
  config first. `MD_GET_FLIGHT_VERSION(flight_id, version_number)` returns a version's
  `source_code`, `config`, `requirements_txt`, `metadata` and `flight_secret_names`.
- **The keyless-capture guard has to be deployed before append mode.** Every keyed path
  refuses a table registered for keyless (append) capture (`tables.capture_mode`), and a
  table meant to be applied that has no key: apply reports it `needs_action` and writes
  nothing, and start-from-log, backfill, the table actions and add-table refuse it.
  Unmanaged keyless publication members behave as before.
  - The apply, backfill, table-action, table-membership and quarantine Flights each
    record, on every run, the guard their code carries (`releases` in the source schema).
    The release that adds append mode refuses it until all five are recorded at or past
    the guard. A Flight that hasn't run since its update hasn't recorded it, so run each
    once after deploying.
  - A rollback to a build from before the guard can't be detected. Don't roll a Flight
    back past the guard once append tables exist.
  - An ingest or preflight from before the guard holds an append table `no_key`. As for
    any validation hold, clearing it forces a resync of that table.

## Limits

- On the Lite plan with limits, only one run executes at a time, and the calling
  run is still running when it triggers. Chaining doesn't work there; the crons
  carry ingest and apply, and backfill must be rerun by hand.
- If MotherDuck rejects a chaining call (`MD_RUN_FLIGHT`), ingest logs `trigger_failed` and
  relies on the apply cron, and backfill exits 3.
- Not yet exercised on deployed Flights: backfill re-triggering itself (backfill
  has run through the local CLI only), scheduled cron runs, and the Lite plan.

Sources: [MD_RUN_FLIGHT](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/md-run-flight/),
[GRANT RUN ON FLIGHT](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/grant-run-on-flight),
[Flights concepts](https://motherduck.com/docs/concepts/flights/).
