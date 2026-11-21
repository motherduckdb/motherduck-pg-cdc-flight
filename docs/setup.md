# Setting up a new source

This is a runbook for an **agent** setting up CDC from one PostgreSQL database (the
*source*) into MotherDuck. The agent runs every step it can. At each **STOP**, a human
acts or approves, and the agent waits for their answer before going on. Never skip a STOP,
never guess an answer, and never print a DSN, password or token.

**The agent never uses admin credentials.** Every statement that needs more than the CDC
role's own rights (creating the role, grants, replica identity, the heartbeat table and
the publication) goes into one SQL file, the **DBA script**. The agent writes it; a human
runs it with their own client and their own credentials. The agent never asks for,
fetches or stores a master or superuser password, even if one is within reach.

The details live in the reference docs; this page only sets the order:
[flights.md](flights.md) (building and creating Flights),
[operations.md](operations.md) (running them, the table actions and their limits),
[running.md](running.md) (preflight and the source contract) and [dive.md](dive.md) (the Dive).

Names used below:
- `<source>` is the source id (lowercase letters, digits, `_`).
- `<role>` is the CDC role in PostgreSQL, for example `cdc_repl`.
- The slot is `md_cdc_<source>`, the publication `md_cdc_<source>_pub`, and the heartbeat
  `public.md_cdc_heartbeat`. These are the defaults; keep them unless the source already
  has its own (see "Objects that already exist", step 6).
- The stores are `cdc_internal` (the change log) and `<source>_mirror` (the mirror tables).
  `cdc_ops` is the operations database the Dive reads.

## Tools

- **MotherDuck reads:** prefer the MotherDuck MCP tools, once step 1 has confirmed they
  are signed in to the right org. Otherwise use local DuckDB with the `.env` token:
  `uv run --env-file .env python -c "import duckdb; ..."`. Both paths must point at the
  same org.
- **MotherDuck writes**, never before step 1: the Flights through
  `scripts/deploy_flights.py` (step 7), `cdc_ops` and the share through SQL or the MCP
  tools. The human creates the secrets in the UI (step 4).
- **PostgreSQL:** the agent connects only as `<role>`, with the DSN in its local `.env`
  (git-ignored), read from the variable and never printed.
- **Repo scripts** run in the repo's environment: `uv run python scripts/<script>.py ...`.
  Plain `python3` lacks the package's dependencies, and the action-Flight builds fail with
  `ModuleNotFoundError`.
- **AWS CLI**, if the human wants the agent to read the instance's settings: ask which
  profile to use (don't try each one), and pass it as an environment variable:
  `AWS_PROFILE=<profile> aws rds describe-db-instances --region <region> --db-instance-identifier <id>`.
  Read-only calls only, and never `secretsmanager get-secret-value`.
- **Waiting** for a run: poll its status every minute or so, rather than one long sleep.
  An ingest run can take up to 15 min, a backfill run up to 30.

## 0. Before you start

Ask the human for, and record (never in a tracked file):
- the source id, and the PostgreSQL host name (a **host name**, not an IP: TLS checks it);
- the database name;
- the MotherDuck account that will own every Flight (one owner for all of them), and its
  org;
- which tables to capture, if they already know.

**The primary, or a read replica (standby).** By default CDC reads from the primary server:
the slot, the publication and the heartbeat writes all live there, and preflight refuses a
read replica. To capture from a read replica instead (PostgreSQL 16+), follow
[read-replica.md](read-replica.md): its setup differs at steps 2, 3, 6 and 7. Use a
**direct** endpoint: on RDS the instance endpoint, on Aurora the cluster
(writer) endpoint. RDS Proxy, PgBouncer and other poolers don't carry replication
connections. Preflight's own pooler check only catches a host name containing `pooler`
or ports 6432/6543. A proxy endpoint it doesn't catch fails later, at the replication
capability check.

**On a production database, expect nothing to exist yet:** no publication, no heartbeat
table, the default replica identity on every table, and logical replication off. On RDS,
turning it on is a static parameter: it needs a reboot (step 2).

## 1. Confirm the MotherDuck org

Before any MotherDuck write, check that every path the agent uses is signed in to the
human's org, as the Flights' owner:
- **MCP:** `get_connection_info` names the org and user.
- **The `.env` token:** `FROM md_user_info()` returns `username`, `org_name` and `region`
  ([MotherDuck docs](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/md-user-info/)).

Show the human both, and stop if either names another org or user. A person who belongs
to several orgs can easily have the MCP tools signed in to one and the token for another.
Record the org's `region`: step 2 needs it. Also record the org's URL slug
(`https://app.motherduck.com/org/<slug>/...`, from the human or `get_connection_info`)
for step 4's links.

## 2. The human prepares the PostgreSQL server (STOP)

**STOP.** Send the human this list and wait until they confirm each item:

1. **Logical replication.** On RDS, set `rds.logical_replication = 1` in the instance's
   parameter group (a custom one: the default group can't be changed) and **reboot**. It's
   a static parameter, so plan the reboot for a maintenance window. This sets
   `wal_level = logical`. Elsewhere, set `wal_level = logical` and restart.
2. **WAL retention cap.** Set `max_slot_wal_keep_size` to a size comfortably below the
   instance's allocated storage, so that a stalled slot hits this cap before it fills the
   disk. (Unlimited is the default; preflight warns about it.) Also check that
   `max_replication_slots` leaves room for two more slots (ingest's, plus an append table's
   temporary snapshot slot) and `max_wal_senders` for about three more senders (ingest, the
   snapshot copy, and the brief capability probes).
3. **Network.** The security group (or firewall) admits connections on 5432, to the
   direct endpoint, from MotherDuck's Flight egress addresses for the org's region. These
   are two fixed IPs per region, listed in MotherDuck's docs:
   [How Flights are scoped](https://motherduck.com/docs/concepts/flights/#how-flights-are-scoped).
   Also admit the agent's own machine for step 5's read-only checks, or skip those and
   let preflight be the first network test. Either way, preflight (step 8) is the first
   connection from MotherDuck itself.

## 3. The DBA script, part 1: the role (STOP)

The agent writes `dba_<source>.sql` (outside the repo, or under the git-ignored
`artifacts/`) and gives it to the human. It contains no password: the human sets it
themselves.

```sql
-- Part 1: the CDC role. Run as an admin (on RDS, the master user).
CREATE ROLE <role> LOGIN;
\password <role>                          -- psql; other clients: ALTER ROLE <role> PASSWORD '...'
GRANT rds_replication TO <role>;          -- RDS and Aurora; elsewhere: ALTER ROLE <role> REPLICATION;
GRANT CONNECT ON DATABASE <database> TO <role>;
GRANT USAGE ON SCHEMA <schema> TO <role>;  -- each schema with captured tables
GRANT SELECT ON <schema>.<table>, ... TO <role>;  -- every table that may be captured
```

**One role does everything:** preflight, ingest, backfill and the table-membership Flight
all connect as `<role>`, through one MotherDuck secret.

*Optional hardening:* a second, read-only role (`LOGIN`, `CONNECT`, `USAGE`, `SELECT` on the
tables it may add, never replication) used only by the table-membership Flight, with its
own secret. It narrows what that Flight's credentials can do, at the cost of a second
password and secret, and every table's `SELECT` grant goes to both roles. `deploy_flights.py`
takes it as `membership_secret` (step 7). Steps 4, 7 and 9 note where it changes things.

**STOP.** Wait until the human has run part 1. Then the human puts the role's DSN in the
agent's `.env` as `CDC_PG_DSN` (see step 4 for its form), without pasting it into the
chat.

## 4. MotherDuck secrets (STOP)

The human creates each secret in the MotherDuck UI. The agent gives one prefilled link per
secret, with the org slug from step 1. The link carries only the name and the key names,
never a value (MotherDuck's Flight guide, "Creating Flight secrets in external MCP
clients"):

- the role's DSN:
  `https://app.motherduck.com/org/<slug>/settings/secrets?action=create&type=flights&name=cdc_pg&params=DSN`.
  A Flight reads it as `cdc_pg_DSN`. With the optional read-only role, a second secret
  (for example `cdc_pg_ro`, key `DSN`) holds that role's DSN.

The Flights install the package from GitHub anonymously, so no install token is needed.
Only a private copy of the repository needs one: a fine-grained GitHub token with
Contents: read on that repository only, in the secret from
`https://app.motherduck.com/org/<slug>/settings/secrets?action=create&type=flights&name=cdc_flight_pkg&params=CDC_FLIGHT_GITHUB_TOKEN`
(see [flights.md](flights.md#installing-from-a-private-copy)).

**The DSN value** is a libpq keyword string, one line:

```
host=<endpoint host name> port=5432 dbname=<database> user=<role> password=<password> sslmode=require
```

`host` is the endpoint's **name** (the server certificate's name, never an IP). The
PostgreSQL Flights raise `sslmode` to `verify-full` themselves, against the CA built in
(step 7). Choose a password without spaces, quotes or backslashes.

In `.env`, **single-quote** the value: `CDC_PG_DSN='host=... sslmode=require'`. Unquoted,
`uv run --env-file` fails to parse the line ("Failed to parse environment file") and
drops it and every line after it. Treat that warning as a stop. In the MotherDuck secret,
the value goes in without the quotes. To copy it from `.env` without displaying it:
- macOS: `grep '^CDC_PG_DSN=' .env | cut -d= -f2- | sed "s/^'//; s/'\$//" | tr -d '\n' | pbcopy`
- Linux: the same, ending in `xclip -selection clipboard` (X11) or `wl-copy` (Wayland)
- Windows PowerShell: `(Select-String -Path .env -Pattern '^CDC_PG_DSN=').Line.Substring(11).Trim("'") | Set-Clipboard`

**STOP.** Wait for the human to say they're created. Then check that they exist, by name
only (the values are redacted):

```sql
SELECT name FROM duckdb_secrets() WHERE type = 'flights' AND storage = 'motherduck';
```

## 5. Read-only source checks, then choose the tables (STOP)

The agent runs these as `<role>`, with `CDC_PG_DSN` from its `.env`, never printing it:

1. **TLS:** `uv run --env-file .env python scripts/check_pg_tls.py CDC_PG_DSN rds-global`
   (`rds-global`: any RDS region; the default. For another server, see
   [flights.md](flights.md#postgresql-tls-verify-full)). The
   host name must pass verify-full and the IP address must fail. If it doesn't, stop and
   report.
2. **Server:** PostgreSQL 13 or later, `wal_level = logical`, `server_encoding = UTF8`, not
   a replica (`pg_is_in_recovery()` false), free slots and senders, and
   `max_slot_wal_keep_size` not `-1`. Report each value.
3. **Privileges:** the role can open a replication connection, and has `SELECT` on each
   candidate table.
4. **Candidate tables.** For each table the human named (or every table in the schemas
   they named), report:
   - its key: primary key, replica identity index, or none;
   - its replica identity (`d`, `i`, `f`, `n`) and whether it is partitioned;
   - whether it has TOAST-capable columns (any column whose `attstorage` isn't `p`: text,
     varchar, jsonb, numeric, bytea, arrays and the like);
   - for a keyless table, whether it has a unique index that qualifies as a replica
     identity (unique, valid, not deferrable, not partial, plain `NOT NULL` columns).
5. **Objects that already exist:** any publication, a table named like the heartbeat, and
   any replication slot. On a production database there are usually none.

**STOP.** Show the human the candidate tables, with one recommendation each:
- **Has a key, no TOAST-capable column:** capture it keyed; the default identity is
  enough.
- **Has a key and TOAST-capable columns** (most tables): capture it keyed, after
  `ALTER TABLE <t> REPLICA IDENTITY FULL` (preflight refuses it otherwise:
  `identity_not_full`).
- **Keyless, with a qualifying unique index:** make that index its key:
  `ALTER TABLE <t> ADD PRIMARY KEY USING INDEX <index>`, then `REPLICA IDENTITY FULL` if it
  has TOAST-capable columns. (`REPLICA IDENTITY USING INDEX <index>` works only for a
  table with no TOAST-capable column: with FULL identity the index is no longer its key.
  Preflight's hint names the index statement; prefer the primary key.) `ADD PRIMARY KEY
  USING INDEX` takes an ACCESS EXCLUSIVE lock briefly, and refuses an index with a
  non-default sort order. In that case the DBA first creates a plain unique index
  `CONCURRENTLY` on the same columns.
- **Keyless, no such index:** capture it **insert-only (append mode)**, added after the
  others (step 9):
  - it needs `REPLICA IDENTITY FULL` **before** it joins the publication. Published
    without FULL, its own UPDATE and DELETE fail on the source;
  - an UPDATE holds the table; a DELETE holds it too, unless it's added with
    `ignore_deletes` (then DELETEs are skipped and counted);
  - its first mirror is an exact snapshot when the table is under the snapshot limit
    (1 GiB by default, provisional), otherwise rows from the moment it starts on.
  - A keyless partitioned table can't be captured: it needs a key.

So FULL is needed only for keyless tables and tables with TOAST-capable columns. Tell the
human that FULL logs every column of the old row on UPDATE and DELETE, which adds WAL on
busy tables. The change itself takes a brief exclusive lock on the table and doesn't
rewrite it.

Wait for the human's list: keyed tables, append tables (each with or without
`ignore_deletes`).

## 6. The DBA script, part 2: identity, heartbeat, publication (STOP)

The agent appends part 2 to the same file. It runs as the **owner of the captured
tables** (on RDS the master user owns them only if it created them; otherwise
`SET ROLE <owner>;` first, as a member of that role). Only a table's owner can change its
replica identity or put it in a publication: `<role>` can't, which is why this is the
DBA's step. That role also needs `CREATE` on the database (for the publication) and on
schema `public` (for the heartbeat: PostgreSQL 15 and later no longer grant it to
everyone). If it lacks either, an admin grants it first.

```sql
-- Part 2. Run as the tables' owner.
ALTER TABLE <schema>.<table> REPLICA IDENTITY FULL;   -- each table step 5 marked for FULL
-- (any ADD PRIMARY KEY USING INDEX from step 5)
CREATE TABLE public.md_cdc_heartbeat (id text PRIMARY KEY, marker text NOT NULL);
GRANT SELECT, INSERT, UPDATE ON public.md_cdc_heartbeat TO <role>;
CREATE PUBLICATION md_cdc_<source>_pub
  FOR TABLE <the keyed tables>, public.md_cdc_heartbeat
  WITH (publish_via_partition_root = true);
```

- The publication lists exactly the **keyed** tables plus the heartbeat: no
  `FOR ALL TABLES`, no row filters, no column lists. Append tables join later (step 9),
  and a keyless table in it fails the first registration.
- Part 3 comes later, one statement per append table or table added later:
  `ALTER PUBLICATION md_cdc_<source>_pub ADD TABLE <t>;`, after `REPLICA IDENTITY FULL`
  for an append table or a keyed table with TOAST-capable columns (step 5's rules).
- Preflight creates only the slot (step 8), since the slot should start retaining WAL only
  when ingest is about to run.

**Objects that already exist.** If the source already has a publication or heartbeat
table for CDC, reuse it under its own name only if it fits:
- the publication lists exactly the keyed tables and the heartbeat, and publishes
  insert, update, delete and truncate through partition roots. Otherwise part 2 adjusts it
  (`ALTER PUBLICATION ... ADD/DROP TABLE`, as the owner), or creates a new one;
- the heartbeat is exactly `id text PRIMARY KEY, marker text NOT NULL`, and `<role>` has
  `SELECT, INSERT, UPDATE` on it (part 2 then holds only that `GRANT`);
- step 7 passes the names to every Flight that uses them.

**STOP.** Wait until the human has run part 2. The agent re-checks it read-only: each
table's identity, that the heartbeat exists with `<role>`'s grants, and that the
publication exists with exactly the keyed tables and the heartbeat. **This re-check is the
gate:** go on to step 7 only when it passes. Otherwise preflight (with
`CDC_CREATE_OBJECTS=1`) is refused before creating anything (step 8).

## 7. The Flights

1. **`cdc_ops`.** As the Flights' owner, create `cdc_ops` once:
   `CREATE DATABASE IF NOT EXISTS cdc_ops`. A run never creates it. Its share comes in
   step 10.
2. **Write the source file** outside the repository (it names the org), from
   [deploy_flights.example.toml](deploy_flights.example.toml):
   - `org`: the org name step 1 confirmed; `source`; `package`: the full SHA of a commit
     merged to `main`; `pg_ca = "rds-global"` (any RDS region);
   - `[postgres]`: `secret = "cdc_pg"`, `key = "DSN"` (the role's secret from step 4),
     `membership_secret` only for the optional read-only role (7.5), and
     `publication`, `heartbeat` or `slot` only if step 6 kept other names;
   - leave `[secrets]` commented out (the anonymous install), unless the Flights install
     from a private copy of the repository;
   - `[preflight]`: `tables`, the **keyed** tables only (the same as the publication's),
     and `create_objects = true` (preflight then creates only the slot);
   - `[ingest] enabled = false` (step 8 turns it on);
   - `[destructive] enabled`: `true` creates the epoch and delete-source Flights (the Dive's
     Danger zone, owner-only like the rest); `false` leaves them out.
3. **The plan.** From a clean checkout of that commit (it must include `scripts/deploy_flights.py`; the token comes from `.env`):

   ```sh
   git fetch origin && git switch --detach <package SHA>
   uv run --env-file .env python scripts/deploy_flights.py <source file>
   ```

   It prints each Flight with its action (`create` for all on a new source), then
   `plan digest: <digest>`, and changes nothing. It refuses a token signed in to another org
   than the file's, a missing secret, and a same-named Flight it doesn't own.
4. **STOP: approval to create the Flights.** Show the human the plan: up to 10 Flights
   ([flights.md](flights.md#deploy-a-sources-flights) lists their schedules, runtimes and
   sizes), every one owner-only with no RUN grants, the PostgreSQL ones with the `cdc_pg`
   secret (table-membership: `membership_secret`, if set) and the CA, all with the package
   secret. Ingest has no schedule yet. Wait for
   approval.
5. **Apply exactly the approved plan:**
   `uv run --env-file .env python scripts/deploy_flights.py <source file> --apply --expect <digest>`. If
   anything changed since the plan, its digest differs and nothing is changed: show the new
   plan instead. It creates the Flights in dependency order, then reads every one back
   (source, requirements, config, secrets, size, runtime, schedule, chart, owner, no
   grantees) and ends with `"result": "ok"`. Its `read_back` rows carry each Flight's id
   (step 10 needs the action Flights'). A second plan run must show `unchanged` for all.
   - **Two roles** (optional hardening, step 3): add `[postgres] membership_secret =
     "<the read-only role's secret>"` (`membership_key` defaults to `DSN`). Only
     table-membership reads it; every other PostgreSQL Flight keeps `[postgres] secret`.
     Grant `SELECT` on each new table to both roles. The script never moves
     table-membership to another secret: if its current secret differs from the file's, the
     plan refuses it (even with `--adopt`), so name the secret it already uses. Switching
     an existing source between one and two roles is a hand change to that Flight's
     secrets first, then the matching file.
   - The ids later: an `--apply --expect` of an all-`unchanged` plan changes nothing and prints the
     `read_back` rows again, a safe way to get the ids later.
   - The MCP tools stay useful for reads (`get_flight`, `list_flight_runs`); the script
     makes every change.

## 8. Preflight and ingest (STOP)

**STOP: one approval for both.** Preflight creates the slot, and from then on the slot
retains WAL until ingest reads it, so the two go together. Tell the human: preflight checks
everything, creates the slot, and registers the tables; ingest then reads the slot every
3 minutes and hands each batch to apply. If a tool permission prompt is declined after the
human approved, ask again: don't leave a new slot unread.

1. **Run the preflight Flight.** It checks before it creates anything. Its result is
   `{"status": "ready", ...}` with a list of warnings.
   - "this role can't create the heartbeat table and publication; ask the tables' owner
     (the DBA) to run: …": part 2 of the DBA script didn't run, or used other names. The
     refusal lists the owner's statements; take them to the human, then go back to step 6's
     re-check. Nothing was created, so preflight simply runs again.
   - "grant SELECT,INSERT,UPDATE on the heartbeat table": the heartbeat's grant is
     missing (step 6).
   - "publication membership differs from the configured tables and heartbeat": the
     publication and `CDC_TABLES` don't list the same keyed tables.
   - A `no_key` table fails the first registration. Its message carries the statement to
     run; take it back to the human (step 5), and leave the table out of `CDC_TABLES` and
     the publication until it's fixed, or add it later (step 9).
   - Every check (tables, publication, heartbeat, capacity, permissions) runs before
     anything is written, so a refusal leaves nothing to clean up.
   - **If the first preflight fails during provisioning** (rare: a DDL error from a race,
     a lost connection, a crash before registering), it has already created this source's
     empty store schema, `<CDC_DATABASE>.<source>` (nothing is registered there). Its retry
     refuses with "incomplete bootstrap: recreate only after
     inspecting slot …". **STOP.** Drop that schema only when all four hold:
     1. the refusal is exactly that "incomplete bootstrap" message, for this source;
     2. `SELECT count(*) FROM <CDC_DATABASE>.<source>.source` is 0;
     3. the slot doesn't exist in PostgreSQL. If it does, hand it to the human or the DBA
        (an unused slot retains WAL), and drop nothing;
     4. the human approves the drop.

     Then `DROP SCHEMA <CDC_DATABASE>.<source> CASCADE` (only that schema: the database may
     hold other sources), fix the cause, and run preflight again.
   - Report every warning, `max_slot_wal_keep_size` included.
2. **Turn on ingest's `*/3` schedule** right after preflight is `ready`: set
   `[ingest] enabled = true` in the source file and run the plan again. It shows only
   `schedule none -> */3 * * * *` for ingest; then `--apply --expect <digest>` (this step's
   approval covers it). After two runs, check:
   - each ingest run reports `drained` (or `capped`: more to read, the next run continues),
     and apply runs follow it;
   - `cdc_ops.ingest_status` shows the slot's retained and lag bytes falling or steady.

## 9. Backfill each table (STOP)

**STOP: approval of the backfills** (they read each table in full; schedule large ones
away from busy hours).
- **Keyed tables** that preflight registered: run the backfill Flight once per table with
  `CDC_BACKFILL_TABLE=<schema.table>`. It re-triggers itself until the table is published.
- **Append tables** need every keyed-path Flight to have recorded its release stamp first.
  A Flight records it on any run that gets past input validation:
  - table-action: a dry run, `CDC_ACTION=compact-now`, `CDC_DRY_RUN=1`;
  - quarantine: a dry run with a captured table and the source's epoch
    (`CDC_ACTION=quarantine`, `CDC_TABLE`, `CDC_EXPECTED_EPOCH`, `CDC_CONFIRM=<any short
    text>`, `CDC_DRY_RUN=1`: the input check requires a confirmation even on a dry run, which
    doesn't compare it). It is refused after stamping (nothing is held, or the table has
    no mirror yet), so the run shows as failed with exit 2: expected;
  - backfill: any run. If no keyed table was backfilled, run it once with
    `CDC_BACKFILL_TABLE=<a captured table>` and `CDC_BACKFILL_START=0` (it reports idle).
    Its stamp must be 2 for the add to start an append table's snapshot;
  - apply has run by now; table-membership stamps on the add's dry run below.

  After that dry run, check that `cdc-flight-tables releases` lists all five as stamped. (`--not-deployed` is
  only for a command this source has no Flight for; this runbook creates them all.)
  Then the human runs part 3 of the DBA script for each append table (after `REPLICA
  IDENTITY FULL`), and the next ingest run sees it (unmanaged). Add each with the
  table-membership Flight: `CDC_ACTION=add`, `CDC_MODE=append` (and `CDC_IGNORE_DELETES=1`
  if chosen), a dry run first (it stamps table-membership; check `releases` now), then the
  real add. The add starts its snapshot backfill. A table over the snapshot limit is
  refused: that backfill run ends `refused` (exit 2, expected), and the next step is the
  table action `start-from-log`. The backfill's `CDC_RUNTIME_SECONDS` (default 540) must
  leave room for the slot wait (up to 300 s) and the copy. It can go up to about 840 s (it
  must stay below the 900 s lease); `max_runtime_sec` 1800 is only the kill limit.
- **Tables added later:** the human grants `<role>` `SELECT` on the table (`add` checks it,
  and the backfill needs it; with the optional read-only role, grant it to both roles),
  sets `REPLICA IDENTITY FULL` if step 5's rules call for it, then runs part 3 for it. Then the table-membership Flight's `add` registers it and starts
  its backfill.

A table is applied once its backfill publishes; apply needs no separate step.

## 10. The Dive (STOP)

1. **STOP: approval to share,** and whether the Dive and the share should be visible to
   the org or private to the owner. Ask before creating either. Create the share, named
   after its database: `CREATE SHARE cdc_ops FROM cdc_ops ... UPDATE AUTOMATIC`.
2. Write `flights.json` with the action Flights' ids,
   `{"<source>": {"table_action": "<id>", "table_membership": "<id>", "quarantine": "<id>"}}`,
   then preview:
   `uv run python scripts/deploy_dive.py --share md:_share/cdc_ops/<id> --expect-user <owner> --flights flights.json`
   (a dry run, connecting to nothing). Then the same with `--apply`.
3. Make the Dive and the share viewable by the org, if the human approved sharing.

## 11. Final checks

Report each to the human:
- **Builds (the fingerprints):** a plan run of `deploy_flights.py` shows `unchanged` for
  every Flight: each pins the approved SHA, the PostgreSQL ones carry the CA, the action
  Flights their `OWNER`, and none has a RUN grant. `cdc-flight-tables releases` lists all
  five keyed-path commands as stamped.
- **Slot:** retained and lag bytes are steady and well under `max_slot_wal_keep_size`.
- **Tables:** each captured table is `live` and caught up in the Dive, and its mirror's
  row count matches the source within the lag.
- **The Dive** shows the source, its tables and the action buttons, for the owner.

## What the human should know

- **Caught up means every logged change is applied, not that the mirror equals the source.**
  Some DDL changes rows without writing WAL (for example `ADD COLUMN ... DEFAULT` filling
  existing rows). See [operations.md](operations.md#current-as-of) and
  [how-it-works.md](how-it-works.md#what-the-log-cant-capture). Catalog checks hold the tables they can detect; resync after such DDL.
- **Each table is caught up on its own.** A transaction touching two tables can be visible
  in one mirror before the other: there's no cross-table atomicity.
- An append table's snapshot holds the source's xmin horizon for the whole copy, and a
  TRUNCATE of that table waits behind it ([operations.md](operations.md), "The exact snapshot
  backfill").
