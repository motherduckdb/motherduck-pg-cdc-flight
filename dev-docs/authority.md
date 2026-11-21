# Authority and budgets

Two data files record what each Flight may do, and `tests/test_authority.py` checks them
against the code. Neither is shipped in a Flight.

- **`flights/authority.json`**, per command:
  - the secrets it reads (as slots: the PostgreSQL DSN, the MotherDuck token, the runtime's
    Flight and run ids, config);
  - the PostgreSQL DDL and writes it can issue;
  - the MotherDuck databases and schemas it can create;
  - which Flights it can start, and where each target id comes from;
  - what its credentials grant and what actually limits it.

  It also lists every **site** in the Flights' code that uses such authority. The test
  scans each command's reachable modules (every module its imports reach, function-level
  imports included) and fails on any site that isn't listed, or listed but gone. Every
  module in `src/cdc_flight` is either reachable by a command or listed, with its reason, as
  one no Flight imports (CLIs, test fixtures, prose). It also checks that a command's
  authority is the union of its sites, that the commands holding the PostgreSQL secret are
  the ones built with `--pg-ca` and the ones `deploy_flights.py` attaches the PostgreSQL
  secret to, and that trigger targets come only from declared
  `*_FLIGHT_ID` config, an owner `*_flight` setting, or the runtime's own id.
- **`flights/budgets.json`**: every runtime, size, wait and retry budget, with its scope,
  caps, overshoot rule, owner, enforcer and exhaustion behaviour. Each entry points at its
  value in the code, and the test fails if they differ. A hard budget names a test (or a doc,
  when PostgreSQL, MotherDuck or CI enforces it) showing what happens at exhaustion.

Credentials aren't narrowed yet: every Flight runs with its owner's MotherDuck token, and
the PostgreSQL role is whatever its secret holds. Adding a secret read, a DDL statement, a
trigger target or a budget means updating these files in the same PR.
