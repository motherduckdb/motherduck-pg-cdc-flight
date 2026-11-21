# Security

This is an experimental release. MotherDuck doesn't provide official support for it, and
security fixes are made on a best-effort basis, without a response-time commitment.

## Reporting a vulnerability

Please don't open a public issue for a security problem. Email support@motherduck.com
instead, with:

- what you found, and the commit or version it affects;
- steps to reproduce it, or a proof of concept;
- what an attacker could do with it.

Never include real credentials, tokens or connection strings in a report.

## Scope

The code in this repository: the `cdc_flight` package, the Flight builders and deploy
scripts in `scripts/`, and the operations Dive in `dive/`. Problems in MotherDuck itself,
PostgreSQL or a dependency belong with that project, though a report that this repository
uses one unsafely is in scope.
