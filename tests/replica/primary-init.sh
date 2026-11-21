#!/bin/bash
# Replica fixture (compose profile `replica`): the primary's one-time setup, run by the
# image's entrypoint from /docker-entrypoint-initdb.d on a fresh data directory.
# Local-only credentials, as compose.yaml's.
set -euo pipefail
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U postgres -d postgres <<'SQL'
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'phase0-local-only';
SELECT pg_create_physical_replication_slot('standby_slot');
SQL
